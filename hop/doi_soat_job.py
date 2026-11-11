"""Doi soat Datalake - Lakehouse khong giao dien, duoc goi tu workflow Hop doi_soat.hwf.

Dung chung reconcile.py voi tool nen cach sinh SQL va so sanh giong het giao dien.
Mat khau lay tu bien moi truong DATALAKE_PASSWORD va LAKEHOUSE_PASSWORD
(action Shell cua Hop chuyen bien cua workflow thanh bien moi truong).
Ma thoat: 0 khop het, 1 co bang lech, 2 co bang loi hoac bi dung, 3 cau hinh sai.
"""
import argparse
import csv
import os
import signal
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import reconcile  # noqa: E402

SIDES = {"old": "Datalake", "new": "Lakehouse"}
OPTION_PREFIX = {"old": "datalake", "new": "lakehouse"}
PASSWORD_ENV = {"old": "DATALAKE_PASSWORD", "new": "LAKEHOUSE_PASSWORD"}
POLL_SECONDS = 1.0
CONNECT_TIMEOUT_SECONDS = 5
RETRY_DELAY_SECONDS = 15
PROGRESS_SECONDS = 60
MAX_PARALLEL = 16
ERROR_TEXT_LIMIT = 2000
EXIT_OK, EXIT_MISMATCH, EXIT_ERROR, EXIT_CONFIG = 0, 1, 2, 3
VERDICT_TEXT = {"ok": "Khớp", "lech": "Lệch", "trong": "0 dòng"}
HEADER = (
    "bang_datalake", "bang_lakehouse", "partition_datalake", "partition_lakehouse", "chi_so",
    "gia_tri_datalake", "gia_tri_lakehouse", "chenh_lech", "phan_tram_lech", "dat_nguong", "ket_qua_bang", "loi",
)


class QueryCancelled(Exception):
    pass


class QueryFailed(Exception):
    pass


def parse_args(argv):
    p = argparse.ArgumentParser(description="Đối soát Datalake - Lakehouse qua Spark Thrift.")
    p.add_argument("--date", default="", help="Ngày đối soát YYYY-MM-DD, để trống là hôm qua.")
    p.add_argument("--rows", required=True, help="File danh sách bảng (cùng định dạng với tool).")
    p.add_argument("--out-dir", required=True, help="Thư mục ghi file kết quả.")
    p.add_argument("--threshold", default="0", help="Ngưỡng % chênh lệch cho phép.")
    p.add_argument("--timeout-minutes", type=int, default=30, help="Hủy query chạy quá số phút này.")
    p.add_argument("--retries", type=int, default=1, help="Số lần thử lại khi lỗi kết nối.")
    p.add_argument("--fail-on-mismatch", default="Y", help="Y: có bảng lệch thì thoát mã 1.")
    for side, prefix in OPTION_PREFIX.items():
        p.add_argument(f"--{prefix}-host", required=True)
        p.add_argument(f"--{prefix}-port", type=int, default=10000)
        p.add_argument(f"--{prefix}-user", required=True)
        p.add_argument(f"--{prefix}-parallel", type=int, default=4)
    return p.parse_args(argv)


def build_config(args):
    run_date = args.date.strip() or (date.today() - timedelta(days=1)).isoformat()
    try:
        threshold = Decimal(args.threshold.strip())
    except InvalidOperation:
        raise ValueError(f"Ngưỡng chênh lệch không hợp lệ: {args.threshold}") from None
    if not threshold.is_finite() or threshold < 0:
        raise ValueError("Ngưỡng chênh lệch phải là số không âm.")
    if args.timeout_minutes < 1 or args.retries < 0:
        raise ValueError("--timeout-minutes phải >= 1 và --retries phải >= 0.")

    cfgs, parallel = {}, {}
    for side, prefix in OPTION_PREFIX.items():
        name = SIDES[side]
        password = os.environ.get(PASSWORD_ENV[side], "")
        if not password:
            raise ValueError(f"Chưa đặt biến {PASSWORD_ENV[side]} cho mật khẩu {name}.")
        host = getattr(args, f"{prefix}_host").strip()
        user = getattr(args, f"{prefix}_user").strip()
        if not host or not user:
            raise ValueError(f"Chưa nhập host hoặc user của Thrift {name}.")
        cfgs[side] = {"host": host, "port": getattr(args, f"{prefix}_port"), "username": user, "password": password}
        parallel[side] = getattr(args, f"{prefix}_parallel")
        if not 1 <= parallel[side] <= MAX_PARALLEL:
            raise ValueError(f"Số query song song của {name} phải từ 1 đến {MAX_PARALLEL}.")

    with open(args.rows, "rb") as fh:
        rows = reconcile.parse_csv(fh.read())
    return run_date, threshold, cfgs, parallel, rows


def run_query(cfg, sql, cancel, timeout):
    from pyhive import hive
    from TCLIService.ttypes import TOperationState

    running = (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE, TOperationState.PENDING_STATE)
    # PyHive khong co timeout ket noi, kiem tra truoc de bao loi nhanh khi sai host/port.
    socket.create_connection((cfg["host"], cfg["port"]), timeout=CONNECT_TIMEOUT_SECONDS).close()
    conn = hive.connect(
        host=cfg["host"], port=cfg["port"], username=cfg["username"], password=cfg["password"], auth="LDAP"
    )
    deadline = time.monotonic() + timeout
    try:
        cursor = conn.cursor()
        cursor.execute(sql, async_=True)
        while True:
            status = cursor.poll()
            if status.operationState == TOperationState.FINISHED_STATE:
                return cursor.fetchone()
            if status.operationState not in running:
                raise QueryFailed(status.errorMessage or "Query không chạy thành công.")
            if cancel.wait(POLL_SECONDS):
                cursor.cancel()
                raise QueryCancelled()
            if time.monotonic() > deadline:
                cursor.cancel()
                raise QueryFailed(f"Query chạy quá {timeout // 60} phút nên đã bị hủy.")
    finally:
        conn.close()


def error_text(exc):
    args = exc.args[0] if exc.args else None
    message = getattr(getattr(args, "status", None), "errorMessage", None) or str(exc) or type(exc).__name__
    return message[:ERROR_TEXT_LIMIT]


def run_side(cfg, sql, cancel, timeout, retries):
    """Chi thu lai loi ket noi; loi SQL hay qua thoi gian thi thu lai cung vo ich."""
    for attempt in range(retries + 1):
        if cancel.is_set():
            return ("cancelled", None)
        try:
            return ("ok", run_query(cfg, sql, cancel, timeout))
        except QueryCancelled:
            return ("cancelled", None)
        except QueryFailed as exc:
            return ("error", error_text(exc))
        except Exception as exc:
            if attempt == retries or cancel.wait(RETRY_DELAY_SECONDS):
                return ("error", error_text(exc))
    return ("error", "Không chạy được query.")


def settle(item, sides, threshold):
    if item["error"]:
        return {"state": "error", "error": item["error"], "verdict": "", "metrics": []}
    errors = [f"{SIDES[s]}: {o[1]}" for s, o in sides.items() if o[0] == "error"]
    if errors:
        return {"state": "error", "error": "\n".join(errors), "verdict": "", "metrics": []}
    if any(o[0] == "cancelled" for o in sides.values()):
        return {"state": "cancelled", "error": "", "verdict": "", "metrics": []}
    try:
        metrics = reconcile.compare(item["metrics"], sides["old"][1], sides["new"][1], threshold)
    except ValueError as exc:
        return {"state": "error", "error": str(exc), "verdict": "", "metrics": []}
    return {"state": "done", "error": "", "verdict": reconcile.verdict(metrics), "metrics": metrics}


def write_result(rows, results, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\r\n")
        writer.writerow(HEADER)
        for config, result in zip(rows, results):
            base = [config["old_table"], config["new_table"], config["old_partition"], config["new_partition"]]
            if result["metrics"]:
                for m in result["metrics"]:
                    writer.writerow(base + [
                        m["label"], m["old"], m["new"], m["diff"], m["pct"],
                        "Đạt" if m["ok"] else "Không đạt", VERDICT_TEXT[result["verdict"]], "",
                    ])
            else:
                status = "Đã dừng" if result["state"] == "cancelled" else "Lỗi"
                writer.writerow(base + [""] * 6 + [status, result["error"]])
    os.replace(temp, path)


def main(argv):
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    args = parse_args(argv)
    try:
        run_date, threshold, cfgs, parallel, rows = build_config(args)
    except (OSError, ValueError) as exc:
        print(f"LỖI CẤU HÌNH: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    cancel = threading.Event()

    # Hop/Airflow dung task bang SIGTERM: huy cac query dang chay tren Thrift truoc khi thoat.
    def stop(signum, _frame):
        print(f"Nhận tín hiệu {signum}, đang hủy các query...", flush=True)
        cancel.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    prepared = [reconcile.prepare_row(r, run_date) for r in rows]
    started = time.monotonic()
    print(
        f"Đối soát ngày {run_date}: {len(rows)} bảng, song song Datalake {parallel['old']}"
        f" / Lakehouse {parallel['new']}, ngưỡng {threshold}%, timeout {args.timeout_minutes} phút.",
        flush=True,
    )

    # Moi he thong mot pool rieng de khong ben nao chiem cho cua ben kia.
    pools = {side: ThreadPoolExecutor(max_workers=parallel[side]) for side in SIDES}
    futures = {}
    for index, item in enumerate(prepared):
        if item["error"]:
            continue
        for side in SIDES:
            futures[(index, side)] = pools[side].submit(
                run_side, cfgs[side], item[f"{side}_sql"], cancel, args.timeout_minutes * 60, args.retries
            )
    pending = set(futures.values())
    while pending:
        _, pending = wait(pending, timeout=PROGRESS_SECONDS)
        if pending:
            done = len(futures) - len(pending)
            minutes = int(time.monotonic() - started) // 60
            print(f"  ... {done}/{len(futures)} query xong sau {minutes} phút", flush=True)
    for pool in pools.values():
        pool.shutdown()

    results = [
        settle(item, {s: futures[(i, s)].result() for s in SIDES} if not item["error"] else {}, threshold)
        for i, item in enumerate(prepared)
    ]
    out = os.path.join(args.out_dir, f"ket_qua_doi_soat_{run_date}.csv")
    write_result(rows, results, out)

    groups = {"ok": [], "trong": [], "lech": [], "error": [], "cancelled": []}
    for config, result in zip(rows, results):
        groups[result["verdict"] if result["state"] == "done" else result["state"]].append((config, result))
    print(
        f"Xong sau {(time.monotonic() - started) / 60:.1f} phút. Khớp {len(groups['ok'])},"
        f" 0 dòng {len(groups['trong'])}, lệch {len(groups['lech'])}, lỗi {len(groups['error'])},"
        f" đã dừng {len(groups['cancelled'])}."
    )
    print(f"Kết quả: {out}")
    for config, result in groups["lech"]:
        bad = ", ".join(f"{m['label']} lệch {m['pct']}%" for m in result["metrics"] if not m["ok"])
        print(f"  LỆCH {config['old_table']} -> {config['new_table']}: {bad}")
    for config, result in groups["error"]:
        print(f"  LỖI  {config['old_table']} -> {config['new_table']}: {result['error'].splitlines()[0]}")

    if groups["error"] or groups["cancelled"]:
        return EXIT_ERROR
    if groups["lech"] and args.fail_on_mismatch.strip().upper() == "Y":
        return EXIT_MISMATCH
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
