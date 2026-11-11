"""Web local doi soat du lieu qua Spark Thrift Server."""
import json
import os
import re
import socket
import sys
import threading
import uuid
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation

from flask import Flask, Response, jsonify, request, send_from_directory
from pyhive import hive
from TCLIService.ttypes import TOperationState

import reconcile

BASE_DIR = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")
# Exe giai nen vao thu muc tam nen du lieu phai luu canh file exe.
DATA_DIR = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
ROWS_FILE = "danh_sach_bang.csv"
SETTINGS_FILE = "cau_hinh.json"
SIDES = {"old": "Datalake", "new": "Lakehouse"}
# Mat khau luu thang trong cau_hinh.json: VDI rieng tu, nguoi dung khong muon nhap lai moi lan mo tool.
CONN_KEYS = ("host", "port", "username", "password")
SETTING_KEYS = tuple(f"{side}_{key}" for side in SIDES for key in CONN_KEYS) + ("parallel", "threshold")
FIRST_PORT = 8765
PORT_ATTEMPTS = 20
POLL_SECONDS = 1.0
CONNECT_TIMEOUT_SECONDS = 5
MAX_PARALLEL = 16
ERROR_TEXT_LIMIT = 2000

RUNNING_STATES = (
    TOperationState.INITIALIZED_STATE,
    TOperationState.RUNNING_STATE,
    TOperationState.PENDING_STATE,
)

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="/static")
jobs = {}
jobs_lock = threading.Lock()
state_lock = threading.Lock()


class QueryCancelled(Exception):
    pass


class QueryFailed(Exception):
    pass


def connect(cfg):
    # PyHive khong co timeout ket noi, kiem tra truoc de bao loi nhanh khi sai host/port.
    socket.create_connection((cfg["host"], cfg["port"]), timeout=CONNECT_TIMEOUT_SECONDS).close()
    return hive.connect(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        auth="LDAP",
    )


def run_query(cfg, sql, cancel):
    conn = connect(cfg)
    try:
        cursor = conn.cursor()
        cursor.execute(sql, async_=True)
        while True:
            status = cursor.poll()
            if status.operationState == TOperationState.FINISHED_STATE:
                return cursor.fetchone()
            if status.operationState not in RUNNING_STATES:
                raise QueryFailed(status.errorMessage or "Query không chạy thành công.")
            if cancel.wait(POLL_SECONDS):
                cursor.cancel()
                raise QueryCancelled()
    finally:
        conn.close()


def error_text(exc):
    args = exc.args[0] if exc.args else None
    message = getattr(getattr(args, "status", None), "errorMessage", None) or str(exc) or type(exc).__name__
    return message[:ERROR_TEXT_LIMIT]


class Job:
    def __init__(self, prepared, cfgs, threshold):
        self.id = uuid.uuid4().hex
        self.cfgs = cfgs
        self.threshold = threshold
        self.cancel = threading.Event()
        self.lock = threading.Lock()
        self.state = "running"
        self.remaining = 0
        self.rows = []
        for item in prepared:
            failed = bool(item["error"])
            self.rows.append(
                {
                    "state": "error" if failed else "pending",
                    "error": item["error"],
                    "verdict": "",
                    "metrics": [],
                    "prepared": item,
                    "sides": {},
                }
            )
            if not failed:
                self.remaining += 2
        if not self.remaining:
            self.state = "done"

    def run_side(self, index, side):
        row = self.rows[index]
        if self.cancel.is_set():
            self.finish_side(index, side, ("cancelled", None))
            return
        with self.lock:
            row["state"] = "running"
        try:
            values = run_query(self.cfgs[side], row["prepared"][f"{side}_sql"], self.cancel)
            outcome = ("ok", values)
        except QueryCancelled:
            outcome = ("cancelled", None)
        except Exception as exc:
            outcome = ("error", error_text(exc))
        self.finish_side(index, side, outcome)

    def finish_side(self, index, side, outcome):
        row = self.rows[index]
        with self.lock:
            row["sides"][side] = outcome
            if len(row["sides"]) == 2:
                self.settle(row)
            self.remaining -= 1
            if not self.remaining:
                self.state = "cancelled" if self.cancel.is_set() else "done"

    def settle(self, row):
        errors = [f"{SIDES[s]}: {o[1]}" for s, o in row["sides"].items() if o[0] == "error"]
        if errors:
            row.update(state="error", error="\n".join(errors))
        elif any(o[0] == "cancelled" for o in row["sides"].values()):
            row["state"] = "cancelled"
        else:
            try:
                metrics = reconcile.compare(
                    row["prepared"]["metrics"], row["sides"]["old"][1], row["sides"]["new"][1], self.threshold
                )
                row.update(state="done", metrics=metrics, verdict=reconcile.verdict(metrics))
            except ValueError as exc:
                row.update(state="error", error=str(exc))

    def snapshot(self):
        with self.lock:
            return {
                "id": self.id,
                "state": self.state,
                "rows": [
                    {k: row[k] for k in ("state", "error", "verdict", "metrics")}
                    for row in self.rows
                ],
            }


def read_connection(data, side):
    name = SIDES[side]
    data = data or {}
    host = str(data.get("host") or "").strip()
    username = str(data.get("username") or "").strip()
    if not host or not username:
        raise ValueError(f"Chưa nhập host hoặc user của Thrift {name}.")
    try:
        port = int(data.get("port"))
    except (TypeError, ValueError):
        raise ValueError(f"Port Thrift {name} không hợp lệ.") from None
    return {"host": host, "port": port, "username": username, "password": str(data.get("password") or "")}


def write_if_changed(path, content):
    # Bo qua khi khong doi: file dang mo bang Excel se bi khoa ghi.
    try:
        with open(path, "rb") as fh:
            if fh.read() == content:
                return
    except FileNotFoundError:
        pass
    temp = path + ".tmp"
    with open(temp, "wb") as fh:
        fh.write(content)
    os.replace(temp, path)


def write_sql_file(rows, prepared, date):
    """Ghi bang va cau query cua lan chay canh file exe: phan tren Datalake, phan duoi Lakehouse."""
    stamp = re.sub(r"[^0-9-]", "", date) or "khong_ngay"
    path = os.path.join(DATA_DIR, f"sql_doi_soat_{stamp}.txt")
    lines = [f"-- Câu query đối soát ngày {date or '(chưa chọn ngày)'}", ""]
    for side, name in SIDES.items():
        tables = [str(r.get(f"{side}_table") or "") for r in rows]
        lines += ["=" * 60, f"-- {name.upper()}", "=" * 60, "", f"-- Bảng ({len(tables)}):"]
        lines += [f"--   {i}. {t}" for i, t in enumerate(tables, 1)]
        lines += ["", "-- Query:", ""]
        for i, (table, item) in enumerate(zip(tables, prepared), 1):
            lines.append(f"-- [{i}] {table}")
            if item["error"]:
                lines.append(f"-- LỖI CẤU HÌNH, không chạy: {item['error']}")
            else:
                lines.append(item[f"{side}_sql"] + ";")
            lines.append("")
    content = "\r\n".join(lines).encode("utf-8")
    temp = path + ".tmp"
    with open(temp, "wb") as fh:
        fh.write(content)
    os.replace(temp, path)
    return path


def read_state():
    rows, settings, problems = [], {}, []
    rows_path = os.path.join(DATA_DIR, ROWS_FILE)
    try:
        with open(rows_path, "rb") as fh:
            rows = reconcile.parse_csv(fh.read(), allow_empty=True)
    except FileNotFoundError:
        pass
    except (OSError, ValueError) as exc:
        problems.append(f"Không đọc được {ROWS_FILE}: {exc}")
    try:
        with open(os.path.join(DATA_DIR, SETTINGS_FILE), encoding="utf-8") as fh:
            settings = json.load(fh)
    except FileNotFoundError:
        pass
    except (OSError, ValueError) as exc:
        problems.append(f"Không đọc được {SETTINGS_FILE}: {exc}")
    return {"rows": rows, "settings": settings, "path": rows_path, "error": " ".join(problems)}


@app.errorhandler(ValueError)
def handle_bad_input(exc):
    return jsonify(error=str(exc)), 400


@app.get("/api/state")
def get_state():
    with state_lock:
        return jsonify(read_state())


@app.post("/api/state")
def save_state():
    data = request.get_json()
    rows = [{f: str(r.get(f) or "") for f in reconcile.FIELDS} for r in data.get("rows", [])]
    incoming = data.get("settings") or {}
    settings = {k: str(incoming.get(k) or "") for k in SETTING_KEYS}
    try:
        with state_lock:
            write_if_changed(os.path.join(DATA_DIR, ROWS_FILE), reconcile.to_csv(rows))
            write_if_changed(
                os.path.join(DATA_DIR, SETTINGS_FILE),
                json.dumps(settings, ensure_ascii=False, indent=2).encode("utf-8"),
            )
    except OSError as exc:
        return jsonify(error=f"Chưa lưu được danh sách, hãy đóng file nếu đang mở bằng Excel: {exc}"), 500
    return jsonify(ok=True)


@app.get("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.get("/api/template.csv")
def template():
    return Response(
        "﻿" + reconcile.TEMPLATE_CSV,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=mau_doi_soat.csv"},
    )


@app.post("/api/parse-csv")
def parse_csv():
    upload = request.files.get("file")
    if upload is None:
        raise ValueError("Chưa chọn file CSV.")
    return jsonify(rows=reconcile.parse_csv(upload.read()))


@app.post("/api/preview")
def preview():
    data = request.get_json()
    date = str(data.get("date") or "")
    return jsonify(rows=[reconcile.prepare_row(r, date) for r in data.get("rows", [])])


@app.post("/api/test-connection")
def test_connection():
    data = request.get_json()
    side = data.get("side")
    if side not in SIDES:
        raise ValueError("Chưa chọn kết nối cần kiểm tra.")
    cfg = read_connection(data, side)
    try:
        row = run_query(cfg, "SELECT 1", threading.Event())
    except Exception as exc:
        return jsonify(ok=False, error=error_text(exc))
    return jsonify(ok=row is not None)


@app.post("/api/run")
def run():
    data = request.get_json()
    connections = data.get("connections") or {}
    cfgs = {side: read_connection(connections.get(side), side) for side in SIDES}
    try:
        threshold = Decimal(str(data.get("threshold")))
    except InvalidOperation:
        raise ValueError("Ngưỡng chênh lệch không hợp lệ.") from None
    if not threshold.is_finite() or threshold < 0:
        raise ValueError("Ngưỡng chênh lệch phải là số không âm.")
    try:
        parallel = int(data.get("parallel"))
    except (TypeError, ValueError):
        raise ValueError("Số query song song không hợp lệ.") from None
    if not 1 <= parallel <= MAX_PARALLEL:
        raise ValueError(f"Số query song song phải từ 1 đến {MAX_PARALLEL}.")
    if not data.get("rows"):
        raise ValueError("Danh sách bảng đang trống.")

    date = str(data.get("date") or "")
    prepared = [reconcile.prepare_row(r, date) for r in data["rows"]]
    job = Job(prepared, cfgs, threshold)

    with jobs_lock:
        if any(j.state == "running" for j in jobs.values()):
            return jsonify(error="Đang có một lượt đối soát chạy."), 409
        try:
            sql_path = write_sql_file(data["rows"], prepared, date)
        except OSError as exc:
            return jsonify(error=f"Chưa ghi được file câu query: {exc}"), 500
        jobs.clear()
        jobs[job.id] = job

    pool = ThreadPoolExecutor(max_workers=parallel)
    for index, row in enumerate(job.rows):
        if row["state"] == "pending":
            pool.submit(job.run_side, index, "old")
            pool.submit(job.run_side, index, "new")
    pool.shutdown(wait=False)
    return jsonify(id=job.id, sql_file=sql_path)


def find_job(job_id):
    with jobs_lock:
        return jobs.get(job_id)


@app.get("/api/jobs/<job_id>")
def job_status(job_id):
    job = find_job(job_id)
    if job is None:
        return jsonify(error="Không tìm thấy lượt đối soát."), 404
    return jsonify(job.snapshot())


@app.post("/api/jobs/<job_id>/cancel")
def job_cancel(job_id):
    job = find_job(job_id)
    if job is None:
        return jsonify(error="Không tìm thấy lượt đối soát."), 404
    job.cancel.set()
    return jsonify(ok=True)


def find_free_port():
    # VDI co the dung chung may, nen khong gia dinh cong dau tien con trong.
    for port in range(FIRST_PORT, FIRST_PORT + PORT_ATTEMPTS):
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise SystemExit("Khong tim duoc cong trong de chay giao dien.")


def main():
    port = find_free_port()
    url = f"http://127.0.0.1:{port}"
    # Khong dau: console Windows cu co the khong in duoc tieng Viet va lam exe dung ngay.
    print(f"Cong cu doi soat dang chay tai {url}")
    print("Dong cua so nay de tat cong cu.")
    threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    app.run(host="127.0.0.1", port=port, threaded=True)


if __name__ == "__main__":
    main()
