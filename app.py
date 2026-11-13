"""Web local doi soat du lieu qua Spark Thrift Server."""
import json
import os
import re
import shutil
import socket
import sqlite3
import sys
import tempfile
import threading
import uuid
import webbrowser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal, InvalidOperation

from flask import Flask, Response, jsonify, request, send_from_directory
import thrift_sasl
from pyhive import hive
from TCLIService.ttypes import TOperationState
from thrift.transport import TSocket
from thrift.transport.TTransport import TTransportException

import db
import lineage
import reconcile

BASE_DIR = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
STATIC_DIR = os.path.join(BASE_DIR, "static")
# Exe giai nen vao thu muc tam nen du lieu phai luu canh file exe.
DATA_DIR = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
# Bien.txt canh exe (nguoi dung tu them bien) uu tien hon ban dong goi trong exe.
reconcile.VARIABLE_FILES.insert(0, os.path.join(DATA_DIR, "Bien.txt"))
# Ban cu luu danh sach o CSV: lan dau mo ban moi se tu chuyen vao DB, file CSV giu nguyen khong dung nua.
ROWS_FILE = "danh_sach_bang.csv"
DB_FILE = "doi_soat.db"
SETTINGS_FILE = "cau_hinh.json"
SIDES = {"old": "Datalake", "new": "Lakehouse"}
# Mat khau luu thang trong cau_hinh.json: VDI rieng tu, nguoi dung khong muon nhap lai moi lan mo tool.
CONN_KEYS = ("host", "port", "username", "password")
SETTING_KEYS = tuple(f"{side}_{key}" for side in SIDES for key in CONN_KEYS) + ("parallel", "threshold", "dependency_gate")
FIRST_PORT = 8765
PORT_ATTEMPTS = 20
POLL_SECONDS = 1.0
CONNECT_TIMEOUT_SECONDS = 5
# Bat tay SASL + mo session binh thuong xong trong vai giay: timeout ngan roi thu lai nhieu lan
# thi bao loi nhanh va vuot qua duoc mang chap chon, thay vi treo mot lan rat lau.
HANDSHAKE_TIMEOUT_SECONDS = 15
CONNECT_RETRIES = 3
RETRY_DELAYS_SECONDS = (2, 4, 8)
# Sau khi ket noi, moi lenh Thrift (submit, poll trang thai, lay ket qua) deu tra ve nhanh vi query chay async.
RPC_TIMEOUT_SECONDS = 60
MAX_PARALLEL = 16
ERROR_TEXT_LIMIT = 2000
# Cau lenh nap bang dinh kem trong ket qua "nghi do" (luu ca vao lich su) chi giu toi da chung nay ky tu.
SQL_SNIPPET = 4000

RUNNING_STATES = (
    TOperationState.INITIALIZED_STATE,
    TOperationState.RUNNING_STATE,
    TOperationState.PENDING_STATE,
)

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="/static")
# Moi lan dong goi lai, giao dien phai doi ngay: bat trinh duyet hoi lai file tinh thay vi dung ban luu dem.
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


@app.after_request
def no_stale_ui(response):
    if request.path == "/" or request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response

jobs = {}
jobs_lock = threading.Lock()
state_lock = threading.Lock()
store_lock = threading.Lock()
store = None
# Loi chuyen CSV cu sang DB: bao len giao dien khi mo tool.
migrate_problem = ""
# Graph phu thuoc doc tu DB mot lan, quet lai thi thay moi.
lineage_lock = threading.Lock()
lineage_cache = {"loaded": False, "data": None, "graph": None}


def get_store():
    global store, migrate_problem
    with store_lock:
        if store is None:
            opened = db.Store(os.path.join(DATA_DIR, DB_FILE))
            try:
                with open(os.path.join(DATA_DIR, ROWS_FILE), "rb") as fh:
                    opened.migrate_csv(reconcile.parse_csv(fh.read(), allow_empty=True))
            except FileNotFoundError:
                pass
            except (OSError, ValueError) as exc:
                migrate_problem = f"Không chuyển được {ROWS_FILE} cũ vào DB: {exc}"
            store = opened
        return store


class QueryCancelled(Exception):
    pass


class QueryFailed(Exception):
    pass


class ConnectRetryable(Exception):
    """Loi ket noi tam thoi (mang, timeout, server dong ket noi): dang thu lai."""


def connect_once(cfg):
    # PyHive khong co timeout ket noi, kiem tra truoc de bao loi nhanh khi sai host/port.
    try:
        socket.create_connection((cfg["host"], cfg["port"]), timeout=CONNECT_TIMEOUT_SECONDS).close()
    except OSError as exc:
        raise ConnectRetryable(f"Không mở được cổng {cfg['host']}:{cfg['port']}: {exc}") from None
    # Tu dung transport de dat timeout: server chay HTTP/SSL hoac khac kieu xac thuc se khong tra loi
    # buoc bat tay SASL, khong co timeout thi PyHive treo vinh vien.
    sock = TSocket.TSocket(cfg["host"], cfg["port"])
    sock.setTimeout(HANDSHAKE_TIMEOUT_SECONDS * 1000)
    transport = thrift_sasl.TSaslClientTransport(
        lambda: hive.get_installed_sasl(
            host=cfg["host"], sasl_auth="PLAIN", username=cfg["username"], password=cfg["password"]
        ),
        "PLAIN",
        sock,
    )
    try:
        conn = hive.connect(username=cfg["username"], thrift_transport=transport)
    except TTransportException as exc:
        transport.close()
        if exc.type == TTransportException.TIMED_OUT:
            raise ConnectRetryable(
                f"Đã mở được cổng nhưng server không phản hồi bước xác thực sau {HANDSHAKE_TIMEOUT_SECONDS} giây. "
                "Nếu lần nào cũng vậy, kiểm tra trong DBeaver xem URL JDBC có transportMode=http, ssl=true "
                "hay xác thực khác LDAP (Kerberos/NOSASL) không."
            ) from None
        if exc.type == TTransportException.END_OF_FILE:
            raise ConnectRetryable(f"Server đóng kết nối giữa chừng: {exc}") from None
        # Con lai (thuong la sai user/mat khau): khong thu lai de tranh khoa tai khoan LDAP.
        raise
    except OSError as exc:
        transport.close()
        raise ConnectRetryable(f"Mất kết nối khi xác thực: {exc}") from None
    # Da ket noi: poll/lay ket qua duoc cho lau hon.
    sock.setTimeout(RPC_TIMEOUT_SECONDS * 1000)
    return conn


def connect(cfg, cancel):
    """Thu ket noi nhieu lan voi timeout ngan; chi thu lai loi tam thoi, khong thu lai sai mat khau."""
    for attempt in range(CONNECT_RETRIES + 1):
        try:
            return connect_once(cfg)
        except ConnectRetryable as exc:
            if attempt == CONNECT_RETRIES:
                raise QueryFailed(f"{exc} (đã thử {CONNECT_RETRIES + 1} lần)") from None
            if cancel.wait(RETRY_DELAYS_SECONDS[min(attempt, len(RETRY_DELAYS_SECONDS) - 1)]):
                raise QueryCancelled() from None


def run_query(cfg, sql, cancel):
    # Chi thu lai buoc ket noi; query da gui len server thi khong chay lai de tranh tai gap doi.
    conn = connect(cfg, cancel)
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


def row_nodes(graph, raw_rows):
    """Bang Lakehouse cua tung dong -> nut trong graph phu thuoc (None neu chua quet/khong thay)."""
    if graph is None:
        return [None] * len(raw_rows)
    return [graph.match(r.get("new_table")) for r in raw_rows]


def listed_nodes(nodes):
    listed = {}
    for i, node in enumerate(nodes):
        if node:
            listed.setdefault(node, []).append(i)
    return listed


def row_dependencies(graph, nodes, direction="up"):
    """Moi dong -> cac dong trong danh sach la nguon (hoac dich) gan nhat cua no."""
    listed = listed_nodes(nodes)
    result = []
    for i, node in enumerate(nodes):
        found = graph.nearest_listed(node, listed, direction) if graph is not None and node else {}
        result.append(sorted({j for n in found for j in listed[n] if j != i}))
    return result


def metric_column(label):
    """Nhan chi so 'a → b' hoac 'b' -> bieu thuc cot Lakehouse."""
    return label.split(" → ")[-1]


def decimal_or_none(text):
    try:
        return Decimal(str(text)) if text not in (None, "") else None
    except InvalidOperation:
        return None


class Job:
    def __init__(self, prepared, cfgs, threshold, selected=None, raw_rows=None, date="", gate=False, graph=None):
        self.id = uuid.uuid4().hex
        # Bien thoi gian tinh lai ngay truoc khi chay tung bang (N theo gio thuc te luc chay).
        self.raw_rows = raw_rows
        self.date = date
        self.sql_path = None
        self.cfgs = cfgs
        self.threshold = threshold
        self.cancel = threading.Event()
        self.lock = threading.Lock()
        self.state = "running"
        # Gan sau khi tao luot chay trong DB; ghi log loi thi bao len giao dien, khong lam hong luot chay.
        self.store = None
        self.run_id = None
        self.log_error = ""
        self.remaining = 0
        self.pool = None
        self.gate = gate
        self.graph = graph
        self.rows = []
        for index, item in enumerate(prepared):
            base = {"error": "", "verdict": "", "metrics": [], "prepared": item, "sides": {},
                    "warnings": [], "cause": None, "upstream": []}
            if selected is not None and index not in selected:
                # Dong khong chon chay lai: giao dien giu ket qua cu cua dong nay.
                self.rows.append({**base, "state": "skipped"})
                continue
            failed = bool(item["error"])
            self.rows.append({**base, "state": "error" if failed else "pending", "error": item["error"]})
            if not failed:
                self.remaining += 2
        if not self.remaining:
            self.state = "done"
        self.nodes = row_nodes(graph, raw_rows or [{} for _ in prepared])
        self.listed = listed_nodes(self.nodes)
        self.deps = {i: set() for i in range(len(self.rows))}
        self.dependents = {i: set() for i in range(len(self.rows))}
        self.waiting = {}
        self.plan()

    def name(self, index):
        raw = self.raw_rows[index] if self.raw_rows is not None else {}
        return raw.get("new_table") or raw.get("old_table") or f"dòng {index + 1}"

    def plan(self):
        """Tim bang nguon trong luot nay cua tung bang; bat chan thi bang chi chay khi nguon da xong."""
        if self.graph is None:
            return
        picked = {i for i, r in enumerate(self.rows) if r["state"] != "skipped"}
        runnable = {i for i in picked if self.rows[i]["state"] == "pending"}
        for i in sorted(picked):
            node = self.nodes[i]
            if not node:
                continue
            for up_node in self.graph.nearest_listed(node, self.listed):
                for j in self.listed[up_node]:
                    if j == i:
                        continue
                    self.rows[i]["upstream"].append(self.name(j))
                    if j not in picked:
                        continue
                    if self.rows[j]["state"] == "error":
                        self.rows[i]["warnings"].append(
                            f"Bảng nguồn {self.name(j)} sai cấu hình nên không đối soát được, "
                            "kết quả bảng này chưa chắc do nguồn hay do chính nó."
                        )
                    elif self.gate and i in runnable and j in runnable:
                        self.deps[i].add(j)
        # Vong phu thuoc (a <- b <- a): bo chan giua cac bang trong vong, van chay binh thuong.
        left = {i: set(d) for i, d in self.deps.items()}
        changed = True
        while changed:
            changed = False
            for i in [i for i, d in left.items() if not d]:
                del left[i]
                for d in left.values():
                    d.discard(i)
                changed = True
        for i in left:
            self.deps[i] -= set(left)
            self.rows[i]["warnings"].append("Nằm trong vòng phụ thuộc nên không chờ các bảng cùng vòng.")
        for i, ups in self.deps.items():
            for j in ups:
                self.dependents[j].add(i)
            self.waiting[i] = len(ups)

    def start(self, parallel):
        self.pool = ThreadPoolExecutor(max_workers=parallel)
        with self.lock:
            ready = [i for i, r in enumerate(self.rows) if r["state"] == "pending" and not self.waiting.get(i)]
            for index in ready:
                self.submit(index)
            if not self.remaining:
                self.pool.shutdown(wait=False)

    def submit(self, index):
        self.pool.submit(self.run_side, index, "old")
        self.pool.submit(self.run_side, index, "new")

    def run_side(self, index, side):
        row = self.rows[index]
        if self.cancel.is_set():
            self.finish_side(index, side, ("cancelled", None))
            return
        with self.lock:
            row["state"] = "running"
            # Hai phia cua mot bang dung chung mot thoi diem de bien gio/phut giong nhau.
            now = row.setdefault("now", datetime.now())
        started = datetime.now()
        try:
            sql = row["prepared"][f"{side}_sql"]
            if self.raw_rows is not None:
                fresh = reconcile.prepare_row(self.raw_rows[index], self.date, now)
                if fresh["error"]:
                    raise ValueError(fresh["error"])
                sql = fresh[f"{side}_sql"]
                row["prepared"][f"{side}_sql"] = sql
            values = run_query(self.cfgs[side], sql, self.cancel)
            outcome = ("ok", values)
        except QueryCancelled:
            outcome = ("cancelled", None)
        except Exception as exc:
            outcome = ("error", error_text(exc))
        with self.lock:
            row.setdefault("times", {})[side] = (started, datetime.now())
        self.finish_side(index, side, outcome)

    def finish_side(self, index, side, outcome):
        row = self.rows[index]
        with self.lock:
            row["sides"][side] = outcome
            if len(row["sides"]) == 2:
                self.settle(index)
                self.log_row(index)
                self.release(index)
            self.remaining -= 1
            finished = not self.remaining
            if finished:
                self.state = "cancelled" if self.cancel.is_set() else "done"
                self.refresh_causes()
                self.log_finish()
        if finished:
            if self.pool is not None:
                self.pool.shutdown(wait=False)
            if self.sql_path:
                # Ghi lai file cau query theo dung gia tri bien da chay.
                try:
                    write_sql_file([r["prepared"] for r in self.rows if r["state"] != "skipped"], self.date)
                except OSError:
                    pass

    def release(self, index):
        """Bang index da xong (hoac bi bo qua): mo cho cac bang dang cho no. Goi khi dang giu self.lock."""
        for d in sorted(self.dependents.get(index, ())):
            self.waiting[d] -= 1
            if self.waiting[d] == 0:
                self.unlock(d)

    def unlock(self, index):
        row = self.rows[index]
        bad = []
        for j in sorted(self.deps[index]):
            up = self.rows[j]
            if up["state"] == "blocked" or (up["state"] == "done" and up["verdict"] == "lech"):
                bad.append(j)
            elif up["state"] == "error":
                row["warnings"].append(
                    f"Bảng nguồn {self.name(j)} lỗi query, kết quả bảng này chưa chắc do nguồn hay do chính nó."
                )
        if bad and not self.cancel.is_set():
            reasons = [
                f"{self.name(j)} {'bị bỏ qua' if self.rows[j]['state'] == 'blocked' else 'lệch quá ngưỡng'}"
                for j in bad
            ]
            row.update(state="blocked", error="Không đối soát vì bảng nguồn " + ", ".join(reasons) + ".")
            self.log_row(index)
            self.remaining -= 2
            self.release(index)
        else:
            self.submit(index)

    def settle(self, index):
        row = self.rows[index]
        errors = [f"{SIDES[s]}: {o[1]}" for s, o in row["sides"].items() if o[0] == "error"]
        if errors:
            row.update(state="error", error="\n".join(errors))
        elif any(o[0] == "cancelled" for o in row["sides"].values()):
            row["state"] = "cancelled"
        else:
            try:
                metrics = reconcile.compare(
                    row["prepared"]["metrics"],
                    row["sides"]["old"][1],
                    row["sides"]["new"][1],
                    reconcile.row_threshold(row["prepared"], self.threshold),
                )
                row.update(state="done", metrics=metrics, verdict=reconcile.verdict(metrics))
            except ValueError as exc:
                row.update(state="error", error=str(exc))
                return
            if not self.gate and self.graph is not None:
                for name in self.upstream_lech(index):
                    row["warnings"].append(f"Bảng nguồn {name} lệch quá ngưỡng trong lượt này.")
            if any(not m["ok"] for m in metrics):
                try:
                    row["cause"] = self.explain(index)
                except (sqlite3.Error, ValueError, KeyError, IndexError) as exc:
                    row["warnings"].append(f"Không tìm được nguyên nhân lệch: {exc}")

    def refresh_causes(self):
        """Cuoi luot: tinh lai nghi do khi luc bang xong con bang nguon chua chay xong (chay song song)."""
        for index, row in enumerate(self.rows):
            if row["state"] != "done" or not row["cause"]:
                continue
            try:
                fresh = self.explain(index)
            except (sqlite3.Error, ValueError, KeyError, IndexError):
                continue
            if fresh == row["cause"]:
                continue
            row["cause"] = fresh
            if self.store is not None and row.get("result_id"):
                try:
                    self.store.update_cause(row["result_id"], json.dumps(fresh, ensure_ascii=False))
                except sqlite3.Error as exc:
                    self.log_error = f"Chưa ghi được lịch sử vào {DB_FILE}: {exc}"

    def upstream_lech(self, index):
        node = self.nodes[index]
        if not node:
            return []
        names = []
        for up_node in self.graph.nearest_listed(node, self.listed):
            for j in self.listed[up_node]:
                up = self.rows[j]
                if j != index and up["state"] == "done" and up["verdict"] == "lech":
                    names.append(self.name(j))
        return names

    # ---------- truy nguyen nhan lech ----------

    def source_result(self, j, cache):
        """Ket qua cua bang trong danh sach (dong j): uu tien luot nay, khong co thi lich su cung ngay."""
        up = self.rows[j]
        if up["state"] == "done":
            return {"metrics": up["metrics"], "from": "Lượt này"}
        key = (self.raw_rows[j].get("new_table") or "").strip().lower() if self.raw_rows else ""
        if not key:
            return None
        if key not in cache:
            found = self.store.latest_results(self.date, [key]) if self.store is not None else {}
            cache[key] = found.get(key)
        hit = cache[key]
        if not hit:
            return None
        return {"metrics": hit["metrics"], "from": f"Lịch sử lượt #{hit['luot_chay_id']} ({hit['ket_thuc_luc']})"}

    def explain(self, index):
        """Voi tung chi so lech cua bang: bang nguon nao (va cot nao) co the gay ra."""
        node = self.nodes[index]
        if self.graph is None or not node:
            return None
        row = self.rows[index]
        cache = {}
        writers = [
            {"id": s["id"], "kind": s["kind"], "file": s["file"], "step": s["step"], "sql": s["sql"][:SQL_SNIPPET]}
            for s in self.graph.writers_of(node)
        ]
        items = []
        for metric in row["metrics"]:
            if metric["ok"]:
                continue
            entries, mode = [], "bang"
            if metric["kind"] == "sum":
                for col in lineage.expression_columns(metric_column(metric["label"])):
                    hits, has_lineage = self.graph.trace_column(node, col, self.listed)
                    if has_lineage:
                        mode = "cot"
                    for hit in hits:
                        for j in self.listed[hit["table"]]:
                            if j != index:
                                entries.append(self.column_entry(j, hit, cache))
            if mode == "bang":
                for up_node, path in self.graph.nearest_listed(node, self.listed).items():
                    for j in self.listed[up_node]:
                        if j != index:
                            entries.append(self.table_entry(j, path, metric["kind"], cache))
            entries.sort(key=lambda e: (e["pct"] is None, -(decimal_or_none(e["pct"]) or 0)))
            trees = []
            if metric["kind"] == "sum":
                # Cay truy nguon cot: cot nay duoc tinh tu cot nao, o job nao (ca bang ngoai danh sach).
                trees = [self.graph.column_tree(node, col, self.listed)
                         for col in lineage.expression_columns(metric_column(metric["label"]))]
            items.append({"label": metric["label"], "mode": mode, "entries": entries, "trees": trees,
                          "summary": self.summary(metric, mode, entries, node)})
        return {"table": node, "writers": writers, "metrics": items}

    def column_entry(self, j, hit, cache):
        entry = {"table": self.name(j), "column": hit["column"], "path": hit["path"],
                 "label": "", "pct": None, "diff": None, "ok": None, "from": "", "note": ""}
        result = self.source_result(j, cache)
        if result is None:
            entry["note"] = "Chưa có kết quả đối soát cùng ngày."
            return entry
        entry["from"] = result["from"]
        for m in result["metrics"]:
            if m["kind"] == "sum" and hit["column"] in lineage.expression_columns(metric_column(m["label"])):
                entry.update(label=m["label"], pct=m["pct"], diff=m["diff"], ok=m["ok"])
                return entry
        entry["note"] = f"Cột {hit['column']} chưa có trong cột tiền đối soát của bảng này."
        count = next((m for m in result["metrics"] if m["kind"] == "count"), None)
        if count:
            entry.update(label=count["label"], pct=count["pct"], diff=count["diff"], ok=count["ok"])
        return entry

    def table_entry(self, j, path, kind, cache):
        entry = {"table": self.name(j), "column": "", "path": path, "label": "", "pct": None, "diff": None,
                 "ok": None, "from": "", "note": ""}
        result = self.source_result(j, cache)
        if result is None:
            entry["note"] = "Chưa có kết quả đối soát cùng ngày."
            return entry
        entry["from"] = result["from"]
        pool = [m for m in result["metrics"] if m["kind"] == kind] or result["metrics"]
        worst = max(pool, key=lambda m: decimal_or_none(m["pct"]) if m["pct"] is not None else Decimal("Infinity"),
                    default=None)
        if worst:
            entry.update(label=worst["label"], pct=worst["pct"], diff=worst["diff"], ok=worst["ok"])
        return entry

    def summary(self, metric, mode, entries, node):
        if not entries:
            return "Không có bảng nguồn nào của chỉ số này nằm trong danh sách đối soát."
        with_result = [e for e in entries if e["diff"] is not None]
        moved = [e for e in with_result if decimal_or_none(e["diff"]) not in (None, Decimal(0))]
        if moved:
            top = moved[0]
            where = f"{top['table']}.{top['column']}" if top["column"] else top["table"]
            pct = f"{top['pct']}%" if top["pct"] is not None else "(Datalake = 0)"
            return f"Nghi do {where} lệch {pct}" + (f" và {len(moved) - 1} nguồn khác." if len(moved) > 1 else ".")
        if with_result and len(with_result) == len(entries):
            return f"Các nguồn khớp tuyệt đối, lệch phát sinh ở câu lệnh nạp {node}."
        return "Các nguồn đã có kết quả đều khớp, còn nguồn chưa đối soát cùng ngày."

    def log_row(self, index):
        """Ghi ket qua bang vao DB ngay khi xong, tat tool giua chung van con log. Goi khi dang giu self.lock."""
        if self.store is None or self.run_id is None:
            return
        row = self.rows[index]
        item = row["prepared"]
        raw = self.raw_rows[index] if self.raw_rows is not None else {}
        side_errors = {s: o[1] for s, o in row["sides"].items() if o[0] == "error"}
        times = row.get("times", {})
        starts = [t[0] for t in times.values()]

        def took(side):
            if side not in times:
                return None
            return int((times[side][1] - times[side][0]).total_seconds() * 1000)

        if item["error"]:
            threshold = str(raw.get("threshold") or self.threshold)
        else:
            threshold = str(reconcile.row_threshold(item, self.threshold))
        done = row["state"] == "done"
        state = (
            "success" if done else "cancelled" if row["state"] == "cancelled"
            else "blocked" if row["state"] == "blocked" else "error"
        )
        result = {
            "bang_id": db.as_id(raw.get("id")),
            **{f: str(raw.get(f) or "") for f in ("old_table", "new_table", "old_partition", "new_partition")},
            "nguong_ap_dung": threshold,
            "sql_datalake": item["old_sql"],
            "sql_lakehouse": item["new_sql"],
            "trang_thai": state,
            "ket_luan": row["verdict"] if done else "",
            "loi": row["error"],
            "loi_datalake": side_errors.get("old", ""),
            "loi_lakehouse": side_errors.get("new", ""),
            "bat_dau_luc": db.now_text(min(starts)) if starts else None,
            "ket_thuc_luc": db.now_text(),
            "thoi_gian_datalake_ms": took("old"),
            "thoi_gian_lakehouse_ms": took("new"),
            "canh_bao": "\n".join(row["warnings"]),
            "nghi_do": json.dumps(row["cause"], ensure_ascii=False) if row["cause"] else "",
        }
        try:
            row["result_id"] = self.store.add_result(self.run_id, result, row["metrics"] if done else [])
        except sqlite3.Error as exc:
            self.log_error = f"Chưa ghi được lịch sử vào {DB_FILE}: {exc}"

    def log_finish(self):
        if self.store is None or self.run_id is None:
            return
        try:
            self.store.finish_run(self.run_id, self.state)
        except sqlite3.Error as exc:
            self.log_error = f"Chưa ghi được lịch sử vào {DB_FILE}: {exc}"

    def side_progress(self, side):
        # Tien do rieng tung he thong de thay ben nao cham/treo/loi; dong loi cau hinh/bi bo qua khong chay.
        runnable = [
            row for row in self.rows
            if row["state"] not in ("skipped", "blocked") and not row["prepared"]["error"]
        ]
        outcomes = [row["sides"][side][0] for row in runnable if side in row["sides"]]
        return {
            "total": len(runnable),
            "done": sum(o in ("ok", "error") for o in outcomes),
            "error": outcomes.count("error"),
        }

    def snapshot(self):
        with self.lock:
            rows = []
            for i, row in enumerate(self.rows):
                item = {k: row[k] for k in ("state", "error", "verdict", "metrics", "warnings", "cause", "upstream")}
                if row["state"] == "pending" and self.waiting.get(i):
                    item["waiting"] = [self.name(j) for j in sorted(self.deps[i]) if self.rows[j]["state"] in ("pending", "running")]
                rows.append(item)
            return {
                "id": self.id,
                "run_id": self.run_id,
                "state": self.state,
                "gate": self.gate,
                "log_error": self.log_error,
                "progress": {side: self.side_progress(side) for side in SIDES},
                "rows": rows,
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


def write_sql_file(prepared, date):
    """Ghi bang va cau query cua lan chay canh file exe: phan tren Datalake, phan duoi Lakehouse."""
    stamp = re.sub(r"[^0-9-]", "", date) or "khong_ngay"
    path = os.path.join(DATA_DIR, f"sql_doi_soat_{stamp}.txt")
    # Chi gom cau query de chep chay lai; dong sai cau hinh khong chay nen bo qua.
    rule = "=" * 60
    sections = []
    for side, name in SIDES.items():
        queries = [f"{item[f'{side}_sql']};" for item in prepared if not item["error"]]
        sections.append("\n\n".join([f"{rule}\n-- {name.upper()}\n{rule}", *queries]))
    content = ("\n\n".join(sections) + "\n").replace("\n", "\r\n").encode("utf-8")
    temp = path + ".tmp"
    with open(temp, "wb") as fh:
        fh.write(content)
    os.replace(temp, path)
    return path


def read_state():
    rows, settings, problems = [], {}, []
    db_path = os.path.join(DATA_DIR, DB_FILE)
    try:
        rows = get_store().list_tables()
    except sqlite3.Error as exc:
        problems.append(f"Không mở được {DB_FILE}: {exc}")
    if migrate_problem:
        problems.append(migrate_problem)
    try:
        with open(os.path.join(DATA_DIR, SETTINGS_FILE), encoding="utf-8") as fh:
            settings = json.load(fh)
    except FileNotFoundError:
        pass
    except (OSError, ValueError) as exc:
        problems.append(f"Không đọc được {SETTINGS_FILE}: {exc}")
    return {"rows": rows, "settings": settings, "path": db_path, "error": " ".join(problems)}


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
    rows = []
    for r in data.get("rows", []):
        row = {f: str(r.get(f) or "") for f in reconcile.FIELDS}
        row["id"] = r.get("id")
        row["nguon"] = str(r.get("nguon") or "")
        rows.append(row)
    incoming = data.get("settings") or {}
    settings = {k: str(incoming.get(k) or "") for k in SETTING_KEYS}
    with state_lock:
        try:
            ids = get_store().sync_tables(rows)
        except sqlite3.Error as exc:
            return jsonify(error=f"Chưa lưu được danh sách vào {DB_FILE}: {exc}"), 500
        try:
            write_if_changed(
                os.path.join(DATA_DIR, SETTINGS_FILE),
                json.dumps(settings, ensure_ascii=False, indent=2).encode("utf-8"),
            )
        except OSError as exc:
            return jsonify(error=f"Chưa lưu được cấu hình: {exc}", ids=ids), 500
    return jsonify(ok=True, ids=ids)


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
    rows = reconcile.parse_csv(upload.read())
    # Ghi nguon de lich su biet bang duoc them tu file nao.
    for row in rows:
        row["nguon"] = f"csv:{upload.filename or ''}"
    return jsonify(rows=rows)


@app.post("/api/preview")
def preview():
    data = request.get_json()
    date = str(data.get("date") or "")
    now = datetime.now()
    try:
        base = reconcile.base_time(date, now).strftime("%d/%m/%Y %H:%M:%S")
    except ValueError:
        base = ""
    return jsonify(rows=[reconcile.prepare_row(r, date, now) for r in data.get("rows", [])], base=base)


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
    threshold = reconcile.parse_threshold(data.get("threshold"))
    if threshold is None:
        raise ValueError("Chưa nhập ngưỡng chênh lệch chung.")
    try:
        parallel = int(data.get("parallel"))
    except (TypeError, ValueError):
        raise ValueError("Số query song song không hợp lệ.") from None
    if not 1 <= parallel <= MAX_PARALLEL:
        raise ValueError(f"Số query song song phải từ 1 đến {MAX_PARALLEL}.")
    if not data.get("rows"):
        raise ValueError("Danh sách bảng đang trống.")

    selected = None
    if data.get("indices") is not None:
        try:
            selected = {int(i) for i in data["indices"]}
        except (TypeError, ValueError):
            raise ValueError("Danh sách bảng cần chạy không hợp lệ.") from None
        if not selected or not all(0 <= i < len(data["rows"]) for i in selected):
            raise ValueError("Danh sách bảng cần chạy không hợp lệ.")

    date = str(data.get("date") or "")
    gate = bool(data.get("gate"))
    now = datetime.now()
    prepared = [reconcile.prepare_row(r, date, now) for r in data["rows"]]
    try:
        _, graph = get_lineage()
    except sqlite3.Error:
        graph = None
    job = Job(prepared, cfgs, threshold, selected, data["rows"], date, gate, graph)
    picked = [i for i in range(len(prepared)) if selected is None or i in selected]

    with jobs_lock:
        if any(j.state == "running" for j in jobs.values()):
            return jsonify(error="Đang có một lượt đối soát chạy."), 409
        try:
            sql_path = write_sql_file([prepared[i] for i in picked], date)
        except OSError as exc:
            return jsonify(error=f"Chưa ghi được file câu query: {exc}"), 500
        job.sql_path = sql_path
        try:
            job.store = get_store()
            job.run_id = job.store.start_run(
                date, threshold, parallel, cfgs, len(picked), sql_path, gate and graph is not None
            )
        except sqlite3.Error as exc:
            return jsonify(error=f"Chưa ghi được lượt chạy vào {DB_FILE}: {exc}"), 500
        jobs.clear()
        jobs[job.id] = job

    # Bang sai cau hinh khong chay query nhung van ghi log loi de biet.
    with job.lock:
        for index in picked:
            if job.rows[index]["state"] == "error":
                job.log_row(index)
        if not job.remaining:
            job.log_finish()

    job.start(parallel)
    return jsonify(id=job.id, sql_file=sql_path)


def optional_int(name):
    value = request.args.get(name)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Tham số {name} không hợp lệ.") from None


@app.get("/api/history/runs")
def history_runs():
    return jsonify(runs=get_store().list_runs())


@app.get("/api/history/results")
def history_results():
    return jsonify(
        results=get_store().list_results(
            run_id=optional_int("run"),
            table_id=optional_int("table"),
            term=(request.args.get("q") or "").strip(),
            status=request.args.get("status") or "",
        )
    )


@app.errorhandler(sqlite3.Error)
def handle_db_error(exc):
    return jsonify(error=f"Lỗi đọc/ghi {DB_FILE}: {exc}"), 500


def get_lineage():
    with lineage_lock:
        if not lineage_cache["loaded"]:
            data = get_store().load_lineage()
            graph = lineage.Graph(data["statements"], data["edges"], data["columns"]) if data else None
            lineage_cache.update(loaded=True, data=data, graph=graph)
        return lineage_cache["data"], lineage_cache["graph"]


def lineage_payload(data):
    if data is None:
        return {"scanned": False}
    return {"scanned": True, **{k: data[k] for k in (
        "folder", "file_count", "ignored", "warnings", "statements", "edges", "columns")},
        "project_home": data.get("project_home") or "",
        "scanned_at": data.get("scanned_at") or db.now_text()}


@app.get("/api/lineage")
def lineage_get():
    data, _ = get_lineage()
    return jsonify(lineage_payload(data))


scan_jobs = {}
scan_jobs_lock = threading.Lock()


def clean_path(text):
    return str(text or "").strip().strip('"').strip()


def start_scan(folder, project_home, label=None, cleanup=None):
    """Quet o luong rieng; giao dien hoi /api/lineage/scan/<id> de hien tien do."""
    with scan_jobs_lock:
        if any(j["state"] == "running" for j in scan_jobs.values()):
            if cleanup:
                shutil.rmtree(cleanup, ignore_errors=True)
            return None
        scan_jobs.clear()
        job = {"id": uuid.uuid4().hex, "state": "running", "error": "", "progress": {"phase": "index"}}
        scan_jobs[job["id"]] = job

    def report(**info):
        job["progress"] = {**job["progress"], **info}

    def work():
        try:
            scan = lineage.scan_folder(folder, project_home or None, report, label)
            report(phase="save")
            with lineage_lock:
                get_store().save_lineage(scan)
                lineage_cache.update(loaded=False)
            job["state"] = "done"
        except ValueError as exc:
            job.update(state="error", error=str(exc))
        except Exception as exc:  # loi bat ngo: bao len giao dien thay vi im lang
            job.update(state="error", error=f"Lỗi khi quét: {type(exc).__name__}: {exc}")
        finally:
            if cleanup:
                shutil.rmtree(cleanup, ignore_errors=True)

    threading.Thread(target=work, daemon=True).start()
    return job["id"]


@app.post("/api/lineage/scan")
def lineage_scan():
    data = request.get_json() or {}
    folder = clean_path(data.get("folder"))
    if not folder:
        raise ValueError("Chưa nhập thư mục chứa job Hop.")
    project_home = clean_path(data.get("project_home"))
    job_id = start_scan(os.path.abspath(folder), os.path.abspath(project_home) if project_home else None)
    if job_id is None:
        return jsonify(error="Đang có một lượt quét chạy."), 409
    return jsonify(id=job_id)


@app.post("/api/lineage/upload")
def lineage_upload():
    """Thu muc keo tha vao trinh duyet: ghi tam cac file .hwf/.hpl/.sql ra dia roi quet nhu thu muc that.

    Trinh duyet khong cho biet duong dan that nen ${PROJECT_HOME} duoc doan bang cach khop duoi duong dan.
    """
    data = request.get_json() or {}
    name = re.sub(r'[\\/:*?"<>|]', "_", str(data.get("name") or "").strip()) or "thu_muc"
    files = data.get("files") or []
    if not files:
        raise ValueError("Thư mục kéo thả không có file .hwf/.hpl nào.")
    temp = tempfile.mkdtemp(prefix="doi_soat_hop_")
    root = os.path.join(temp, name)
    written = 0
    try:
        for item in files:
            parts = [x for x in re.split(r"[\\/]+", str(item.get("path") or "")) if x and x not in (".", "..")]
            if not parts or any(":" in x for x in parts):
                continue
            target = os.path.join(root, *parts)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(str(item.get("content") or ""))
            written += 1
    except OSError as exc:
        shutil.rmtree(temp, ignore_errors=True)
        raise ValueError(f"Không ghi tạm được file kéo thả: {exc}") from None
    project_home = clean_path(data.get("project_home"))
    job_id = start_scan(root, os.path.abspath(project_home) if project_home else None,
                        label=f"{name} (kéo thả, {written} file)", cleanup=temp)
    if job_id is None:
        return jsonify(error="Đang có một lượt quét chạy."), 409
    return jsonify(id=job_id)


@app.get("/api/lineage/scan/<job_id>")
def lineage_scan_status(job_id):
    with scan_jobs_lock:
        job = scan_jobs.get(job_id)
    if job is None:
        return jsonify(error="Không tìm thấy lượt quét."), 404
    return jsonify({k: job[k] for k in ("state", "error", "progress")})


@app.post("/api/pick-folder")
def pick_folder():
    """Mo hop thoai chon thu muc cua Windows (tool chay tren chinh may nguoi dung)."""
    try:
        import tkinter
        from tkinter import filedialog
    except ImportError:
        return jsonify(error="Máy này không mở được hộp thoại chọn thư mục, hãy dán đường dẫn."), 500
    start = clean_path((request.get_json() or {}).get("initial"))
    root = tkinter.Tk()
    try:
        root.withdraw()
        root.attributes("-topmost", True)
        folder = filedialog.askdirectory(
            parent=root, title="Chọn thư mục chứa job Hop",
            initialdir=start if start and os.path.isdir(start) else None,
        )
    finally:
        root.destroy()
    return jsonify(folder=os.path.normpath(folder) if folder else "")


@app.post("/api/lineage/deps")
def lineage_deps():
    """Bang nguon/dich gan nhat cua tung dong trong danh sach (theo chi so dong)."""
    rows = (request.get_json() or {}).get("rows") or []
    _, graph = get_lineage()
    if graph is None:
        return jsonify(scanned=False, rows=[])
    nodes = row_nodes(graph, rows)
    ups = row_dependencies(graph, nodes, "up")
    downs = row_dependencies(graph, nodes, "down")
    return jsonify(
        scanned=True,
        rows=[{"node": n, "upstream": u, "downstream": d} for n, u, d in zip(nodes, ups, downs)],
    )


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
    # Mo DB som: chuyen CSV cu va danh dau cac luot bi tat giua chung truoc khi giao dien goi vao.
    try:
        get_store()
    except sqlite3.Error as exc:
        print(f"Khong mo duoc {DB_FILE}: {exc}")
    url = f"http://127.0.0.1:{port}"
    # Khong dau: console Windows cu co the khong in duoc tieng Viet va lam exe dung ngay.
    print(f"Cong cu doi soat dang chay tai {url}")
    print("Dong cua so nay de tat cong cu.")
    threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    app.run(host="127.0.0.1", port=port, threaded=True)


if __name__ == "__main__":
    main()
