"""Luu danh sach bang va lich su doi soat vao SQLite (file doi_soat.db canh exe).

Moi lan them bang la mot id rieng (ke ca trung ten), sua bang giu nguyen id, xoa chi an (da_xoa=1)
de lich su van tro ve duoc. Ket qua tung bang chup lai cau hinh luc chay nen sua bang sau nay
khong lam sai lich su. Log giu mai, khong tu xoa.
"""
import json
import sqlite3
import threading
from datetime import datetime

import reconcile

SCHEMA = """
CREATE TABLE IF NOT EXISTS bang (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    old_table TEXT NOT NULL DEFAULT '',
    new_table TEXT NOT NULL DEFAULT '',
    old_partition TEXT NOT NULL DEFAULT '',
    new_partition TEXT NOT NULL DEFAULT '',
    old_sum_columns TEXT NOT NULL DEFAULT '',
    new_sum_columns TEXT NOT NULL DEFAULT '',
    old_sql TEXT NOT NULL DEFAULT '',
    new_sql TEXT NOT NULL DEFAULT '',
    threshold TEXT NOT NULL DEFAULT '',
    thu_tu INTEGER NOT NULL DEFAULT 0,
    nguon TEXT NOT NULL DEFAULT '',
    tao_luc TEXT NOT NULL,
    sua_luc TEXT NOT NULL,
    da_xoa INTEGER NOT NULL DEFAULT 0,
    xoa_luc TEXT
);

CREATE TABLE IF NOT EXISTS luot_chay (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ngay_doi_soat TEXT NOT NULL DEFAULT '',
    bat_dau_luc TEXT NOT NULL,
    ket_thuc_luc TEXT,
    trang_thai TEXT NOT NULL,
    nguong_chung TEXT NOT NULL DEFAULT '',
    so_song_song INTEGER,
    host_datalake TEXT NOT NULL DEFAULT '',
    host_lakehouse TEXT NOT NULL DEFAULT '',
    tong_bang INTEGER NOT NULL DEFAULT 0,
    so_ok INTEGER NOT NULL DEFAULT 0,
    so_lech INTEGER NOT NULL DEFAULT 0,
    so_trong INTEGER NOT NULL DEFAULT 0,
    so_loi INTEGER NOT NULL DEFAULT 0,
    so_dung INTEGER NOT NULL DEFAULT 0,
    file_sql TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS ket_qua (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    luot_chay_id INTEGER NOT NULL REFERENCES luot_chay(id),
    bang_id INTEGER REFERENCES bang(id),
    old_table TEXT NOT NULL DEFAULT '',
    new_table TEXT NOT NULL DEFAULT '',
    old_partition TEXT NOT NULL DEFAULT '',
    new_partition TEXT NOT NULL DEFAULT '',
    nguong_ap_dung TEXT NOT NULL DEFAULT '',
    sql_datalake TEXT NOT NULL DEFAULT '',
    sql_lakehouse TEXT NOT NULL DEFAULT '',
    trang_thai TEXT NOT NULL,
    ket_luan TEXT NOT NULL DEFAULT '',
    loi TEXT NOT NULL DEFAULT '',
    loi_datalake TEXT NOT NULL DEFAULT '',
    loi_lakehouse TEXT NOT NULL DEFAULT '',
    bat_dau_luc TEXT,
    ket_thuc_luc TEXT,
    thoi_gian_datalake_ms INTEGER,
    thoi_gian_lakehouse_ms INTEGER
);

CREATE TABLE IF NOT EXISTS ket_qua_chi_so (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ket_qua_id INTEGER NOT NULL REFERENCES ket_qua(id),
    thu_tu INTEGER NOT NULL,
    nhan TEXT NOT NULL,
    loai TEXT NOT NULL,
    gia_tri_datalake TEXT,
    gia_tri_lakehouse TEXT,
    chenh_lech TEXT,
    phan_tram TEXT,
    trong_nguong INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    khoa TEXT PRIMARY KEY,
    gia_tri TEXT NOT NULL
);

-- Ket qua quet job Hop gan nhat (quet lai thi xoa het va ghi moi).
CREATE TABLE IF NOT EXISTS lineage_quet (
    id INTEGER PRIMARY KEY,
    thu_muc TEXT NOT NULL,
    quet_luc TEXT NOT NULL,
    so_file INTEGER NOT NULL DEFAULT 0,
    so_cau_bo_qua INTEGER NOT NULL DEFAULT 0,
    canh_bao TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS lineage_cau_lenh (
    id INTEGER PRIMARY KEY,
    file TEXT NOT NULL,
    duong_di TEXT NOT NULL DEFAULT '',
    buoc TEXT NOT NULL DEFAULT '',
    loai_buoc TEXT NOT NULL DEFAULT '',
    loai TEXT NOT NULL,
    sql TEXT NOT NULL DEFAULT '',
    phan_tich_mot_phan INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS lineage_cau_lenh_bang (
    cau_lenh_id INTEGER NOT NULL REFERENCES lineage_cau_lenh(id),
    bang TEXT NOT NULL,
    vai_tro TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage_canh (
    nguon TEXT NOT NULL,
    dich TEXT NOT NULL,
    cau_lenh_id INTEGER NOT NULL REFERENCES lineage_cau_lenh(id)
);

CREATE TABLE IF NOT EXISTS lineage_cot (
    dich_bang TEXT NOT NULL,
    dich_cot TEXT NOT NULL,
    nguon_bang TEXT NOT NULL,
    nguon_cot TEXT NOT NULL,
    cau_lenh_id INTEGER NOT NULL REFERENCES lineage_cau_lenh(id)
);

CREATE INDEX IF NOT EXISTS ix_lineage_bang ON lineage_cau_lenh_bang(bang);
CREATE INDEX IF NOT EXISTS ix_ket_qua_bang ON ket_qua(bang_id, bat_dau_luc);
CREATE INDEX IF NOT EXISTS ix_ket_qua_luot ON ket_qua(luot_chay_id);
CREATE INDEX IF NOT EXISTS ix_chi_so_ket_qua ON ket_qua_chi_so(ket_qua_id);
"""

RESULT_COLUMNS = (
    "id", "luot_chay_id", "bang_id", "old_table", "new_table", "old_partition", "new_partition",
    "nguong_ap_dung", "sql_datalake", "sql_lakehouse", "trang_thai", "ket_luan", "loi",
    "loi_datalake", "loi_lakehouse", "bat_dau_luc", "ket_thuc_luc",
    "thoi_gian_datalake_ms", "thoi_gian_lakehouse_ms", "canh_bao", "nghi_do",
)
RUN_COUNTERS = {"ok": "so_ok", "lech": "so_lech", "trong": "so_trong"}
# Cot them sau khi da co DB cu: tu ALTER TABLE ADD COLUMN khi mo.
ADDED_COLUMNS = (
    ("luot_chay", "so_bo_qua", "INTEGER NOT NULL DEFAULT 0"),
    ("luot_chay", "chan_phu_thuoc", "INTEGER NOT NULL DEFAULT 0"),
    ("ket_qua", "canh_bao", "TEXT NOT NULL DEFAULT ''"),
    ("ket_qua", "nghi_do", "TEXT NOT NULL DEFAULT ''"),
    ("lineage_quet", "project_home", "TEXT NOT NULL DEFAULT ''"),
    ("lineage_cot", "bieu_thuc", "TEXT NOT NULL DEFAULT ''"),
)
HISTORY_LIMIT = 500


def now_text(moment=None):
    return (moment or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")


class Store:
    """Mot ket noi dung chung cho ca app, khoa lai vi cac luong chay query cung ghi ket qua."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self.conn.row_factory = sqlite3.Row
        with self.lock, self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA foreign_keys=ON")
            self.conn.executescript(SCHEMA)
            for table, column, spec in ADDED_COLUMNS:
                existing = {r["name"] for r in self.conn.execute(f"PRAGMA table_info({table})")}
                if column not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {spec}")
            # Tool bi tat giua chung: luot dang chay khong bao gio xong, danh dau de khong hien "dang chay" mai.
            self.conn.execute(
                "UPDATE luot_chay SET trang_thai='interrupted', ket_thuc_luc=COALESCE(ket_thuc_luc, ?) "
                "WHERE trang_thai='running'",
                (now_text(),),
            )

    # ---------- danh sach bang ----------

    def migrate_csv(self, rows):
        """Chuyen danh_sach_bang.csv cu vao DB mot lan duy nhat."""
        with self.lock, self.conn:
            if self.conn.execute("SELECT 1 FROM meta WHERE khoa='da_chuyen_csv'").fetchone():
                return False
            stamp = now_text()
            for order, row in enumerate(rows):
                self._insert_table(row, order, "csv:danh_sach_bang.csv", stamp)
            self.conn.execute("INSERT INTO meta(khoa, gia_tri) VALUES ('da_chuyen_csv', ?)", (stamp,))
            return True

    def list_tables(self):
        with self.lock:
            records = self.conn.execute(
                "SELECT * FROM bang WHERE da_xoa=0 ORDER BY thu_tu, id"
            ).fetchall()
        return [self._table_dict(r) for r in records]

    def sync_tables(self, rows):
        """Dong bo danh sach tren giao dien xuong DB, tra ve id theo dung thu tu cac dong.

        Dong co id: cap nhat (giu id). Dong chua co id: them moi. Id trong DB ma khong con tren
        giao dien: danh dau da_xoa.
        """
        stamp = now_text()
        ids = []
        with self.lock, self.conn:
            current = {
                r["id"]: r
                for r in self.conn.execute("SELECT * FROM bang WHERE da_xoa=0").fetchall()
            }
            for order, row in enumerate(rows):
                table_id = as_id(row.get("id"))
                if table_id in current and table_id not in ids:
                    old = current[table_id]
                    changed = any(str(old[f]) != row[f] for f in reconcile.FIELDS)
                    if changed or old["thu_tu"] != order:
                        assigns = ", ".join(f"{f}=?" for f in reconcile.FIELDS)
                        values = [row[f] for f in reconcile.FIELDS]
                        sua_luc = stamp if changed else old["sua_luc"]
                        self.conn.execute(
                            f"UPDATE bang SET {assigns}, thu_tu=?, sua_luc=? WHERE id=?",
                            (*values, order, sua_luc, table_id),
                        )
                    ids.append(table_id)
                else:
                    ids.append(self._insert_table(row, order, row.get("nguon") or "tay", stamp))
            gone = set(current) - set(ids)
            self.conn.executemany(
                "UPDATE bang SET da_xoa=1, xoa_luc=? WHERE id=?", [(stamp, i) for i in gone]
            )
        return ids

    def _insert_table(self, row, order, source, stamp):
        columns = ", ".join(reconcile.FIELDS)
        marks = ", ".join("?" for _ in reconcile.FIELDS)
        cursor = self.conn.execute(
            f"INSERT INTO bang ({columns}, thu_tu, nguon, tao_luc, sua_luc) VALUES ({marks}, ?, ?, ?, ?)",
            (*[str(row.get(f) or "") for f in reconcile.FIELDS], order, str(source)[:200], stamp, stamp),
        )
        return cursor.lastrowid

    @staticmethod
    def _table_dict(record):
        data = {f: record[f] for f in reconcile.FIELDS}
        data["id"] = record["id"]
        return data

    # ---------- lich su chay ----------

    def start_run(self, date, threshold, parallel, cfgs, total, sql_file, gate=False):
        host = lambda cfg: f"{cfg['host']}:{cfg['port']} · {cfg['username']}"
        with self.lock, self.conn:
            cursor = self.conn.execute(
                "INSERT INTO luot_chay (ngay_doi_soat, bat_dau_luc, trang_thai, nguong_chung, so_song_song, "
                "host_datalake, host_lakehouse, tong_bang, file_sql, chan_phu_thuoc) "
                "VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)",
                (date, now_text(), str(threshold), parallel, host(cfgs["old"]), host(cfgs["new"]), total, sql_file,
                 int(bool(gate))),
            )
            return cursor.lastrowid

    def add_result(self, run_id, result, metrics):
        """Ghi ket qua mot bang ngay khi bang do xong, kem tung chi so."""
        columns = [c for c in RESULT_COLUMNS if c not in ("id", "luot_chay_id")]
        with self.lock, self.conn:
            cursor = self.conn.execute(
                f"INSERT INTO ket_qua (luot_chay_id, {', '.join(columns)}) "
                f"VALUES (?, {', '.join('?' for _ in columns)})",
                (run_id, *[result.get(c) for c in columns]),
            )
            result_id = cursor.lastrowid
            self.conn.executemany(
                "INSERT INTO ket_qua_chi_so (ket_qua_id, thu_tu, nhan, loai, gia_tri_datalake, gia_tri_lakehouse, "
                "chenh_lech, phan_tram, trong_nguong) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (result_id, i, m["label"], m["kind"], m["old"], m["new"], m["diff"], m["pct"], int(m["ok"]))
                    for i, m in enumerate(metrics)
                ],
            )
            column = (
                RUN_COUNTERS.get(result["ket_luan"]) if result["trang_thai"] == "success"
                else "so_loi" if result["trang_thai"] == "error"
                else "so_bo_qua" if result["trang_thai"] == "blocked" else "so_dung"
            )
            self.conn.execute(f"UPDATE luot_chay SET {column}={column}+1 WHERE id=?", (run_id,))
            return result_id

    def update_cause(self, result_id, cause_json):
        with self.lock, self.conn:
            self.conn.execute("UPDATE ket_qua SET nghi_do=? WHERE id=?", (cause_json, result_id))

    def finish_run(self, run_id, state):
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE luot_chay SET trang_thai=?, ket_thuc_luc=? WHERE id=?", (state, now_text(), run_id)
            )

    def list_runs(self, limit=HISTORY_LIMIT):
        with self.lock:
            records = self.conn.execute(
                "SELECT * FROM luot_chay ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in records]

    def list_results(self, run_id=None, table_id=None, term="", status="", limit=HISTORY_LIMIT):
        where, params = [], []
        if run_id is not None:
            where.append("k.luot_chay_id=?")
            params.append(run_id)
        if table_id is not None:
            where.append("k.bang_id=?")
            params.append(table_id)
        if term:
            where.append("(k.old_table LIKE ? ESCAPE '\\' OR k.new_table LIKE ? ESCAPE '\\')")
            like = "%" + term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            params += [like, like]
        if status in ("ok", "lech", "trong"):
            where.append("k.trang_thai='success' AND k.ket_luan=?")
            params.append(status)
        elif status in ("error", "cancelled", "blocked"):
            where.append("k.trang_thai=?")
            params.append(status)
        sql = (
            "SELECT k.*, l.ngay_doi_soat, l.bat_dau_luc AS luot_bat_dau_luc FROM ket_qua k "
            "JOIN luot_chay l ON l.id=k.luot_chay_id"
            + (f" WHERE {' AND '.join(where)}" if where else "")
            + " ORDER BY k.id DESC LIMIT ?"
        )
        with self.lock:
            records = [dict(r) for r in self.conn.execute(sql, (*params, limit)).fetchall()]
            if records:
                marks = ", ".join("?" for _ in records)
                metrics = self.conn.execute(
                    f"SELECT * FROM ket_qua_chi_so WHERE ket_qua_id IN ({marks}) ORDER BY ket_qua_id, thu_tu",
                    [r["id"] for r in records],
                ).fetchall()
            else:
                metrics = []
        by_result = {}
        for m in metrics:
            by_result.setdefault(m["ket_qua_id"], []).append(
                {
                    "label": m["nhan"], "kind": m["loai"], "old": m["gia_tri_datalake"],
                    "new": m["gia_tri_lakehouse"], "diff": m["chenh_lech"], "pct": m["phan_tram"],
                    "ok": bool(m["trong_nguong"]),
                }
            )
        for r in records:
            r["metrics"] = by_result.get(r["id"], [])
        return records

    def latest_results(self, date, tables, exclude_run=None):
        """Ket qua thanh cong gan nhat cung ngay doi soat cua tung bang Lakehouse (ten viet thuong)."""
        if not tables:
            return {}
        marks = ", ".join("?" for _ in tables)
        sql = (
            "SELECT k.id, lower(k.new_table) AS bang, k.ket_luan, k.ket_thuc_luc, k.luot_chay_id FROM ket_qua k "
            "JOIN luot_chay l ON l.id=k.luot_chay_id "
            f"WHERE k.trang_thai='success' AND l.ngay_doi_soat=? AND lower(k.new_table) IN ({marks})"
            + (" AND k.luot_chay_id<>?" if exclude_run is not None else "")
            + " ORDER BY k.id DESC"
        )
        params = [date, *tables] + ([exclude_run] if exclude_run is not None else [])
        with self.lock:
            latest = {}
            for r in self.conn.execute(sql, params).fetchall():
                latest.setdefault(r["bang"], dict(r))
            if latest:
                ids = [r["id"] for r in latest.values()]
                metrics = self.conn.execute(
                    f"SELECT * FROM ket_qua_chi_so WHERE ket_qua_id IN ({', '.join('?' for _ in ids)}) "
                    "ORDER BY ket_qua_id, thu_tu",
                    ids,
                ).fetchall()
            else:
                metrics = []
        by_result = {}
        for m in metrics:
            by_result.setdefault(m["ket_qua_id"], []).append(
                {"label": m["nhan"], "kind": m["loai"], "diff": m["chenh_lech"], "pct": m["phan_tram"],
                 "ok": bool(m["trong_nguong"])}
            )
        for r in latest.values():
            r["metrics"] = by_result.get(r["id"], [])
        return latest

    # ---------- phu thuoc giua cac bang (quet job Hop) ----------

    def save_lineage(self, scan):
        with self.lock, self.conn:
            for table in ("lineage_cot", "lineage_canh", "lineage_cau_lenh_bang", "lineage_cau_lenh", "lineage_quet"):
                self.conn.execute(f"DELETE FROM {table}")
            self.conn.execute(
                "INSERT INTO lineage_quet (id, thu_muc, quet_luc, so_file, so_cau_bo_qua, canh_bao, project_home) "
                "VALUES (1, ?, ?, ?, ?, ?, ?)",
                (scan["folder"], now_text(), scan["file_count"], scan["ignored"],
                 json.dumps(scan["warnings"], ensure_ascii=False), scan.get("project_home") or ""),
            )
            self.conn.executemany(
                "INSERT INTO lineage_cau_lenh (id, file, duong_di, buoc, loai_buoc, loai, sql, phan_tich_mot_phan) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [(s["id"], s["file"], s["chain"], s["step"], s["step_type"], s["kind"], s["sql"], int(s["partial"]))
                 for s in scan["statements"]],
            )
            self.conn.executemany(
                "INSERT INTO lineage_cau_lenh_bang (cau_lenh_id, bang, vai_tro) VALUES (?, ?, ?)",
                [(s["id"], t, role) for s in scan["statements"]
                 for role, names in (("dich", s["targets"]), ("nguon", s["sources"])) for t in names],
            )
            self.conn.executemany("INSERT INTO lineage_canh (nguon, dich, cau_lenh_id) VALUES (?, ?, ?)", scan["edges"])
            self.conn.executemany(
                "INSERT INTO lineage_cot (dich_bang, dich_cot, nguon_bang, nguon_cot, cau_lenh_id, bieu_thuc) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [tuple(c) + ("",) * (6 - len(c)) for c in scan["columns"]],
            )

    def load_lineage(self):
        """Tra ve dict giong ket qua quet (kem quet_luc), hoac None neu chua quet lan nao."""
        with self.lock:
            head = self.conn.execute("SELECT * FROM lineage_quet WHERE id=1").fetchone()
            if head is None:
                return None
            roles = {}
            for r in self.conn.execute("SELECT * FROM lineage_cau_lenh_bang ORDER BY rowid"):
                roles.setdefault(r["cau_lenh_id"], {"dich": [], "nguon": []})[r["vai_tro"]].append(r["bang"])
            statements = [
                {
                    "id": r["id"], "file": r["file"], "chain": r["duong_di"], "step": r["buoc"],
                    "step_type": r["loai_buoc"], "kind": r["loai"], "sql": r["sql"],
                    "partial": bool(r["phan_tich_mot_phan"]),
                    "targets": roles.get(r["id"], {}).get("dich", []),
                    "sources": roles.get(r["id"], {}).get("nguon", []),
                }
                for r in self.conn.execute("SELECT * FROM lineage_cau_lenh ORDER BY id")
            ]
            edges = [tuple(r) for r in self.conn.execute("SELECT nguon, dich, cau_lenh_id FROM lineage_canh")]
            columns = [
                tuple(r) for r in self.conn.execute(
                    "SELECT dich_bang, dich_cot, nguon_bang, nguon_cot, cau_lenh_id, bieu_thuc FROM lineage_cot"
                )
            ]
        try:
            warnings = json.loads(head["canh_bao"])
        except ValueError:
            warnings = []
        return {
            "folder": head["thu_muc"], "scanned_at": head["quet_luc"], "file_count": head["so_file"],
            "ignored": head["so_cau_bo_qua"], "warnings": warnings, "project_home": head["project_home"],
            "statements": statements, "edges": edges, "columns": columns,
        }


def as_id(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None

