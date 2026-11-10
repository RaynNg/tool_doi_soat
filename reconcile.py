"""Doc CSV, sinh cau SQL va so sanh ket qua giua he thong cu va moi."""
import csv
import io
import re
from datetime import datetime
from decimal import Context, Decimal, InvalidOperation, localcontext

FIELDS = (
    "old_table",
    "new_table",
    "old_partition",
    "new_partition",
    "old_sum_columns",
    "new_sum_columns",
)
REQUIRED_HEADERS = ("old_table", "new_table")

TEMPLATE_CSV = (
    ",".join(FIELDS) + "\n"
    "old_db.giao_dich,new_db.giao_dich,dt='{date}',data_date={yyyymmdd},so_tien|phi,amount|fee\n"
    "old_db.cuoc,new_db.cuoc,mon={yymm} AND pay_type=2,mon={yyyymm} AND pay_type=2,tien_cuoc,\n"
    "old_db.tai_khoan,new_db.tai_khoan_v2,,,so_du,\n"
)

TABLE_RE = re.compile(r"^[\w.`\-]+$")
FORBIDDEN_RE = re.compile(r";|--|/\*")
WHERE_PREFIX_RE = re.compile(r"^\s*where\s+", re.IGNORECASE)
SUM_SPLIT_RE = re.compile(r"[|;]")
DATE_TOKEN_RE = re.compile(r"\{(date|(?:yyyy|yy|mm|dd)(?:[-/_]?(?:yyyy|yy|mm|dd))*)\}", re.IGNORECASE)
DATE_PARTS = {"yyyy": "%Y", "yy": "%y", "mm": "%m", "dd": "%d"}
LEFTOVER_TOKEN_RE = re.compile(r"\{[^}]*\}")


def parse_csv(raw, allow_empty=False):
    """Tra ve danh sach dong cau hinh tu noi dung file CSV (bytes)."""
    text = _decode(raw)
    lines = text.splitlines()
    if not lines or not lines[0].strip():
        raise ValueError("File CSV trống hoặc dòng đầu không có tên cột.")

    delimiter = max(",;\t", key=lines[0].count)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    headers = [(h or "").strip().lower() for h in reader.fieldnames or []]
    missing = [h for h in REQUIRED_HEADERS if h not in headers]
    if missing:
        raise ValueError(
            "File CSV thiếu cột: " + ", ".join(missing) + ". Tải file mẫu để xem đúng định dạng."
        )
    reader.fieldnames = headers

    rows = []
    for record in reader:
        row = {f: (record.get(f) or "").strip() for f in FIELDS}
        if any(row.values()):
            rows.append(row)
    if not rows and not allow_empty:
        raise ValueError("File CSV không có dòng dữ liệu nào.")
    return rows


def to_csv(rows):
    out = io.StringIO()
    writer = csv.DictWriter(out, FIELDS, extrasaction="ignore", lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue().encode("utf-8-sig")


def _decode(raw):
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1258", errors="replace")


def prepare_row(row, date=""):
    """Sinh SQL cho mot dong. Dong sai cau hinh tra ve khoa `error`."""
    try:
        old_cols = _split_columns(row.get("old_sum_columns", ""))
        new_cols = _split_columns(row.get("new_sum_columns", "")) or old_cols
        if len(old_cols) != len(new_cols):
            raise ValueError(
                f"Số cột tiền không khớp: hệ thống cũ {len(old_cols)} cột, hệ thống mới {len(new_cols)} cột."
            )
        for col in old_cols + new_cols:
            _check_fragment(col, "Cột tiền")

        old_sql = _build_sql(row.get("old_table", ""), row.get("old_partition", ""), old_cols, date, "cũ")
        new_sql = _build_sql(row.get("new_table", ""), row.get("new_partition", ""), new_cols, date, "mới")
    except ValueError as exc:
        return {"error": str(exc), "old_sql": "", "new_sql": "", "metrics": []}

    metrics = [{"label": "Số dòng", "kind": "count"}]
    for old_col, new_col in zip(old_cols, new_cols):
        label = old_col if old_col == new_col else f"{old_col} → {new_col}"
        metrics.append({"label": label, "kind": "sum"})
    return {"error": "", "old_sql": old_sql, "new_sql": new_sql, "metrics": metrics}


def _split_columns(value):
    return [c.strip() for c in SUM_SPLIT_RE.split(value or "") if c.strip()]


def _check_fragment(text, name):
    if FORBIDDEN_RE.search(text):
        raise ValueError(f"{name} không được chứa ; hoặc chú thích SQL: {text}")


def _fill_date(text, date, name):
    if not DATE_TOKEN_RE.search(text):
        return text
    if not date:
        raise ValueError(f"{name} có dùng biến ngày nhưng chưa chọn ngày đối soát.")
    try:
        parsed = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Ngày đối soát không hợp lệ: {date}") from None

    def expand(match):
        token = match.group(1).lower()
        if token == "date":
            token = "yyyy-mm-dd"
        return parsed.strftime(re.sub("yyyy|yy|mm|dd", lambda m: DATE_PARTS[m.group()], token))

    return DATE_TOKEN_RE.sub(expand, text)


def _build_sql(table, partition, columns, date, side):
    table = _fill_date(table.strip(), date, f"Bảng hệ thống {side}")
    if not table:
        raise ValueError(f"Chưa nhập bảng hệ thống {side}.")
    if not TABLE_RE.match(table):
        raise ValueError(f"Tên bảng hệ thống {side} không hợp lệ: {table}")

    where = WHERE_PREFIX_RE.sub("", partition.strip())
    where = _fill_date(where, date, f"Partition hệ thống {side}")
    _check_fragment(where, f"Partition hệ thống {side}")
    leftover = LEFTOVER_TOKEN_RE.search(where)
    if leftover:
        raise ValueError(f"Partition hệ thống {side} có biến ngày không hỗ trợ: {leftover.group()}")

    selects = ["COUNT(*) AS cnt"]
    selects += [f"SUM({col}) AS sum_{i}" for i, col in enumerate(columns, 1)]
    sql = "SELECT " + ",\n       ".join(selects) + f"\nFROM {table}"
    if where:
        sql += f"\nWHERE {where}"
    return sql


def to_decimal(value):
    """Spark tra DECIMAL dang chuoi, BIGINT dang int, DOUBLE dang float; NULL giu nguyen None."""
    if value is None:
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"Kết quả không phải là số: {value}") from None
    if not number.is_finite():
        raise ValueError(f"Kết quả không phải là số hữu hạn: {value}")
    return number


def compare(metrics, old_values, new_values, threshold):
    """So sanh tung chi so, lay he thong cu lam moc tinh phan tram.

    NULL (bang rong hoac toan NULL) duoc tinh la 0.
    """
    result = []
    with localcontext(Context(prec=80)):
        for metric, raw_old, raw_new in zip(metrics, old_values, new_values):
            old, new = to_decimal(raw_old), to_decimal(raw_new)
            old_num, new_num = old or Decimal(0), new or Decimal(0)
            diff = new_num - old_num
            if old_num == 0:
                pct = Decimal(0) if new_num == 0 else None
            else:
                pct = abs(diff) / abs(old_num) * 100
            places = "0" if metric["kind"] == "count" else "2"
            result.append(
                {
                    "label": metric["label"],
                    "kind": metric["kind"],
                    "old": _format(old, places),
                    "new": _format(new, places),
                    "diff": _format(diff, places),
                    "pct": _format(pct, "4"),
                    "ok": pct is not None and pct <= threshold,
                }
            )
    return result


def _format(value, places):
    return None if value is None else format(value, f".{places}f")


def verdict(metrics):
    """ok: trong nguong, lech: co chi so vuot nguong, trong: ca hai ben 0 dong."""
    if not all(m["ok"] for m in metrics):
        return "lech"
    if Decimal(metrics[0]["old"] or 0) == 0 and Decimal(metrics[0]["new"] or 0) == 0:
        return "trong"
    return "ok"
