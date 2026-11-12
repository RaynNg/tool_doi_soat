"""Doc CSV, sinh cau SQL va so sanh ket qua giua Datalake va Lakehouse."""
import calendar
import csv
import io
import os
import re
from datetime import datetime, timedelta
from decimal import Context, Decimal, InvalidOperation, localcontext

FIELDS = (
    "old_table",
    "new_table",
    "old_partition",
    "new_partition",
    "old_sum_columns",
    "new_sum_columns",
    "old_sql",
    "new_sql",
    # Nguong lech rieng cua bang (%), de trong thi dung nguong chung.
    "threshold",
)
REQUIRED_HEADERS = ("old_table", "new_table")

TEMPLATE_CSV = (
    ",".join(FIELDS) + "\n"
    "old_db.giao_dich,new_db.giao_dich,partition=${YYYYMMDD:DD-1},partition=${YYYYMMDD:DD-1},so_tien|phi,amount|fee,,,0.5\n"
    "old_db.cuoc,new_db.cuoc,mon=${YYMM:DD-1} AND pay_type=2,mon=${YYYYMM:DD-1} AND pay_type=2,tien_cuoc,,,,\n"
    "old_db.tai_khoan,new_db.tai_khoan_v2,,,so_du,,,,\n"
)

TABLE_RE = re.compile(r"^[\w.`\-]+$")
FORBIDDEN_RE = re.compile(r";|--|/\*")
WHERE_PREFIX_RE = re.compile(r"^\s*where\s+", re.IGNORECASE)
SUM_SPLIT_RE = re.compile(r"[|;]")
DATE_TOKEN_RE = re.compile(r"\{(date|(?:yyyy|yy|mm|dd)(?:[-/_]?(?:yyyy|yy|mm|dd))*)\}", re.IGNORECASE)
DATE_PARTS = {"yyyy": "%Y", "yy": "%y", "mm": "%m", "dd": "%d"}
LEFTOVER_TOKEN_RE = re.compile(r"\{[^}]*\}")
# Cot don gian, co the kem tien to bang/backtick: t.charge, `charge`. Bieu thuc thi khong lay lam alias.
COLUMN_NAME_RE = re.compile(r"^\s*(?:(?:\w+|`[^`]+`)\.)*(\w+|`[^`]+`)\s*$")
SELECT_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)

# Bien thoi gian theo Bien.txt, viet dang ${YYYYMMDD:DD-1}. Moc tinh (ngay N) = ngay doi soat, gio hien tai:
# chon ngay 20261101 thi ${YYYYMMDD:DD-1} ra 20261031.
VARIABLE_RE = re.compile(r"\$\{([^}]*)\}")
JAVA_FORMAT_RE = re.compile(r"yyyy|yy|MM|M|dd|HH|mm|ss")
# File canh exe (nguoi dung tu them bien) duoc uu tien hon ban dong goi kem.
VARIABLE_FILES = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "Bien.txt")]
_variable_cache = {}


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


def load_variables():
    """Doc bang bien thoi gian tu file Bien.txt dau tien tim thay; doc lai khi file thay doi."""
    path = next((p for p in VARIABLE_FILES if os.path.isfile(p)), None)
    if path is None:
        return {}
    stamp = (path, os.path.getmtime(path))
    if _variable_cache.get("stamp") != stamp:
        with open(path, "rb") as fh:
            reader = csv.DictReader(io.StringIO(_decode(fh.read())))
            reader.fieldnames = [(h or "").strip().upper() for h in reader.fieldnames or []]
            table = {}
            for record in reader:
                name = (record.get("NAME") or "").strip()
                if not name:
                    continue
                try:
                    shift = {k: int(record.get(k) or 0) for k in ("ADD_DAY", "ADD_MON", "ADD_YEAR", "ADD_MIN", "ADD_HOUR")}
                except ValueError:
                    continue
                table[name.upper()] = {
                    **shift,
                    "format": (record.get("FORMAT") or "").strip(),
                    "last_day": "LAST_M" in (record.get("EXTEND_FORMAT") or "").upper(),
                }
        _variable_cache.update(stamp=stamp, table=table)
    return _variable_cache["table"]


def _add_months(moment, months):
    # Giong Java Calendar: 31/03 lui 1 thang thanh 28 (29)/02.
    total = moment.year * 12 + moment.month - 1 + months
    year, month = divmod(total, 12)
    day = min(moment.day, calendar.monthrange(year, month + 1)[1])
    return moment.replace(year=year, month=month + 1, day=day)


def _java_format(moment, pattern):
    parts = {
        "yyyy": f"{moment.year:04d}", "yy": f"{moment.year % 100:02d}", "MM": f"{moment.month:02d}",
        "M": str(moment.month), "dd": f"{moment.day:02d}", "HH": f"{moment.hour:02d}",
        "mm": f"{moment.minute:02d}", "ss": f"{moment.second:02d}",
    }
    return JAVA_FORMAT_RE.sub(lambda m: parts[m.group()], pattern)


def eval_variable(name, base):
    spec = load_variables().get(name.strip().upper())
    if spec is None:
        return None
    moment = base + timedelta(days=spec["ADD_DAY"])
    moment = _add_months(moment, spec["ADD_MON"] + 12 * spec["ADD_YEAR"])
    moment += timedelta(minutes=spec["ADD_MIN"], hours=spec["ADD_HOUR"])
    if spec["last_day"]:
        moment = moment.replace(day=calendar.monthrange(moment.year, moment.month)[1])
    return _java_format(moment, spec["format"])


def base_time(date, now=None):
    """Ngay N de tinh bien: chinh ngay doi soat, giu gio hien tai."""
    now = now or datetime.now()
    parsed = datetime.strptime(date, "%Y-%m-%d")
    return datetime.combine(parsed.date(), now.time().replace(microsecond=0))


def parse_threshold(value, name="Ngưỡng chênh lệch"):
    """Nguong % lech: so khong am. Chuoi rong tra ve None."""
    text = str(value if value is not None else "").strip().rstrip("%").strip().replace(",", ".")
    if not text:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        raise ValueError(f"{name} không hợp lệ: {value}") from None
    if not number.is_finite() or number < 0:
        raise ValueError(f"{name} phải là số không âm: {value}")
    return number


def row_threshold(item, default):
    """Nguong ap cho mot bang da prepare: nguong rieng neu co, khong thi nguong chung."""
    own = (item or {}).get("threshold")
    return Decimal(own) if own else default


def prepare_row(row, date="", now=None):
    """Sinh SQL cho mot dong. Dong sai cau hinh tra ve khoa `error`.

    now: thoi diem chay, truyen chung cho ca luot de moi bang dung cung gia tri bien gio/phut.
    """
    now = now or datetime.now()
    try:
        old_cols = _split_columns(row.get("old_sum_columns", ""))
        new_cols = _split_columns(row.get("new_sum_columns", "")) or old_cols
        if len(old_cols) != len(new_cols):
            raise ValueError(
                f"Số cột tiền không khớp: Datalake {len(old_cols)} cột, Lakehouse {len(new_cols)} cột."
            )
        for col in old_cols + new_cols:
            _check_fragment(col, "Cột tiền")
        own_threshold = parse_threshold(row.get("threshold"), "Ngưỡng lệch riêng")

        sqls, templates = {}, {}
        for side, cols, name in (("old", old_cols, "Datalake"), ("new", new_cols, "Lakehouse")):
            table, partition = row.get(f"{side}_table", ""), row.get(f"{side}_partition", "")
            # Cau tu sinh giu nguyen bien ngay de nguoi dung chep ra sua.
            templates[side] = _build_sql(table, partition, cols, None, name, now)
            custom = (row.get(f"{side}_sql") or "").strip()
            if custom:
                if not table.strip():
                    raise ValueError(f"Chưa nhập bảng {name}.")
                sqls[side] = _custom_sql(custom, date, name, now)
            else:
                sqls[side] = _build_sql(table, partition, cols, date, name, now)
    except ValueError as exc:
        return {
            "error": str(exc), "old_sql": "", "new_sql": "", "old_template": "", "new_template": "",
            "metrics": [], "threshold": "",
        }

    metrics = [{"label": "Số dòng", "kind": "count"}]
    for old_col, new_col in zip(old_cols, new_cols):
        label = old_col if old_col == new_col else f"{old_col} → {new_col}"
        metrics.append({"label": label, "kind": "sum"})
    return {
        "error": "",
        "old_sql": sqls["old"],
        "new_sql": sqls["new"],
        "old_template": templates["old"],
        "new_template": templates["new"],
        "metrics": metrics,
        "threshold": "" if own_threshold is None else str(own_threshold),
    }


def _split_columns(value):
    return [c.strip() for c in SUM_SPLIT_RE.split(value or "") if c.strip()]


def _check_fragment(text, name):
    if FORBIDDEN_RE.search(text):
        raise ValueError(f"{name} không được chứa ; hoặc chú thích SQL: {text}")


def _custom_sql(text, date, name, now):
    """Cau query tu viet: chi mot cau SELECT, tra ve COUNT roi den cac SUM theo dung thu tu cot tien."""
    sql = text.strip().rstrip(";").strip()
    if ";" in sql:
        raise ValueError(f"Query {name} chỉ được có một câu, không chứa dấu ; ở giữa.")
    if not SELECT_RE.match(sql):
        raise ValueError(f"Query {name} phải bắt đầu bằng SELECT hoặc WITH.")
    return _fill_date(sql, date, f"Query {name}", now)


def _fill_date(text, date, name, now=None):
    if date is None or not (DATE_TOKEN_RE.search(text) or VARIABLE_RE.search(text)):
        return text
    if not date:
        raise ValueError(f"{name} có dùng biến ngày nhưng chưa chọn ngày đối soát.")
    try:
        parsed = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Ngày đối soát không hợp lệ: {date}") from None

    base = base_time(date, now)

    def expand_variable(match):
        value = eval_variable(match.group(1), base)
        if value is None:
            raise ValueError(f"{name} dùng biến thời gian không có trong Bien.txt: {match.group()}")
        return value

    text = VARIABLE_RE.sub(expand_variable, text)

    def expand(match):
        token = match.group(1).lower()
        if token == "date":
            token = "yyyy-mm-dd"
        return parsed.strftime(re.sub("yyyy|yy|mm|dd", lambda m: DATE_PARTS[m.group()], token))

    return DATE_TOKEN_RE.sub(expand, text)


def _sum_alias(col, index, used):
    """Lay ten cot lam alias (SUM(charge) AS charge); bieu thuc hoac ten trung thi dung sum_<i>."""
    match = COLUMN_NAME_RE.match(col)
    alias = match.group(1) if match else ""
    key = alias.strip("`").lower()
    if not alias or key in used:
        alias, key = f"sum_{index}", f"sum_{index}"
    used.add(key)
    return alias


def _build_sql(table, partition, columns, date, side, now=None):
    table = _fill_date(table.strip(), date, f"Bảng {side}", now)
    if not table:
        raise ValueError(f"Chưa nhập bảng {side}.")
    # Cau mau (date=None) giu nguyen bien ngay trong ten bang, nen bo qua bien khi kiem tra.
    if not TABLE_RE.match(DATE_TOKEN_RE.sub("x", VARIABLE_RE.sub("x", table))):
        raise ValueError(f"Tên bảng {side} không hợp lệ: {table}")

    where = WHERE_PREFIX_RE.sub("", partition.strip())
    where = _fill_date(where, date, f"Partition {side}", now)
    _check_fragment(where, f"Partition {side}")
    leftover = LEFTOVER_TOKEN_RE.search(where) if date is not None else None
    if leftover:
        raise ValueError(f"Partition {side} có biến ngày không hỗ trợ: {leftover.group()}")

    selects = ["COUNT(*) AS cnt"]
    used = {"cnt"}
    for i, col in enumerate(columns, 1):
        selects.append(f"SUM({col}) AS {_sum_alias(col, i, used)}")
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
    """So sanh tung chi so, lay Datalake lam moc tinh phan tram.

    NULL (bang rong hoac toan NULL) duoc tinh la 0.
    """
    for side, values in (("Datalake", old_values), ("Lakehouse", new_values)):
        if values is None or len(values) != len(metrics):
            got = 0 if values is None else len(values)
            names = ", ".join(m["label"] for m in metrics)
            raise ValueError(f"Query {side} trả về {got} cột, cần {len(metrics)} cột theo thứ tự: {names}.")
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
