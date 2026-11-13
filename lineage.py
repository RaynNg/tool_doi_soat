"""Quet job Apache Hop (.hwf/.hpl) de biet bang Lakehouse nao duoc nap tu bang nao.

Chi lay cac action/transform thuc su chay: workflow di tu action Start theo cac hop dang bat,
pipeline duoc goi tu workflow do thi lay toan bo transform (Hop chay het transform trong pipeline).
Cau lenh giu lai: CREATE, INSERT, MERGE, UPDATE, ALTER (doi location, rename, exchange partition)
va SELECT (doc du lieu, vi du Table Input). Cac cau khac (DROP, TRUNCATE, SET...) bo qua.

Phan tich SQL tu viet (khong dung thu vien ngoai), lay duoc:
- bang dich, bang nguon (bo CTE, alias, ham bang)
- quan he cot: cot dich duoc tinh tu cot nao cua bang nao (lam duoc thi lay, khong thi bo qua)
Temp view duoc giu thanh nut trung gian rieng cua tung file; location HDFS cung la nut trung gian
(INSERT OVERWRITE DIRECTORY / CREATE ... LOCATION / ALTER ... SET LOCATION cung mot duong dan).
"""
import os
import re
import xml.etree.ElementTree as ET
from collections import deque

WORKFLOW_EXT = ".hwf"
PIPELINE_EXT = ".hpl"
SQL_EXT = ".sql"
# Hop (Lakehouse) tach ra tu Pentaho/Kettle (Datalake) nen XML gan giong nhau, chi khac ten the va loai buoc.
FORMATS = {
    "hop": {
        "label": "Apache Hop", "wf": ".hwf", "pl": ".hpl", "action": "action", "step": "transform",
        "wf_types": ("WORKFLOW",), "pl_types": ("PIPELINE",), "id_base": 0,
    },
    "kettle": {
        "label": "Pentaho", "wf": ".kjb", "pl": ".ktr", "action": "entry", "step": "step",
        "wf_types": ("JOB",), "pl_types": ("TRANS",), "id_base": 1_000_000,
    },
}
EXECUTOR_TYPES = ("PipelineExecutor", "WorkflowExecutor", "SimpleMapping", "Mapping", "MetaInject",
                  "SingleThreader", "TransExecutor", "JobExecutor")
MAX_DEPTH = 40
MAX_EXPR = 500
TEMP_PREFIX = "temp:"
LOCATION_PREFIX = "location:"

TOKEN_RE = re.compile(
    r"""
    (?P<ws>\s+)
  | (?P<lc>--[^\n]*)
  | (?P<bc>/\*.*?(?:\*/|\Z))
  | (?P<var>\$\{[^}]*\}?)
  | (?P<str>'(?:[^'\\]|\\.|'')*'?|"(?:[^"\\]|\\.|"")*"?)
  | (?P<bq>`(?:[^`]|``)*`?)
  | (?P<word>[A-Za-z0-9_][\w$]*)
  | (?P<op><=>|<>|!=|>=|<=|\|\||::|=>|->|.)
    """,
    re.X | re.S,
)

# Tu khoa khong bao gio la ten bang/cot/alias.
KEYWORDS = set(
    """
    select from where group by having order limit offset union intersect except minus all distinct
    join inner left right full outer cross semi anti natural on using as and or not is null in like
    rlike ilike regexp between exists case when then else end true false insert into overwrite table
    values create replace temporary temp view external if with update set delete merge matched alter
    partition lateral window over cluster distribute sort asc desc nulls first last tablesample
    interval cast try_cast extract qualify pivot unpivot rows range unbounded preceding following
    current row filter within escape div any some local directory location stored tblproperties
    comment partitioned clustered buckets row format delimited fields terminated lines collection
    items keys serde serdeproperties options use rename to add drop columns column exchange
    both leading trailing
    """.split()
)
TIME_UNITS = set("year years month months week weeks day days hour hours minute minutes second seconds".split())
TYPE_WORDS = set(
    """
    string varchar char int integer bigint smallint tinyint double float real decimal numeric date
    timestamp timestamp_ntz timestamp_ltz boolean binary array map struct long short byte
    """.split()
)
NOT_ALIAS = KEYWORDS | TIME_UNITS
CLAUSE_END = set(
    """
    where group having order limit offset union intersect except minus window cluster distribute
    sort qualify lateral pivot unpivot
    """.split()
)
SET_OPS = {"union", "intersect", "except", "minus"}
JOIN_WORDS = {"join"}
# Ham co tu FROM nam ben trong ngoac: EXTRACT(YEAR FROM x), TRIM(BOTH ' ' FROM s)...
FROM_INSIDE_FUNCS = {"extract", "trim", "substring", "substr", "overlay", "position"}
QUERY_STARTS = {"select", "with", "from", "values"}
VAR_RE = re.compile(r"\$\{([^}]*)\}")
SCHEME_RE = re.compile(r"^[a-z][a-z0-9+.-]*://[^/]*", re.I)


# ---------------------------------------------------------------- token

class Tok:
    __slots__ = ("kind", "text", "low", "start", "end")

    def __init__(self, kind, text, start=0, end=0):
        self.kind = kind
        self.text = text
        self.low = text.lower()
        self.start = start
        self.end = end

    def __repr__(self):
        return f"Tok({self.kind},{self.text!r})"


def tokenize(sql):
    toks = []
    for m in TOKEN_RE.finditer(sql):
        kind = m.lastgroup
        if kind in ("ws", "lc", "bc"):
            continue
        toks.append(Tok(kind, m.group(), m.start(), m.end()))
    return toks


def split_statements(sql):
    """Tach script thanh tung cau theo dau ; (bo qua ; trong chuoi va chu thich)."""
    statements, current = [], []
    for tok in tokenize(sql):
        if tok.kind == "op" and tok.text == ";":
            if current:
                statements.append(current)
            current = []
        else:
            current.append(tok)
    if current:
        statements.append(current)
    return statements


def is_word(tok):
    return tok is not None and tok.kind == "word"


def is_kw(tok, *words):
    return tok is not None and tok.kind == "word" and tok.low in words


def is_op(tok, text):
    return tok is not None and tok.kind == "op" and tok.text == text


def name_part(tok):
    """Mot phan cua ten (bang/cot): tu thuong, `quoted` hoac ${bien}."""
    if tok is None:
        return None
    if tok.kind == "word":
        return tok.low
    if tok.kind == "bq":
        return tok.text.strip("`").replace("``", "`").lower()
    if tok.kind == "var":
        return tok.text.lower()
    return None


def at(toks, i):
    return toks[i] if 0 <= i < len(toks) else None


def close_paren(toks, i):
    """i tro vao '(' -> tra ve vi tri ')' tuong ung (hoac cuoi danh sach)."""
    depth = 0
    for j in range(i, len(toks)):
        if toks[j].kind == "op":
            if toks[j].text == "(":
                depth += 1
            elif toks[j].text == ")":
                depth -= 1
                if depth == 0:
                    return j
    return len(toks) - 1


def read_name(toks, i, allow_keyword=False):
    """Doc ten co dau cham (db.bang). Tra ve (ten, vi_tri_sau) hoac (None, i)."""
    tok = at(toks, i)
    first = name_part(tok)
    if first is None or (tok.kind == "word" and not allow_keyword and tok.low in KEYWORDS):
        return None, i
    if tok.kind == "word" and tok.text[0].isdigit():
        return None, i
    parts = [first]
    j = i + 1
    while is_op(at(toks, j), ".") and name_part(at(toks, j + 1)) is not None:
        parts.append(name_part(toks[j + 1]))
        j += 2
    return ".".join(parts), j


def literal(tok):
    if tok is None or tok.kind != "str":
        return None
    return tok.text[1:-1] if len(tok.text) >= 2 else tok.text


# ---------------------------------------------------------------- truy van (SELECT)

class Select:
    def __init__(self):
        self.items = []      # (alias, expr_tokens, star) ; star: None | "*" | qualifier
        self.sources = {}    # alias -> ("table", ten) | ("query", Query)


class Query:
    def __init__(self):
        self.branches = []   # cac nhanh UNION


def parse_query(toks, i, end, ctes):
    """Phan tich truy van tu toks[i:end]. Tra ve Query (co the rong neu khong hieu)."""
    query = Query()
    ctes = dict(ctes)
    if is_kw(at(toks, i), "with"):
        i = parse_ctes(toks, i + 1, end, ctes)
    while i < end:
        tok = toks[i]
        if is_op(tok, "("):
            close = close_paren(toks, i)
            inner = parse_query(toks, i + 1, close, ctes)
            query.branches.extend(inner.branches)
            i = close + 1
        elif is_kw(tok, "select"):
            sel, i = parse_select(toks, i + 1, end, ctes)
            query.branches.append(sel)
        elif is_kw(tok, "from"):
            # FROM bang SELECT ... (cu phap Hive/Spark): FROM dung truoc SELECT.
            sel = Select()
            j = parse_from(toks, i + 1, end, ctes, sel)
            if is_kw(at(toks, j), "select"):
                sel2, j = parse_select(toks, j + 1, end, ctes)
                sel.items = sel2.items
                sel.sources.update(sel2.sources)
            query.branches.append(sel)
            i = j
        else:
            i += 1
            continue
        # Bo qua den toan tu tap hop tiep theo o cung muc.
        while i < end and not (is_word(toks[i]) and toks[i].low in SET_OPS) and not is_op(toks[i], ")"):
            if is_op(toks[i], "("):
                i = close_paren(toks, i)
            i += 1
        if i < end and is_word(toks[i]) and toks[i].low in SET_OPS:
            i += 1
            if is_kw(at(toks, i), "all", "distinct"):
                i += 1
            continue
        break
    return query


def parse_ctes(toks, i, end, ctes):
    if is_kw(at(toks, i), "recursive"):
        i += 1
    while i < end:
        name, j = read_name(toks, i)
        if name is None:
            return i
        if is_op(at(toks, j), "("):
            j = close_paren(toks, j) + 1
        if not is_kw(at(toks, j), "as") or not is_op(at(toks, j + 1), "("):
            return i
        close = close_paren(toks, j + 1)
        ctes[name] = parse_query(toks, j + 2, close, ctes)
        i = close + 1
        if is_op(at(toks, i), ","):
            i += 1
            continue
        return i
    return i


def parse_select(toks, i, end, ctes):
    sel = Select()
    while is_kw(at(toks, i), "distinct", "all"):
        i += 1
    items, current = [], []
    while i < end:
        tok = toks[i]
        if is_op(tok, ")"):
            break
        if is_word(tok) and (tok.low == "from" or tok.low in CLAUSE_END):
            break
        if is_op(tok, "("):
            close = min(close_paren(toks, i), end - 1)
            current.extend(toks[i:close + 1])
            i = close + 1
            continue
        if is_op(tok, ","):
            items.append(current)
            current = []
        else:
            current.append(tok)
        i += 1
    if current:
        items.append(current)
    sel.items = [split_alias(t) for t in items if t]
    if is_kw(at(toks, i), "from"):
        i = parse_from(toks, i + 1, end, ctes, sel)
    return sel, i


def split_alias(t):
    """Tach bieu thuc va alias cua mot cot trong SELECT."""
    if len(t) == 1 and is_op(t[0], "*"):
        return (None, [], "*")
    if len(t) >= 3 and is_op(t[-1], "*") and is_op(t[-2], "."):
        qual, _ = read_name(t, 0, allow_keyword=True)
        return (None, [], qual or "*")
    alias, expr = None, t
    if len(t) >= 2 and is_kw(t[-2], "as") and name_part(t[-1]) is not None:
        alias, expr = name_part(t[-1]), t[:-2]
    elif len(t) >= 2 and name_part(t[-1]) is not None and t[-1].kind != "var" and not (
        t[-1].kind == "word" and (t[-1].low in NOT_ALIAS or t[-1].text[0].isdigit())
    ):
        prev = t[-2]
        if is_op(prev, ")") or (
            prev.kind in ("word", "bq", "str") and not (prev.kind == "word" and prev.low in NOT_ALIAS - {"end"})
        ):
            alias, expr = name_part(t[-1]), t[:-1]
    if alias is None:
        name, j = read_name(expr, 0)
        if name is not None and j == len(expr):
            alias = name.split(".")[-1]
    return (alias, expr, None)


def parse_from(toks, i, end, ctes, sel):
    """Doc danh sach bang sau FROM (ke ca JOIN, dau phay). Tra ve vi tri ket thuc menh de."""
    expect_ref = True
    while i < end:
        tok = toks[i]
        if is_op(tok, ")"):
            return i
        if expect_ref:
            i = parse_table_ref(toks, i, end, ctes, sel)
            expect_ref = False
            continue
        if is_op(tok, ","):
            expect_ref = True
            i += 1
            continue
        if is_word(tok) and tok.low in JOIN_WORDS:
            expect_ref = True
            i += 1
            continue
        if is_kw(tok, "lateral"):
            # LATERAL VIEW explode(x) t AS c: bo qua.
            i += 1
            continue
        if is_word(tok) and tok.low in CLAUSE_END | {"select"}:
            return i
        if is_op(tok, "("):
            i = close_paren(toks, i) + 1
            continue
        i += 1
    return i


def parse_table_ref(toks, i, end, ctes, sel):
    tok = at(toks, i)
    source, key = None, None
    if is_op(tok, "("):
        close = close_paren(toks, i)
        if is_word(at(toks, i + 1)) and toks[i + 1].low in QUERY_STARTS or is_op(at(toks, i + 1), "("):
            source = ("query", parse_query(toks, i + 1, close, ctes))
        i = close + 1
    else:
        name, j = read_name(toks, i)
        if name is None:
            return i + 1
        if is_op(at(toks, j), "("):
            # Ham bang (range(10), explode(...)): khong phai bang.
            return close_paren(toks, j) + 1
        source = ("query", ctes[name]) if name in ctes else ("table", name)
        key = name
        i = j
    if is_kw(at(toks, i), "as"):
        i += 1
    alias = name_part(at(toks, i))
    if alias is not None and not (toks[i].kind == "word" and toks[i].low in NOT_ALIAS):
        key = alias
        i += 1
        if is_op(at(toks, i), "("):
            i = close_paren(toks, i) + 1
    if source is not None:
        sel.sources[key or f"__sub{len(sel.sources)}"] = source
        if key and source[0] == "table" and "." in source[1] and key == source[1]:
            sel.sources[source[1].split(".")[-1]] = source
    return i


def column_refs(expr):
    """Cac tham chieu cot trong bieu thuc: [(qualifier|None, cot)]."""
    refs, i = [], 0
    while i < len(expr):
        tok = expr[i]
        if is_kw(tok, "over") and is_op(at(expr, i + 1), "("):
            i = close_paren(expr, i + 1) + 1
            continue
        if is_op(tok, "(") and is_kw(at(expr, i + 1), "select", "with"):
            i = close_paren(expr, i) + 1
            continue
        if is_kw(tok, "as") or is_op(tok, "::"):
            i += 2  # CAST(x AS kieu), x::kieu
            continue
        if is_kw(tok, "interval"):
            i += 3
            continue
        if tok.kind in ("word", "bq") and not is_op(at(expr, i - 1), "."):
            name, j = read_name(expr, i)
            if name is None or (tok.kind == "word" and (tok.low in TYPE_WORDS and is_op(at(expr, j), "("))):
                i += 1
                continue
            if is_op(at(expr, j), "(") or (is_op(at(expr, j), "-") and is_op(at(expr, j + 1), ">")) or is_op(at(expr, j), "->"):
                i = j  # ham hoac lambda
                continue
            parts = name.split(".")
            if len(parts) == 1 and parts[0] in TIME_UNITS | {"current_date", "current_timestamp"}:
                i = j
                continue
            refs.append(parts)
            i = j
            continue
        i += 1
    return refs


def _source_lookup(sel, parts):
    """Tim nguon cho tham chieu dang [a, b, (c)]. Tra ve (nguon, cot) hoac None."""
    if len(parts) >= 2:
        if parts[0] in sel.sources:
            return [(sel.sources[parts[0]], parts[1])]
        joined = ".".join(parts[:-1])
        for src in sel.sources.values():
            if src[0] == "table" and src[1] == joined:
                return [(src, parts[-1])]
        if len(parts) >= 3 and ".".join(parts[:2]) in sel.sources:
            return [(sel.sources[".".join(parts[:2])], parts[2])]
    col = parts[0]
    srcs = list({id(s): s for s in sel.sources.values()}.values())
    if len(srcs) == 1:
        return [(srcs[0], col)]
    hits = [s for s in srcs if s[0] == "query" and _query_has(s[1], col)]
    if hits:
        return [(s, col) for s in hits]
    tables = [s for s in srcs if s[0] == "table"]
    # Cot khong ghi ro bang va co nhieu bang: gan cho moi bang (chua biet chinh xac).
    return [(s, col) for s in tables]


def _query_has(query, col):
    for sel in query.branches:
        for alias, _, star in sel.items:
            if alias == col or star:
                return True
    return False


def resolve_column(sel, parts, depth=0):
    """Tra ve tap (bang, cot) goc cua tham chieu cot trong mot SELECT."""
    out = set()
    if depth > MAX_DEPTH:
        return out
    for src, col in _source_lookup(sel, parts) or []:
        if src[0] == "table":
            out.add((src[1], col))
        else:
            out |= query_output(src[1], col, depth + 1)
    return out


def query_output(query, col, depth=0):
    out = set()
    for sel in query.branches:
        found = False
        for alias, expr, star in sel.items:
            if alias == col and not star:
                found = True
                for ref in column_refs(expr):
                    out |= resolve_column(sel, ref, depth + 1)
        if not found:
            for alias, expr, star in sel.items:
                if star:
                    parts = [col] if star == "*" else star.split(".") + [col]
                    out |= resolve_column(sel, parts, depth + 1)
    return out


def output_columns(query, depth=0):
    """Danh sach cot dau ra theo vi tri: [(ten|None, {(bang, cot)}, star_sources)]."""
    if not query.branches:
        return []
    width = max(len(s.items) for s in query.branches)
    result = []
    for k in range(width):
        name, refs, star = None, set(), None
        for b, sel in enumerate(query.branches):
            if k >= len(sel.items):
                continue
            alias, expr, item_star = sel.items[k]
            if b == 0:
                name = alias
            if item_star:
                star = star or set()
                star |= star_tables(sel, item_star, depth)
            else:
                for ref in column_refs(expr):
                    refs |= resolve_column(sel, ref, depth)
        result.append((name, refs, star))
    return result


def star_tables(sel, qualifier, depth=0):
    """Bang goc ma SELECT * (hoac t.*) lay het cot sang."""
    if depth > MAX_DEPTH:
        return set()
    if qualifier == "*":
        srcs = sel.sources.values()
    else:
        srcs = [sel.sources[qualifier]] if qualifier in sel.sources else []
    out = set()
    for src in srcs:
        if src[0] == "table":
            out.add(src[1])
        else:
            for inner in src[1].branches:
                for alias, _, star in inner.items:
                    if star:
                        out |= star_tables(inner, star, depth + 1)
    return out


# ---------------------------------------------------------------- quet bang (muc bang)

def scan_tables(toks):
    """Moi bang xuat hien sau FROM/JOIN/USING (MERGE) o moi muc long nhau, bo ten CTE."""
    ctes = set()
    for i, tok in enumerate(toks):
        if is_kw(tok, "with"):
            j = i + 1
            if is_kw(at(toks, j), "recursive"):
                j += 1
            while True:
                name, k = read_name(toks, j)
                if name is None:
                    break
                if is_op(at(toks, k), "("):
                    k = close_paren(toks, k) + 1
                if not (is_kw(at(toks, k), "as") and is_op(at(toks, k + 1), "(")):
                    break
                ctes.add(name)
                k = close_paren(toks, k + 1) + 1
                if not is_op(at(toks, k), ","):
                    break
                j = k + 1
    found = []
    owners = []
    i = 0
    while i < len(toks):
        tok = toks[i]
        if is_op(tok, "("):
            prev = at(toks, i - 1)
            owners.append(prev.low if is_word(prev) else "")
        elif is_op(tok, ")"):
            if owners:
                owners.pop()
        elif is_kw(tok, "from", "join", "using") and not (owners and owners[-1] in FROM_INSIDE_FUNCS):
            if is_kw(tok, "using") and is_op(at(toks, i + 1), "("):
                i += 1
                continue  # JOIN ... USING (cot)
            j = i + 1
            while True:
                name, k = read_name(toks, j)
                if name is None or is_op(at(toks, k), "("):
                    break
                if name not in ctes:
                    found.append(name)
                if is_kw(at(toks, k), "as"):
                    k += 1
                alias_tok = at(toks, k)
                if name_part(alias_tok) is not None and not (alias_tok.kind == "word" and alias_tok.low in NOT_ALIAS):
                    k += 1
                if not is_op(at(toks, k), ","):
                    break
                j = k + 1
        i += 1
    seen, out = set(), []
    for name in found:
        if name not in seen:
            seen.add(name)
            out.append(name)
    return out


# ---------------------------------------------------------------- phan loai cau lenh

def normalize_location(path):
    """Duong dan HDFS de so khop: bo scheme://host, cat tu bien ${...}, bo phan partition k=v o cuoi."""
    if not path:
        return ""
    text = SCHEME_RE.sub("", path.strip())
    cut = text.find("${")
    if cut >= 0:
        text = text[:cut]
    parts = [p for p in text.split("/") if p]
    while parts and "=" in parts[-1]:
        parts.pop()
    return "/" + "/".join(parts) if parts else ""


def _skip_partition(toks, i):
    if is_kw(at(toks, i), "partition") and is_op(at(toks, i + 1), "("):
        return close_paren(toks, i + 1) + 1
    return i


def _top_level_find(toks, start, words):
    i = start
    while i < len(toks):
        if is_op(toks[i], "("):
            i = close_paren(toks, i) + 1
            continue
        if is_word(toks[i]) and toks[i].low in words:
            return i
        i += 1
    return -1


def _column_list(toks, i):
    """(a, b, c) ngay sau ten bang trong INSERT: tra ve ([cot], vi_tri_sau) neu dung la danh sach cot."""
    if not is_op(at(toks, i), "("):
        return None, i
    close = close_paren(toks, i)
    inner = toks[i + 1:close]
    if not inner or (is_word(inner[0]) and inner[0].low in QUERY_STARTS):
        return None, i
    cols, expect = [], True
    for tok in inner:
        if expect:
            part = name_part(tok)
            if part is None:
                return None, i
            cols.append(part)
            expect = False
        elif is_op(tok, ","):
            expect = True
        else:
            return None, i
    return cols, close + 1


def tokens_text(toks):
    """Ghep token lai thanh bieu thuc de doc: nvl ( p . phi , 0 ) -> nvl(p.phi, 0)."""
    out = ""
    for i, tok in enumerate(toks):
        prev = toks[i - 1] if i else None
        glue = (
            not out
            or is_op(tok, ".") or is_op(tok, ")") or is_op(tok, ",")
            or (prev is not None and (is_op(prev, ".") or is_op(prev, "(")))
            or (is_op(tok, "(") and prev is not None and prev.kind in ("word", "bq"))
        )
        out += ("" if glue else " ") + tok.text
    return out[:MAX_EXPR]


def describe_expr(sel, expr, depth=0):
    """Bieu thuc cua cot; neu chi la tham chieu toi cot cua CTE/subquery thi mo tiep: a.tien = SUM(so_tien)."""
    text = tokens_text(expr)
    name, j = read_name(expr, 0)
    if depth >= 5 or name is None or j != len(expr):
        return text
    hits = _source_lookup(sel, name.split("."))
    if len(hits) != 1 or hits[0][0][0] != "query" or not hits[0][0][1].branches:
        return text
    inner_sel, col = hits[0][0][1].branches[0], hits[0][1]
    for alias, inner, star in inner_sel.items:
        if alias == col and not star and inner:
            deeper = describe_expr(inner_sel, inner, depth + 1)
            # Ben trong chi la lay lai nguyen cot cung ten (a.kh_id <- kh_id) thi khong ghi them.
            plain, k = read_name(inner, 0)
            if plain is not None and k == len(inner) and plain.split(".")[-1] == col and deeper == tokens_text(inner):
                return text
            return f"{text} = {deeper}"[:MAX_EXPR]
    return text


def _insert_columns(target, columns, query, exprs=None, positional=None):
    """Quan he cot cho INSERT ... SELECT: theo vi tri (co danh sach cot) hoac theo ten alias.

    exprs: dict nhan them {(bang, cot): bieu thuc nguon} de hien cot duoc tinh the nao.
    positional: list nhan them tung cot theo vi tri (khi khong ghi danh sach cot) de luc dung graph
    ghep lai theo dung thu tu cot trong CREATE TABLE (Hive/Spark INSERT ghep cot theo vi tri).
    """
    edges = []
    first = query.branches[0].items if query.branches else []
    outputs = output_columns(query)
    if positional is not None and not columns:
        for k, (alias, refs, star) in enumerate(outputs):
            expr = describe_expr(query.branches[0], first[k][1]) if k < len(first) and first[k][1] else ""
            positional.append({"alias": alias, "refs": sorted(refs), "star": sorted(star) if star else None, "expr": expr})
    for k, (alias, refs, star) in enumerate(outputs):
        tcol = columns[k] if columns and k < len(columns) else alias
        if exprs is not None and tcol and star is None and k < len(first) and first[k][1]:
            exprs[(target, tcol)] = describe_expr(query.branches[0], first[k][1])
        if star is not None and not (columns and k < len(columns)):
            for table in star:
                edges.append((target, "*", table, "*"))
            continue
        if not tcol:
            continue
        for table, col in refs:
            edges.append((target, tcol, table, col))
        for table in star or ():
            edges.append((target, tcol, table, tcol))
    return edges


def analyze(toks, use_db=None):
    """Phan tich mot cau lenh. Tra ve dict hoac None neu khong thuoc loai can lay.

    dict: kind, targets, sources, columns [(bang_dich, cot_dich, bang_nguon, cot_nguon)],
    locations [(bang, duong_dan)], dir_target (duong dan INSERT OVERWRITE DIRECTORY), temp_views, use.
    """
    if not toks:
        return None
    first = toks[0].low if toks[0].kind == "word" else ""
    result = {
        "kind": "", "targets": [], "sources": [], "columns": [], "locations": [],
        "temp_views": [], "use": None, "partial": False, "output": [], "exprs": {}, "output_exprs": {},
        "positional": [], "schema": [],
    }
    if first == "use":
        name, _ = read_name(toks, 1, allow_keyword=True)
        result["use"] = name
        return result

    body_start = 0
    ctes = {}
    if first == "with":
        k = parse_ctes(toks, 1, len(toks), ctes)
        if is_kw(at(toks, k), "insert"):
            first, body_start = "insert", k
        else:
            first = "select"

    try:
        if first in ("select", "from", "values") or is_op(toks[0], "("):
            result["kind"] = "SELECT"
            result["sources"] = scan_tables(toks)
            query = parse_query(toks, 0, len(toks), {})
            result["output"] = output_columns(query)
            if query.branches:
                result["output_exprs"] = {
                    alias: tokens_text(expr) for alias, expr, star in query.branches[0].items if alias and not star
                }
        elif first == "insert":
            _analyze_insert(toks, body_start, ctes, result)
        elif first == "create":
            if not _analyze_create(toks, result):
                return None
        elif first == "alter":
            if not _analyze_alter(toks, result):
                return None
        elif first == "merge":
            _analyze_merge(toks, result)
        elif first == "update":
            name, j = read_name(toks, 1)
            if name is None:
                return None
            result["kind"] = "UPDATE"
            result["targets"] = [name]
            result["sources"] = [s for s in scan_tables(toks) if s != name]
            _set_columns(toks, j, name, result)
        else:
            return None
    except (IndexError, RecursionError):
        result["partial"] = True
    result["columns"] = _unique(result["columns"])
    result.setdefault("exprs", {})
    if use_db:
        def qualify(n):
            return n if "." in n or n.startswith(("${", TEMP_PREFIX, LOCATION_PREFIX)) else f"{use_db}.{n}"

        result["targets"] = [qualify(n) for n in result["targets"]]
        result["sources"] = [qualify(n) for n in result["sources"]]
        result["columns"] = [(qualify(a), b, qualify(c), d) for a, b, c, d in result["columns"]]
        result["locations"] = [(qualify(a), b) for a, b in result["locations"]]
        result["exprs"] = {(qualify(t), c): e for (t, c), e in result["exprs"].items()}
        for item in result["positional"]:
            item["refs"] = [(qualify(t), c) for t, c in item["refs"]]
            if item["star"]:
                item["star"] = [qualify(t) for t in item["star"]]
        result["output"] = [
            (alias, {(qualify(t), c) for t, c in refs}, None if star is None else {qualify(t) for t in star})
            for alias, refs, star in result["output"]
        ]
    return result


def _analyze_insert(toks, i, ctes, result):
    result["kind"] = "INSERT"
    i += 1
    if is_kw(at(toks, i), "into", "overwrite"):
        i += 1
    if is_kw(at(toks, i), "local"):
        i += 1
    if is_kw(at(toks, i), "directory"):
        # INSERT OVERWRITE DIRECTORY 'duong_dan' SELECT ...: dich la nut location.
        path = normalize_location(literal(at(toks, i + 1)) or "")
        if not path:
            result["partial"] = True
            return
        target = LOCATION_PREFIX + path
        result["targets"] = [target]
        start = _top_level_find(toks, i + 2, {"select", "with", "from"})
        if start >= 0:
            result["sources"] = [s for s in scan_tables(toks[start:]) if s not in ctes]
            result["columns"] = _insert_columns(
                target, None, parse_query(toks, start, len(toks), ctes), result["exprs"]
            )
        return
    if is_kw(at(toks, i), "table"):
        i += 1
    name, i = read_name(toks, i)
    if name is None:
        result["partial"] = True
        return
    result["targets"] = [name]
    i = _skip_partition(toks, i)
    if is_kw(at(toks, i), "if"):
        i += 3  # IF NOT EXISTS
    columns, i = _column_list(toks, i)
    i = _skip_partition(toks, i)
    # Quet ca cau (ke ca phan WITH truoc INSERT); scan_tables tu bo ten CTE.
    result["sources"] = [s for s in scan_tables(toks) if s != name]
    query = parse_query(toks, i, len(toks), ctes)
    result["columns"] = _insert_columns(name, columns, query, result["exprs"], result["positional"])


def _analyze_create(toks, result):
    i, temp = 1, False
    while is_kw(at(toks, i), "or", "replace", "global", "temporary", "temp", "external", "materialized", "transient"):
        temp = temp or toks[i].low in ("temporary", "temp")
        i += 1
    if not is_kw(at(toks, i), "table", "view"):
        return False
    is_view = toks[i].low == "view"
    i += 1
    if is_kw(at(toks, i), "if"):
        i += 3
    name, i = read_name(toks, i)
    if name is None:
        return False
    result["kind"] = "CREATE"
    result["targets"] = [name]
    if temp and is_view:
        result["temp_views"] = [name]
    loc = _top_level_find(toks, i, {"location"})
    if loc >= 0 and literal(at(toks, loc + 1)):
        result["locations"].append((name, normalize_location(literal(toks[loc + 1]))))
    columns, _ = _column_list(toks, i) if is_view else (None, i)
    if not is_view and is_op(at(toks, i), "("):
        result["schema"] = _column_defs(toks, i)
        part = _top_level_find(toks, i, {"partitioned"})
        if part >= 0 and is_kw(at(toks, part + 1), "by") and is_op(at(toks, part + 2), "("):
            result["schema"] += _column_defs(toks, part + 2)
    as_pos = _top_level_find(toks, i, {"as"})
    start = -1
    if as_pos >= 0 and (is_word(at(toks, as_pos + 1)) and toks[as_pos + 1].low in QUERY_STARTS or is_op(at(toks, as_pos + 1), "(")):
        start = as_pos + 1
    else:
        sel = _top_level_find(toks, i, {"select", "with"})
        start = sel
    if start >= 0:
        result["sources"] = [s for s in scan_tables(toks[start:]) if s != name]
        query = parse_query(toks, start, len(toks), {})
        result["columns"] = _insert_columns(name, columns, query, result["exprs"], result["positional"])
    return True


def _column_defs(toks, i):
    """(a int, b decimal(18,2), PRIMARY KEY (a)) -> [a, b]: ten cot theo thu tu khai bao."""
    close = close_paren(toks, i)
    cols = []
    for part in _split_commas(toks[i + 1:close]):
        if not part or (part[0].kind == "word" and part[0].low in ("primary", "constraint", "foreign", "unique", "key", "index", "check")):
            continue
        name = name_part(part[0])
        if name and len(part) > 1:
            cols.append(name)
    return cols


def _analyze_alter(toks, result):
    if not is_kw(at(toks, 1), "table", "view"):
        return False
    name, i = read_name(toks, 2)
    if name is None:
        return False
    result["kind"] = "ALTER"
    rename = _top_level_find(toks, i, {"rename"})
    if rename >= 0 and is_kw(at(toks, rename + 1), "to"):
        new_name, _ = read_name(toks, rename + 2)
        if new_name:
            result["targets"] = [new_name]
            result["sources"] = [name]
            return True
    result["targets"] = [name]
    exchange = _top_level_find(toks, i, {"exchange"})
    if exchange >= 0:
        with_pos = _top_level_find(toks, exchange, {"with"})
        if with_pos >= 0 and is_kw(at(toks, with_pos + 1), "table"):
            other, _ = read_name(toks, with_pos + 2)
            if other:
                result["sources"] = [other]
    pos = i
    while True:
        loc = _top_level_find(toks, pos, {"location"})
        if loc < 0:
            break
        path = literal(at(toks, loc + 1))
        if path:
            result["locations"].append((name, normalize_location(path)))
        pos = loc + 1
    as_pos = _top_level_find(toks, i, {"as"})
    if toks[1].low == "view" and as_pos >= 0:
        result["sources"] = [s for s in scan_tables(toks[as_pos + 1:]) if s != name]
        result["columns"] = _insert_columns(
            name, None, parse_query(toks, as_pos + 1, len(toks), {}), result["exprs"]
        )
    return True


def _analyze_merge(toks, result):
    i = 1
    if is_kw(at(toks, i), "into"):
        i += 1
    name, i = read_name(toks, i)
    if name is None:
        result["partial"] = True
        return
    result["kind"] = "MERGE"
    result["targets"] = [name]
    result["sources"] = [s for s in scan_tables(toks) if s != name]
    # Alias bang dich va bang nguon de doc cac phep gan cot.
    sel = Select()
    j = i
    if is_kw(at(toks, j), "as"):
        j += 1
    alias = name_part(at(toks, j))
    if alias and not (toks[j].kind == "word" and toks[j].low in NOT_ALIAS):
        sel.sources[alias] = ("table", name)
        j += 1
    sel.sources.setdefault(name, ("table", name))
    if is_kw(at(toks, j), "using"):
        parse_table_ref(toks, j + 1, len(toks), {}, sel)
    target_aliases = {k for k, v in sel.sources.items() if v == ("table", name)}
    using = Select()
    using.sources = {k: v for k, v in sel.sources.items() if k not in target_aliases}
    k = j
    while k < len(toks):
        if is_kw(toks[k], "set"):
            k = _assignments(toks, k + 1, name, using, result)
            continue
        if is_kw(toks[k], "insert"):
            cols, after = _column_list(toks, k + 1)
            if cols and is_kw(at(toks, after), "values") and is_op(at(toks, after + 1), "("):
                close = close_paren(toks, after + 1)
                exprs = _split_commas(toks[after + 2:close])
                for col, expr in zip(cols, exprs):
                    result["exprs"][(name, col)] = tokens_text(expr)
                    for ref in column_refs(expr):
                        for table, scol in resolve_column(using, ref):
                            result["columns"].append((name, col, table, scol))
                k = close + 1
                continue
            if is_op(at(toks, k + 1), "*"):
                for src in using.sources.values():
                    if src[0] == "table":
                        result["columns"].append((name, "*", src[1], "*"))
        k += 1


def _split_commas(toks):
    parts, current, i = [], [], 0
    while i < len(toks):
        if is_op(toks[i], "("):
            close = close_paren(toks, i)
            current.extend(toks[i:close + 1])
            i = close + 1
            continue
        if is_op(toks[i], ","):
            parts.append(current)
            current = []
        else:
            current.append(toks[i])
        i += 1
    if current:
        parts.append(current)
    return parts


def _assignments(toks, i, target, sel, result):
    """SET a = expr, b = expr ... (MERGE/UPDATE). Tra ve vi tri sau danh sach."""
    end = i
    while end < len(toks) and not (is_word(toks[end]) and toks[end].low in ("when", "where", "from")):
        if is_op(toks[end], "("):
            end = close_paren(toks, end)
        end += 1
    if is_op(at(toks, i), "*"):
        for src in sel.sources.values():
            if src[0] == "table":
                result["columns"].append((target, "*", src[1], "*"))
        return end
    for part in _split_commas(toks[i:end]):
        eq = next((k for k, t in enumerate(part) if is_op(t, "=")), -1)
        if eq <= 0:
            continue
        col, _ = read_name(part, 0, allow_keyword=True)
        if not col:
            continue
        col = col.split(".")[-1]
        result["exprs"][(target, col)] = tokens_text(part[eq + 1:])
        for ref in column_refs(part[eq + 1:]):
            for table, scol in resolve_column(sel, ref):
                if table != target or scol != col:
                    result["columns"].append((target, col, table, scol))
    return end


def _set_columns(toks, j, name, result):
    """UPDATE bang [alias] SET ... [FROM ...]: quan he cot tu phep gan."""
    sel = Select()
    sel.sources[name] = ("table", name)
    if not is_kw(at(toks, j), "set"):
        alias = name_part(at(toks, j))
        if alias and not (toks[j].kind == "word" and toks[j].low in NOT_ALIAS):
            sel.sources[alias] = ("table", name)
            j += 1
    from_pos = _top_level_find(toks, j, {"from"})
    if from_pos >= 0:
        parse_from(toks, from_pos + 1, len(toks), {}, sel)
    if is_kw(at(toks, j), "set"):
        _assignments(toks, j + 1, name, sel, result)


# ---------------------------------------------------------------- doc file Hop

def _text(elem, tag):
    child = elem.find(tag)
    return (child.text or "").strip() if child is not None and child.text else ""


def _first_text(elem, tag):
    for child in elem.iter(tag):
        if child.text and child.text.strip():
            return child.text.strip()
    return ""


DIR_VARS = (
    "Internal.Entry.Current.Folder", "Internal.Workflow.Filename.Directory",
    "Internal.Pipeline.Filename.Directory", "Internal.Transform.Current.Folder",
    # Pentaho
    "Internal.Entry.Current.Directory", "Internal.Job.Filename.Directory",
    "Internal.Transformation.Filename.Directory",
)
LEADING_VAR_RE = re.compile(r"^\$\{([^}]+)\}[\\/]*")
PROJECT_VARS = ("PROJECT_HOME", "Internal.Project.Home")
PATH_SPLIT_RE = re.compile(r"[\\/]+")


def _segments(path):
    return [p.lower() for p in PATH_SPLIT_RE.split(path or "") if p and p not in (".", "..")]


class Scanner:
    """project_home: gia tri ${PROJECT_HOME}; de trong thi tu do trong cac thu muc cha cua thu muc quet.

    progress(**trang_thai): goi lien tuc trong luc quet de giao dien hien tien do.
    """

    def __init__(self, root, project_home=None, progress=None, fmt="hop"):
        self.root = os.path.abspath(root)
        self.fmt = FORMATS[fmt]
        self.project_home = os.path.abspath(project_home) if project_home else None
        # Thu muc goc tu do cho tung bien dau duong dan (${PROJECT_HOME}, ${ETL_HOME}...).
        self.detected_homes = {}
        self.progress = progress or (lambda **_: None)
        self.warnings = []
        self.statements = []
        self.files_seen = set()
        self.by_name = {}
        self.file_count = 0
        self.ignored = 0
        self._xml_cache = {}

    # ----- tien ich

    def rel(self, path):
        try:
            return os.path.relpath(path, self.root).replace("\\", "/")
        except ValueError:
            return path

    def warn(self, text):
        if text not in self.warnings:
            self.warnings.append(text)

    def index_files(self):
        count = 0
        for folder, _, files in os.walk(self.root):
            for f in files:
                if f.lower().endswith((self.fmt["wf"], self.fmt["pl"], SQL_EXT)):
                    self.by_name.setdefault(f.lower(), []).append(os.path.join(folder, f))
                    count += 1
            self.progress(phase="index", files_found=count, current=self.rel(folder))

    def resolve_file(self, raw, current_file):
        """Duong dan trong action Hop -> file that.

        ${Internal...Folder} la thu muc file hien tai. ${PROJECT_HOME} lay theo o nhap, khong co thi
        thu tung thu muc cha cua thu muc quet (thu muc nao ghep voi phan con lai ra file co that).
        Van khong thay thi khop theo duoi duong dan voi cac file trong thu muc quet.
        """
        if not raw:
            return None
        here = os.path.dirname(current_file)
        text = raw.strip()
        for var in DIR_VARS:
            text = text.replace("${" + var + "}", here)
        rest, var = None, None
        lead = LEADING_VAR_RE.match(text)
        if lead:
            var, rest = lead.group(1), text[lead.end():]
        candidates = []
        if rest is not None and "${" not in rest:
            candidates = [os.path.join(home, rest) for home in self.home_candidates(rest, var)]
        elif rest is None and "${" not in text:
            candidates = [text if os.path.isabs(text) else os.path.join(here, text),
                          os.path.join(self.root, text.lstrip("/\\"))]
        for path in candidates:
            if os.path.isfile(path):
                return os.path.normpath(path)
        hit = self.match_suffix(VAR_RE.sub("", rest if rest is not None else text), here)
        if hit:
            return hit
        self.warn(f"{self.rel(current_file)}: không tìm thấy file {raw} (có thể nằm ngoài thư mục quét)")
        return None

    def home_candidates(self, rest, var="PROJECT_HOME"):
        if self.project_home:
            return [self.project_home]
        if var in self.detected_homes:
            return [self.detected_homes[var]]
        folder = self.root
        while True:
            if os.path.isfile(os.path.join(folder, rest)):
                self.detected_homes[var] = folder
                return [folder]
            parent = os.path.dirname(folder)
            if parent == folder:
                return []
            folder = parent

    def match_suffix(self, path, here):
        """File trong thu muc quet co phan duoi duong dan giong nhieu nhat (ten file phai trung)."""
        segs = _segments(path)
        if not segs:
            return None
        matches = self.by_name.get(segs[-1], [])
        if not matches:
            return None
        base = os.path.basename(self.root).lower()

        def score(candidate):
            have = [base] + _segments(os.path.relpath(candidate, self.root))
            n = 0
            while n < min(len(have), len(segs)) and have[-1 - n] == segs[-1 - n]:
                n += 1
            return n

        best = max(score(m) for m in matches)
        tied = [m for m in matches if score(m) == best]
        return min(tied, key=lambda m: len(os.path.relpath(m, here)))

    def load_xml(self, path):
        if path in self._xml_cache:
            return self._xml_cache[path]
        self.progress(files=len(self._xml_cache) + 1, statements=len(self.statements), current=self.rel(path))
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, OSError) as exc:
            self.warn(f"{self.rel(path)}: không đọc được XML ({exc})")
            root = None
        self._xml_cache[path] = root
        return root

    def add_sql(self, sql, file, chain, step, step_type, extra_sources=None):
        """Tach script thanh cau, phan tich va luu cac cau can lay."""
        use_db = None
        for toks in split_statements(sql or ""):
            info = analyze(toks, use_db)
            if info is None:
                self.ignored += 1
                continue
            if info["use"]:
                use_db = info["use"]
                continue
            text = _raw_text(sql, toks)
            self.statements.append(
                {
                    "file": self.rel(file), "chain": list(chain), "step": step, "step_type": step_type,
                    "kind": info["kind"], "sql": text, "info": info,
                }
            )

    # ----- workflow

    def scan(self):
        self.index_files()
        workflows = sorted(
            p for paths in self.by_name.values() for p in paths if p.lower().endswith(self.fmt["wf"])
        )
        for k, path in enumerate(workflows):
            self.progress(phase="scan", roots_done=k, roots_total=len(workflows))
            self.walk_workflow(path, [], set())
        self.progress(phase="build", roots_done=len(workflows), roots_total=len(workflows))
        self.file_count = len(self.files_seen)
        return self

    def walk_workflow(self, path, chain, stack):
        if path in stack or len(stack) > MAX_DEPTH:
            return
        root = self.load_xml(path)
        if root is None:
            return
        if path in self.files_seen:
            return
        self.files_seen.add(path)
        name = os.path.basename(path)
        actions = {}
        start = None
        for action in root.iter(self.fmt["action"]):
            act_name = _text(action, "name")
            if not act_name:
                continue
            actions[act_name] = action
            act_type = _text(action, "type").upper()
            if start is None and act_type in ("SPECIAL", "START") and _text(action, "start").upper() != "N" \
                    and _text(action, "dummy").upper() != "Y":
                start = act_name
        if start is None:
            start = next((n for n in actions if n.lower() == "start"), None)
        if start is None:
            self.warn(f"{self.rel(path)}: không có action Start, bỏ qua.")
            return
        nexts = {}
        for hop in root.iter("hop"):
            if _text(hop, "enabled").upper() == "N":
                continue
            src, dst = _text(hop, "from"), _text(hop, "to")
            if src and dst:
                nexts.setdefault(src, []).append(dst)
        seen, queue = {start}, deque([start])
        while queue:
            current = queue.popleft()
            action = actions.get(current)
            if action is not None and current != start:
                self.visit_action(path, action, chain + [f"{name} › {current}"], stack | {path})
            for nxt in nexts.get(current, []):
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)

    def visit_action(self, path, action, chain, stack):
        act_name = _text(action, "name")
        act_type = _text(action, "type")
        upper = act_type.upper()
        sql = _text(action, "sql")
        if _text(action, "sqlfromfile").upper() in ("Y", "T", "TRUE"):
            target = self.resolve_file(_text(action, "sqlfilename"), path)
            if target:
                sql = _read_text(target, self)
        if sql:
            self.add_sql(sql, path, chain, act_name, act_type)
        filename = _text(action, "filename")
        if not filename and upper in self.fmt["pl_types"] + self.fmt["wf_types"]:
            # Pentaho luu trong repository: chi co ten + thu muc, tim theo duoi duong dan.
            name = _text(action, "transname") or _text(action, "jobname")
            if name:
                ext = self.fmt["pl"] if upper in self.fmt["pl_types"] else self.fmt["wf"]
                filename = f"{_text(action, 'directory').rstrip('/')}/{name}{'' if name.lower().endswith(ext) else ext}"
        if filename:
            target = self.resolve_file(filename, path)
            if target is None:
                return
            if target.lower().endswith(self.fmt["pl"]) or upper in self.fmt["pl_types"]:
                self.walk_pipeline(target, chain, stack)
            elif target.lower().endswith(self.fmt["wf"]) or upper in self.fmt["wf_types"]:
                self.walk_workflow(target, chain, stack)

    # ----- pipeline

    def walk_pipeline(self, path, chain, stack):
        if path in self.files_seen:
            return
        root = self.load_xml(path)
        if root is None:
            return
        self.files_seen.add(path)
        name = os.path.basename(path)
        transforms = {}
        for t in root.iter(self.fmt["step"]):
            t_name = _text(t, "name")
            if t_name:
                transforms[t_name] = t
        prevs = {}
        order = root.find("order")
        for hop in (order.iter("hop") if order is not None else []):
            if _text(hop, "enabled").upper() == "N":
                continue
            src, dst = _text(hop, "from"), _text(hop, "to")
            if src and dst:
                prevs.setdefault(dst, []).append(src)

        reads = {}      # transform -> (bang nguon, query)
        for t_name, t in transforms.items():
            t_type = _text(t, "type")
            step_chain = chain + [f"{name} › {t_name}"]
            sql = _text(t, "sql")
            if sql:
                before = len(self.statements)
                self.add_sql(sql, path, step_chain, t_name, t_type)
                tables, queries = [], []
                for st in self.statements[before:]:
                    if st["kind"] == "SELECT":
                        tables += st["info"]["sources"]
                        queries.append(st)
                reads[t_name] = (tables, queries)
            elif t_type in ("DBLookup", "DatabaseLookup"):
                table = _qualified(_first_text(t, "schema"), _first_text(t, "table"))
                if table:
                    reads[t_name] = ([table], [])
            elif t_type in EXECUTOR_TYPES:
                target = self.resolve_file(_first_text(t, "filename"), path)
                if target and target.lower().endswith(self.fmt["pl"]):
                    self.walk_pipeline(target, step_chain, stack)
                elif target and target.lower().endswith(self.fmt["wf"]):
                    self.walk_workflow(target, step_chain, stack | {path})

        for t_name, t in transforms.items():
            t_type = _text(t, "type")
            if not _is_writer(t_type):
                continue
            table = _qualified(_first_text(t, "schema"), _first_text(t, "table"))
            if not table:
                continue
            upstream = _upstream(t_name, prevs)
            tables, queries = [], []
            for u in upstream:
                if u in reads:
                    tables += reads[u][0]
                    queries += reads[u][1]
            mapping = _field_mapping(t)
            columns, exprs = [], {}
            for st in queries:
                for alias, refs, star in st["info"]["output"]:
                    if star is not None:
                        for src in star:
                            columns.append((table, "*", src, "*"))
                        continue
                    if not alias:
                        continue
                    tcol = mapping.get(alias, alias) if mapping is not None else alias
                    if mapping and alias not in mapping:
                        continue
                    for src, scol in refs:
                        columns.append((table, tcol, src, scol))
                    expr = st["info"].get("output_exprs", {}).get(alias)
                    if expr:
                        exprs[(table, tcol)] = f"{expr}  -- {st['step']}"
            steps = " → ".join(upstream[::-1] + [t_name]) if upstream else t_name
            info = {
                "kind": "INSERT", "targets": [table], "sources": _unique([s for s in tables if s != table]),
                "columns": columns, "locations": [], "temp_views": [], "use": None,
                "partial": False, "output": [], "exprs": exprs, "output_exprs": {},
            }
            self.statements.append(
                {
                    "file": self.rel(path), "chain": chain + [f"{name} › {t_name}"], "step": t_name,
                    "step_type": t_type, "kind": "INSERT",
                    "sql": f"-- Transform {t_type} ghi vào {table}\n-- Luồng dữ liệu: {steps}",
                    "info": info,
                }
            )


def _qualified(schema, table):
    table = (table or "").strip().strip("`").lower()
    if not table:
        return ""
    schema = (schema or "").strip().strip("`").lower()
    return f"{schema}.{table}" if schema and "." not in table else table


def _is_writer(t_type):
    low = t_type.lower()
    return low in ("tableoutput", "insertupdate", "update", "synchronizeaftermerge") or low.endswith("bulkloader")


def _upstream(name, prevs):
    seen, queue, order = {name}, deque([name]), []
    while queue:
        cur = queue.popleft()
        for p in prevs.get(cur, []):
            if p not in seen:
                seen.add(p)
                order.append(p)
                queue.append(p)
    return order


def _field_mapping(t):
    """Anh xa ten field trong luong -> cot bang dich. None: khong khai bao (dung nguyen ten)."""
    mapping = {}
    fields = t.find("fields")
    if fields is not None and _text(t, "specify_fields").upper() == "Y":
        for f in fields.iter("field"):
            col, stream = _text(f, "column_name").lower(), _text(f, "stream_name").lower()
            if col:
                mapping[stream or col] = col
    lookup = t.find("lookup")
    if lookup is not None:
        for v in lookup.iter("value"):
            col, stream = _text(v, "name").lower(), _text(v, "rename").lower()
            if col:
                mapping[stream or col] = col
    return mapping or None


def _unique(items):
    seen, out = set(), []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _read_text(path, scanner):
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        scanner.warn(f"Không đọc được {scanner.rel(path)}: {exc}")
        return ""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1258", errors="replace")


def _raw_text(sql, toks):
    """Doan SQL goc cua cau (giu nguyen xuong dong, chu thich ben trong)."""
    return sql[toks[0].start:toks[-1].end].strip()


# ---------------------------------------------------------------- dung graph

WRITE_KINDS = ("INSERT", "MERGE", "UPDATE")


def scan_folder(folder, project_home=None, progress=None, label=None, fmt="hop"):
    """Quet thu muc, tra ve dict san sang luu DB: statements, edges, columns, warnings.

    Chi lay workflow goc nam trong thu muc nay (ke ca thu muc con); file chung goi toi o ngoai
    van doc neu tim duoc. label: ten hien thi thay cho duong dan (thu muc keo tha).
    """
    if not folder or not os.path.isdir(folder):
        raise ValueError(f"Không tìm thấy thư mục: {folder}")
    if project_home and not os.path.isdir(project_home):
        raise ValueError(f"Không tìm thấy thư mục PROJECT_HOME: {project_home}")
    scanner = Scanner(folder, project_home, progress, fmt)
    scanner.scan()
    ext = scanner.fmt["wf"]
    if not any(p.lower().endswith(ext) for paths in scanner.by_name.values() for p in paths):
        raise ValueError(f"Thư mục {label or folder} không có file {ext} nào ({scanner.fmt['label']}).")
    result = build(scanner, scanner.fmt["id_base"])
    if label:
        result["folder"] = label
    if scanner.project_home:
        result["project_home"] = scanner.project_home
    else:
        result["project_home"] = " · ".join(f"${{{v}}} = {h}" for v, h in scanner.detected_homes.items())
    return result


def build(scanner, id_base=0):
    # Temp view chi co nghia trong file tao ra no: doi ten thanh nut temp:<file>:<ten>.
    temps = {}
    for st in scanner.statements:
        for name in st["info"]["temp_views"]:
            temps.setdefault(st["file"], set()).add(name)

    def node(name, file):
        if name in temps.get(file, ()):
            return f"{TEMP_PREFIX}{file}:{name}"
        return name

    # Cot cua bang theo thu tu, lay tu CREATE TABLE trong job (cau sau cung thang).
    schemas = {}
    for st in scanner.statements:
        if st["info"].get("schema"):
            for t in st["info"]["targets"]:
                schemas[node(t, st["file"])] = st["info"]["schema"]

    statements, edges, columns = [], set(), set()
    location_tables = {}
    for sid, st in enumerate(scanner.statements, id_base + 1):
        info, file = st["info"], st["file"]
        if info.get("positional") and len(info["targets"]) == 1 and node(info["targets"][0], file) in schemas:
            info = _apply_positional(info, schemas[node(info["targets"][0], file)])
        targets = [node(n, file) for n in info["targets"]]
        sources = [node(n, file) for n in info["sources"]]
        statements.append(
            {
                "id": sid, "file": file, "chain": " / ".join(st["chain"]), "step": st["step"],
                "step_type": st["step_type"], "kind": st["kind"], "sql": st["sql"],
                "targets": _unique(targets), "sources": _unique(sources), "partial": bool(info["partial"]),
            }
        )
        for t in targets:
            for s in sources:
                if s != t:
                    edges.add((s, t, sid))
        exprs = info.get("exprs", {})
        for t, tc, s, sc in info["columns"]:
            expr = exprs.get((t, tc), "")
            t, s = node(t, file), node(s, file)
            if (t, tc) != (s, sc):
                columns.add((t, tc, s, sc, sid, expr))
        for table, path in info["locations"]:
            if path:
                location_tables.setdefault(path, []).append((node(table, file), sid))

    # Bang duoc ghi bang INSERT/MERGE/UPDATE ma lai nam o location L thi la nguon cua L.
    writers = {t for st in statements if st["kind"] in WRITE_KINDS for t in st["targets"]}
    writers |= {t for st in statements if st["kind"] == "CREATE" and st["sources"] for t in st["targets"]}
    for path, items in location_tables.items():
        loc = LOCATION_PREFIX + path
        for table, sid in items:
            if table in writers:
                edges.add((table, loc, sid))
                columns.add((loc, "*", table, "*", sid, ""))
            else:
                edges.add((loc, table, sid))
                columns.add((table, "*", loc, "*", sid, ""))
                statements[sid - id_base - 1]["sources"] = _unique(statements[sid - id_base - 1]["sources"] + [loc])

    return {
        "folder": scanner.root,
        "file_count": scanner.file_count,
        "ignored": scanner.ignored,
        "warnings": scanner.warnings,
        "statements": statements,
        "edges": sorted(edges),
        "columns": sorted(columns),
    }


def _apply_positional(info, schema):
    """INSERT khong ghi cot: cot thu k cua SELECT do vao cot thu k cua bang dich (theo CREATE TABLE)."""
    target = info["targets"][0]
    info = dict(info)
    columns = [c for c in info["columns"] if c[0] != target]
    exprs = {k: v for k, v in info["exprs"].items() if k[0] != target}
    for k, item in enumerate(info["positional"]):
        tcol = schema[k] if k < len(schema) else item["alias"]
        if item["star"] is not None:
            for table in item["star"]:
                columns.append((target, "*", table, "*"))
            continue
        if not tcol:
            continue
        for table, col in item["refs"]:
            columns.append((target, tcol, table, col))
        if item["expr"]:
            exprs[(target, tcol)] = item["expr"]
    info["columns"] = _unique(columns)
    info["exprs"] = exprs
    return info


def node_kind(name):
    if name.startswith(TEMP_PREFIX):
        return "temp"
    if name.startswith(LOCATION_PREFIX):
        return "location"
    return "table"


def bare(name):
    return name.rsplit(".", 1)[-1]


class Graph:
    """Graph da quet, dung de tra cuu phu thuoc va truy nguyen nhan lech."""

    def __init__(self, statements, edges, columns):
        self.statements = {s["id"]: s for s in statements}
        self.up, self.down = {}, {}
        for s, t, sid in edges:
            self.up.setdefault(t, {}).setdefault(s, []).append(sid)
            self.down.setdefault(s, {}).setdefault(t, []).append(sid)
        self.col_up = {}
        for t, tc, s, sc, sid, *rest in columns:
            self.col_up.setdefault((t, tc), []).append((s, sc, sid, rest[0] if rest else ""))
        self.nodes = set(self.up) | set(self.down)
        for st in statements:
            self.nodes.update(st["targets"])
            self.nodes.update(st["sources"])
        self.by_bare = {}
        for n in self.nodes:
            if node_kind(n) == "table":
                self.by_bare.setdefault(bare(n), []).append(n)

    def match(self, table):
        """Ten bang trong danh sach doi soat -> nut trong graph (khop ca ten, roi khop bo schema)."""
        name = (table or "").strip().strip("`").lower().replace("`", "")
        if not name:
            return None
        if name in self.nodes:
            return name
        hits = self.by_bare.get(bare(name), [])
        if len(hits) == 1 and ("." not in hits[0] or "." not in name):
            return hits[0]
        return None

    def nearest_listed(self, start, listed, direction="up"):
        """Bang trong danh sach gan nhat theo moi nhanh (di xuyen qua bang/nut khong co trong danh sach).

        Tra ve {bang: [duong di tu bang do toi start]}.
        """
        links = self.up if direction == "up" else self.down
        found, seen = {}, {start}
        queue = deque([(start, [start])])
        while queue:
            current, path = queue.popleft()
            for nxt in links.get(current, {}):
                if nxt in seen:
                    continue
                seen.add(nxt)
                route = [nxt] + path if direction == "up" else path + [nxt]
                if nxt in listed:
                    found.setdefault(nxt, route)
                elif len(route) <= MAX_DEPTH:
                    queue.append((nxt, route))
        return found

    def trace_column(self, table, column, listed):
        """Cot cua bang -> cac (bang trong danh sach, cot) ma no duoc tinh tu, kem duong di.

        Tra ve (ket_qua, co_quan_he_cot). Ket qua: [{table, column, path, statements}].
        """
        column = column.lower()
        results, seen = {}, {(table, column)}
        queue = deque([(table, column, [], [])])
        any_edge = False
        while queue:
            node, col, path, sids = queue.popleft()
            ups = [(s, sc, sid) for s, sc, sid, _ in self.col_up.get((node, col), [])]
            ups += [(s, col if sc == "*" else sc, sid) for s, sc, sid, _ in self.col_up.get((node, "*"), [])]
            if ups and node == table:
                any_edge = True
            for src, scol, sid in ups:
                key = (src, scol)
                if key in seen:
                    continue
                seen.add(key)
                route = [f"{src}.{scol}"] + path
                if src in listed and src != table:
                    results.setdefault(key, {"table": src, "column": scol, "path": route, "statements": [sid] + sids})
                elif len(route) <= MAX_DEPTH:
                    queue.append((src, scol, route, [sid] + sids))
        return list(results.values()), any_edge

    def column_tree(self, table, column, listed=(), max_depth=8, max_nodes=160):
        """Cay truy nguon cua mot cot: cot duoc tinh bang bieu thuc nao, o cau lenh/job nao, tu cot nao.

        Moi nut: {table, kind, column, listed, from: [{stmt, expr, sources: [nut]}], cut}. Di nguoc ca qua
        bang khong co trong danh sach doi soat; dung khi lap vong, qua sau hoac qua nhieu nut.
        """
        budget = [max_nodes]

        def build(node, col, depth, path):
            item = {"table": node, "kind": node_kind(node), "column": col, "listed": node in listed,
                    "from": [], "cut": ""}
            budget[0] -= 1
            ups = [(s, sc, sid, e) for s, sc, sid, e in self.col_up.get((node, col), [])]
            if col != "*":
                ups += [(s, col if sc == "*" else sc, sid, e) for s, sc, sid, e in self.col_up.get((node, "*"), [])]
            if not ups:
                return item
            if (node, col) in path:
                item["cut"] = "vòng lặp"
                return item
            if depth >= max_depth or budget[0] <= 0:
                item["cut"] = "còn nữa"
                return item
            by_stmt = {}
            for s, sc, sid, expr in ups:
                entry = by_stmt.setdefault(sid, {"expr": expr, "sources": []})
                if (s, sc) not in [(x[0], x[1]) for x in entry["sources"]]:
                    entry["sources"].append((s, sc))
            for sid, entry in sorted(by_stmt.items()):
                st = self.statements.get(sid, {})
                item["from"].append({
                    "stmt": {k: st.get(k, "") for k in ("id", "kind", "file", "step", "step_type", "chain")},
                    "expr": entry["expr"],
                    "sources": [build(s, sc, depth + 1, path | {(node, col)}) for s, sc in entry["sources"]],
                })
            return item

        return build(table, column.lower(), 0, frozenset())

    def statements_of(self, name):
        return [s for s in self.statements.values() if name in s["targets"] or name in s["sources"]]

    def writers_of(self, name):
        return [s for s in self.statements.values() if name in s["targets"]]


def expression_columns(expr):
    """Cot (ten cuoi) xuat hien trong mot bieu thuc cot tien cua danh sach doi soat: 'a.fee + tax' -> [fee, tax]."""
    return _unique(parts[-1] for parts in column_refs(tokenize(expr or "")))
