"use strict";

const POLL_MS = 1000;
const SAVE_DELAY_MS = 300;
const FIELDS = [
  "old_table",
  "new_table",
  "old_partition",
  "new_partition",
  "old_sum_columns",
  "new_sum_columns",
  "old_sql",
  "new_sql",
  "threshold",
];
const SIDES = { old: "Datalake", new: "Lakehouse" };
const CONN_KEYS = ["host", "port", "username", "password"];
const CONN_FIELDS = [
  ...Object.keys(SIDES).flatMap((side) => CONN_KEYS.map((k) => `${side}_${k}`)),
  "parallel",
];
const SETTING_FIELDS = [...CONN_FIELDS, "threshold"];
const $ = (id) => document.getElementById(id);

let rows = [];
let prepared = [];
let job = null;
let expanded = new Set();
// Cac dong duoc tick: bam Chay doi soat chi chay cac dong nay, khong tick dong nao thi chay tat ca.
let selected = new Set();
let editingIndex = null;
let previewTimer = null;
let previewSeq = 0;
let saveTimer = null;
let savedPath = "";
let filter = "all";
let runStarted = 0;
let clockTimer = null;
// Luot chay rieng vai bang: runSet la cac dong dang chay, kept la ket qua cu cua cac dong con lai.
let runSet = null;
let kept = [];
let pendingRun = null;

const esc = (value) =>
  String(value).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );

function groupDigits(text) {
  if (text === null || text === undefined) return null;
  const [whole, frac] = text.split(".");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return frac === undefined ? grouped : `${grouped}.${frac}`;
}

const numCell = (text) => {
  const shown = groupDigits(text);
  return shown === null ? '<span class="dash">NULL</span>' : esc(shown);
};

const signedCell = (text) => (Number(text) > 0 ? "+" : "") + numCell(text);

async function api(path, options) {
  const res = await fetch(path, options);
  let body = {};
  try {
    body = await res.json();
  } catch (err) {
    throw new Error(`Máy chủ trả về lỗi ${res.status}.`);
  }
  if (!res.ok) {
    const err = new Error(body.error || `Máy chủ trả về lỗi ${res.status}.`);
    err.body = body;
    throw err;
  }
  return body;
}

const postJson = (path, data) =>
  api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });

function setSaved(text, bad) {
  $("saved").textContent = text;
  $("saved").classList.toggle("bad", Boolean(bad));
}

// Luu lan luot tung lan: dong moi chi co id sau khi server tra ve, luu chong nhau se them trung bang.
let saveChain = Promise.resolve();
function save() {
  clearTimeout(saveTimer);
  saveChain = saveChain.then(saveNow);
  return saveChain;
}

async function saveNow() {
  const settings = {};
  SETTING_FIELDS.forEach((f) => (settings[f] = $(f).value));
  // Giu tham chieu dong da gui de gan id dung dong, ke ca khi danh sach doi trong luc cho.
  const posted = rows.slice();
  const applyIds = (ids) =>
    (ids || []).forEach((id, k) => (posted[k].id = id));
  try {
    const body = await postJson("/api/state", { rows: posted, settings });
    applyIds(body.ids);
    setSaved(`Tự động lưu tại ${savedPath}`);
  } catch (err) {
    if (err.body) applyIds(err.body.ids);
    setSaved(err.message, true);
  }
  render();
}

function saveSoon() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(save, SAVE_DELAY_MS);
}

async function restore() {
  let state;
  try {
    state = await api("/api/state");
  } catch (err) {
    setSaved(err.message, true);
    return;
  }
  savedPath = state.path;
  rows = state.rows;
  const settings = { ...state.settings };
  // Cau hinh cu chi co mot Thrift: dung chung cho ca hai ben.
  Object.keys(SIDES).forEach((side) =>
    CONN_KEYS.forEach((k) => {
      if (!settings[`${side}_${k}`] && settings[k])
        settings[`${side}_${k}`] = settings[k];
    }),
  );
  SETTING_FIELDS.forEach((f) => {
    if (settings[f]) $(f).value = settings[f];
  });
  if (state.error) setSaved(state.error, true);
  else setSaved(`Tự động lưu tại ${savedPath}`);
}

// Ngay doi soat la ngay N cua bien: mac dinh hom nay, ${YYYYMMDD:DD-1} ra hom qua.
function today() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

const isRunning = () => job !== null && job.state === "running";

function connection(side) {
  return {
    host: $(`${side}_host`).value,
    port: $(`${side}_port`).value,
    username: $(`${side}_username`).value,
    password: $(`${side}_password`).value,
  };
}

function updateScope() {
  Object.entries(SIDES).forEach(([side, name]) => {
    const host = $(`${side}_host`).value.trim();
    const user = $(`${side}_username`).value.trim() || "chưa có user";
    $(`scope-${side}`).textContent = host
      ? `${name}: ${host}:${$(`${side}_port`).value} · ${user}`
      : `${name}: chưa cấu hình`;
  });
}

function clearResults() {
  job = null;
  expanded = new Set();
  $("clock").textContent = "";
}

// Sua/them/xoa mot dong chi bo ket qua cua dong do, de con chay lai rieng ma khong mat cac bang khac.
function rowEdited(change) {
  if (job) change(job.rows);
  expanded = new Set();
  save();
  render();
  schedulePreview();
}

function rowsChanged() {
  clearResults();
  selected = new Set();
  save();
  render();
  schedulePreview();
}

// N = ngay doi soat, gio chay theo dong ho that; luc chay moi bang lay N tai thoi diem bat dau bang do.
let baseOffset = null;
function setBase(text) {
  const m = /^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2}):(\d{2})$/.exec(
    text || "",
  );
  baseOffset = m
    ? new Date(m[3], m[2] - 1, m[1], m[4], m[5], m[6]).getTime() - Date.now()
    : null;
  tickBase();
}
function tickBase() {
  if (baseOffset === null) {
    $("base-note").textContent = "";
    return;
  }
  const d = new Date(Date.now() + baseOffset);
  const p = (n) => String(n).padStart(2, "0");
  const text = `${p(d.getDate())}/${p(d.getMonth() + 1)}/${d.getFullYear()} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  $("base-note").textContent = `Biến tính với N = ${text}`;
}
setInterval(tickBase, 1000);

function schedulePreview() {
  clearTimeout(previewTimer);
  previewTimer = setTimeout(refreshPreview, 200);
}

async function refreshPreview() {
  const seq = ++previewSeq;
  try {
    const body = await postJson("/api/preview", {
      rows,
      date: $("date").value,
    });
    if (seq !== previewSeq) return;
    prepared = body.rows;
    setBase(body.base);
  } catch (err) {
    setFinding(err.message, "bad");
    return;
  }
  render();
}

function setFinding(text, kind) {
  $("finding").textContent = text;
  $("finding").dataset.kind = kind || "";
}

const svgIcon = (size, body) =>
  `<svg viewBox="0 0 16 16" width="${size}" height="${size}" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">${body}</svg>`;
const ICONS = {
  check: svgIcon(13, '<path d="m3 8.5 3.2 3.2L13 4.8"/>'),
  cross: svgIcon(13, '<path d="m4 4 8 8M12 4l-8 8"/>'),
  alert: svgIcon(13, '<path d="M8 3v6M8 12.5v.1"/>'),
  edit: svgIcon(
    16,
    '<path d="M10.5 3.2 12.8 5.5 5.6 12.7l-3 .7.7-3zM9.2 4.5l2.3 2.3"/>',
  ),
  trash: svgIcon(
    16,
    '<path d="M2.8 4.5h10.4M6.2 4.5V3h3.6v1.5M4.2 4.5l.6 8.5h6.4l.6-8.5M6.8 7v3.5M9.2 7v3.5"/>',
  ),
  play: svgIcon(16, '<path d="M5 3.3v9.4L12.4 8z"/>'),
  history: svgIcon(
    16,
    '<path d="M2.8 8a5.2 5.2 0 1 0 1.5-3.7M2.8 2.8v2.3h2.3M8 5.2V8l2 1.4"/>',
  ),
};

const highlightSql = (sql) =>
  esc(sql)
    .replace(/\b(SELECT|FROM|WHERE|AND|AS)\b/g, '<b class="kw">$1</b>')
    .replace(/\b(COUNT|SUM)\b/g, '<b class="fn">$1</b>');

function statusOf(i) {
  if (prepared[i] && prepared[i].error) return "loi";
  const result = job ? job.rows[i] : null;
  if (!result) return "cho";
  if (result.state === "pending" || result.state === "running") return "chay";
  if (result.state === "cancelled") return "dung";
  if (result.state === "error") return "loi";
  return result.verdict;
}

function tally() {
  const counts = { ok: 0, lech: 0, trong: 0, loi: 0, done: 0 };
  rows.forEach((_, i) => {
    const status = statusOf(i);
    if (status in counts) {
      counts[status] += 1;
      counts.done += 1;
    }
  });
  return counts;
}

// Nguong rieng cua bang neu co, khong thi nguong chung.
const ownThreshold = (i) =>
  rows[i] ? String(rows[i].threshold || "").trim().replace(/%$/, "").replace(",", ".") : "";
const limitOf = (i) => ownThreshold(i) || $("threshold").value;

function tableName(i) {
  return rows[i] ? rows[i].new_table || rows[i].old_table : "";
}

function renderFinding(counts) {
  const limit = `${$("threshold").value}%`;
  if (!job) {
    if (counts.loi)
      setFinding(
        `${counts.loi} bảng sai cấu hình, mở từng dòng để xem chi tiết.`,
        "bad",
      );
    else
      setFinding(
        rows.length
          ? `${rows.length} bảng sẵn sàng đối soát, ngưỡng chung ${limit}${rows.some((_, i) => ownThreshold(i)) ? ", có bảng dùng ngưỡng riêng" : ""}.`
          : "Chưa có bảng nào trong danh sách.",
      );
    return;
  }
  if (isRunning()) {
    if (runSet) {
      const left = [...runSet].filter((i) => statusOf(i) === "chay").length;
      setFinding(
        `Đang chạy riêng ${runSet.size} bảng, còn ${left} bảng chưa xong.`,
      );
    } else
      setFinding(`Đang chạy, đã xong ${counts.done} / ${rows.length} bảng.`);
    return;
  }
  if (job.state === "cancelled") {
    setFinding(`Đã dừng, có kết quả ${counts.done} / ${rows.length} bảng.`);
    return;
  }
  if (counts.lech || counts.loi) {
    const names = rows
      .map((_, i) => (statusOf(i) === "lech" ? tableName(i) : null))
      .filter(Boolean);
    const shown =
      names.slice(0, 3).join(", ") +
      (names.length > 3 ? ` và ${names.length - 3} bảng khác` : "");
    const parts = [];
    if (counts.lech)
      parts.push(`${counts.lech} bảng lệch vượt ngưỡng: ${shown}`);
    if (counts.loi) parts.push(`${counts.loi} bảng lỗi query`);
    setFinding(`${parts.join(". ")}.`, "bad");
    return;
  }
  const note = counts.trong
    ? ` Có ${counts.trong} bảng 0 dòng ở cả hai bên, nên kiểm tra lại partition.`
    : "";
  setFinding(
    `Tất cả ${counts.ok + counts.trong} bảng nằm trong ngưỡng.${note}`,
    counts.trong ? "" : "good",
  );
}

function renderBoard(counts) {
  $("k-all").textContent = rows.length;
  $("k-lech").textContent = counts.lech;
  $("k-lech").classList.toggle("bad", counts.lech > 0);
  $("k-ok").textContent = counts.ok;
  $("k-trong").textContent = counts.trong;
  $("k-loi").textContent = counts.loi;
  $("k-tong").textContent = `${counts.done} / ${rows.length}`;
  document.querySelectorAll(".tab").forEach((tab) => {
    const active = tab.dataset.filter === filter;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-pressed", active);
    const part =
      tab.dataset.filter === "all" ? counts.done : counts[tab.dataset.filter];
    const share = rows.length ? (part / rows.length) * 100 : 0;
    tab
      .querySelector(".arc-value")
      .setAttribute("stroke-dasharray", `${share} 100`);
    tab
      .querySelector(".arc-value")
      .setAttribute("stroke-opacity", share ? 1 : 0);
  });
}

function renderSideProgress(side) {
  const progress = job && job.progress ? job.progress[side] : null;
  $(`total-${side}`).textContent = progress
    ? `${progress.done}/${progress.total}`
    : "—";
  const errors = $(`errors-${side}`);
  errors.hidden = !progress || !progress.error;
  errors.textContent = progress ? `${progress.error} lỗi` : "";
}

function renderBridge(counts) {
  $("bridge").classList.toggle("running", isRunning());
  renderSideProgress("old");
  renderSideProgress("new");
  const share = job && counts.done ? (counts.ok / rows.length) * 100 : 0;
  $("ring-value").textContent =
    job && counts.done ? `${Math.floor(share)}%` : "—";
  $("ring-arc").setAttribute("stroke-dasharray", `${share} 100`);
  $("ring-arc").setAttribute("stroke-opacity", share ? 1 : 0);
}

// Giu nguyen phan tu o de hieu ung nhap nhay khong bi chay lai moi lan cap nhat.
function renderTape() {
  const tape = $("tape");
  while (tape.children.length > rows.length) tape.lastChild.remove();
  while (tape.children.length < rows.length) {
    const cell = document.createElement("button");
    cell.type = "button";
    tape.appendChild(cell);
  }
  rows.forEach((_, i) => {
    const cell = tape.children[i];
    const className = `cell ${statusOf(i)}`;
    if (cell.className !== className) cell.className = className;
    cell.dataset.index = i;
    cell.title = tableName(i);
  });
}

function pill(i) {
  const config = prepared[i];
  const result = job ? job.rows[i] : null;
  if (config && config.error)
    return '<span class="pill error">Sai cấu hình</span>';
  if (!result) return '<span class="pill neutral">Chưa chạy</span>';
  if (result.state === "pending")
    return '<span class="pill neutral">Đang chờ</span>';
  if (result.state === "running")
    return '<span class="pill neutral running">Đang chạy</span>';
  if (result.state === "cancelled")
    return '<span class="pill neutral">Đã dừng</span>';
  if (result.state === "error")
    return `<span class="pill error">${ICONS.alert}Lỗi query</span>`;
  if (result.verdict === "lech")
    return `<span class="pill lech">${ICONS.cross}Lệch</span>`;
  if (result.verdict === "trong")
    return `<span class="pill trong">${ICONS.alert}0 dòng</span>`;
  return `<span class="pill ok">${ICONS.check}Khớp</span>`;
}

function pctText(metric) {
  if (!metric) return '<span class="dash">—</span>';
  return metric.pct === null ? "Datalake = 0" : `${esc(metric.pct)}%`;
}

function worstSum(metrics) {
  let worst = null;
  metrics.slice(1).forEach((m) => {
    if (
      worst === null ||
      (worst.pct !== null &&
        (m.pct === null || Number(m.pct) > Number(worst.pct)))
    )
      worst = m;
  });
  return worst;
}

function shortPct(metric) {
  if (metric.pct === null) return "Datalake = 0";
  const value = Number(metric.pct);
  return value > 0 && value < 0.01 ? "<0.01%" : `${value.toFixed(2)}%`;
}

// Vach giua thanh la nguong, nen thanh dai gap doi nguong.
function meter(label, metric, i) {
  const limit = Number(limitOf(i)) || 0;
  const value = Number(metric.pct);
  let share = 0;
  if (metric.pct === null) share = 100;
  else
    share =
      limit > 0
        ? Math.min((value / (limit * 2)) * 100, 100)
        : value > 0
          ? 100
          : 0;
  return `<div class="meter-row${metric.ok ? "" : " over"}"><span>${label}</span><div class="meter"><i style="width:${share}%"></i></div><em class="num">${esc(shortPct(metric))}</em></div>`;
}

function meters(metrics, i) {
  const own = ownThreshold(i);
  const tag = own
    ? `<small class="own-limit" title="Ngưỡng riêng của bảng này">Ngưỡng riêng ${esc(own)}%</small>`
    : "";
  if (!metrics.length) return tag || '<span class="dash">—</span>';
  const money = worstSum(metrics);
  return `<div class="meters" title="Vạch giữa là ngưỡng ${esc(limitOf(i))}%">${meter("Số dòng", metrics[0], i)}${money ? meter("Tiền", money, i) : ""}${tag}</div>`;
}

function tableLabel(text) {
  const cut = text.lastIndexOf(".") + 1;
  return `<span class="ns">${esc(text.slice(0, cut))}</span><b>${esc(text.slice(cut))}</b>`;
}

function sideLines(oldText, newText, emptyText, format) {
  const line = (label, text) =>
    `<div title="${esc(text || emptyText)}"><span class="side">${label}</span>${text ? format(text) : `<span class="none">${emptyText}</span>`}</div>`;
  return line("Datalake", oldText) + line("Lakehouse", newText);
}

const partitionLabel = (text) => `<span class="val">${esc(text)}</span>`;

const CHEVRON =
  '<svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true"><path d="M6 3.5 10.5 8 6 12.5" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>';

function detailHtml(i, result, config) {
  const metrics = result && result.metrics.length ? result.metrics : null;
  const errorText = (result && result.error) || (config && config.error) || "";
  const table = metrics
    ? `<table class="metrics"><thead><tr><th>Chỉ số</th><th class="num">Datalake</th><th class="num">Lakehouse</th><th class="num">Lakehouse − Datalake</th><th class="num">Lệch</th></tr></thead><tbody>${metrics
        .map(
          (m) =>
            `<tr><td class="label">${esc(m.label)}</td><td class="num">${numCell(m.old)}</td><td class="num">${numCell(m.new)}</td><td class="num">${signedCell(m.diff)}</td><td class="num${m.ok ? "" : " over"}">${pctText(m)}</td></tr>`,
        )
        .join("")}</tbody></table>`
    : "";
  const sql =
    config && !config.error
      ? `<div class="sql-stack">${["old", "new"]
          .map(
            (side) =>
              `<div class="sql-block"><h3>Query ${side === "old" ? "Datalake" : "Lakehouse"}${rows[i][`${side}_sql`] ? '<span class="mark"> · tự viết</span>' : ""}</h3><pre>${highlightSql(config[`${side}_sql`])}</pre><button class="copy" type="button" data-copy="${side}" data-index="${i}">Chép</button></div>`,
          )
          .join("")}</div>`
      : "";
  const left = errorText
    ? `<p class="error-text">${esc(errorText)}</p>${table}`
    : table || '<p class="dash">Chưa có kết quả, bấm Chạy đối soát.</p>';
  return `<tr class="detail is-${statusOf(i)}"><td colspan="8"><div class="detail-grid"><div>${left}</div>${sql}</div></td></tr>`;
}

function rowHtml(row, i) {
  const result = job ? job.rows[i] : null;
  const config = prepared[i];
  const metrics = result ? result.metrics : [];
  const count = metrics[0];
  const open = expanded.has(i);
  const locked = isRunning() ? " disabled" : "";
  const counts = count
    ? `<div class="stack"><div>${numCell(count.old)}</div><div>${numCell(count.new)}</div></div>`
    : '<span class="dash">—</span>';
  return `<tr class="row is-${statusOf(i)}${open ? " open" : ""}" id="row-${i}">
    <td><input class="pick" type="checkbox" data-pick="${i}" aria-label="Chọn dòng ${i + 1}"${selected.has(i) ? " checked" : ""}${locked}></td>
    <td><button class="toggle" type="button" data-toggle="${i}" aria-expanded="${open}" aria-label="Xem chi tiết dòng ${i + 1}">${CHEVRON}</button></td>
    <td class="tables">${row.id ? `<span class="row-id num" title="Id bảng trong lịch sử">#${row.id}</span>` : ""}${sideLines(row.old_table, row.new_table, "chưa nhập", tableLabel)}</td>
    <td class="parts">${sideLines(row.old_partition, row.new_partition, "không có", partitionLabel)}</td>
    <td class="num">${counts}</td>
    <td>${meters(metrics, i)}</td>
    <td>${pill(i)}</td>
    <td><div class="row-actions"><button class="icon-btn" type="button" title="Chạy riêng bảng này" aria-label="Chạy riêng dòng ${i + 1}" data-run="${i}"${locked || (config && config.error) ? " disabled" : ""}>${ICONS.play}</button><button class="icon-btn" type="button" title="Xem lịch sử bảng này" aria-label="Lịch sử dòng ${i + 1}" data-history="${i}">${ICONS.history}</button><button class="icon-btn" type="button" title="Sửa" aria-label="Sửa dòng ${i + 1}" data-edit="${i}"${locked}>${ICONS.edit}</button><button class="icon-btn danger" type="button" title="Xóa" aria-label="Xóa dòng ${i + 1}" data-remove="${i}"${locked}>${ICONS.trash}</button></div></td>
  </tr>${open ? detailHtml(i, result, config) : ""}`;
}

function matches(row, i) {
  if (filter !== "all" && statusOf(i) !== filter) return false;
  const term = $("search").value.trim().toLowerCase();
  return (
    !term || `${row.old_table} ${row.new_table}`.toLowerCase().includes(term)
  );
}

function visibleIndices() {
  return rows.map((row, i) => (matches(row, i) ? i : -1)).filter((i) => i >= 0);
}

function renderSelection() {
  const shown = visibleIndices();
  const picked = shown.filter((i) => selected.has(i)).length;
  const all = $("check-all");
  all.checked = shown.length > 0 && picked === shown.length;
  all.indeterminate = picked > 0 && picked < shown.length;
  all.disabled = isRunning() || shown.length === 0;
  $("run-label").textContent = selected.size
    ? `Chạy ${selected.size} bảng đã chọn`
    : "Chạy đối soát";
}

function render() {
  const counts = tally();
  const visible = rows
    .map((row, i) => (matches(row, i) ? rowHtml(row, i) : ""))
    .join("");
  $("rows").innerHTML = visible;
  $("table").hidden = rows.length === 0;
  $("empty").hidden = rows.length > 0;
  $("no-match").hidden = rows.length === 0 || visible !== "";
  renderBoard(counts);
  renderBridge(counts);
  renderTape();
  renderFinding(counts);
  renderSelection();
  $("run").hidden = isRunning();
  $("run").disabled = rows.length === 0;
  $("stop").hidden = !isRunning();
  $("export").disabled = !job || isRunning();
  $("add").disabled = isRunning();
  $("clear-all").disabled = isRunning() || rows.length === 0;
  $("csv").disabled = isRunning();
  $("date").disabled = isRunning();
  $("threshold").disabled = isRunning();
}

function formatClock(ms) {
  const total = Math.floor(ms / 1000);
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(Math.floor(total / 60))}:${pad(total % 60)}`;
}

function tickClock() {
  $("clock").textContent = formatClock(Date.now() - runStarted);
}

async function poll(id) {
  let next;
  try {
    next = await api(`/api/jobs/${id}`);
  } catch (err) {
    clearInterval(clockTimer);
    setFinding(`Mất kết nối với công cụ: ${err.message}`, "bad");
    return;
  }
  next.rows = next.rows.map((r, i) =>
    r.state === "skipped" ? kept[i] || null : r,
  );
  const changed = JSON.stringify(next) !== JSON.stringify(job);
  job = next;
  if (next.log_error) setSaved(next.log_error, true);
  if (changed) render();
  if (isRunning()) setTimeout(() => poll(id), POLL_MS);
  else {
    clearInterval(clockTimer);
    if (location.hash === "#lich-su") loadHistory();
  }
}

// indices = null: chay tat ca; mang chi so: chay rieng cac dong do, giu ket qua cac dong khac.
async function runAll(indices = null) {
  for (const [side, name] of Object.entries(SIDES)) {
    if (
      !$(`${side}_host`).value.trim() ||
      !$(`${side}_username`).value.trim()
    ) {
      location.hash = "ket-noi";
      setConnStatus(
        side,
        `Nhập host và user của Thrift ${name} trước khi chạy.`,
        "bad",
      );
      $(`${side}_host`).focus();
      return;
    }
  }
  const missing = Object.keys(SIDES).filter(
    (side) => !$(`${side}_password`).value,
  );
  if (missing.length) {
    $("pw-user").textContent =
      missing
        .map(
          (side) =>
            `${SIDES[side]}: ${$(`${side}_username`).value.trim()} tại ${$(`${side}_host`).value.trim()}`,
        )
        .join(" · ") + ". Mật khẩu sẽ được lưu để lần sau không phải nhập lại.";
    Object.keys(SIDES).forEach((side) => {
      $(`pw-${side}`).value = "";
      $(`pw-${side}-label`).hidden = !missing.includes(side);
    });
    pendingRun = indices;
    $("pw-dialog").showModal();
    $(`pw-${missing[0]}`).focus();
    return;
  }
  if (!indices) {
    expanded = new Set();
    filter = "all";
  }
  $("run").disabled = true;
  // Moi bang phai co id trong DB truoc khi chay de lich su gan dung bang.
  await save();
  try {
    const body = await postJson("/api/run", {
      connections: { old: connection("old"), new: connection("new") },
      rows,
      indices,
      date: $("date").value,
      threshold: $("threshold").value,
      parallel: $("parallel").value,
    });
    runSet = indices ? new Set(indices) : null;
    kept = runSet && job ? job.rows.slice() : [];
    const pending = { state: "pending", error: "", verdict: "", metrics: [] };
    job = {
      id: body.id,
      state: "running",
      rows: rows.map((_, i) =>
        runSet && !runSet.has(i) ? kept[i] || null : { ...pending },
      ),
    };
    setSaved(`Đã ghi câu query vào ${body.sql_file}`);
    runStarted = Date.now();
    tickClock();
    clearInterval(clockTimer);
    clockTimer = setInterval(tickClock, 1000);
    render();
    poll(body.id);
  } catch (err) {
    setFinding(err.message, "bad");
    $("run").disabled = false;
  }
}

async function stopRun() {
  if (!job || !job.id) return;
  $("stop").disabled = true;
  setFinding("Đang dừng các query...");
  try {
    await api(`/api/jobs/${job.id}/cancel`, { method: "POST" });
  } catch (err) {
    setFinding(err.message, "bad");
  }
  $("stop").disabled = false;
}

const connKinds = { old: "", new: "" };

function setConnStatus(side, text, kind) {
  $(`${side}_status`).textContent = text;
  $(`${side}_status`).className = `conn-status${kind ? ` ${kind}` : ""}`;
  connKinds[side] = kind || "";
  const kinds = Object.values(connKinds);
  const overall = kinds.includes("bad")
    ? "bad"
    : kinds.every((k) => k === "good")
      ? "good"
      : "";
  $("scope-dot").className = `dot${overall ? ` ${overall}` : ""}`;
}

async function testConnection(side) {
  $(`${side}_test`).disabled = true;
  setConnStatus(side, "Đang kết nối...");
  try {
    const body = await postJson("/api/test-connection", {
      side,
      ...connection(side),
    });
    if (body.ok) setConnStatus(side, "Kết nối thành công.", "good");
    else
      setConnStatus(
        side,
        `Không kết nối được: ${body.error || "không rõ lý do"}`,
        "bad",
      );
  } catch (err) {
    setConnStatus(side, err.message, "bad");
  }
  $(`${side}_test`).disabled = false;
}

async function uploadCsv(file) {
  if (
    rows.length &&
    !confirm(
      `Thay ${rows.length} bảng hiện có bằng nội dung file ${file.name}?`,
    )
  )
    return;
  const form = new FormData();
  form.append("file", file);
  try {
    const body = await api("/api/parse-csv", { method: "POST", body: form });
    rows = body.rows;
    prepared = [];
    filter = "all";
    rowsChanged();
  } catch (err) {
    setFinding(err.message, "bad");
  }
}

function csvCell(value) {
  const text = value === null || value === undefined ? "" : String(value);
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

const VERDICT_TEXT = { ok: "Khớp", lech: "Lệch", trong: "0 dòng" };

function exportResults() {
  const header = [
    "bang_datalake",
    "bang_lakehouse",
    "partition_datalake",
    "partition_lakehouse",
    "nguong_phan_tram",
    "chi_so",
    "gia_tri_datalake",
    "gia_tri_lakehouse",
    "chenh_lech",
    "phan_tram_lech",
    "dat_nguong",
    "ket_qua_bang",
    "loi",
  ];
  const lines = [header];
  job.rows.forEach((r, i) => {
    const base = [
      rows[i].old_table,
      rows[i].new_table,
      rows[i].old_partition,
      rows[i].new_partition,
      limitOf(i),
    ];
    if (!r) {
      lines.push([...base, "", "", "", "", "", "", "Chưa chạy", ""]);
    } else if (r.metrics.length) {
      r.metrics.forEach((m) =>
        lines.push([
          ...base,
          m.label,
          m.old,
          m.new,
          m.diff,
          m.pct,
          m.ok ? "Đạt" : "Không đạt",
          VERDICT_TEXT[r.verdict],
          "",
        ]),
      );
    } else {
      lines.push([
        ...base,
        "",
        "",
        "",
        "",
        "",
        "",
        r.state === "cancelled" ? "Đã dừng" : "Lỗi",
        r.error,
      ]);
    }
  });
  const text = "﻿" + lines.map((l) => l.map(csvCell).join(",")).join("\r\n");
  const url = URL.createObjectURL(
    new Blob([text], { type: "text/csv;charset=utf-8" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = `ket_qua_doi_soat_${$("date").value || "khong_ngay"}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

function draft() {
  const data = new FormData($("editor-form"));
  const row = {};
  FIELDS.forEach((f) => (row[f] = String(data.get(f) || "").trim()));
  return row;
}

async function refreshEditor() {
  const seq = ++previewSeq;
  let item;
  try {
    item = (
      await postJson("/api/preview", { rows: [draft()], date: $("date").value })
    ).rows[0];
  } catch (err) {
    $("editor-error").textContent = err.message;
    return null;
  }
  if (seq === previewSeq) {
    $("editor-error").textContent = item.error;
    $("editor-old-sql").innerHTML = highlightSql(item.old_sql);
    $("editor-new-sql").innerHTML = highlightSql(item.new_sql);
    editorTemplates = {
      old: item.old_template || "",
      new: item.new_template || "",
    };
  }
  return item;
}

let editorTemplates = { old: "", new: "" };

function showSqlEditor(side, editing) {
  const area = $("editor-form").elements[`${side}_sql`];
  area.hidden = !editing;
  $(`editor-${side}-mark`).hidden = !editing;
  document.querySelector(`[data-sql-edit="${side}"]`).textContent = editing
    ? "Dùng câu tự sinh"
    : "Sửa câu query";
}

function toggleSqlEditor(side) {
  const area = $("editor-form").elements[`${side}_sql`];
  if (area.hidden) {
    // Chep cau tu sinh (con giu bien ngay) de sua tiep, thay vi go lai tu dau.
    if (!area.value.trim()) area.value = editorTemplates[side];
    showSqlEditor(side, true);
    area.focus();
  } else {
    area.value = "";
    showSqlEditor(side, false);
  }
  refreshEditor();
}

function openEditor(index) {
  editingIndex = index;
  const form = $("editor-form");
  const row = index === null ? {} : rows[index];
  FIELDS.forEach((f) => (form.elements[f].value = row[f] || ""));
  ["old", "new"].forEach((side) =>
    showSqlEditor(side, Boolean(row[`${side}_sql`])),
  );
  $("editor-title").textContent = index === null ? "Thêm bảng" : "Sửa bảng";
  $("editor").showModal();
  refreshEditor();
}

const VIEWS = { "#ket-noi": "conn", "#lich-su": "hist" };

function showView() {
  const view = VIEWS[location.hash] || "main";
  ["main", "conn", "hist"].forEach((name) => {
    $(`view-${name}`).hidden = name !== view;
    $(`nav-${name}`).classList.toggle("active", name === view);
  });
  if (view === "hist") loadHistory();
}

// ---------- Lich su (doc tu SQLite) ----------

const HISTORY_LIMIT = 500;
const hist = {
  run: null,
  table: null,
  tableLabel: "",
  expanded: new Set(),
  results: [],
  seq: 0,
};
let histTimer = null;

const RUN_STATE_TEXT = {
  running: "Đang chạy",
  done: "Xong",
  cancelled: "Đã dừng",
  interrupted: "Bị ngắt (tắt tool)",
};

function dateText(text) {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?: (\d{2}:\d{2}:\d{2}))?$/.exec(
    text || "",
  );
  if (!m) return text || "";
  return `${m[3]}/${m[2]}/${m[1]}${m[4] ? ` ${m[4]}` : ""}`;
}

function spanText(start, end) {
  if (!start || !end) return "";
  const ms =
    new Date(end.replace(" ", "T")) - new Date(start.replace(" ", "T"));
  return Number.isFinite(ms) ? formatClock(ms) : "";
}

function msText(ms) {
  if (ms === null || ms === undefined) return '<span class="dash">—</span>';
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`;
}

function runPill(state) {
  if (state === "success")
    return `<span class="pill ok">${ICONS.check}Thành công</span>`;
  if (state === "error")
    return `<span class="pill error">${ICONS.alert}Lỗi</span>`;
  return '<span class="pill neutral">Đã dừng</span>';
}

function verdictPill(r) {
  if (r.trang_thai !== "success") return '<span class="dash">—</span>';
  if (r.ket_luan === "lech")
    return `<span class="pill lech">${ICONS.cross}Lệch</span>`;
  if (r.ket_luan === "trong")
    return `<span class="pill trong">${ICONS.alert}0 dòng</span>`;
  return `<span class="pill ok">${ICONS.check}Khớp</span>`;
}

const countCell = (n, tone) =>
  `<td class="num${n && tone ? ` ${tone}` : ""}">${n || '<span class="dash">0</span>'}</td>`;
const orDash = (text) => esc(text) || '<span class="dash">—</span>';

function renderRuns(runs) {
  $("hist-runs").innerHTML = runs
    .map(
      (r) => `<tr class="run-row${hist.run === r.id ? " picked" : ""}" data-run-id="${r.id}" tabindex="0" title="Xem kết quả từng bảng của lượt này">
      <td class="num">#${r.id}</td>
      <td><span class="num">${esc(dateText(r.bat_dau_luc))}</span></td>
      <td><span class="num">${orDash(dateText(r.ngay_doi_soat))}</span></td>
      <td class="num">${orDash(spanText(r.bat_dau_luc, r.ket_thuc_luc))}</td>
      <td><span class="run-state ${esc(r.trang_thai)}">${esc(RUN_STATE_TEXT[r.trang_thai] || r.trang_thai)}</span></td>
      <td class="num">${r.tong_bang}</td>
      ${countCell(r.so_ok, "good")}${countCell(r.so_lech, "bad")}${countCell(r.so_trong, "warn")}${countCell(r.so_loi, "bad")}${countCell(r.so_dung)}
    </tr>`,
    )
    .join("");
  $("hist-runs-empty").hidden = runs.length > 0;
}

function histDetail(r) {
  const metrics = r.metrics.length
    ? `<table class="metrics"><thead><tr><th>Chỉ số</th><th class="num">Datalake</th><th class="num">Lakehouse</th><th class="num">Lakehouse − Datalake</th><th class="num">Lệch</th></tr></thead><tbody>${r.metrics
        .map(
          (m) =>
            `<tr><td class="label">${esc(m.label)}</td><td class="num">${numCell(m.old)}</td><td class="num">${numCell(m.new)}</td><td class="num">${signedCell(m.diff)}</td><td class="num${m.ok ? "" : " over"}">${pctText(m)}</td></tr>`,
        )
        .join("")}</tbody></table>`
    : "";
  const error = r.loi ? `<p class="error-text">${esc(r.loi)}</p>` : "";
  const parts = sideLines(
    r.old_partition,
    r.new_partition,
    "không có",
    partitionLabel,
  );
  const sql = [
    ["Datalake", r.sql_datalake],
    ["Lakehouse", r.sql_lakehouse],
  ]
    .filter(([, text]) => text)
    .map(
      ([name, text]) =>
        `<div class="sql-block"><h3>Query ${name} đã chạy</h3><pre>${highlightSql(text)}</pre></div>`,
    )
    .join("");
  return `<tr class="hist-detail"><td></td><td colspan="9"><div class="detail-grid">${error}${metrics}<div class="parts"><h3>Partition</h3>${parts}</div>${sql ? `<div class="sql-stack">${sql}</div>` : ""}</div></td></tr>`;
}

function renderResults() {
  $("hist-results").innerHTML = hist.results
    .map((r) => {
      const open = hist.expanded.has(r.id);
      const tableId = r.bang_id
        ? `<button class="link-btn num" type="button" data-hist-table="${r.bang_id}" title="Chỉ xem bảng này">#${r.bang_id}</button>`
        : '<span class="dash">—</span>';
      const sideCell = (error, ms) =>
        `<td class="num" title="${esc(error)}">${error ? '<span class="side-err">Lỗi</span> ' : ""}${msText(ms)}</td>`;
      return `<tr class="res-row${open ? " open" : ""}">
        <td><button class="toggle" type="button" data-hist-open="${r.id}" aria-expanded="${open}" aria-label="Xem chi tiết">${CHEVRON}</button></td>
        <td><span class="num">${esc(dateText(r.ket_thuc_luc || r.luot_bat_dau_luc))}</span></td>
        <td><button class="link-btn num" type="button" data-hist-run="${r.luot_chay_id}" title="Chỉ xem lượt này">#${r.luot_chay_id}</button></td>
        <td>${tableId}</td>
        <td class="tables">${sideLines(r.old_table, r.new_table, "chưa nhập", tableLabel)}</td>
        <td>${runPill(r.trang_thai)}</td>
        <td>${verdictPill(r)}</td>
        <td class="num">${r.nguong_ap_dung ? `${esc(r.nguong_ap_dung)}%` : ""}</td>
        ${sideCell(r.loi_datalake, r.thoi_gian_datalake_ms)}${sideCell(r.loi_lakehouse, r.thoi_gian_lakehouse_ms)}
      </tr>${open ? histDetail(r) : ""}`;
    })
    .join("");
  $("hist-results-empty").hidden = hist.results.length > 0;
  const chips = [];
  if (hist.run !== null)
    chips.push(
      `<button class="chip" type="button" data-hist-clear="run" title="Bỏ lọc">Lượt #${hist.run} ✕</button>`,
    );
  if (hist.table !== null)
    chips.push(
      `<button class="chip" type="button" data-hist-clear="table" title="Bỏ lọc">Bảng #${hist.table}${hist.tableLabel ? ` · ${esc(hist.tableLabel)}` : ""} ✕</button>`,
    );
  $("hist-chips").innerHTML = chips.join("");
}

async function loadHistory() {
  const seq = ++hist.seq;
  const params = new URLSearchParams();
  if (hist.run !== null) params.set("run", hist.run);
  if (hist.table !== null) params.set("table", hist.table);
  const term = $("hist-search").value.trim();
  if (term) params.set("q", term);
  if ($("hist-status").value) params.set("status", $("hist-status").value);
  try {
    const [runs, results] = await Promise.all([
      api("/api/history/runs"),
      api(`/api/history/results?${params}`),
    ]);
    if (seq !== hist.seq) return;
    hist.results = results.results;
    renderRuns(runs.runs);
    renderResults();
    $("hist-note").textContent =
      hist.results.length >= HISTORY_LIMIT
        ? `Đang hiện ${HISTORY_LIMIT} kết quả mới nhất, lọc theo bảng hoặc lượt để xem cũ hơn.`
        : "";
    $("hist-note").classList.remove("bad");
  } catch (err) {
    $("hist-note").textContent = err.message;
    $("hist-note").classList.add("bad");
  }
}

async function openTableHistory(i) {
  const row = rows[i];
  if (!row.id) await save();
  if (!row.id) return;
  hist.table = row.id;
  hist.tableLabel = row.new_table || row.old_table;
  hist.run = null;
  hist.expanded = new Set();
  $("hist-search").value = "";
  $("hist-status").value = "";
  if (location.hash === "#lich-su") loadHistory();
  else location.hash = "lich-su";
}

$("hist-runs").addEventListener("click", (event) => {
  const tr = event.target.closest("[data-run-id]");
  if (!tr) return;
  const id = Number(tr.dataset.runId);
  hist.run = hist.run === id ? null : id;
  hist.expanded = new Set();
  loadHistory();
});
$("hist-runs").addEventListener("keydown", (event) => {
  if (event.key === "Enter" && event.target.matches("[data-run-id]"))
    event.target.click();
});

$("hist-results").addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button) return;
  if (button.dataset.histOpen !== undefined) {
    const id = Number(button.dataset.histOpen);
    if (hist.expanded.has(id)) hist.expanded.delete(id);
    else hist.expanded.add(id);
    renderResults();
  } else if (button.dataset.histRun !== undefined) {
    hist.run = Number(button.dataset.histRun);
    loadHistory();
  } else if (button.dataset.histTable !== undefined) {
    hist.table = Number(button.dataset.histTable);
    const r = hist.results.find((x) => x.bang_id === hist.table);
    hist.tableLabel = r ? r.new_table || r.old_table : "";
    loadHistory();
  }
});

$("hist-chips").addEventListener("click", (event) => {
  const chip = event.target.closest("[data-hist-clear]");
  if (!chip) return;
  hist[chip.dataset.histClear] = null;
  if (chip.dataset.histClear === "table") hist.tableLabel = "";
  loadHistory();
});

$("hist-search").addEventListener("input", () => {
  clearTimeout(histTimer);
  histTimer = setTimeout(loadHistory, 250);
});
$("hist-status").addEventListener("change", loadHistory);
$("hist-reload").addEventListener("click", loadHistory);

$("editor-form").addEventListener("input", () => {
  clearTimeout(previewTimer);
  previewTimer = setTimeout(refreshEditor, 200);
});

$("editor-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const row = draft();
  const item = await refreshEditor();
  if (!item || item.error) return;
  const index = editingIndex;
  if (index === null) rows.push(row);
  // Sua tren chinh doi tuong cu de giu id (ke ca id dang cho server tra ve).
  else Object.assign(rows[index], row);
  $("editor").close();
  rowEdited((results) =>
    index === null ? results.push(null) : (results[index] = null),
  );
});

$("editor-cancel").addEventListener("click", () => $("editor").close());
document
  .querySelectorAll("[data-sql-edit]")
  .forEach((button) =>
    button.addEventListener("click", () =>
      toggleSqlEditor(button.dataset.sqlEdit),
    ),
  );

$("pw-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const missing = Object.keys(SIDES).filter(
    (side) => !$(`pw-${side}-label`).hidden,
  );
  const empty = missing.find((side) => !$(`pw-${side}`).value);
  if (empty) {
    $(`pw-${empty}`).focus();
    return;
  }
  missing.forEach((side) => {
    $(`${side}_password`).value = $(`pw-${side}`).value;
    $(`pw-${side}`).value = "";
  });
  $("pw-dialog").close();
  save();
  runAll(pendingRun);
});

$("pw-cancel").addEventListener("click", () => $("pw-dialog").close());

$("rows").addEventListener("change", (event) => {
  const box = event.target.closest("[data-pick]");
  if (!box) return;
  const i = Number(box.dataset.pick);
  if (box.checked) selected.add(i);
  else selected.delete(i);
  renderSelection();
});

$("check-all").addEventListener("change", (event) => {
  visibleIndices().forEach((i) =>
    event.target.checked ? selected.add(i) : selected.delete(i),
  );
  render();
});

$("rows").addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button) return;
  if (button.dataset.copy !== undefined) {
    const sql =
      prepared[Number(button.dataset.index)][`${button.dataset.copy}_sql`];
    navigator.clipboard
      .writeText(sql)
      .then(() => (button.textContent = "Đã chép"));
  } else if (button.dataset.toggle !== undefined) {
    const i = Number(button.dataset.toggle);
    if (expanded.has(i)) expanded.delete(i);
    else expanded.add(i);
    render();
  } else if (button.dataset.run !== undefined) {
    const i = Number(button.dataset.run);
    expanded.add(i);
    runAll([i]);
  } else if (button.dataset.history !== undefined) {
    openTableHistory(Number(button.dataset.history));
  } else if (button.dataset.edit !== undefined) {
    openEditor(Number(button.dataset.edit));
  } else if (button.dataset.remove !== undefined) {
    const i = Number(button.dataset.remove);
    rows.splice(i, 1);
    prepared = [];
    selected = new Set(
      [...selected].filter((j) => j !== i).map((j) => (j > i ? j - 1 : j)),
    );
    rowEdited((results) => results.splice(i, 1));
  }
});

$("tape").addEventListener("click", (event) => {
  const cell = event.target.closest(".cell");
  if (!cell) return;
  const i = Number(cell.dataset.index);
  filter = "all";
  $("search").value = "";
  expanded.add(i);
  render();
  $(`row-${i}`).scrollIntoView({ block: "center" });
});

document.querySelector(".tabs").addEventListener("click", (event) => {
  const tab = event.target.closest(".tab");
  if (!tab) return;
  filter = tab.dataset.filter === filter ? "all" : tab.dataset.filter;
  render();
});

$("search").addEventListener("input", render);

// Co tick thi chay rieng cac dong da chon, khong tick dong nao thi chay tat ca.
$("run").addEventListener("click", () =>
  runAll(selected.size ? [...selected].sort((a, b) => a - b) : null),
);
$("stop").addEventListener("click", stopRun);
Object.keys(SIDES).forEach((side) => {
  $(`${side}_test`).addEventListener("click", () => testConnection(side));
  ["host", "port", "username", "password"].forEach((k) =>
    $(`${side}_${k}`).addEventListener("input", () =>
      setConnStatus(side, "Chưa kiểm tra kết nối."),
    ),
  );
});
$("add").addEventListener("click", () => openEditor(null));
$("clear-all").addEventListener("click", () => {
  if (!rows.length || isRunning()) return;
  if (!confirm(`Xóa tất cả ${rows.length} bảng khỏi danh sách?`)) return;
  rows = [];
  prepared = [];
  rowsChanged();
});
$("export").addEventListener("click", exportResults);
$("csv").addEventListener("change", (event) => {
  const file = event.target.files[0];
  event.target.value = "";
  if (file) uploadCsv(file);
});
document
  .querySelector("label[tabindex]")
  .addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      $("csv").click();
    }
  });

const listPanel = $("list-panel");
["dragenter", "dragover"].forEach((name) =>
  listPanel.addEventListener(name, (event) => {
    event.preventDefault();
    if (!isRunning()) listPanel.classList.add("dragging");
  }),
);
listPanel.addEventListener("dragleave", (event) => {
  if (!listPanel.contains(event.relatedTarget))
    listPanel.classList.remove("dragging");
});
listPanel.addEventListener("drop", (event) => {
  event.preventDefault();
  listPanel.classList.remove("dragging");
  const file = event.dataTransfer.files[0];
  if (file && !isRunning()) uploadCsv(file);
});

CONN_FIELDS.forEach((f) =>
  $(f).addEventListener("input", () => {
    updateScope();
    saveSoon();
  }),
);
$("date").addEventListener("input", () => {
  clearResults();
  render();
  schedulePreview();
});
$("threshold").addEventListener("input", () => {
  clearResults();
  saveSoon();
  render();
});
addEventListener("hashchange", showView);

$("date").value = today();
showView();
render();
restore().then(() => {
  updateScope();
  render();
  schedulePreview();
});
