"use strict";

const POLL_MS = 1000;
const SAVE_DELAY_MS = 300;
const FIELDS = ["old_table", "new_table", "old_partition", "new_partition", "old_sum_columns", "new_sum_columns"];
const CONN_FIELDS = ["host", "port", "username", "parallel"];
const SETTING_FIELDS = [...CONN_FIELDS, "threshold"];
const $ = (id) => document.getElementById(id);

let rows = [];
let prepared = [];
let job = null;
let expanded = new Set();
let editingIndex = null;
let previewTimer = null;
let previewSeq = 0;
let saveTimer = null;
let savedPath = "";
let filter = "all";
let runStarted = 0;
let clockTimer = null;

const esc = (value) =>
  String(value).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

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
  if (!res.ok) throw new Error(body.error || `Máy chủ trả về lỗi ${res.status}.`);
  return body;
}

const postJson = (path, data) =>
  api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) });

function setSaved(text, bad) {
  $("saved").textContent = text;
  $("saved").classList.toggle("bad", Boolean(bad));
}

async function save() {
  clearTimeout(saveTimer);
  const settings = {};
  SETTING_FIELDS.forEach((f) => (settings[f] = $(f).value));
  try {
    await postJson("/api/state", { rows, settings });
    setSaved(`Tự động lưu tại ${savedPath}`);
  } catch (err) {
    setSaved(err.message, true);
  }
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
  SETTING_FIELDS.forEach((f) => {
    if (state.settings[f]) $(f).value = state.settings[f];
  });
  if (state.error) setSaved(state.error, true);
  else setSaved(`Tự động lưu tại ${savedPath}`);
}

function yesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

const isRunning = () => job !== null && job.state === "running";

function connection() {
  return {
    host: $("host").value,
    port: $("port").value,
    username: $("username").value,
    password: $("password").value,
  };
}

function updateScope() {
  const host = $("host").value.trim();
  $("scope").textContent = host ? `${host}:${$("port").value} · ${$("username").value.trim() || "chưa có user"}` : "Chưa kết nối Thrift";
}

function clearResults() {
  job = null;
  expanded = new Set();
  $("clock").textContent = "";
}

function rowsChanged() {
  clearResults();
  save();
  render();
  schedulePreview();
}

function schedulePreview() {
  clearTimeout(previewTimer);
  previewTimer = setTimeout(refreshPreview, 200);
}

async function refreshPreview() {
  const seq = ++previewSeq;
  try {
    const body = await postJson("/api/preview", { rows, date: $("date").value });
    if (seq !== previewSeq) return;
    prepared = body.rows;
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

const svgIcon = (size, body) => `<svg viewBox="0 0 16 16" width="${size}" height="${size}" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">${body}</svg>`;
const ICONS = {
  check: svgIcon(13, '<path d="m3 8.5 3.2 3.2L13 4.8"/>'),
  cross: svgIcon(13, '<path d="m4 4 8 8M12 4l-8 8"/>'),
  alert: svgIcon(13, '<path d="M8 3v6M8 12.5v.1"/>'),
  edit: svgIcon(16, '<path d="M10.5 3.2 12.8 5.5 5.6 12.7l-3 .7.7-3zM9.2 4.5l2.3 2.3"/>'),
  trash: svgIcon(16, '<path d="M2.8 4.5h10.4M6.2 4.5V3h3.6v1.5M4.2 4.5l.6 8.5h6.4l.6-8.5M6.8 7v3.5M9.2 7v3.5"/>'),
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

function tableName(i) {
  return rows[i] ? rows[i].new_table || rows[i].old_table : "";
}

function renderFinding(counts) {
  const limit = `${$("threshold").value}%`;
  if (!job) {
    if (counts.loi) setFinding(`${counts.loi} bảng sai cấu hình, mở từng dòng để xem chi tiết.`, "bad");
    else setFinding(rows.length ? `${rows.length} bảng sẵn sàng đối soát với ngưỡng ${limit}.` : "Chưa có bảng nào trong danh sách.");
    return;
  }
  if (isRunning()) {
    setFinding(`Đang chạy, đã xong ${counts.done} / ${rows.length} bảng.`);
    return;
  }
  if (job.state === "cancelled") {
    setFinding(`Đã dừng, có kết quả ${counts.done} / ${rows.length} bảng.`);
    return;
  }
  if (counts.lech || counts.loi) {
    const names = rows.map((_, i) => (statusOf(i) === "lech" ? tableName(i) : null)).filter(Boolean);
    const shown = names.slice(0, 3).join(", ") + (names.length > 3 ? ` và ${names.length - 3} bảng khác` : "");
    const parts = [];
    if (counts.lech) parts.push(`${counts.lech} bảng lệch quá ${limit}: ${shown}`);
    if (counts.loi) parts.push(`${counts.loi} bảng lỗi query`);
    setFinding(`${parts.join(". ")}.`, "bad");
    return;
  }
  const note = counts.trong ? ` Có ${counts.trong} bảng 0 dòng ở cả hai bên, nên kiểm tra lại partition.` : "";
  setFinding(`Tất cả ${counts.ok + counts.trong} bảng nằm trong ngưỡng ${limit}.${note}`, counts.trong ? "" : "good");
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
    const part = tab.dataset.filter === "all" ? counts.done : counts[tab.dataset.filter];
    const share = rows.length ? (part / rows.length) * 100 : 0;
    tab.querySelector(".arc-value").setAttribute("stroke-dasharray", `${share} 100`);
    tab.querySelector(".arc-value").setAttribute("stroke-opacity", share ? 1 : 0);
  });
}

function sumCounts(key) {
  if (!job) return null;
  let total = 0n;
  let seen = false;
  job.rows.forEach((r) => {
    if (r.metrics.length && r.metrics[0][key] !== null) {
      total += BigInt(r.metrics[0][key]);
      seen = true;
    }
  });
  return seen ? total.toString() : null;
}

function renderBridge(counts) {
  $("bridge").classList.toggle("running", isRunning());
  $("total-old").textContent = groupDigits(sumCounts("old")) || "—";
  $("total-new").textContent = groupDigits(sumCounts("new")) || "—";
  const share = job && counts.done ? (counts.ok / rows.length) * 100 : 0;
  $("ring-value").textContent = job && counts.done ? `${Math.floor(share)}%` : "—";
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
  if (config && config.error) return '<span class="pill error">Sai cấu hình</span>';
  if (!result) return '<span class="pill neutral">Chưa chạy</span>';
  if (result.state === "pending") return '<span class="pill neutral">Đang chờ</span>';
  if (result.state === "running") return '<span class="pill neutral running">Đang chạy</span>';
  if (result.state === "cancelled") return '<span class="pill neutral">Đã dừng</span>';
  if (result.state === "error") return `<span class="pill error">${ICONS.alert}Lỗi query</span>`;
  if (result.verdict === "lech") return `<span class="pill lech">${ICONS.cross}Lệch</span>`;
  if (result.verdict === "trong") return `<span class="pill trong">${ICONS.alert}0 dòng</span>`;
  return `<span class="pill ok">${ICONS.check}Khớp</span>`;
}

function pctText(metric) {
  if (!metric) return '<span class="dash">—</span>';
  return metric.pct === null ? "Datalake = 0" : `${esc(metric.pct)}%`;
}

function worstSum(metrics) {
  let worst = null;
  metrics.slice(1).forEach((m) => {
    if (worst === null || (worst.pct !== null && (m.pct === null || Number(m.pct) > Number(worst.pct)))) worst = m;
  });
  return worst;
}

function shortPct(metric) {
  if (metric.pct === null) return "Datalake = 0";
  const value = Number(metric.pct);
  return value > 0 && value < 0.01 ? "<0.01%" : `${value.toFixed(2)}%`;
}

// Vach giua thanh la nguong, nen thanh dai gap doi nguong.
function meter(label, metric) {
  const limit = Number($("threshold").value) || 0;
  const value = Number(metric.pct);
  let share = 0;
  if (metric.pct === null) share = 100;
  else share = limit > 0 ? Math.min((value / (limit * 2)) * 100, 100) : value > 0 ? 100 : 0;
  return `<div class="meter-row${metric.ok ? "" : " over"}"><span>${label}</span><div class="meter"><i style="width:${share}%"></i></div><em class="num">${esc(shortPct(metric))}</em></div>`;
}

function meters(metrics) {
  if (!metrics.length) return '<span class="dash">—</span>';
  const money = worstSum(metrics);
  return `<div class="meters" title="Vạch giữa là ngưỡng ${esc($("threshold").value)}%">${meter("Số dòng", metrics[0])}${money ? meter("Tiền", money) : ""}</div>`;
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

const CHEVRON = '<svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true"><path d="M6 3.5 10.5 8 6 12.5" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>';

function detailHtml(i, result, config) {
  const metrics = result && result.metrics.length ? result.metrics : null;
  const errorText = (result && result.error) || (config && config.error) || "";
  const table = metrics
    ? `<table class="metrics"><thead><tr><th>Chỉ số</th><th class="num">Datalake</th><th class="num">Lakehouse</th><th class="num">Lakehouse − Datalake</th><th class="num">Lệch</th></tr></thead><tbody>${metrics
        .map(
          (m) =>
            `<tr><td class="label">${esc(m.label)}</td><td class="num">${numCell(m.old)}</td><td class="num">${numCell(m.new)}</td><td class="num">${signedCell(m.diff)}</td><td class="num${m.ok ? "" : " over"}">${pctText(m)}</td></tr>`
        )
        .join("")}</tbody></table>`
    : "";
  const sql = config && !config.error
    ? `<div class="sql-stack">${["old", "new"]
        .map(
          (side) =>
            `<div class="sql-block"><h3>Query ${side === "old" ? "Datalake" : "Lakehouse"}</h3><pre>${highlightSql(config[`${side}_sql`])}</pre><button class="copy" type="button" data-copy="${side}" data-index="${i}">Chép</button></div>`
        )
        .join("")}</div>`
    : "";
  const left = errorText ? `<p class="error-text">${esc(errorText)}</p>${table}` : table || '<p class="dash">Chưa có kết quả, bấm Chạy đối soát.</p>';
  return `<tr class="detail is-${statusOf(i)}"><td colspan="7"><div class="detail-grid"><div>${left}</div>${sql}</div></td></tr>`;
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
    <td><button class="toggle" type="button" data-toggle="${i}" aria-expanded="${open}" aria-label="Xem chi tiết dòng ${i + 1}">${CHEVRON}</button></td>
    <td class="tables">${sideLines(row.old_table, row.new_table, "chưa nhập", tableLabel)}</td>
    <td class="parts">${sideLines(row.old_partition, row.new_partition, "không có", partitionLabel)}</td>
    <td class="num">${counts}</td>
    <td>${meters(metrics)}</td>
    <td>${pill(i)}</td>
    <td><div class="row-actions"><button class="icon-btn" type="button" title="Sửa" aria-label="Sửa dòng ${i + 1}" data-edit="${i}"${locked}>${ICONS.edit}</button><button class="icon-btn danger" type="button" title="Xóa" aria-label="Xóa dòng ${i + 1}" data-remove="${i}"${locked}>${ICONS.trash}</button></div></td>
  </tr>${open ? detailHtml(i, result, config) : ""}`;
}

function matches(row, i) {
  if (filter !== "all" && statusOf(i) !== filter) return false;
  const term = $("search").value.trim().toLowerCase();
  return !term || `${row.old_table} ${row.new_table}`.toLowerCase().includes(term);
}

function render() {
  const counts = tally();
  const visible = rows.map((row, i) => (matches(row, i) ? rowHtml(row, i) : "")).join("");
  $("rows").innerHTML = visible;
  $("table").hidden = rows.length === 0;
  $("empty").hidden = rows.length > 0;
  $("no-match").hidden = rows.length === 0 || visible !== "";
  renderBoard(counts);
  renderBridge(counts);
  renderTape();
  renderFinding(counts);
  $("run").hidden = isRunning();
  $("run").disabled = rows.length === 0;
  $("stop").hidden = !isRunning();
  $("export").disabled = !job || isRunning();
  $("add").disabled = isRunning();
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
  const changed = JSON.stringify(next) !== JSON.stringify(job);
  job = next;
  if (changed) render();
  if (isRunning()) setTimeout(() => poll(id), POLL_MS);
  else clearInterval(clockTimer);
}

async function runAll() {
  if (!$("host").value.trim() || !$("username").value.trim()) {
    location.hash = "ket-noi";
    setConnStatus("Nhập host và user của Thrift trước khi chạy.", "bad");
    $("host").focus();
    return;
  }
  if (!$("password").value) {
    $("pw-user").textContent = `Tài khoản ${$("username").value.trim()} tại ${$("host").value.trim()}. Mật khẩu không được lưu lại.`;
    $("pw-input").value = "";
    $("pw-dialog").showModal();
    return;
  }
  expanded = new Set();
  filter = "all";
  $("run").disabled = true;
  try {
    const body = await postJson("/api/run", {
      ...connection(),
      rows,
      date: $("date").value,
      threshold: $("threshold").value,
      parallel: $("parallel").value,
    });
    job = { id: body.id, state: "running", rows: rows.map(() => ({ state: "pending", error: "", verdict: "", metrics: [] })) };
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

function setConnStatus(text, kind) {
  $("conn-status").textContent = text;
  $("conn-status").className = `conn-status${kind ? ` ${kind}` : ""}`;
  $("scope-dot").className = `dot${kind ? ` ${kind}` : ""}`;
}

async function testConnection() {
  $("test").disabled = true;
  setConnStatus("Đang kết nối...");
  try {
    const body = await postJson("/api/test-connection", connection());
    if (body.ok) setConnStatus("Kết nối thành công.", "good");
    else setConnStatus(`Không kết nối được: ${body.error || "không rõ lý do"}`, "bad");
  } catch (err) {
    setConnStatus(err.message, "bad");
  }
  $("test").disabled = false;
}

async function uploadCsv(file) {
  if (rows.length && !confirm(`Thay ${rows.length} bảng hiện có bằng nội dung file ${file.name}?`)) return;
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
  const header = ["bang_datalake", "bang_lakehouse", "partition_datalake", "partition_lakehouse", "chi_so", "gia_tri_datalake", "gia_tri_lakehouse", "chenh_lech", "phan_tram_lech", "dat_nguong", "ket_qua_bang", "loi"];
  const lines = [header];
  job.rows.forEach((r, i) => {
    const base = [rows[i].old_table, rows[i].new_table, rows[i].old_partition, rows[i].new_partition];
    if (r.metrics.length) {
      r.metrics.forEach((m) => lines.push([...base, m.label, m.old, m.new, m.diff, m.pct, m.ok ? "Đạt" : "Không đạt", VERDICT_TEXT[r.verdict], ""]));
    } else {
      lines.push([...base, "", "", "", "", "", "", r.state === "cancelled" ? "Đã dừng" : "Lỗi", r.error]);
    }
  });
  const text = "﻿" + lines.map((l) => l.map(csvCell).join(",")).join("\r\n");
  const url = URL.createObjectURL(new Blob([text], { type: "text/csv;charset=utf-8" }));
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
    item = (await postJson("/api/preview", { rows: [draft()], date: $("date").value })).rows[0];
  } catch (err) {
    $("editor-error").textContent = err.message;
    return null;
  }
  if (seq === previewSeq) {
    $("editor-error").textContent = item.error;
    $("editor-old-sql").innerHTML = highlightSql(item.old_sql);
    $("editor-new-sql").innerHTML = highlightSql(item.new_sql);
  }
  return item;
}

function openEditor(index) {
  editingIndex = index;
  const form = $("editor-form");
  const row = index === null ? {} : rows[index];
  FIELDS.forEach((f) => (form.elements[f].value = row[f] || ""));
  $("editor-title").textContent = index === null ? "Thêm bảng" : "Sửa bảng";
  $("editor").showModal();
  refreshEditor();
}

function showView() {
  const onConn = location.hash === "#ket-noi";
  $("view-main").hidden = onConn;
  $("view-conn").hidden = !onConn;
  $("nav-main").classList.toggle("active", !onConn);
  $("nav-conn").classList.toggle("active", onConn);
}

$("editor-form").addEventListener("input", () => {
  clearTimeout(previewTimer);
  previewTimer = setTimeout(refreshEditor, 200);
});

$("editor-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const row = draft();
  const item = await refreshEditor();
  if (!item || item.error) return;
  if (editingIndex === null) rows.push(row);
  else rows[editingIndex] = row;
  $("editor").close();
  rowsChanged();
});

$("editor-cancel").addEventListener("click", () => $("editor").close());

$("pw-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (!$("pw-input").value) return;
  $("password").value = $("pw-input").value;
  $("pw-input").value = "";
  $("pw-dialog").close();
  runAll();
});

$("pw-cancel").addEventListener("click", () => $("pw-dialog").close());

$("rows").addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button) return;
  if (button.dataset.copy !== undefined) {
    const sql = prepared[Number(button.dataset.index)][`${button.dataset.copy}_sql`];
    navigator.clipboard.writeText(sql).then(() => (button.textContent = "Đã chép"));
  } else if (button.dataset.toggle !== undefined) {
    const i = Number(button.dataset.toggle);
    if (expanded.has(i)) expanded.delete(i);
    else expanded.add(i);
    render();
  } else if (button.dataset.edit !== undefined) {
    openEditor(Number(button.dataset.edit));
  } else if (button.dataset.remove !== undefined) {
    rows.splice(Number(button.dataset.remove), 1);
    prepared = [];
    rowsChanged();
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

$("run").addEventListener("click", runAll);
$("stop").addEventListener("click", stopRun);
$("test").addEventListener("click", testConnection);
$("add").addEventListener("click", () => openEditor(null));
$("export").addEventListener("click", exportResults);
$("csv").addEventListener("change", (event) => {
  const file = event.target.files[0];
  event.target.value = "";
  if (file) uploadCsv(file);
});
document.querySelector("label[tabindex]").addEventListener("keydown", (event) => {
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
  })
);
listPanel.addEventListener("dragleave", (event) => {
  if (!listPanel.contains(event.relatedTarget)) listPanel.classList.remove("dragging");
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
  })
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

$("date").value = yesterday();
showView();
render();
restore().then(() => {
  updateScope();
  render();
  schedulePreview();
});
