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
  settings.dependency_gate = $("dependency_gate").checked ? "1" : "0";
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
  $("dependency_gate").checked = settings.dependency_gate !== "0";
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
    $(`kpi-${side}`).textContent = host ? `${host}:${$(`${side}_port`).value}` : "Chưa cấu hình";
    $(`kpi-${side}`).title = host ? `${host}:${$(`${side}_port`).value} · ${user}` : "";
    if (!$(`kpi-${side}-note`).dataset.status)
      $(`kpi-${side}-note`).textContent = host ? `${user} · chưa kiểm tra` : "Nhập host và user bên dưới";
  });
  $("kpi-parallel").textContent = `${$("parallel").value || "—"} query`;
  $("kpi-parallel-note").textContent = $("dependency_gate").checked
    ? "Chặn bảng khi bảng nguồn lệch"
    : "Không chặn theo phụ thuộc";
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
    refreshDeps();
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
  skip: svgIcon(13, '<path d="M3.5 8h9M9.5 5l3 3-3 3"/>'),
  history: svgIcon(
    16,
    '<path d="M2.8 8a5.2 5.2 0 1 0 1.5-3.7M2.8 2.8v2.3h2.3M8 5.2V8l2 1.4"/>',
  ),
};

const highlightSql = (sql) =>
  esc(sql)
    .replace(
      /\b(SELECT|FROM|WHERE|AND|AS|INSERT|INTO|OVERWRITE|TABLE|CREATE|MERGE|USING|UPDATE|SET|ALTER|JOIN|ON|WITH|GROUP BY|UNION ALL|UNION|LOCATION|PARTITION|WHEN|MATCHED|THEN)\b/gi,
      '<b class="kw">$1</b>',
    )
    .replace(/\b(COUNT|SUM)\b/g, '<b class="fn">$1</b>');

function statusOf(i) {
  if (prepared[i] && prepared[i].error) return "loi";
  const result = job ? job.rows[i] : null;
  if (!result) return "cho";
  if (result.state === "pending" || result.state === "running") return "chay";
  if (result.state === "blocked") return "boqua";
  if (result.state === "cancelled") return "dung";
  if (result.state === "error") return "loi";
  return result.verdict;
}

function tally() {
  const counts = { ok: 0, lech: 0, trong: 0, loi: 0, boqua: 0, done: 0 };
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
  if (counts.lech || counts.loi || counts.boqua) {
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
    if (counts.boqua)
      parts.push(`${counts.boqua} bảng bỏ qua vì bảng nguồn lệch`);
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
    return result.waiting && result.waiting.length
      ? `<span class="pill neutral" title="Chờ ${esc(result.waiting.join(", "))} chạy xong">Chờ bảng nguồn</span>`
      : '<span class="pill neutral">Đang chờ</span>';
  if (result.state === "blocked")
    return `<span class="pill boqua" title="${esc(result.error)}">${ICONS.skip}Bỏ qua</span>`;
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
  const extra =
    warningsHtml(result && result.warnings) +
    causeHtml(result && result.cause) +
    depsLine(i);
  const left = errorText
    ? `<p class="error-text">${esc(errorText)}</p>${table}${extra}`
    : (table || '<p class="dash">Chưa có kết quả, bấm Chạy đối soát.</p>') +
      extra;
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
  if (changed) {
    render();
    if (location.hash === "#phu-thuoc") {
      refreshAutoFocus();
      renderGraph();
    }
  }
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
      gate: $("dependency_gate").checked,
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
  const note = $(`kpi-${side}-note`);
  note.dataset.status = kind || "";
  note.textContent = kind === "good" ? "Kết nối được" : kind === "bad" ? "Kết nối lỗi" : text;
  $(`kpi-${side}-dot`).className = `dot${kind ? ` ${kind}` : ""}`;
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
        r.state === "cancelled"
          ? "Đã dừng"
          : r.state === "blocked"
            ? "Bỏ qua (bảng nguồn lệch)"
            : "Lỗi",
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

const VIEWS = { "#ket-noi": "conn", "#lich-su": "hist", "#phu-thuoc": "dep" };

function showView() {
  const view = VIEWS[location.hash] || "main";
  ["main", "conn", "hist", "dep"].forEach((name) => {
    $(`view-${name}`).hidden = name !== view;
    $(`nav-${name}`).classList.toggle("active", name === view);
  });
  if (view === "hist") loadHistory();
  if (view === "dep") loadLineage();
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
  if (state === "blocked")
    return `<span class="pill boqua">${ICONS.skip}Bỏ qua</span>`;
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

// Thanh ty le ket qua cua mot luot: moi mau la mot loai ket qua.
function runBar(r) {
  const parts = [
    ["ok", r.so_ok, "khớp"],
    ["lech", r.so_lech, "lệch"],
    ["trong", r.so_trong, "0 dòng"],
    ["loi", r.so_loi, "lỗi"],
    ["boqua", r.so_bo_qua, "bỏ qua"],
    ["dung", r.so_dung, "dừng"],
  ].filter(([, n]) => n);
  const total = r.tong_bang || parts.reduce((a, [, n]) => a + n, 0);
  if (!total) return '<span class="dash">—</span>';
  const title = parts.map(([, n, t]) => `${n} ${t}`).join(" · ");
  return `<span class="run-bar" title="${esc(title)}">${parts
    .map(([k, n]) => `<i class="${k}" style="width:${(n / total) * 100}%"></i>`)
    .join("")}</span>`;
}

function renderHistKpis(runs) {
  $("kpi-runs").textContent = runs.length >= HISTORY_LIMIT ? `${HISTORY_LIMIT}+` : runs.length;
  $("kpi-runs-note").textContent = runs.length ? `từ ${dateText(runs[runs.length - 1].bat_dau_luc).slice(0, 10)}` : "chưa có lượt nào";
  const last = runs[0];
  if (!last) {
    ["kpi-last", "kpi-rate", "kpi-bad"].forEach((id) => {
      $(id).textContent = "—";
      $(`${id}-note`).textContent = "";
    });
    return;
  }
  $("kpi-last").textContent = `#${last.id}`;
  $("kpi-last-note").textContent = `${dateText(last.bat_dau_luc)} · ${RUN_STATE_TEXT[last.trang_thai] || last.trang_thai}`;
  const total = last.tong_bang || 0;
  $("kpi-rate").textContent = total ? `${Math.round((last.so_ok / total) * 100)}%` : "—";
  $("kpi-rate-note").textContent = `${last.so_ok} / ${total} bảng${last.ngay_doi_soat ? ` · ngày ${dateText(last.ngay_doi_soat)}` : ""}`;
  $("kpi-bad").textContent = `${last.so_lech} · ${last.so_loi} · ${last.so_bo_qua || 0}`;
  $("kpi-bad-note").textContent = last.so_lech || last.so_loi ? "cần xem lại" : "không có bảng lệch hay lỗi";
}

function renderRuns(runs) {
  renderHistKpis(runs);
  $("hist-runs").innerHTML = runs
    .map(
      (r) => `<tr class="run-row${hist.run === r.id ? " picked" : ""}" data-run-id="${r.id}" tabindex="0" title="Xem kết quả từng bảng của lượt này">
      <td class="num">#${r.id}</td>
      <td><span class="num">${esc(dateText(r.bat_dau_luc))}</span></td>
      <td><span class="num">${orDash(dateText(r.ngay_doi_soat))}</span></td>
      <td class="num">${orDash(spanText(r.bat_dau_luc, r.ket_thuc_luc))}</td>
      <td><span class="run-state ${esc(r.trang_thai)}">${esc(RUN_STATE_TEXT[r.trang_thai] || r.trang_thai)}</span></td>
      <td>${runBar(r)}</td>
      <td class="num">${r.tong_bang}</td>
      ${countCell(r.so_ok, "good")}${countCell(r.so_lech, "bad")}${countCell(r.so_trong, "warn")}${countCell(r.so_loi, "bad")}${countCell(r.so_bo_qua, "warn")}${countCell(r.so_dung)}
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
  const error =
    (r.loi ? `<p class="error-text">${esc(r.loi)}</p>` : "") +
    warningsHtml(r.canh_bao ? r.canh_bao.split("\n") : []) +
    causeHtml(parseCause(r.nghi_do));
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

// ---------- Phu thuoc giua cac bang (quet job Hop) ----------

// Bang nguon/dich trong danh sach cua tung dong, tinh tren server tu graph da quet.
let deps = null;
let depsSeq = 0;
const dep = {
  loaded: false,
  data: null,
  up: new Map(),
  down: new Map(),
  stmts: new Map(),
  colsByTarget: new Map(),
  nodes: new Set(),
  focus: null,
  edge: null,
};

const KIND_TEXT = {
  CREATE: "CREATE",
  INSERT: "INSERT",
  MERGE: "MERGE",
  UPDATE: "UPDATE",
  ALTER: "ALTER",
  SELECT: "SELECT",
};

async function refreshDeps() {
  const seq = ++depsSeq;
  try {
    const body = await postJson("/api/lineage/deps", { rows });
    if (seq !== depsSeq) return;
    deps = body.scanned ? body.rows : null;
  } catch (err) {
    deps = null;
  }
  render();
  if (typeof renderDepKpis === "function") renderDepKpis();
  if (typeof refreshAutoFocus === "function") refreshAutoFocus();
}

function renderDepKpis() {
  const d = dep.data;
  const tables = d ? [...dep.nodes].filter((n) => !n.startsWith("temp:") && !n.startsWith("location:")).length : 0;
  $("kpi-tables").textContent = d ? tables : "—";
  $("kpi-tables-note").textContent = d ? `${d.file_count} file chạy từ Start` : "chưa quét";
  $("kpi-edges").textContent = d ? d.edges.length : "—";
  $("kpi-edges-note").textContent = d ? "nguồn → đích" : "";
  $("kpi-stmts").textContent = d ? d.statements.length : "—";
  $("kpi-stmts-note").textContent = d && d.warnings.length ? `${d.warnings.length} cảnh báo khi quét` : d ? "không có cảnh báo" : "";
  const matched = deps ? deps.filter((x) => x && x.node).length : 0;
  $("kpi-listed").textContent = d ? `${matched} / ${rows.length}` : "—";
  $("kpi-listed-note").textContent = d
    ? matched < rows.length ? `${rows.length - matched} bảng không thấy trong job` : "đều có trong job"
    : "";
}

function warningsHtml(list) {
  const items = (list || []).filter(Boolean);
  if (!items.length) return "";
  return `<ul class="warn-list">${items.map((w) => `<li>${ICONS.alert}<span>${esc(w)}</span></li>`).join("")}</ul>`;
}

function parseCause(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (err) {
    return null;
  }
}

function pctOf(e) {
  if (e.diff === null || e.diff === undefined) return '<span class="dash">—</span>';
  if (e.pct === null || e.pct === undefined) return "Datalake = 0";
  return `${esc(e.pct)}%`;
}

function causeHtml(cause) {
  if (!cause || !cause.metrics || !cause.metrics.length) return "";
  const items = cause.metrics
    .map((m) => {
      const entries = m.entries.length
        ? `<table class="metrics cause-table"><thead><tr><th>Bảng nguồn</th><th>Cột</th><th class="num">Lệch</th><th class="num">Lakehouse − Datalake</th><th>Kết quả lấy từ</th></tr></thead><tbody>${m.entries
            .map(
              (e) =>
                `<tr title="${esc((e.path || []).join(" → "))}"><td class="tables">${tableLabel(e.table)}</td><td>${e.column ? esc(e.column) : '<span class="dash">cả bảng</span>'}${e.label ? `<small class="muted"> · ${esc(e.label)}</small>` : ""}</td><td class="num${e.ok === false ? " over" : ""}">${pctOf(e)}</td><td class="num">${e.diff === null || e.diff === undefined ? '<span class="dash">—</span>' : signedCell(e.diff)}</td><td class="small muted">${esc([e.from, e.note].filter(Boolean).join(" · "))}</td></tr>`,
            )
            .join("")}</tbody></table>`
        : "";
      return `<div class="cause-item"><p><b>${esc(m.label)}</b>: ${esc(m.summary)} <small class="muted">(${m.mode === "cot" ? "theo quan hệ cột" : "theo quan hệ bảng"})</small></p>${entries}${(m.trees || []).map(traceHtml).join("")}</div>`;
    })
    .join("");
  const writers = (cause.writers || []).length
    ? `<details class="writers"><summary>Xem ${cause.writers.length} câu lệnh nạp ${esc(cause.table)}</summary>${cause.writers
        .map(
          (w) =>
            `<div class="sql-block"><h3>${esc(w.kind)} · ${esc(w.file)} › ${esc(w.step)}</h3><pre>${highlightSql(w.sql)}</pre></div>`,
        )
        .join("")}</details>`
    : "";
  return `<div class="cause"><h3>Nghi do</h3>${items}${writers}</div>`;
}

// Cay truy nguon mot cot SUM bi lech: cot duoc tinh bang bieu thuc nao, o job nao, tu cot nao.
function traceNode(n) {
  const kindTag =
    n.kind === "temp"
      ? '<span class="ct-tag">view tạm</span>'
      : n.kind === "location"
        ? '<span class="ct-tag">location</span>'
        : "";
  const name =
    n.kind === "table"
      ? tableLabel(n.table)
      : `<b>${esc(n.kind === "temp" ? n.table.slice(n.table.lastIndexOf(":") + 1) : n.table.slice("location:".length))}</b>`;
  const col = n.column === "*" ? "mọi cột" : n.column;
  const listed = n.listed ? '<span class="ct-tag listed">trong danh sách đối soát</span>' : "";
  const cut = n.cut ? `<span class="ct-tag cut">${esc(n.cut)}</span>` : "";
  const leaf = !n.from.length && !n.cut && n.kind === "table"
    ? '<span class="ct-tag leaf">bảng gốc trong job đã quét</span>'
    : "";
  const steps = n.from
    .map(
      (f) => `<li class="ct-step"><div class="ct-how"><span class="kind kind-${esc(String(f.stmt.kind || "").toLowerCase())}">${esc(f.stmt.kind || "")}</span><span class="ct-job" title="${esc(f.stmt.chain || "")}">${esc(f.stmt.file)} › ${esc(f.stmt.step)}${f.stmt.id ? ` <span class="muted">#${f.stmt.id}</span>` : ""}</span>${f.expr ? `<code class="ct-expr" title="${esc(f.expr)}">= ${esc(f.expr)}</code>` : ""}</div><ul>${f.sources.map((c) => `<li>${traceNode(c)}</li>`).join("")}</ul></li>`,
    )
    .join("");
  return `<div class="ct-node"><span class="ct-col">${name}<span class="ct-dot">.</span><code>${esc(col)}</code></span>${kindTag}${listed}${leaf}${cut}</div>${steps ? `<ul class="ct-steps">${steps}</ul>` : ""}`;
}

function traceHtml(tree) {
  if (!tree) return "";
  const body = tree.from.length
    ? traceNode(tree)
    : '<p class="dash">Không tìm thấy câu lệnh nào tính ra cột này trong job đã quét (cần quét lại job nếu vừa cập nhật tool).</p>';
  return `<details class="col-trace" open><summary>Cột <code>${esc(tree.column)}</code> của ${esc(tree.table)} được tính từ những cột nào</summary><div class="ct-tree">${body}</div></details>`;
}

function depsLine(i) {
  if (!deps || !deps[i]) return "";
  const info = deps[i];
  if (!info.node)
    return '<p class="deps-line muted">Không thấy bảng Lakehouse này trong job Hop đã quét.</p>';
  const names = (list) =>
    list.length
      ? list.map((j) => esc(tableName(j))).join(", ")
      : '<span class="dash">không có</span>';
  return `<p class="deps-line"><span><span class="muted">Bảng nguồn trong danh sách:</span> ${names(info.upstream)}</span><span><span class="muted">Bảng phía sau:</span> ${names(info.downstream)}</span><button class="btn small" type="button" data-dep-open="${esc(info.node)}">Xem sơ đồ</button></p>`;
}

function openDependency(node) {
  dep.focus = node;
  dep.autoFocus = false;
  dep.edge = null;
  $("dep-search").value = node;
  if (location.hash === "#phu-thuoc") {
    renderGraph();
    renderDepDetail();
  } else location.hash = "phu-thuoc";
}

const nodeKind = (n) =>
  n.startsWith("temp:") ? "temp" : n.startsWith("location:") ? "location" : "table";
const isTable = (n) => nodeKind(n) === "table";

function nodeLabel(n) {
  const kind = nodeKind(n);
  if (kind === "temp") return ["view tạm", n.slice(n.lastIndexOf(":") + 1)];
  if (kind === "location") return ["location", n.slice("location:".length)];
  const cut = n.lastIndexOf(".");
  return [cut > 0 ? n.slice(0, cut) : "", n.slice(cut + 1)];
}

function nodeRow(n) {
  return deps ? deps.findIndex((d) => d && d.node === n) : -1;
}

// Nut nam trong danh sach doi soat: lay trang thai cua dong do de to mau.
function nodeStatus(n) {
  if (!isTable(n)) return "temp";
  const i = nodeRow(n);
  return i < 0 ? "out" : statusOf(i);
}

// ---------- doc / quet ----------

const DEP_PREFS = "doi_soat_phu_thuoc";
function readPrefs() {
  try {
    return JSON.parse(localStorage.getItem(DEP_PREFS) || "{}");
  } catch (err) {
    return {};
  }
}
function writePrefs() {
  try {
    localStorage.setItem(
      DEP_PREFS,
      JSON.stringify({ folder: dep.folder, home: dep.home }),
    );
  } catch (err) {
    /* trinh duyet chan luu tru: bo qua */
  }
}
Object.assign(dep, readPrefs(), {
  colIndex: new Map(),
  view: { x: 0, y: 0, k: 1 },
  viewFocus: null,
  size: { w: 0, h: 0 },
  drag: null,
  moved: false,
});

function renderPickers() {
  const show = (id, value) => {
    const box = $(id);
    box.textContent = value || box.dataset.empty;
    box.classList.toggle("empty", !value);
    box.title = value || "";
  };
  show("dep-folder", dep.folder);
  show("dep-home", dep.home);
  $("dep-home-clear").hidden = !dep.home;
  $("dep-scan").textContent = dep.folder ? "Quét lại" : "Chọn thư mục và quét";
}

function setLineage(data) {
  dep.loaded = true;
  dep.data = data.scanned ? data : null;
  dep.up = new Map();
  dep.down = new Map();
  dep.stmts = new Map();
  dep.colsByTarget = new Map();
  dep.colIndex = new Map();
  dep.nodes = new Set();
  if (dep.data) {
    const link = (map, a, b, sid) => {
      if (!map.has(a)) map.set(a, new Map());
      const inner = map.get(a);
      if (!inner.has(b)) inner.set(b, []);
      if (!inner.get(b).includes(sid)) inner.get(b).push(sid);
    };
    dep.data.edges.forEach(([s, t, sid]) => {
      link(dep.up, t, s, sid);
      link(dep.down, s, t, sid);
      dep.nodes.add(s);
      dep.nodes.add(t);
    });
    dep.data.statements.forEach((st) => {
      dep.stmts.set(st.id, st);
      st.targets.forEach((n) => dep.nodes.add(n));
      st.sources.forEach((n) => dep.nodes.add(n));
    });
    dep.data.columns.forEach(([t, tc, s, sc, sid]) => {
      if (!dep.colsByTarget.has(t)) dep.colsByTarget.set(t, []);
      dep.colsByTarget.get(t).push([tc, s, sc, sid]);
      const key = `${t}\u0000${tc}`;
      if (!dep.colIndex.has(key)) dep.colIndex.set(key, []);
      dep.colIndex.get(key).push([s, sc, sid]);
    });
    if (!dep.folder && !/\(kéo thả/.test(dep.data.folder)) dep.folder = dep.data.folder;
  }
  $("dep-tables").innerHTML = [...dep.nodes]
    .filter(isTable)
    .sort()
    .map((n) => `<option value="${esc(n)}"></option>`)
    .join("");
  renderPickers();
  renderDepStatus();
  renderGraph();
  renderDepDetail();
}

function renderDepStatus() {
  const box = $("dep-status");
  const warn = $("dep-warnings");
  box.classList.remove("bad");
  if (!dep.data) {
    box.textContent = "Chưa quét lần nào.";
    warn.hidden = true;
    renderDepKpis();
    return;
  }
  renderDepKpis();
  const d = dep.data;
  const tables = [...dep.nodes].filter(isTable).length;
  box.textContent = `Đã quét ${d.folder} lúc ${dateText(d.scanned_at)} · ${d.file_count} file chạy được từ Start · ${d.statements.length} câu lệnh · ${tables} bảng · ${d.edges.length} quan hệ${d.ignored ? ` · bỏ qua ${d.ignored} câu không cần (DROP, SET...)` : ""}${d.project_home ? ` · PROJECT_HOME = ${d.project_home}` : ""}.`;
  warn.hidden = !d.warnings.length;
  warn.querySelector("summary").textContent = `${d.warnings.length} cảnh báo khi quét`;
  warn.querySelector("ul").innerHTML = d.warnings
    .map((w) => `<li>${esc(w)}</li>`)
    .join("");
}

// Chua chon bang nao: tu chon bang dang lech (hoac bi bo qua) dau tien, khong co thi bang dau tien
// trong danh sach doi soat co mat trong job, de vao trang la thay so do ngay.
function pickDefaultFocus() {
  // Bang nguoi dung tu chon thi giu nguyen; bang tu chon thi duoc chon lai khi danh sach/ket qua doi.
  if (!dep.data || (dep.focus && dep.nodes.has(dep.focus) && !dep.autoFocus)) return;
  const matched = (deps || [])
    .map((d, i) => (d && d.node && dep.nodes.has(d.node) ? [d.node, statusOf(i)] : null))
    .filter(Boolean);
  const links = (n) => realNeighbors(n, "up").size + realNeighbors(n, "down").size;
  const busiest = matched.reduce((best, m) => (!best || links(m[0]) > links(best[0]) ? m : best), null);
  const pick =
    matched.find(([, st]) => st === "lech") ||
    matched.find(([, st]) => st === "boqua") ||
    busiest;
  let node = pick ? pick[0] : null;
  if (!node) {
    // Danh sach doi soat chua co bang nao trong job: lay bang co nhieu quan he nhat.
    let best = -1;
    dep.nodes.forEach((n) => {
      if (!isTable(n)) return;
      const score = realNeighbors(n, "up").size + realNeighbors(n, "down").size;
      if (score > best) [best, node] = [score, n];
    });
  }
  if (node && node !== dep.focus) {
    dep.focus = node;
    dep.edge = null;
    $("dep-search").value = node;
  }
  if (node) dep.autoFocus = true;
}

// Danh sach bang hoac ket qua doi soat vua doi: chon lai bang tu dong neu dang o trang Phu thuoc.
function refreshAutoFocus() {
  if (!dep.loaded || !dep.autoFocus || location.hash !== "#phu-thuoc") return;
  const before = dep.focus;
  pickDefaultFocus();
  if (dep.focus !== before) {
    renderGraph();
    renderDepDetail();
  }
}

async function loadLineage(force = false) {
  renderPickers();
  if (dep.loaded && !force) {
    pickDefaultFocus();
    renderGraph();
    renderDepDetail();
    return;
  }
  try {
    const data = await api("/api/lineage");
    if (!deps) await refreshDeps();
    dep.loaded = true;
    dep.data = data.scanned ? data : null;
    setLineage(data);
    pickDefaultFocus();
    renderGraph();
    renderDepDetail();
  } catch (err) {
    $("dep-status").textContent = err.message;
    $("dep-status").classList.add("bad");
  }
}

let scanning = false;

function setScanBusy(busy) {
  scanning = busy;
  ["dep-scan", "dep-folder", "dep-home", "dep-home-clear"].forEach(
    (id) => ($(id).disabled = busy),
  );
  $("dep-drop").classList.toggle("busy", busy);
  $("dep-progress").hidden = !busy;
}

function showScanProgress(text, share) {
  $("dep-progress").hidden = false;
  $("dep-progress").querySelector("p").textContent = text;
  const bar = $("dep-progress").querySelector("i");
  bar.style.width = share === null ? "" : `${Math.max(2, share * 100)}%`;
  bar.classList.toggle("indeterminate", share === null);
}

function scanProgressText(p) {
  if (p.phase === "index")
    return [`Đang liệt kê file: tìm thấy ${p.files_found || 0} file .hwf/.hpl/.sql${p.current ? ` · ${p.current}` : ""}`, null];
  if (p.phase === "scan")
    return [
      `Đang đọc job: ${p.roots_done || 0}/${p.roots_total || 0} workflow gốc · đã đọc ${p.files || 0} file · ${p.statements || 0} câu lệnh${p.current ? ` · ${p.current}` : ""}`,
      p.roots_total ? p.roots_done / p.roots_total : null,
    ];
  if (p.phase === "build") return ["Đang dựng sơ đồ phụ thuộc...", 1];
  if (p.phase === "save") return ["Đang lưu vào doi_soat.db...", 1];
  return ["Đang quét...", null];
}

async function followScan(id) {
  for (;;) {
    let body;
    try {
      body = await api(`/api/lineage/scan/${id}`);
    } catch (err) {
      throw new Error(`Mất kết nối khi quét: ${err.message}`);
    }
    if (body.state === "error") throw new Error(body.error);
    if (body.state === "done") return;
    showScanProgress(...scanProgressText(body.progress || {}));
    await new Promise((r) => setTimeout(r, 400));
  }
}

async function runScan(start) {
  if (scanning) return;
  setScanBusy(true);
  $("dep-status").classList.remove("bad");
  $("dep-status").textContent = "";
  try {
    const { id } = await start();
    await followScan(id);
    await loadLineage(true);
    await refreshDeps();
    renderGraph();
  } catch (err) {
    $("dep-status").textContent = err.message;
    $("dep-status").classList.add("bad");
  }
  setScanBusy(false);
}

function scanLineage() {
  if (!dep.folder) {
    pickFolder("folder");
    return;
  }
  runScan(() =>
    postJson("/api/lineage/scan", { folder: dep.folder, project_home: dep.home || "" }),
  );
}

// Mo hop thoai chon thu muc cua Windows (tool chay tren chinh may nay).
async function pickFolder(which) {
  if (scanning) return;
  const button = $(which === "folder" ? "dep-folder" : "dep-home");
  button.disabled = true;
  $("dep-status").classList.remove("bad");
  try {
    const body = await postJson("/api/pick-folder", {
      initial: (which === "folder" ? dep.folder : dep.home) || dep.folder || "",
    });
    if (body.folder) {
      if (which === "folder") dep.folder = body.folder;
      else dep.home = body.folder;
      writePrefs();
      renderPickers();
      if (which === "folder") scanLineage();
    }
  } catch (err) {
    $("dep-status").textContent = err.message;
    $("dep-status").classList.add("bad");
  }
  if (!scanning) button.disabled = false;
}

const HOP_FILE_RE = /\.(hwf|hpl|sql)$/i;

// Doc het file .hwf/.hpl/.sql trong thu muc keo tha (ke ca thu muc con).
async function readDroppedFolder(entry) {
  const files = [];
  let listed = 0;
  const readBatch = (reader) =>
    new Promise((resolve, reject) => reader.readEntries(resolve, reject));
  const walk = async (item, prefix) => {
    if (item.isFile) {
      listed += 1;
      if (!HOP_FILE_RE.test(item.name)) return;
      const file = await new Promise((resolve, reject) => item.file(resolve, reject));
      files.push({ path: prefix + item.name, content: await file.text() });
      showScanProgress(`Đang đọc thư mục kéo thả: ${files.length} file job (đã xem ${listed} file) · ${prefix}${item.name}`, null);
      return;
    }
    const reader = item.createReader();
    for (;;) {
      const batch = await readBatch(reader);
      if (!batch.length) break;
      for (const child of batch) await walk(child, `${prefix}${item.name}/`);
    }
  };
  const reader = entry.createReader();
  for (;;) {
    const batch = await readBatch(reader);
    if (!batch.length) break;
    for (const child of batch) await walk(child, "");
  }
  return files;
}

function dropScan(entry) {
  if (!entry || !entry.isDirectory) {
    $("dep-status").textContent = "Hãy kéo thả cả thư mục (không phải từng file).";
    $("dep-status").classList.add("bad");
    return;
  }
  runScan(async () => {
    showScanProgress("Đang đọc thư mục kéo thả...", null);
    const files = await readDroppedFolder(entry);
    if (!files.length) throw new Error(`Thư mục ${entry.name} không có file .hwf/.hpl nào.`);
    showScanProgress(`Đang gửi ${files.length} file lên công cụ...`, null);
    return postJson("/api/lineage/upload", {
      name: entry.name,
      files,
      project_home: dep.home || "",
    });
  });
}

// ---------- quan he bang / truong (di xuyen qua view tam va location) ----------

// Bang that gan nhat theo mot huong: Map bang -> { via: [nut trung gian], sids: [cau lenh] }.
function realNeighbors(start, direction) {
  const links = direction === "up" ? dep.up : dep.down;
  const found = new Map();
  const seen = new Set([start]);
  const queue = [[start, [], []]];
  while (queue.length) {
    const [node, via, sids] = queue.shift();
    (links.get(node) || new Map()).forEach((ids, next) => {
      if (seen.has(next)) return;
      seen.add(next);
      const allIds = [...sids, ...ids];
      if (isTable(next)) {
        if (!found.has(next)) found.set(next, { via, sids: allIds });
      } else queue.push([next, [...via, next], allIds]);
    });
  }
  return found;
}

// Truong cua bang target lay tu truong nao cua cac bang that: Map bang -> [{tc, sc, sids}].
function fieldsOf(target) {
  const result = new Map();
  const add = (table, tc, sc, sids) => {
    if (!result.has(table)) result.set(table, []);
    const list = result.get(table);
    const hit = list.find((f) => f.tc === tc && f.sc === sc);
    if (hit) sids.forEach((id) => hit.sids.includes(id) || hit.sids.push(id));
    else list.push({ tc, sc, sids: [...sids] });
  };
  const resolve = (node, col, tc, sids, depth, seen) => {
    if (isTable(node) || depth > 30) {
      add(node, tc, col, sids);
      return;
    }
    const key = `${node}\u0000${col}`;
    if (seen.has(key)) return;
    seen.add(key);
    const ups = [...(dep.colIndex.get(key) || [])];
    if (col !== "*")
      (dep.colIndex.get(`${node}\u0000*`) || []).forEach(([s, sc, sid]) =>
        ups.push([s, sc === "*" ? col : sc, sid]),
      );
    if (!ups.length) add(node, tc, col, sids);
    ups.forEach(([s, sc, sid]) => resolve(s, sc, tc, [...sids, sid], depth + 1, seen));
  };
  (dep.colsByTarget.get(target) || []).forEach(([tc, s, sc, sid]) =>
    resolve(s, sc, tc, [sid], 0, new Set()),
  );
  result.forEach((list) =>
    list.sort((a, b) => a.tc.localeCompare(b.tc) || a.sc.localeCompare(b.sc)),
  );
  return result;
}

// ---------- so do: chi bang lien quan truc tiep ----------

const G = { w: 264, h: 64, gapX: 150, gapY: 16, pad: 28, head: 34 };
// Moi schema mot mau de nhin la phan biet duoc bang thuoc vung du lieu nao.
const STATUS_COLORS = {
  ok: "#12a150", lech: "#ef0032", trong: "#e8a100", boqua: "#8d76c9", loi: "#131a2e",
  chay: "#ff9db0", cho: "#ffffff", dung: "#b5b8c2",
};
const STATUS_TEXT = {
  ok: "Khớp", lech: "Lệch", trong: "0 dòng", boqua: "Bỏ qua", loi: "Lỗi",
  chay: "Đang chạy", cho: "Chưa chạy", dung: "Đã dừng", out: "Không có trong danh sách đối soát",
};


function shorten(text, max) {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function applyView() {
  const g = $("dep-graph").querySelector(".viewport");
  if (g)
    g.setAttribute("transform", `translate(${dep.view.x},${dep.view.y}) scale(${dep.view.k})`);
  const zoom = $("dep-zoom-level");
  if (zoom) zoom.textContent = `${Math.round(dep.view.k * 100)}%`;
}

function fitView() {
  const box = $("dep-graph");
  const cw = box.clientWidth || 800;
  const ch = box.clientHeight || 500;
  const { w, h } = dep.size;
  if (!w || !h) return;
  const k = Math.min(cw / w, ch / h, 1.1);
  dep.view = { k, x: (cw - w * k) / 2, y: Math.max(0, (ch - h * k) / 2) };
  applyView();
}

function zoomAt(factor, px, py) {
  const k = Math.min(3, Math.max(0.08, dep.view.k * factor));
  const ratio = k / dep.view.k;
  dep.view = { k, x: px - (px - dep.view.x) * ratio, y: py - (py - dep.view.y) * ratio };
  applyView();
}

function nodeSvg(n, x, y, focus) {
  const [ns, name] = nodeLabel(n);
  const status = nodeStatus(n);
  // Vach ben trai the the hien ket qua doi soat; bang khong co trong danh sach thi mau be.
  const color = STATUS_COLORS[status] && status !== "cho" ? STATUS_COLORS[status] : status === "cho" ? "#131a2e" : "#d8c9ac";
  const row = nodeRow(n);
  const more = [];
  if (!focus) {
    const ups = realNeighbors(n, "up").size;
    const downs = realNeighbors(n, "down").size;
    if (ups) more.push(`${ups} nguồn`);
    if (downs) more.push(`${downs} phụ thuộc`);
  }
  const meta = focus
    ? row >= 0 ? `Dòng ${row + 1} trong danh sách đối soát` : "Không có trong danh sách đối soát"
    : more.length ? more.join(" · ") : "không có quan hệ khác";
  const tag =
    status === "out" || status === "temp"
      ? ""
      : (() => {
          // Nhan trang thai gon: cao 15px, chu 9.5px, vua voi dong ten schema ben trai.
          const tw = STATUS_TEXT[status].length * 5.4 + 10;
          return `<g class="stag" transform="translate(${G.w - 10},9)"><rect x="${-tw}" width="${tw}" height="15" rx="7.5" style="fill:${color}"/><text x="${-tw / 2}" y="11">${esc(STATUS_TEXT[status])}</text></g>`;
        })();
  return `<g class="gnode${focus ? " focus" : ""} st-${status}" data-node="${esc(n)}" transform="translate(${x},${y})" tabindex="0" style="--bar:${color}"><title>${esc(n)} · ${esc(STATUS_TEXT[status] || "")}${focus ? "" : " · bấm để xem bảng này"}</title>${focus ? `<rect class="halo" x="-5" y="-5" width="${G.w + 10}" height="${G.h + 10}" rx="16"/>` : ""}<rect class="card" width="${G.w}" height="${G.h}" rx="12"/><rect class="bar" width="6" height="${G.h}" rx="3"/>${tag}<text x="18" y="20" class="ns">${esc(shorten(ns || "(không schema)", 36))}</text><text x="18" y="39" class="nm">${esc(shorten(name, 30))}</text><text x="18" y="55" class="meta">${esc(shorten(meta, 42))}</text></g>`;
}

function renderGraph() {
  const box = $("dep-graph");
  $("dep-tools").hidden = true;
  $("dep-graph-note").hidden = true;
  if (!dep.data) {
    box.innerHTML = '<p class="no-match">Chưa có dữ liệu, quét job Hop trước.</p>';
    return;
  }
  const focus = dep.focus;
  if (!focus || !dep.nodes.has(focus)) {
    box.innerHTML = `<p class="no-match">${focus ? `Không có bảng ${esc(focus)} trong job đã quét.` : "Gõ tên bảng ở ô bên trên để xem bảng đó được tổng hợp từ bảng nào và bảng nào dùng nó."}</p>`;
    return;
  }
  const sortNear = (map) => [...map].sort(([a], [b]) => a.localeCompare(b));
  const ups = sortNear(realNeighbors(focus, "up"));
  const downs = sortNear(realNeighbors(focus, "down"));
  const colH = (n) => Math.max(1, n) * G.h + (Math.max(1, n) - 1) * G.gapY;
  const tallest = Math.max(ups.length, downs.length, 1);
  const width = G.w * 3 + G.gapX * 2 + G.pad * 2;
  const height = colH(tallest) + G.pad * 2 + G.head;
  dep.size = { w: width, h: height };
  const colX = [G.pad, G.pad + G.w + G.gapX, G.pad + 2 * (G.w + G.gapX)];
  const top = (n) => G.pad + G.head + (colH(tallest) - colH(n)) / 2;
  const fx = colX[1];
  const fy = top(1);

  const upFields = fieldsOf(focus);
  const parts = [];
  const edge = (s, t, info, side, x1, y1, x2, y2, fields) => {
    const a = [x1, y1];
    const b = [x2, y2];
    const mid = (a[0] + b[0]) / 2;
    const d = `M${a[0]},${a[1]} C${mid},${a[1]} ${mid},${b[1]} ${b[0] - 3},${b[1]}`;
    const picked = dep.edge && dep.edge[0] === s && dep.edge[1] === t;
    // Chi ghi ten view tam tren nhan; location chi hien trong chu thich khi re chuot.
    const temps = info.via.filter((n) => nodeKind(n) === "temp");
    const via = temps.length
      ? `qua ${temps.length === 1 ? nodeLabel(temps[0]).join(" ") : `${temps.length} view tạm`} · `
      : "";
    const count = fields.some((f) => f.tc === "*" || f.sc === "*")
      ? "mọi trường"
      : fields.length ? `${fields.length} trường` : "cả bảng";
    const label = shorten(`${via}${count}`, 36);
    const lw = label.length * 6.2 + 16;
    // Nhan dat giua doan cong, lech ve phia bang xung quanh de khong de len nhau o giua.
    const t2 = side === "up" ? 0.4 : 0.6;
    const u = 1 - t2;
    const lx = a[0] * u ** 3 + mid * 3 * u * t2 + b[0] * t2 ** 3;
    const ly = a[1] + (b[1] - a[1]) * (3 * t2 * t2 - 2 * t2 ** 3);
    parts.push(
      `<g class="gedge side-${side}${info.via.length ? " via" : ""}${picked ? " picked" : ""}" data-edge-from="${esc(s)}" data-edge-to="${esc(t)}"><title>${esc(s)} → ${esc(t)}${info.via.length ? ` (qua ${esc(info.via.join(" → "))})` : ""} · bấm để xem trường và câu lệnh</title><path class="hit" d="${d}"/><path class="line" d="${d}" marker-end="url(#arrow-${picked ? "picked" : side})"/><g class="elabel" transform="translate(${lx - lw / 2},${ly - 10})"><rect width="${lw}" height="20" rx="10"/><text x="${lw / 2}" y="14">${esc(label)}</text></g></g>`,
    );
  };
  ups.forEach(([n, info], k) => {
    const y = top(ups.length) + k * (G.h + G.gapY);
    edge(n, focus, info, "up", colX[0] + G.w, y + G.h / 2, fx, fy + G.h / 2, upFields.get(n) || []);
  });
  downs.forEach(([n, info], k) => {
    const y = top(downs.length) + k * (G.h + G.gapY);
    edge(focus, n, info, "down", fx + G.w, fy + G.h / 2, colX[2], y + G.h / 2, fieldsOf(n).get(focus) || []);
  });
  ups.forEach(([n], k) => parts.push(nodeSvg(n, colX[0], top(ups.length) + k * (G.h + G.gapY), false)));
  downs.forEach(([n], k) => parts.push(nodeSvg(n, colX[2], top(downs.length) + k * (G.h + G.gapY), false)));
  parts.push(nodeSvg(focus, fx, fy, true));
  const empty = (x, text) =>
    `<text class="empty" x="${x + G.w / 2}" y="${top(1) + G.h / 2 + 4}">${text}</text>`;
  if (!ups.length) parts.push(empty(colX[0], "Không có bảng nguồn"));
  if (!downs.length) parts.push(empty(colX[2], "Không bảng nào dùng bảng này"));
  const heads = [
    [colX[0], `BẢNG NGUỒN · ${ups.length}`, "up"],
    [colX[1], "ĐANG XEM", "focus"],
    [colX[2], `BẢNG PHỤ THUỘC · ${downs.length}`, "down"],
  ]
    .map(([x, text, cls]) => `<text class="colhead ${cls}" x="${x + G.w / 2}" y="${G.pad + 12}">${text}</text>`)
    .join("");
  const marker = (id) =>
    `<marker id="arrow-${id}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" class="arrow ${id}"/></marker>`;
  const note = $("dep-graph-note");
  note.hidden = false;
  note.innerHTML = `<span class="gn-table">${tableLabel(focus)}</span><span class="gn-chip up"><i></i>${ups.length} bảng nguồn trực tiếp</span><span class="gn-chip down"><i></i>${downs.length} bảng phụ thuộc trực tiếp</span>`;
  box.innerHTML = `<svg class="graph-svg" role="img" aria-label="Sơ đồ phụ thuộc của ${esc(focus)}"><defs>${marker("up")}${marker("down")}${marker("picked")}</defs><g class="viewport">${heads}${parts.join("")}</g></svg>`;
  $("dep-tools").hidden = false;
  if (dep.viewFocus !== focus) {
    dep.viewFocus = focus;
    fitView();
  } else applyView();
}

// ---------- chi tiet bang ----------

function stmtLine(id, open = false) {
  const st = dep.stmts.get(id);
  if (!st) return "";
  return `<details class="stmt"${open ? " open" : ""}><summary><span class="kind kind-${esc(st.kind.toLowerCase())}">${esc(KIND_TEXT[st.kind] || st.kind)}</span><span class="where">${esc(st.file)} › ${esc(st.step)}${st.step_type ? ` <small class="muted">(${esc(st.step_type)})</small>` : ""}</span>${st.partial ? '<span class="role warn">phân tích một phần</span>' : ""}<span class="stmt-id muted">#${st.id}</span></summary><p class="chain small muted">${esc(st.chain)}</p><div class="sql-block"><pre>${highlightSql(st.sql)}</pre><button class="copy" type="button" data-copy-stmt="${st.id}">Chép</button></div></details>`;
}

// Mot nhom "bang X": dong tom tat, bam mo ra cac truong va cau lenh tao quan he.
function relationGroup(table, info, fields, mode, open) {
  const via = info.via.length
    ? `<span class="via" title="${esc(info.via.join(" → "))}">qua ${info.via.length === 1 ? esc(nodeLabel(info.via[0]).join(" ")) : `${info.via.length} bước trung gian`}</span>`
    : "";
  const status = nodeStatus(table);
  const fieldText = fields.length
    ? `${fields.length} trường`
    : "không rõ trường (dùng để lọc/join hoặc ghi cả bảng)";
  const sids = [...new Set([...info.sids, ...fields.flatMap((f) => f.sids)])];
  const rowsHtml = fields
    .map((f) => {
      const [left, right] =
        mode === "up" ? [f.tc, f.sc] : [f.sc, f.tc];
      const show = (c) => (c === "*" ? '<span class="muted">mọi trường (SELECT *)</span>' : `<code>${esc(c)}</code>`);
      return `<tr><td>${show(left)}</td><td class="arrow-cell">←</td><td>${show(right)}</td></tr>`;
    })
    .join("");
  const head =
    mode === "up"
      ? `<tr><th>Trường của ${esc(nodeLabel(dep.focus)[1])}</th><th></th><th>Lấy từ trường của ${esc(nodeLabel(table)[1])}</th></tr>`
      : `<tr><th>Trường của ${esc(nodeLabel(table)[1])}</th><th></th><th>Lấy từ trường của ${esc(nodeLabel(dep.focus)[1])}</th></tr>`;
  return `<details class="rel"${open ? " open" : ""}><summary><i class="sw st-${status}"></i><span class="rel-name">${tableLabel(table)}</span>${via}<span class="rel-count muted">${fieldText} · ${sids.length} câu lệnh</span><button class="btn small" type="button" data-focus="${esc(table)}">Xem sơ đồ bảng này</button></summary>
    ${fields.length ? `<table class="metrics fields"><thead>${head}</thead><tbody>${rowsHtml}</tbody></table>` : ""}
    <div class="rel-stmts"><h3>Câu lệnh tạo quan hệ</h3>${sids.map(stmtLine).join("")}</div>
  </details>`;
}

function renderDepDetail() {
  const panel = $("dep-detail");
  const n = dep.focus;
  if (!dep.data || !n || !dep.nodes.has(n)) {
    panel.hidden = true;
    return;
  }
  panel.hidden = false;
  const row = nodeRow(n);
  $("dep-detail-title").textContent = n;
  $("dep-detail-chips").innerHTML =
    row >= 0
      ? `<span class="chip static">Dòng ${row + 1} trong danh sách đối soát</span>`
      : '<span class="chip static muted-chip">Không có trong danh sách đối soát</span>';

  const upFields = fieldsOf(n);
  const ups = realNeighbors(n, "up");
  const downs = realNeighbors(n, "down");
  const pickedSource = dep.edge && dep.edge[1] === n ? dep.edge[0] : null;
  const pickedTarget = dep.edge && dep.edge[0] === n ? dep.edge[1] : null;
  const upHtml = [...ups]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([t, info]) =>
      relationGroup(t, info, upFields.get(t) || [], "up", t === pickedSource || info.via.includes(pickedSource)),
    )
    .join("");
  const downHtml = [...downs]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([t, info]) =>
      relationGroup(t, info, fieldsOf(t).get(n) || [], "down", t === pickedTarget || info.via.includes(pickedTarget)),
    )
    .join("");
  const own = [...dep.stmts.values()].filter(
    (st) => st.targets.includes(n) || st.sources.includes(n),
  );
  // Cau CREATE cua bang (neu job Hop co tao bang): hien san SQL o dau phan chi tiet.
  const creates = own.filter((st) => st.kind === "CREATE" && st.targets.includes(n));
  const createHtml = creates.length
    ? `<section class="rel-section rel-create"><h3><i></i>Câu lệnh tạo bảng${creates.length > 1 ? ` (${creates.length} câu)` : ""}</h3>${creates.map((st) => stmtLine(st.id, true)).join("")}</section>`
    : '<section class="rel-section rel-create"><h3><i></i>Câu lệnh tạo bảng</h3><p class="dash">Không thấy câu CREATE của bảng này trong job đã quét (bảng có thể được tạo ở nơi khác).</p></section>';
  $("dep-detail-body").innerHTML = `
    ${createHtml}
    <section class="rel-section rel-up"><h3><i></i>Được tổng hợp từ ${ups.size} bảng</h3>${upHtml || '<p class="dash">Không có bảng nguồn nào trong job đã quét.</p>'}</section>
    <section class="rel-section rel-down"><h3><i></i>${downs.size} bảng phụ thuộc vào bảng này</h3>${downHtml || '<p class="dash">Không có bảng nào dùng bảng này.</p>'}</section>
    <details class="rel-section all-stmts"><summary>Tất cả ${own.length} câu lệnh có bảng này</summary>${own.map((st) => stmtLine(st.id)).join("")}</details>`;
}

// ---------- su kien ----------

$("dep-folder").addEventListener("click", () => pickFolder("folder"));
$("dep-home").addEventListener("click", () => pickFolder("home"));
$("dep-home-clear").addEventListener("click", () => {
  dep.home = "";
  writePrefs();
  renderPickers();
});
$("dep-scan").addEventListener("click", scanLineage);
const depDrop = $("dep-drop");
["dragenter", "dragover"].forEach((name) =>
  depDrop.addEventListener(name, (event) => {
    event.preventDefault();
    if (!scanning) depDrop.classList.add("dragging");
  }),
);
depDrop.addEventListener("dragleave", (event) => {
  if (!depDrop.contains(event.relatedTarget)) depDrop.classList.remove("dragging");
});
depDrop.addEventListener("drop", (event) => {
  event.preventDefault();
  depDrop.classList.remove("dragging");
  if (scanning) return;
  // Phai lay entry ngay trong su kien drop, sau await trinh duyet xoa du lieu keo tha.
  const item = [...event.dataTransfer.items].find((x) => x.kind === "file");
  dropScan(item && item.webkitGetAsEntry ? item.webkitGetAsEntry() : null);
});
// Tha file/thu muc ra ngoai vung nhan thi trinh duyet se mo file do: chan lai.
["dragover", "drop"].forEach((name) =>
  addEventListener(name, (event) => {
    if (!event.defaultPrevented) event.preventDefault();
  }),
);

$("dep-search").addEventListener("change", () => {
  const value = $("dep-search").value.trim().toLowerCase();
  if (!value) return;
  dep.focus = value;
  dep.autoFocus = false;
  dep.edge = null;
  renderGraph();
  renderDepDetail();
});

// Keo de di chuyen, lan chuot de phong to quanh vi tri con tro.
const graphBox = $("dep-graph");
graphBox.addEventListener("pointerdown", (event) => {
  if (event.button !== 0 || !graphBox.querySelector(".viewport")) return;
  dep.drag = { x: event.clientX, y: event.clientY, vx: dep.view.x, vy: dep.view.y };
  dep.moved = false;
});
addEventListener("pointermove", (event) => {
  if (!dep.drag) return;
  const dx = event.clientX - dep.drag.x;
  const dy = event.clientY - dep.drag.y;
  if (!dep.moved && Math.hypot(dx, dy) < 4) return;
  dep.moved = true;
  graphBox.classList.add("panning");
  dep.view.x = dep.drag.vx + dx;
  dep.view.y = dep.drag.vy + dy;
  applyView();
});
addEventListener("pointerup", () => {
  dep.drag = null;
  graphBox.classList.remove("panning");
});
graphBox.addEventListener(
  "wheel",
  (event) => {
    if (!graphBox.querySelector(".viewport")) return;
    event.preventDefault();
    const rect = graphBox.getBoundingClientRect();
    zoomAt(Math.exp(-event.deltaY * 0.0015), event.clientX - rect.left, event.clientY - rect.top);
  },
  { passive: false },
);
$("dep-tools").addEventListener("click", (event) => {
  const button = event.target.closest("[data-zoom]");
  if (!button) return;
  const w = graphBox.clientWidth / 2;
  const h = graphBox.clientHeight / 2;
  if (button.dataset.zoom === "in") zoomAt(1.25, w, h);
  else if (button.dataset.zoom === "out") zoomAt(0.8, w, h);
  else fitView();
});

graphBox.addEventListener("click", (event) => {
  if (dep.moved) {
    dep.moved = false;
    return;
  }
  const node = event.target.closest("[data-node]");
  if (node) {
    dep.focus = node.dataset.node;
    dep.autoFocus = false;
    dep.edge = null;
    $("dep-search").value = dep.focus;
    renderGraph();
    renderDepDetail();
    return;
  }
  const edge = event.target.closest("[data-edge-from]");
  if (edge) {
    // Bam canh: mo dung nhom bang tuong ung trong phan chi tiet cua bang dang xem.
    dep.edge = [edge.dataset.edgeFrom, edge.dataset.edgeTo];
    if (dep.edge[0] !== dep.focus && dep.edge[1] !== dep.focus)
      dep.focus = isTable(dep.edge[1]) ? dep.edge[1] : dep.edge[0];
      dep.autoFocus = false;
    $("dep-search").value = dep.focus;
    renderGraph();
    renderDepDetail();
    const open = $("dep-detail").querySelector("details.rel[open]");
    (open || $("dep-detail")).scrollIntoView({ behavior: "smooth", block: "start" });
  }
});
graphBox.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && event.target.matches("[data-node]"))
    event.target.dispatchEvent(new MouseEvent("click", { bubbles: true }));
});
$("dep-detail").addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button) return;
  if (button.dataset.copyStmt !== undefined) {
    const st = dep.stmts.get(Number(button.dataset.copyStmt));
    navigator.clipboard.writeText(st.sql).then(() => (button.textContent = "Đã chép"));
  } else if (button.dataset.focus !== undefined) {
    event.preventDefault();
    dep.focus = button.dataset.focus;
    dep.autoFocus = false;
    dep.edge = null;
    $("dep-search").value = dep.focus;
    renderGraph();
    renderDepDetail();
    $("dep-graph").scrollIntoView({ behavior: "smooth", block: "center" });
  }
});
addEventListener("resize", () => {
  if (location.hash === "#phu-thuoc" && dep.focus) applyView();
});

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
  } else if (button.dataset.depOpen !== undefined) {
    openDependency(button.dataset.depOpen);
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
$("dependency_gate").addEventListener("change", () => {
  updateScope();
  saveSoon();
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
