function esc(v) {
  return String(v ?? "").replace(/[&<>"']/g, s => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[s]));
}

function fmtTime(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  const pad = n => String(n).padStart(2, "0");
  return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function fmtAgo(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "";
  const s = Math.max(0, Math.floor((Date.now() - d.getTime()) / 1000));
  if (s < 60) return `${s}s前`;
  if (s < 3600) return `${Math.floor(s / 60)}m前`;
  if (s < 86400) return `${Math.floor(s / 3600)}h${Math.floor((s % 3600) / 60)}m前`;
  return `${Math.floor(s / 86400)}d前`;
}

function fmtDurSec(s) {
  if (s == null) return "-";
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60), rem = s % 60;
  if (m < 60) return `${m}m ${rem}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

function fmtDurMs(ms) {
  if (ms == null) return "-";
  if (ms < 1000) return `${ms}ms`;
  return fmtDurSec(Math.round(ms / 1000));
}

function statusPill(st) {
  const map = {
    processing: "cyan", active: "cyan", in_progress: "cyan", develop: "cyan",
    done: "green", merged: "green", sent: "green", ready: "green", pass: "green", emit: "green", ok: "green", info: "blue",
    pending: "amber", suspended: "amber", suspend: "amber", pr_open: "blue", backlog: "gray", warn: "amber", warning: "amber",
    review: "purple", review_pending: "purple", no_reviewer: "purple", needs_input: "amber", user: "blue",
    failed: "red", error: "red", critical: "red", cancelled: "gray", dropped: "gray", closed: "gray",
  };
  return `<span class="pill ${map[st] || "gray"}">${esc(st || "-")}</span>`;
}

function openDrawer(title, html) {
  document.getElementById("drawerTitle").textContent = title;
  document.getElementById("drawerBody").innerHTML = html;
  document.getElementById("drawerBackdrop").classList.add("open");
}

function closeDrawer() {
  document.getElementById("drawerBackdrop").classList.remove("open");
}

function bindDrawerEvents() {
  const closeBtn = document.getElementById("drawerClose");
  const backdrop = document.getElementById("drawerBackdrop");
  if (closeBtn) closeBtn.addEventListener("click", closeDrawer);
  if (backdrop) {
    backdrop.addEventListener("click", e => {
      if (e.target === backdrop) closeDrawer();
    });
  }
  document.addEventListener("keydown", e => {
    if (e.key === "Escape") closeDrawer();
  });
}

async function openRunDetail(sessionId) {
  openDrawer(`Run 详情 · ${sessionId}`, `<div class="empty">加载中...</div>`);
  try {
    const res = await fetch(`/api/run/${encodeURIComponent(sessionId)}`);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "not found");
    const sj = data.state_json || {};
    const inner = sj.state || {};
    let html = `
      <div class="pill-row" style="margin-bottom:10px;">
        ${statusPill(sj.phase)} ${inner.stage ? statusPill(inner.stage) : ""}
        <span class="pill gray">turns: ${(data.turns || []).length}</span>
        <span class="pill gray">handbacks: ${inner.handbacks || 0}</span>
      </div>
      <div class="mono" style="color:var(--text-dim);margin-bottom:12px;">Workdir: ${esc(inner.workdir || "-")}</div>
    `;
    if (inner.summary) html += `<div class="drawer-sec-title">最新 Summary</div><pre class="code-block">${esc(inner.summary)}</pre>`;
    if (sj.prompt) html += `<div class="drawer-sec-title">当前 Prompt</div><pre class="code-block">${esc(sj.prompt)}</pre>`;
    if (inner.requirements) html += `<div class="drawer-sec-title">原始需求 Requirements</div><pre class="code-block">${esc(inner.requirements)}</pre>`;
    if (inner.review_note) html += `<div class="drawer-sec-title">评审意见 Review Note</div><pre class="code-block">${esc(inner.review_note)}</pre>`;
    openDrawer(`Run 详情 · ${sessionId}`, html);
  } catch (err) {
    openDrawer(`Run 详情 · ${sessionId}`, `<div class="empty" style="color:#fca5a5;">加载失败: ${esc(err.message)}</div>`);
  }
}

async function openOutboxFile(filename) {
  openDrawer(`Outbox · ${filename}`, `<div class="empty">加载中...</div>`);
  try {
    const res = await fetch(`/api/outbox/${encodeURIComponent(filename)}`);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "not found");
    openDrawer(`Outbox · ${filename}`, `<pre class="code-block">${esc(data.content)}</pre>`);
  } catch (err) {
    openDrawer(`Outbox · ${filename}`, `<div class="empty" style="color:#fca5a5;">加载失败: ${esc(err.message)}</div>`);
  }
}

function startPolling(onSnapshot) {
  let timer = null;
  async function load(force = false) {
    try {
      const res = await fetch(`/api/snapshot${force ? "?force=1" : ""}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
      document.getElementById("lastUpdated").textContent = `更新于 ${fmtTime(data.generated_at)}`;
      const allOk = data.sb.health?.ok && data.tb.health?.ok && Boolean(data.hb.daemon);
      document.getElementById("liveDot").classList.toggle("err", !allOk);
      onSnapshot(data);
    } catch (err) {
      document.getElementById("liveDot").classList.add("err");
      document.getElementById("lastUpdated").textContent = `刷新失败: ${err.message}`;
    }
  }
  function setupTimer() {
    if (timer) clearInterval(timer);
    const ms = Number(document.getElementById("pollInterval").value);
    if (ms > 0) timer = setInterval(() => load(false), ms);
  }
  document.getElementById("refreshBtn").addEventListener("click", () => load(true));
  document.getElementById("pollInterval").addEventListener("change", setupTimer);
  load(true);
  setupTimer();
  return { reload: () => load(true) };
}
