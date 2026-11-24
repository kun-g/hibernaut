from __future__ import annotations

import base64
import hashlib

_STYLE = """
    :root { --bg:#FAF9F6; --card:#FFF; --subtle:#F2EFE9; --text:#1B1A18; --muted:#5F5B53; --border:#E5E0D6; --accent:#D95D24; --bad:#B42318; }
    * { box-sizing:border-box; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:"Inter",-apple-system,BlinkMacSystemFont,"PingFang SC",sans-serif; font-size:14px; line-height:1.55; }
    [hidden] { display:none !important; }
    .wrap { max-width:1080px; margin:0 auto; padding:28px 20px 64px; }
    header { display:flex; justify-content:space-between; align-items:flex-end; gap:12px; flex-wrap:wrap; border-bottom:1px solid var(--border); padding-bottom:16px; margin-bottom:24px; }
    h1 { margin:0; font-size:22px; }
    .meta { font-family:ui-monospace,SFMono-Regular,monospace; font-size:12px; color:var(--muted); }
    .headline { font-size:16px; font-weight:700; margin-top:4px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:16px; margin:18px 0 24px; }
    .card { background:var(--card); border:1px solid var(--border); border-radius:14px; padding:16px 18px; min-width:0; }
    .card h2 { margin:0 0 10px; font-size:15px; }
    .item { border:1px solid var(--border); background:var(--bg); border-radius:10px; padding:10px 12px; margin-bottom:8px; overflow-wrap:anywhere; }
    .row { display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap; }
    .btn { border:1px solid var(--border); background:var(--card); color:var(--text); padding:5px 10px; border-radius:7px; cursor:pointer; font-size:12px; font-weight:600; }
    .btn.primary { background:var(--accent); color:#fff; border-color:var(--accent); }
    .pill { font-family:ui-monospace,monospace; font-size:11px; padding:2px 7px; border-radius:999px; background:var(--subtle); color:var(--muted); }
    .error { color:var(--bad); }
    pre { white-space:pre-wrap; overflow-wrap:anywhere; max-height:280px; overflow:auto; background:var(--subtle); border-radius:8px; padding:8px; font-size:12px; margin:6px 0; }
    ul { margin:4px 0 8px; padding-left:18px; }
    input { border:1px solid var(--border); border-radius:7px; padding:5px 8px; font-size:13px; min-width:220px; }
"""

# Everything shown was written by agents, so it only ever reaches the page as text nodes.
_SCRIPT = """
    const KEY = 'hibernaut-collaboration-token';
    const $ = id => document.getElementById(id);
    let token = '';
    try { token = sessionStorage.getItem(KEY) || ''; } catch (e) { token = ''; }

    function el(tag, cls, text, ...children) {
      const node = document.createElement(tag);
      if (cls) node.className = cls;
      if (text !== undefined && text !== null) node.textContent = String(text);
      node.append(...children);
      return node;
    }
    function list(items) {
      const node = el('ul');
      for (const item of items) node.append(el('li', '', item));
      return node;
    }
    function fill(id, nodes, empty) { $(id).replaceChildren(...(nodes.length ? nodes : [el('div', 'meta', empty)])); }
    function fail(message) { $('error').textContent = message; $('error').hidden = !message; }

    async function call(path, options) {
      const headers = { 'Content-Type': 'application/json' };
      if (token) headers.Authorization = 'Bearer ' + token;
      const reply = await fetch(path, { ...options, headers, cache: 'no-store' });
      const body = await reply.json().catch(() => ({}));
      if (reply.status === 401) { $('login').hidden = false; throw new Error('需要访问令牌'); }
      if (!reply.ok) throw new Error(body.error || ('HTTP ' + reply.status));
      return body;
    }
    async function act(path, button) {
      button.disabled = true;
      let failed = '';
      try { await call(path, { method: 'POST', body: '{}' }); } catch (e) { failed = e.message; }
      await loadAll();
      if (failed) fail(failed);
    }
    function button(label, cls, path) {
      const node = el('button', cls, label);
      node.type = 'button';
      node.addEventListener('click', () => act(path, node));
      return node;
    }
    function proposalItem(p, full) {
      const node = el('div', 'item', null,
        el('div', 'row', null, el('strong', '', p.target), el('span', 'pill', p.scope)),
        el('div', 'meta', p.reason));
      const detail = full.get(p.proposal_id);
      if (detail) node.append(el('div', 'meta', '提议者 ' + detail.proposed_by), el('pre', '', detail.content));
      if (detail && detail.reviewable) {
        const base = 'api/proposals/' + encodeURIComponent(p.proposal_id);
        node.append(el('div', 'row', null,
          button('批准', 'btn primary', base + '/approve'), button('驳回', 'btn', base + '/reject')));
      }
      return node;
    }
    function closeoutItem(c) {
      const node = el('div', 'item', null,
        el('div', 'row', null, el('strong', '', c.project_key + ' · ' + c.card_id), el('span', 'pill', c.status)),
        el('div', 'meta', c.agent_id + ' · ' + c.created_at), el('div', '', c.summary),
        list(c.decisions.map(d => d.key + ' ' + d.title + '：' + d.choice + (d.rationale ? '（' + d.rationale + '）' : ''))));
      if (c.follow_up_tasks.length) node.append(el('div', 'meta', '后续任务'), list(c.follow_up_tasks));
      for (const m of c.memory_proposals) node.append(el('div', 'meta', '记忆提案 ' + m.target_uri + ' · ' + m.reason), el('pre', '', m.content));
      if (c.status === 'draft' || c.status === 'confirmed') {
        node.append(button(c.status === 'draft' ? '确认并回流' : '继续未完成的回流', 'btn primary',
          'api/closeouts/' + encodeURIComponent(c.card_id) + '/confirm'));
      }
      return node;
    }
    async function loadAll() {
      fail('');
      try {
        const d = await call('api/digest');
        const proposals = await call('api/proposals');
        const full = new Map(proposals.proposals.map(p => [p.proposal_id, p]));
        $('login').hidden = true;
        $('windowLabel').textContent = d.window_label + ' · 更新于 ' + d.generated_at;
        $('headline').textContent = d.headline;
        fill('projectsBox', d.projects.map(p => el('div', 'item', null, el('div', 'row', null, el('strong', '', p.project_key),
          el('span', 'pill', '进行中 ' + p.in_progress + ' · 就绪 ' + p.ready + ' · 完成 ' + p.done_recent + ' · 阻塞 ' + p.blocked)))), '暂无活跃项目');
        fill('proposalsBox', d.pending_proposals.map(p => proposalItem(p, full)), '暂无待审提案');
        fill('closeoutsBox', d.recent_closeouts.map(closeoutItem), '暂无收口卡');
      } catch (e) { fail(e.message); }
    }
    $('login').addEventListener('submit', event => {
      event.preventDefault();
      token = $('token').value.trim();
      $('token').value = '';
      try { sessionStorage.setItem(KEY, token); } catch (e) { fail('浏览器不保存令牌，刷新页面后要重新输入'); }
      loadAll();
    });
    $('refreshBtn').addEventListener('click', loadAll);
    loadAll();
"""

HUB_HTML = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>hub.guokun.ai · 个人协作态势与提案审批</title>
  <style>{_STYLE}</style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <div class="meta">HUB.GUOKUN.AI · ON-DEMAND PULL COMMAND CENTER (D8-B)</div>
        <h1>幕僚长态势简报与待审提案池</h1>
      </div>
      <button class="btn" id="refreshBtn" type="button">刷新态势</button>
    </header>
    <form id="login" class="card" hidden>
      <label class="meta" for="token">访问令牌</label>
      <input id="token" type="password" autocomplete="off" />
      <button class="btn primary" type="submit">进入</button>
    </form>
    <p id="error" class="error" role="alert" hidden></p>
    <div class="card">
      <div class="meta" id="windowLabel">加载中...</div>
      <div class="headline" id="headline"></div>
    </div>
    <div class="grid">
      <section class="card"><h2>跨项目任务与成本</h2><div id="projectsBox"></div></section>
      <section class="card"><h2>待审记忆与角色技能提案</h2><div id="proposalsBox"></div></section>
      <section class="card"><h2>双通道显式收口卡 (CloseoutCard)</h2><div id="closeoutsBox"></div></section>
    </div>
  </div>
  <script>{_SCRIPT}</script>
</body>
</html>
"""


def _source_hash(text: str) -> str:
    return "'sha256-" + base64.b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode("ascii") + "'"


HUB_CSP = (
    f"default-src 'none'; script-src {_source_hash(_SCRIPT)}; style-src {_source_hash(_STYLE)}; connect-src 'self'; "
    "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)
