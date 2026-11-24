from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol, cast
from urllib.parse import urlparse

from collaboration.closeout import CloseoutCoordinator, CloseoutStore
from collaboration.digest import DigestAssembler
from collaboration.expert_pool import ExpertPoolRegistry
from collaboration.role_memory import CASConflictError, RoleSkillMemoryStore

_HUB_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>hub.guokun.ai · 个人协作态势与提案审批</title>
  <style>
    :root { --bg:#FAF9F6; --card:#FFF; --subtle:#F2EFE9; --text:#1B1A18; --muted:#5F5B53; --border:#E5E0D6; --accent:#D95D24; --ok:#15803D; }
    * { box-sizing:border-box; }
    body { margin:0; background:var(--bg); color:var(--text); font-family:"Inter",-apple-system,BlinkMacSystemFont,"PingFang SC",sans-serif; font-size:14px; line-height:1.55; }
    .wrap { max-width:1080px; margin:0 auto; padding:28px 20px 64px; }
    header { display:flex; justify-content:space-between; align-items:flex-end; gap:12px; flex-wrap:wrap; border-bottom:1px solid var(--border); padding-bottom:16px; margin-bottom:24px; }
    h1 { margin:0; font-size:22px; }
    .meta { font-family:ui-monospace,SFMono-Regular,monospace; font-size:12px; color:var(--muted); }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:16px; margin-bottom:24px; }
    .card { background:var(--card); border:1px solid var(--border); border-radius:14px; padding:16px 18px; }
    .card h2 { margin:0 0 10px; font-size:15px; }
    .item { border:1px solid var(--border); background:var(--bg); border-radius:10px; padding:10px 12px; margin-bottom:8px; }
    .row { display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap; }
    .btn { border:1px solid var(--border); background:var(--card); color:var(--text); padding:5px 10px; border-radius:7px; cursor:pointer; font-size:12px; font-weight:600; }
    .btn.primary { background:var(--accent); color:#fff; border-color:var(--accent); }
    .pill { font-family:ui-monospace,monospace; font-size:11px; padding:2px 7px; border-radius:999px; background:var(--subtle); color:var(--muted); }
  </style>
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
    <div class="card" style="margin-bottom:18px;">
      <div class="meta" id="windowLabel">加载中...</div>
      <div id="headline" style="font-size:16px;font-weight:700;margin-top:4px;"></div>
    </div>
    <div class="grid">
      <section class="card"><h2>跨项目任务与成本</h2><div id="projectsBox"></div></section>
      <section class="card"><h2>待审记忆与角色技能提案</h2><div id="proposalsBox"></div></section>
      <section class="card"><h2>双通道显式收口卡 (CloseoutCard)</h2><div id="closeoutsBox"></div></section>
    </div>
  </div>
  <script>
    async function loadAll() {
      const d = await (await fetch('/api/digest')).json();
      document.getElementById('windowLabel').textContent = d.window_label + ' · 更新于 ' + d.generated_at;
      document.getElementById('headline').textContent = d.headline;
      document.getElementById('projectsBox').innerHTML = (d.projects || []).map(p =>
        `<div class="item"><div class="row"><strong>${p.project_key}</strong><span class="pill">进行中 ${p.in_progress} · 就绪 ${p.ready} · 阻塞 ${p.blocked}</span></div></div>`
      ).join('') || '<div class="meta">暂无活跃项目</div>';
      document.getElementById('proposalsBox').innerHTML = (d.pending_proposals || []).map(p =>
        `<div class="item"><div class="row"><strong>${p.target}</strong><span class="pill">${p.scope}</span></div><div class="meta" style="margin:4px 0 8px;">${p.reason}</div><div class="row"><button class="btn primary" onclick="act('/api/proposals/${p.proposal_id}/approve')">批准</button><button class="btn" onclick="act('/api/proposals/${p.proposal_id}/reject')">驳回</button></div></div>`
      ).join('') || '<div class="meta">暂无待审提案</div>';
      document.getElementById('closeoutsBox').innerHTML = (d.recent_closeouts || []).map(c =>
        `<div class="item"><div class="row"><strong>${c.project_key} · ${c.card_id}</strong><span class="pill">${c.status}</span></div><div class="meta" style="margin:4px 0 8px;">${c.summary}</div>${c.status === 'draft' ? `<button class="btn primary" onclick="act('/api/closeouts/${c.card_id}/confirm')">确认并回流</button>` : ''}</div>`
      ).join('') || '<div class="meta">暂无收口卡</div>';
    }
    async function act(url) {
      await fetch(url, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({reviewer:'owner'}) });
      loadAll();
    }
    document.getElementById('refreshBtn').addEventListener('click', loadAll);
    loadAll();
  </script>
</body>
</html>
"""


class HubStateProvider(Protocol):
    def list_tasks(self) -> list[dict[str, Any]]: ...
    def list_runs(self) -> list[dict[str, Any]]: ...
    def list_project_proposals(self) -> list[dict[str, Any]]: ...


class _DefaultEmptyProvider:
    def list_tasks(self) -> list[dict[str, Any]]:
        return []

    def list_runs(self) -> list[dict[str, Any]]:
        return []

    def list_project_proposals(self) -> list[dict[str, Any]]:
        return []


class _HubHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        *,
        role_store: RoleSkillMemoryStore,
        closeout_store: CloseoutStore,
        closeout_coordinator: CloseoutCoordinator,
        expert_pool: ExpertPoolRegistry,
        state_provider: HubStateProvider,
        window_label: str,
    ) -> None:
        self.role_store = role_store
        self.closeout_store = closeout_store
        self.closeout_coordinator = closeout_coordinator
        self.expert_pool = expert_pool
        self.state_provider = state_provider
        self.window_label = window_label
        self.assembler = DigestAssembler()
        super().__init__(address, _HubHandler)


class _HubHandler(BaseHTTPRequestHandler):
    server_version = "HibernautCollaborationHub/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def _srv(self) -> _HubHTTPServer:
        return cast(_HubHTTPServer, self.server)

    def _send_json(self, code: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return cast(dict[str, Any], json.loads(raw))

    def do_GET(self) -> None:
        srv = self._srv()
        path = urlparse(self.path).path

        if path in ("/", "/index.html"):
            body = _HUB_HTML.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/digest":
            snapshot = srv.assembler.assemble(
                window_label=srv.window_label,
                tasks=srv.state_provider.list_tasks(),
                runs=srv.state_provider.list_runs(),
                role_proposals=srv.role_store.list_proposals(),
                project_proposals=srv.state_provider.list_project_proposals(),
                closeouts=srv.closeout_store.list_cards(),
            )
            self._send_json(HTTPStatus.OK, snapshot.to_dict())
            return

        if path == "/api/proposals":
            self._send_json(
                HTTPStatus.OK,
                {"proposals": [p.to_dict() for p in srv.role_store.list_proposals()]},
            )
            return

        if path == "/api/closeouts":
            self._send_json(
                HTTPStatus.OK,
                {"closeouts": [c.to_dict() for c in srv.closeout_store.list_cards()]},
            )
            return

        if path == "/api/experts":
            self._send_json(
                HTTPStatus.OK,
                {"experts": [p.to_dict() for p in srv.expert_pool.list_profiles()]},
            )
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"not found: {path}"})

    def do_POST(self) -> None:
        srv = self._srv()
        path = urlparse(self.path).path
        parts = [p for p in path.split("/") if p]
        body = self._read_json_body()
        reviewer = str(body.get("reviewer") or "owner")
        note = str(body.get("note") or "")

        try:
            if len(parts) == 4 and parts[0] == "api" and parts[1] == "proposals":
                prop_id, action = parts[2], parts[3]
                if action == "approve":
                    updated = srv.role_store.approve_proposal(prop_id, reviewed_by=reviewer, review_note=note)
                    self._send_json(HTTPStatus.OK, updated.to_dict())
                    return
                if action == "reject":
                    updated = srv.role_store.reject_proposal(prop_id, reviewed_by=reviewer, review_note=note)
                    self._send_json(HTTPStatus.OK, updated.to_dict())
                    return

            if len(parts) == 4 and parts[0] == "api" and parts[1] == "closeouts":
                card_id, action = parts[2], parts[3]
                if action == "confirm":
                    res = srv.closeout_coordinator.confirm_and_sync(card_id, confirmed_by=reviewer)
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "card_id": res.card_id,
                            "chief_of_staff_notified": res.chief_of_staff_notified,
                            "tasks_created": list(res.tasks_created),
                            "proposals_queued": list(res.proposals_queued),
                            "already_synced": res.already_synced,
                        },
                    )
                    return
        except KeyError as exc:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": str(exc)})
            return
        except (ValueError, CASConflictError) as exc:
            self._send_json(HTTPStatus.CONFLICT, {"error": str(exc)})
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": f"not found: {path}"})


class CollaborationHubServer:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8790,
        role_store: RoleSkillMemoryStore,
        closeout_store: CloseoutStore,
        closeout_coordinator: CloseoutCoordinator,
        expert_pool: ExpertPoolRegistry | None = None,
        state_provider: HubStateProvider | None = None,
        window_label: str = "最近 24 小时",
    ) -> None:
        self._httpd = _HubHTTPServer(
            (host, port),
            role_store=role_store,
            closeout_store=closeout_store,
            closeout_coordinator=closeout_coordinator,
            expert_pool=expert_pool or ExpertPoolRegistry(),
            state_provider=state_provider or _DefaultEmptyProvider(),
            window_label=window_label,
        )
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        addr = self._httpd.server_address
        return (str(addr[0]), int(addr[1]))

    def start_background(self) -> None:
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def serve_forever(self) -> None:
        self._httpd.serve_forever()

    def close(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
