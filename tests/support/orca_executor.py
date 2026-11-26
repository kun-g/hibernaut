import json
import re
import uuid
from pathlib import Path


_RESULT_RE = re.compile(r"atomically rename that file to (\S+)\. Use ")


class FakeOrca:
    """Orca CLI double: terminals with process incarnations, and Claude Code conversations that keep a
    transcript like the real one and answer each prompt with how many turns their conversation has seen."""

    def __init__(self, claude_root: Path, agent: str = "claude") -> None:
        self.root = claude_root
        self.agent = agent
        self.terminals: dict[str, dict] = {}
        self.conversations: dict[str, list[str]] = {}
        self.calls: list[tuple[str, ...]] = []
        self.answer = True
        self.fail: dict[str, Exception] = {}

    def _transcript(self, sid: str) -> Path:
        return self.root / "projects" / "-work" / f"{sid}.jsonl"

    def _deliver(self, handle: str, prompt: str) -> None:
        sid = self.terminals[handle]["conversation"]
        self.conversations[sid].append(prompt)
        path = self._transcript(sid)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"type": "user", "sessionId": sid, "message": prompt}) + "\n")
        if self.answer:
            result = Path(_RESULT_RE.search(prompt).group(1))
            turns = len(self.conversations[sid])
            result.write_text(json.dumps({"outcome": "emit", "messages": [f"turn {turns} of {sid}"]}))

    def _open(self, sid: str) -> str:
        handle = f"term_{uuid.uuid4().hex[:8]}"
        self.terminals[handle] = {"conversation": sid, "incarnation": uuid.uuid4().hex, "alive": True}
        return handle

    def _info(self, handle: str) -> dict:
        term = self.terminals[handle]
        return {"handle": handle, "incarnationId": term["incarnation"], "connected": True, "writable": True,
                "agentIdentity": term.get("agent", self.agent)}

    async def __call__(self, *args: str, cwd: Path) -> dict:
        self.calls.append(args)
        verb = " ".join(args[:2])
        if verb in self.fail:
            raise self.fail[verb]
        flags = dict(zip(args[2::2], args[3::2]))
        if verb == "agent launch":
            sid = str(uuid.uuid4())
            self.conversations[sid] = []
            handle = self._open(sid)
            self._deliver(handle, flags["--prompt"])
            return {"outcome": {"kind": "terminal", "handle": handle}}
        if verb == "terminal create":
            command = flags["--command"].split()
            sid = command[command.index("--resume") + 1]
            handle = self._open(sid)
            if sid not in self.conversations:
                self.terminals[handle]["agent"] = None
            return {"terminal": {"handle": handle, "incarnationId": self.terminals[handle]["incarnation"]}}
        handle = flags.get("--terminal", "")
        term = self.terminals.get(handle)
        if verb == "terminal show":
            if term is None:
                raise RuntimeError("terminal not found")
            # Like Orca: a background terminal no pane has adopted reports orphaned while it runs.
            return {"terminal": self._info(handle) | {"orphaned": True}}
        if verb == "terminal list":
            return {"terminals": [self._info(h) for h, t in self.terminals.items() if t["alive"]]}
        if verb == "terminal wait":
            return {"wait": {"satisfied": True}}
        if verb == "terminal send":
            assert term is not None and term["alive"]
            self._deliver(handle, flags["--text"])
            return {"send": {"handle": handle, "accepted": True, "prompt": {"processIncarnation": term["incarnation"]}}}
        if verb == "terminal close":
            if term is not None:
                term["alive"] = False
            return {"close": {"handle": handle}}
        raise AssertionError(f"unexpected orca call {args}")

    def verbs(self) -> list[str]:
        return [" ".join(c[:2]) for c in self.calls]
