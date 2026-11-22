from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence

from ccpeer.errors import PeerError
from ccpeer.inbox import Cursor, Inbox
from ccpeer.protocol import DEFAULT_MODE, PEER_MODES
from ccpeer.receiver import config_from, receiver_info, run, state_dir
from ccpeer.sender import send
from ccpeer.sessions import discover


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ccpeer",
        description="Exchange messages with local Claude Code sessions (manual troubleshooting tool).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    def config_dirs(p: argparse.ArgumentParser) -> None:
        p.add_argument("--config-dir", dest="config_dirs", action="append", metavar="DIR",
                       help="Claude Code config dir (or its sessions/ dir); repeatable. "
                            "Default: ~/.claude plus $CLAUDE_CONFIG_DIR")

    def state(p: argparse.ArgumentParser) -> None:
        p.add_argument("--state-dir", default=None, metavar="DIR", help="receiver state root (default ~/.ccpeer)")

    ls = sub.add_parser("list", help="list live sessions")
    config_dirs(ls)
    ls.add_argument("--json", dest="as_json", action="store_true")

    sd = sub.add_parser("send", help="send a message; BODY '-' reads stdin verbatim")
    config_dirs(sd)
    state(sd)
    sd.add_argument("--as", dest="name", required=True, help="your name; replies go to the receiver of this name")
    sd.add_argument("--to", required=True, help="session name, pid, sessionId or uds:/socket path")
    sd.add_argument("--mode", default=DEFAULT_MODE, choices=PEER_MODES, help="declared permission mode")
    sd.add_argument("--timeout", type=float, default=5.0)
    sd.add_argument("body")

    sv = sub.add_parser("serve", help="run the receiver in the foreground")
    config_dirs(sv)
    state(sv)
    sv.add_argument("--as", dest="name", required=True)
    sv.add_argument("--socket-dir", default=None, metavar="DIR", help="default /tmp/cc-socks")
    sv.add_argument("--heartbeat", type=float, default=60.0, metavar="SEC",
                    help="how often the registration is rewritten")

    ib = sub.add_parser("inbox", help="print received records as JSONL; the next cursor goes to stderr")
    state(ib)
    ib.add_argument("--as", dest="name", required=True)
    ib.add_argument("--cursor", default=None, help="byte offset or msg_id to continue after")
    ib.add_argument("--control", action="store_true", help="read control receipts instead of messages")
    ib.add_argument("--limit", type=int, default=None)
    ib.add_argument("--follow", action="store_true")
    return parser


def _cursor(raw: str | None) -> Cursor:
    if raw is None:
        return None
    return int(raw) if raw.isdigit() else raw


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.cmd == "list":
            sessions = discover(args.config_dirs)
            if args.as_json:
                print(json.dumps([s.summary() for s in sessions], ensure_ascii=False, indent=2))
                return 0
            for s in sessions:
                tag = " [standalone]" if s.standalone else ""
                print(f"{s.pid}\t{s.status or '?'}\t{s.name or ''}{tag}\t{s.version or ''}\t{s.cwd or ''}")
            return 0
        if args.cmd == "send":
            body = sys.stdin.read() if args.body == "-" else args.body
            info = receiver_info(args.name, args.state_dir)
            if info is None:
                print(f"warning: no receiver named {args.name!r} is running; replies will have nowhere to go "
                      f"(start one with: python -m ccpeer serve --as {args.name})", file=sys.stderr)
            result = send(args.to, body, sender_name=args.name, reply_addr=info.address if info else None,
                          mode=args.mode, config_dirs=args.config_dirs, timeout=args.timeout)
            print(json.dumps({"msg_id": result.msg_id, "pid": result.pid, "name": result.name,
                              "bytes": result.bytes_sent}, ensure_ascii=False))
            return 0
        if args.cmd == "serve":
            return run(config_from(args.name, args.config_dirs, args.state_dir, args.socket_dir, args.heartbeat))
        if args.cmd == "inbox":
            inbox = Inbox(state_dir(args.name, args.state_dir))
            log = inbox.control if args.control else inbox.messages
            cursor = _cursor(args.cursor)
            while True:
                page = log.read(cursor, args.limit)
                for rec in page.records:
                    print(json.dumps(rec, ensure_ascii=False), flush=True)
                cursor = page.cursor
                if not args.follow:
                    break
                time.sleep(1)
            print(f"cursor={cursor}", file=sys.stderr)
            return 0
    except PeerError as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 2


if __name__ == "__main__":
    sys.exit(main())
