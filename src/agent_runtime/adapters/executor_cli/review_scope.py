from __future__ import annotations

import os
import posixpath
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

UNFIXED = "unfixed_previous"
INTRODUCED = "introduced_since_previous"
BASES = (UNFIXED, INTRODUCED)
_MAX_FILES_SHOWN = 300
_TOKEN = re.compile(r"[^\s:,;()\[\]{}<>`'\"#|，。；：（）「」、]+")
# Where an item says the work is; trigger and impact also name callers that are fine as they are.
_ITEM_FILES = ("location", "fix")
_DOWNGRADED = re.compile(r"（已降级：[^（）]*）$")


@dataclass(frozen=True, slots=True)
class ReviewScope:
    # What a re-review may block on: the previous round's items, and the files changed since (repository-relative).
    root: Path
    workdir: Path
    previous: tuple[Mapping[str, str], ...]
    changed: tuple[str, ...]
    # Follow-ups earlier rounds already recorded.
    notes: tuple[str, ...] = ()

    def narrow(self, review: Mapping[str, Any]) -> dict[str, Any]:
        # An item outside the scope becomes a note; a review left without blocking items is a pass.
        items: list[dict[str, str]] = review["blocking"]
        reasons = self._reasons(items)
        if not any(reasons):
            return dict(review)
        blocking = [item for item, reason in zip(items, reasons) if not reason]
        notes = [*review.get("notes", []),
                 *(_downgraded(item, reason) for item, reason in zip(items, reasons) if reason)]
        return {**review, "verdict": "changes_requested" if blocking else "pass", "blocking": blocking,
                "notes": notes, "downgraded": len(items) - len(blocking), "reviewer_verdict": review["verdict"]}

    def _reasons(self, items: list[dict[str, str]]) -> list[str]:
        # Decided on what the program can check, not on the label alone:
        # - a location in a changed file can be a new problem, whatever the label says;
        # - an item numbered as a previous one stands for that previous item. Its location may be elsewhere (a missing test,
        #   moved code), but then for one item only, so a number cannot carry further findings in unchanged code;
        # - without a label or a usable number, the location must be in a file the previous items name, so a reviewer
        #   that omits them never turns an unfixed item into a pass.
        changed = set(self.changed)
        previous = [(_text(p["location"]), self._files(" ".join(p.get(k, "") for k in _ITEM_FILES)))
                    for p in self.previous]
        files = [self._files(item["location"]) for item in items]

        def about(i: int, n: int) -> bool:
            where, named = previous[n]
            return _text(items[i]["location"]) == where or not files[i].isdisjoint(named)

        claims = [self._claim(item) for item in items]
        used = {n for i, n in enumerate(claims) if n is not None and about(i, n)}
        reasons: list[str] = []
        for i, item in enumerate(items):
            basis, claim, reason = item.get("basis"), claims[i], ""
            if files[i].isdisjoint(changed):
                if basis == INTRODUCED:
                    reason = "标为上一轮之后引入，但 location 不在上一轮之后改动的文件里"
                elif claim is not None and (about(i, claim) or claim not in used):
                    used.add(claim)
                elif claim is not None:
                    reason = f"上一轮第 {claim + 1} 条已由本轮另一条阻断对应，location 也不在它涉及的文件里"
                elif not any(about(i, n) for n in range(len(previous))):
                    reason = ("标为上一轮未修好，但对应不上上一轮的任何阻断" if basis == UNFIXED else
                              "没有写明依据 basis，location 也不在上一轮阻断涉及或上一轮之后改动的文件里")
            reasons.append(reason)
        return reasons

    def _claim(self, item: Mapping[str, str]) -> int | None:
        raw = item.get("previous_item", "")
        if item.get("basis") == INTRODUCED or not (raw.isascii() and raw.isdigit() and len(raw) < 10):
            return None
        return int(raw) - 1 if 1 <= int(raw) <= len(self.previous) else None

    def _files(self, text: str) -> set[str]:
        # Repository files a text names, however the reviewer wrote them: changed ones (deleted files included) and
        # ones that exist. A bare or partial path that is no file itself matches a changed file ending in it.
        for base in (self.root, self.root.resolve(), self.workdir, self.workdir.resolve()):
            text = text.replace(f"{base}/", "")
        found = {p for p in self.changed
                 if p in text and re.search(rf"(?<![\w./-]){re.escape(p)}(?![\w/-]|\.\w)", text)}
        prefix = os.path.relpath(self.workdir.resolve(), self.root.resolve())
        for raw in _TOKEN.findall(text):
            token = posixpath.normpath(raw.rstrip("."))
            if token.startswith(("/", "..")) or token == ".":
                continue
            names = {token, posixpath.normpath(posixpath.join(prefix, token))}
            existing = {n for n in names if os.path.isfile(self.root / n)}
            found |= existing or {p for p in self.changed if p == token or p.endswith(f"/{token}")}
        return found

    def render(self) -> str:
        n = len(self.previous)
        unfixed = (
            f'`{UNFIXED}`: one of the {n} blocking item{"" if n == 1 else "s"} numbered in the previous review round '
            "is still not fixed. Set previous_item to its number and keep its location, or name where the code moved."
            if n else f"`{UNFIXED}`: not applicable, the previous round passed with no blocking item."
        )
        shown = self.changed[:_MAX_FILES_SHOWN]
        files = [f"- {p.encode('utf-8', 'replace').decode()}" for p in shown] or ["- (none)"]
        if len(self.changed) > len(shown):
            files.append(f"- ... and {len(self.changed) - len(shown)} more")
        lines = [
            "## Incremental review",
            "An earlier round reviewed the whole change. This round blocks only on:",
            f"1. {unfixed}",
            f"2. `{INTRODUCED}`: a new problem caused by the changes since that review. The location is the line in a "
            "changed file (listed below) that causes it, even when the damage shows in unchanged code.",
            "Everything else you find goes to notes, whatever its severity: notes are delivered as follow-ups. "
            "Hibernaut turns any other blocking item into a note. The full diff above is context.",
            "Files changed since the previous review:",
            *files,
        ]
        if self.notes:
            lines += ["Follow-ups already recorded (do not repeat them):", *(f"- {_text(n)}" for n in self.notes)]
        return "\n".join(lines)


def merge_notes(known: Iterable[str], new: Iterable[str]) -> list[str]:
    # The same remark in another round (spacing, case or downgrade reason aside) is listed once.
    merged: dict[str, str] = {}
    for note in (*known, *new):
        key = _DOWNGRADED.sub("", _text(note)).casefold()
        if key:
            merged.setdefault(key, note.strip())
    return list(merged.values())


def _text(raw: str) -> str:
    return " ".join(raw.split())


def _downgraded(item: Mapping[str, str], reason: str) -> str:
    fix = f"；建议：{item['fix']}" if item.get("fix") else ""
    return _text(f"{item['location']} — {item['trigger']}；{item['impact']}{fix}") + f"（已降级：{reason}）"
