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
_LINE = re.compile(r"[:#]L?\d")
_SPOT = re.compile(r"(?P<path>[^\s:,;()\[\]{}<>`'\"#|，。；：（）「」、]+)[:#]L?(?P<start>\d{1,9})(?:[-–]L?(?P<end>\d{1,9}))?")
# How surely a finding is a previous item again, best first; the first two identify it without its number.
_SAME_TRIGGER, _SAME_PLACE, _SAME_FILE, _ELSEWHERE = range(4)
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
        # Decided on what the program can check, not on the label alone. A location in a changed file can be a new
        # problem, whatever the label says. Outside those files a finding must stand for a previous item, and each
        # previous item backs one finding, so re-reviews can never block on more unchanged code than the round before.
        changed = set(self.changed)
        claims = [self._claim(item) for item in items]
        outside = [i for i, item in enumerate(items) if self._files(item["location"]).isdisjoint(changed)]
        in_changes = {n for i, n in enumerate(claims) if n is not None and i not in outside}
        pairs: list[tuple[int, int, int]] = []
        for i in outside:
            if items[i].get("basis") == INTRODUCED:
                continue
            for n, previous in enumerate(self.previous):
                fit = self._fit(items[i], previous)
                # Its number is enough for one finding wherever it is (a missing test, moved code), unless a finding
                # in a changed file already stands for that item. Anything else has to identify the item itself.
                if fit <= _SAME_PLACE or (claims[i] == n and n not in in_changes):
                    pairs.append((fit, i, n))
        backed: dict[int, int] = {}
        for _, i, n in sorted(pairs):
            if i not in backed and n not in backed.values():
                backed[i] = n
        reasons = [""] * len(items)
        for i in outside:
            claim = claims[i]
            if items[i].get("basis") == INTRODUCED:
                reasons[i] = "标为上一轮之后引入，但 location 不在上一轮之后改动的文件里"
            elif i not in backed and claim is not None:
                reasons[i] = f"上一轮第 {claim + 1} 条已由本轮另一条阻断对应"
            elif i not in backed:
                reasons[i] = "没有指明上一轮的哪一条，位置与触发条件也对不上，location 不在上一轮之后改动的文件里"
        return reasons

    def _fit(self, item: Mapping[str, str], previous: Mapping[str, str]) -> int:
        where, was = self._files(item["location"]), self._files(previous["location"])
        same_file = not where.isdisjoint(was)
        same_text = _text(item["location"]) == _text(previous["location"])
        if _text(item["trigger"]).casefold() == _text(previous["trigger"]).casefold() and (same_file or same_text):
            return _SAME_TRIGGER
        # Outside the changes the file is as it was, so the item's lines have not moved.
        lines = self._lines(previous["location"])
        if any(path in lines and any(a <= start <= b for a, b in lines[path])
               for path, spans in self._lines(item["location"]).items() for start, _ in spans):
            return _SAME_PLACE
        if same_text and _LINE.search(item["location"]):
            return _SAME_PLACE
        return _SAME_FILE if same_file else _ELSEWHERE

    def _lines(self, text: str) -> dict[str, list[tuple[int, int]]]:
        found: dict[str, list[tuple[int, int]]] = {}
        for m in _SPOT.finditer(self._relative(text)):
            start = int(m["start"])
            for path in self._files(m["path"]):
                found.setdefault(path, []).append((start, max(start, int(m["end"] or start))))
        return found

    def _claim(self, item: Mapping[str, str]) -> int | None:
        raw = item.get("previous_item", "")
        if item.get("basis") == INTRODUCED or not (raw.isascii() and raw.isdigit() and len(raw) < 10):
            return None
        return int(raw) - 1 if 1 <= int(raw) <= len(self.previous) else None

    def _files(self, text: str) -> set[str]:
        # Repository files a text names, however the reviewer wrote them: changed ones (deleted files included) and
        # ones that exist. A bare or partial path that is no file itself matches a changed file ending in it.
        text = self._relative(text)
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

    def _relative(self, text: str) -> str:
        for base in (self.root, self.root.resolve(), self.workdir, self.workdir.resolve()):
            text = text.replace(f"{base}/", "")
        return text

    def render(self) -> str:
        n = len(self.previous)
        unfixed = (
            f'`{UNFIXED}`: one of the {n} blocking item{"" if n == 1 else "s"} numbered in the previous review round '
            "is still not fixed. Set previous_item to its number (each number backs one blocking item) and keep its location, or name where "
            "the code moved."
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
            "changed file (listed below) that causes it, even when the damage shows in unchanged code. A requirement a person "
            "added since that review that the changes do not meet is such a problem, located in the changed file "
            "closest to it.",
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
