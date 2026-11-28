from __future__ import annotations

import importlib
import re
from collections import deque
from collections.abc import Sequence
from functools import lru_cache
from typing import Any

# The private modules behind re.compile have no type stubs, so they are typed Any.
_c: Any = importlib.import_module("re._constants")
_parser: Any = importlib.import_module("re._parser")

_MAX_STATES = 1000
_MAX_PAIRS = 200_000
_CATEGORIES = {
    _c.CATEGORY_DIGIT: re.compile(r"\d"), _c.CATEGORY_NOT_DIGIT: re.compile(r"\D"),
    _c.CATEGORY_SPACE: re.compile(r"\s"), _c.CATEGORY_NOT_SPACE: re.compile(r"\S"),
    _c.CATEGORY_WORD: re.compile(r"\w"), _c.CATEGORY_NOT_WORD: re.compile(r"\W"),
}


class UncomparablePattern(ValueError):
    pass


@lru_cache(maxsize=256)
def common_match(a: str, b: str) -> str | None:
    # A string both patterns fullmatch, None when there is none.
    parsed = [_parse(a), _parse(b)]
    alphabet = _alphabet(parsed)
    left, right = (_Automaton(items, alphabet) for items in parsed)
    start = (left.start, right.start)
    came: dict[tuple[int, int], tuple[tuple[int, int], str]] = {}
    seen, queue = {start}, deque([start])
    while queue:
        pair = queue.popleft()
        if pair == (left.end, right.end):
            found: list[str] = []
            while pair != start:
                pair, char = came[pair]
                found.append(char)
            return "".join(reversed(found))
        p, q = pair
        steps = [((n, q), "") for n in left.free[p]] + [((p, n), "") for n in right.free[q]]
        for chars, n in left.moves[p]:
            for others, m in right.moves[q]:
                both = chars & others
                if both:
                    steps.append(((n, m), min(both, key=lambda c: (not c.isalnum(), c))))
        for nxt, char in steps:
            if nxt not in seen:
                seen.add(nxt)
                came[nxt] = (pair, char)
                queue.append(nxt)
        if len(seen) > _MAX_PAIRS:
            raise UncomparablePattern("it is too large")
    return None


def _parse(pattern: str) -> Any:
    # The parser re.compile itself uses: whatever it reads a pattern as is what the pattern matches.
    parsed = _parser.parse(pattern)
    if parsed.state.flags & ~re.UNICODE:
        raise UncomparablePattern("flags such as (?i) are not supported")
    return parsed


def _walk(items: Any) -> Any:
    for op, value in items:
        yield op, value
        if op is _c.BRANCH:
            for option in value[1]:
                yield from _walk(option)
        elif op is _c.SUBPATTERN:
            yield from _walk(value[3])
        elif op in (_c.MAX_REPEAT, _c.MIN_REPEAT):
            yield from _walk(value[2])
        elif op is _c.IN:
            yield from value


def _alphabet(parsed: Sequence[Any]) -> tuple[str, ...]:
    # One character of every kind the two patterns can tell apart: all of ASCII, where ranges live, every character
    # named outside it, and for the rest one of each combination of \d, \w and \s.
    chars = {chr(n) for n in range(128)}
    named = {chr(value) for items in parsed for op, value in _walk(items) if op in (_c.LITERAL, _c.NOT_LITERAL)}
    kinds: dict[tuple[bool, ...], str] = {}
    for n in range(128, 0x4000):
        char = chr(n)
        if char not in named:
            kinds.setdefault(tuple(bool(_CATEGORIES[k].match(char)) for k in
                                   (_c.CATEGORY_DIGIT, _c.CATEGORY_WORD, _c.CATEGORY_SPACE)), char)
    return tuple(chars | named | set(kinds.values()))


class _Automaton:
    def __init__(self, items: Any, alphabet: tuple[str, ...]) -> None:
        self._alphabet = alphabet
        self.free: list[list[int]] = []
        self.moves: list[list[tuple[frozenset[str], int]]] = []
        self.start = self._state()
        self.end = self._sequence(items, self.start, True, True)

    def _state(self) -> int:
        if len(self.free) >= _MAX_STATES:
            raise UncomparablePattern("it is too large")
        self.free.append([])
        self.moves.append([])
        return len(self.free) - 1

    def _sequence(self, items: Any, state: int, first: bool, last: bool) -> int:
        # first/last: nothing can have been read before, nothing can follow, so ^ and $ say nothing there.
        listed = list(items)
        consumes = [op is not _c.AT for op, _ in listed]
        for index, (op, value) in enumerate(listed):
            state = self._item(op, value, state, first and not any(consumes[:index]),
                               last and not any(consumes[index + 1:]))
        return state

    def _item(self, op: Any, value: Any, state: int, first: bool, last: bool) -> int:
        if op is _c.AT:
            if (value in (_c.AT_BEGINNING, _c.AT_BEGINNING_STRING) and first) or (
                value in (_c.AT_END, _c.AT_END_STRING) and last
            ):
                return state
            raise UncomparablePattern("anchors and \\b are only supported as ^ at its start and $ at its end")
        if op is _c.SUBPATTERN:
            _group, add, remove, items = value
            if add or remove:
                raise UncomparablePattern("flags such as (?i:...) are not supported")
            return self._sequence(items, state, first, last)
        if op is _c.BRANCH:
            end = self._state()
            for option in value[1]:
                begin = self._state()
                self.free[state].append(begin)
                self.free[self._sequence(option, begin, first, last)].append(end)
            return end
        if op in (_c.MAX_REPEAT, _c.MIN_REPEAT):
            return self._repeat(value, state)
        chars = self._chars(op, value)
        end = self._state()
        self.moves[state].append((chars, end))
        return end

    def _repeat(self, value: Any, state: int) -> int:
        low, high, items = value
        for _ in range(low):
            state = self._sequence(items, state, False, False)
        if high is _c.MAXREPEAT or high == _c.MAXREPEAT:
            begin, end = self._state(), self._state()
            self.free[state].append(begin)
            self.free[self._sequence(items, begin, False, False)].append(begin)
            self.free[begin].append(end)
            return end
        end = self._state()
        self.free[state].append(end)
        for _ in range(high - low):
            # A state of its own, so a loop inside one copy never leads into another.
            begin = self._state()
            self.free[state].append(begin)
            state = self._sequence(items, begin, False, False)
            self.free[state].append(end)
        return end

    def _chars(self, op: Any, value: Any) -> frozenset[str]:
        every = frozenset(self._alphabet)
        if op is _c.LITERAL:
            return frozenset((chr(value),))
        if op is _c.NOT_LITERAL:
            return every - {chr(value)}
        if op is _c.ANY:
            return every - {"\n"}
        if op is not _c.IN:
            raise UncomparablePattern(f"{str(op).lower()} is not supported")
        negated, found = False, set[str]()
        for kind, item in value:
            if kind is _c.NEGATE:
                negated = True
            elif kind is _c.LITERAL:
                found.add(chr(item))
            elif kind is _c.RANGE:
                low, high = item
                if high > 127:
                    raise UncomparablePattern("character ranges are only supported within ASCII")
                found.update(chr(n) for n in range(low, high + 1))
            elif kind is _c.CATEGORY and item in _CATEGORIES:
                found.update(c for c in every if _CATEGORIES[item].match(c))
            else:
                raise UncomparablePattern(f"{str(item).lower()} is not supported")
        return every - found if negated else frozenset(found)
