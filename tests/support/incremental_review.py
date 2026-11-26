def block(location, trigger="divide(1, 0)", impact="crashes the caller", **extra):
    return {"location": location, "trigger": trigger, "impact": impact, **extra}


def verdict(*blocking, notes=(), summary="reviewed"):
    return {"verdict": "changes_requested" if blocking else "pass", "summary": summary, "blocking": list(blocking),
            "notes": list(notes)}


FIRST = verdict(block("calc.py:2"), notes=["rename divide to safe_divide"])
OLD_CODE = block("old.py:2", trigger="total(None)", impact="TypeError")
