import textwrap


# Runs whose text starts with "block" wait for the gate file, standing in for a two-hour SDE run.
GATED_CLI = textwrap.dedent(
    """
    import json, os, pathlib, sys, time

    events = json.loads(sys.argv[-1].split("## events\\n", 1)[1])
    text = events[-1]["payload"]["text"]
    gen = os.environ["GEN"]
    with open(os.environ["RUN_LOG"], "a") as log:
        log.write(json.dumps({"text": text, "gen": gen}) + "\\n")
    if text.startswith("block"):
        gate = pathlib.Path(os.environ["GATE"])
        deadline = time.time() + 30
        while not gate.exists():
            if time.time() > deadline:
                sys.exit("gate timeout")
            time.sleep(0.02)
    print(json.dumps({"outcome": "emit", "messages": [f"{text} by gen {gen}"]}))
    """
)
