from pathlib import Path


def write_skill(directory: Path, marker: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "SKILL.md").write_text(f"---\nname: {directory.name}\n---\n{marker}: see checklist.md\n")
    (directory / "checklist.md").write_text("resource\n")
    return directory / "SKILL.md"
