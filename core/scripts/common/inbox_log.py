"""Frontmatter-aware appender for inbox log files.

Inbox log files like wiki/inbox/substack-entities-YYYY-MM-DD.md need to
carry minimal frontmatter to satisfy /lint's universal-field check. This
helper creates the file with frontmatter on first write and appends body
content on subsequent writes without re-emitting the frontmatter.
"""
from __future__ import annotations

from pathlib import Path

from scripts.common.default_tags import default_tags


def _frontmatter_block(*, kind: str, date: str) -> str:
    tags_list = default_tags("note")
    tags_yaml = "\n".join(f"  - {t}" for t in tags_list)
    return (
        "---\n"
        f"id: {kind}-{date}\n"
        f"type: note\n"
        f"title: \"Inbox log: {kind} for {date}\"\n"
        f"kind: {kind}\n"
        f"created: {date}\n"
        f"last_updated: {date}\n"
        "status: active\n"
        "tags:\n"
        f"{tags_yaml}\n"
        "domains:\n"
        "  - meta\n"
        "relates_to:\n"
        "  - \"[[profile]]\"\n"
        "sources: []\n"
        "---\n"
        "\n"
    )


def append_to_inbox_log(
    *,
    target: Path,
    kind: str,
    entry: str,
    date: str,
) -> None:
    """Append entry text to an inbox log file at target.

    If the file doesn't exist, create it with a minimal frontmatter block
    first. If it exists, append entry to its body without modifying the
    existing frontmatter.

    Args:
        target: Path to the inbox log file.
        kind: Short identifier for what kind of log this is
              (e.g. 'substack-entities', 'youtube-failures').
        entry: The new line(s) of body content to append. Should end with newline.
        date: YYYY-MM-DD date string for frontmatter id and created fields.
    """
    target.parent.mkdir(parents=True, exist_ok=True)

    if not target.exists():
        target.write_text(_frontmatter_block(kind=kind, date=date) + entry, encoding="utf-8")
        return

    # Append to existing file
    with target.open("a", encoding="utf-8") as f:
        if not entry.startswith("\n"):
            # Ensure separator between entries if needed
            existing = target.read_text(encoding="utf-8")
            if not existing.endswith("\n"):
                f.write("\n")
        f.write(entry)
