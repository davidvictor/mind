"""Tests for scripts.common.inbox_log — frontmatter prepender for inbox files."""
from __future__ import annotations

from pathlib import Path

from scripts.common.inbox_log import append_to_inbox_log


def test_creates_file_with_frontmatter_on_first_write(tmp_path):
    target = tmp_path / "wiki" / "inbox" / "substack-entities-2026-04-08.md"
    append_to_inbox_log(
        target=target,
        kind="substack-entities",
        entry="- New entity: Foo Bar\n",
        date="2026-04-08",
    )
    assert target.exists()
    text = target.read_text(encoding="utf-8")
    assert text.startswith("---\n")
    assert "id: substack-entities-2026-04-08" in text
    assert "type: note" in text
    assert "kind: substack-entities" in text
    assert "- New entity: Foo Bar" in text


def test_appends_to_existing_file_without_re_emitting_frontmatter(tmp_path):
    target = tmp_path / "wiki" / "inbox" / "substack-entities-2026-04-08.md"
    append_to_inbox_log(target=target, kind="substack-entities", entry="- A\n", date="2026-04-08")
    append_to_inbox_log(target=target, kind="substack-entities", entry="- B\n", date="2026-04-08")
    text = target.read_text(encoding="utf-8")
    # Frontmatter appears once
    assert text.count("---\n") == 2  # opening and closing fence
    assert "- A" in text
    assert "- B" in text


def test_emits_three_axis_tags(tmp_path):
    target = tmp_path / "wiki" / "inbox" / "test.md"
    append_to_inbox_log(target=target, kind="test", entry="x\n", date="2026-04-08")
    text = target.read_text(encoding="utf-8")
    assert "domain/" in text
    assert "function/" in text
    assert "signal/" in text
