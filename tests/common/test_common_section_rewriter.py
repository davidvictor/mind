"""Tests for scripts.common.section_rewriter — replace_or_insert_section.

This utility is used by the Phase G re-ingest mode to update existing
source pages with new sections (## Atom evidence, ## Probationary atoms
surfaced) without touching unrelated body content.

It is implemented in Phase A so it's available and tested early — Phase G
will invoke it from the per-ingestor --reingest pipelines.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.common.section_rewriter import (
    SectionOperation,
    apply_section_operations,
    parse_markdown_body,
    replace_or_insert_section,
)


def test_replace_existing_section(tmp_path):
    page = tmp_path / "page.md"
    page.write_text(
        "# Title\n\n"
        "Some intro paragraph.\n\n"
        "## Atom evidence\n\n"
        "- old entry\n\n"
        "## Other section\n\n"
        "untouched\n",
        encoding="utf-8",
    )
    result = replace_or_insert_section(
        file_path=page,
        section_heading="## Atom evidence",
        new_content="- new entry 1\n- new entry 2\n",
    )
    assert result is True
    body = page.read_text(encoding="utf-8")
    assert "old entry" not in body
    assert "- new entry 1" in body
    assert "- new entry 2" in body
    assert "## Other section" in body, "unrelated sections preserved"
    assert "untouched" in body


def test_insert_new_section_after_heading(tmp_path):
    page = tmp_path / "page.md"
    page.write_text(
        "# Title\n\n"
        "## Summary\n\n"
        "summary text\n\n"
        "## Other section\n\n"
        "other text\n",
        encoding="utf-8",
    )
    result = replace_or_insert_section(
        file_path=page,
        section_heading="## Atom evidence",
        new_content="- match 1\n",
        insert_after="## Summary",
    )
    assert result is True
    body = page.read_text(encoding="utf-8")
    summary_pos = body.index("## Summary")
    atom_pos = body.index("## Atom evidence")
    other_pos = body.index("## Other section")
    assert summary_pos < atom_pos < other_pos, "atom evidence inserted between summary and other"
    assert "- match 1" in body
    assert "summary text" in body
    assert "other text" in body


def test_insert_new_section_at_end_when_no_anchor(tmp_path):
    page = tmp_path / "page.md"
    page.write_text("# Title\n\n## Existing\n\nbody\n", encoding="utf-8")
    result = replace_or_insert_section(
        file_path=page,
        section_heading="## New section",
        new_content="- bullet\n",
    )
    assert result is True
    body = page.read_text(encoding="utf-8")
    assert body.endswith("## New section\n\n- bullet\n")


def test_no_change_when_content_identical(tmp_path):
    page = tmp_path / "page.md"
    page.write_text(
        "# Title\n\n## Atom evidence\n\n- existing entry\n",
        encoding="utf-8",
    )
    result = replace_or_insert_section(
        file_path=page,
        section_heading="## Atom evidence",
        new_content="- existing entry\n",
    )
    assert result is False, "no-op when content matches"


def test_preserves_frontmatter(tmp_path):
    page = tmp_path / "page.md"
    page.write_text(
        "---\n"
        "id: test-page\n"
        "type: article\n"
        "---\n"
        "# Title\n\n"
        "## Atom evidence\n\n"
        "- old\n",
        encoding="utf-8",
    )
    replace_or_insert_section(
        file_path=page,
        section_heading="## Atom evidence",
        new_content="- new\n",
    )
    body = page.read_text(encoding="utf-8")
    assert body.startswith("---\nid: test-page\ntype: article\n---\n")


def test_atomic_write_uses_tempfile(tmp_path, monkeypatch):
    """The rewriter writes to a tempfile and renames over the target so a
    crash mid-write doesn't leave a half-written file."""
    page = tmp_path / "page.md"
    page.write_text("# Title\n\n## Atom evidence\n\n- old\n", encoding="utf-8")

    real_replace = Path.replace
    seen_temp = []

    def spy_replace(self, target):
        seen_temp.append(self.name)
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", spy_replace)
    replace_or_insert_section(
        file_path=page,
        section_heading="## Atom evidence",
        new_content="- new\n",
    )
    assert seen_temp, "Path.replace was used (atomic rename)"
    assert any(name != "page.md" for name in seen_temp), "wrote via a tempfile name"


def test_section_with_multi_line_content(tmp_path):
    page = tmp_path / "page.md"
    page.write_text("# Title\n\n## A\n\nbody\n", encoding="utf-8")
    new_content = (
        "- entry one\n"
        "- entry two\n"
        "- entry three with [[wiki-link]]\n"
    )
    replace_or_insert_section(
        file_path=page,
        section_heading="## Atom evidence",
        new_content=new_content,
        insert_after="## A",
    )
    body = page.read_text(encoding="utf-8")
    assert "- entry one" in body
    assert "- entry two" in body
    assert "[[wiki-link]]" in body


def test_parse_markdown_body_extracts_intro_and_sections():
    parsed = parse_markdown_body(
        "---\n"
        "id: page\n"
        "---\n\n"
        "# Title\n\n"
        "Intro paragraph.\n\n"
        "## One\n\n"
        "- first\n\n"
        "## Two\n\n"
        "second\n"
    )
    assert parsed.frontmatter_block.startswith("---\nid: page\n---")
    assert "# Title" in parsed.intro
    assert [section.heading for section in parsed.sections] == ["## One", "## Two"]


def test_apply_section_operations_replaces_intro_and_unions_bullet_sections():
    updated = apply_section_operations(
        text=(
            "---\n"
            "id: page\n"
            "---\n\n"
            "# Title\n\n"
            "Old intro.\n\n"
            "## Evidence log\n\n"
            "- old\n\n"
            "## Custom\n\n"
            "keep me\n"
        ),
        intro_mode="replace",
        intro_content="# Title\n\nNew intro.\n",
        section_operations=[
            SectionOperation(
                heading="## Evidence log",
                mode="union",
                content="- old\n- new\n",
            )
        ],
    )
    assert "New intro." in updated
    assert updated.count("- old") == 1
    assert "- new" in updated
    assert "## Custom" in updated
    assert "keep me" in updated
