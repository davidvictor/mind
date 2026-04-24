"""Tests for scripts.common.stance — generic stance reader.

The stance reader serves both wiki/people/<slug>-stance.md (for substack
authors and book authors) and wiki/channels/<slug>-stance.md (for YouTube
channels). The kind parameter selects the directory.

This module is read-only. Stance writes (which require source-kind-specific
record types) stay in scripts/substack/stance.py and will gain channel and
book-author writers in their own phases.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.common.stance import load_stance_context, load_stance_snapshot, stance_page_path


def test_stance_page_path_person(tmp_path):
    path = stance_page_path(tmp_path, slug="lex-fridman", kind="person")
    assert path == tmp_path / "wiki" / "people" / "lex-fridman-stance.md"


def test_stance_page_path_channel(tmp_path):
    path = stance_page_path(tmp_path, slug="lex-fridman-podcast", kind="channel")
    assert path == tmp_path / "wiki" / "channels" / "lex-fridman-podcast-stance.md"


def test_stance_page_path_unknown_kind_raises(tmp_path):
    with pytest.raises(ValueError, match="kind must be"):
        stance_page_path(tmp_path, slug="x", kind="invalid")


def test_load_stance_context_missing_returns_empty_string(tmp_path):
    result = load_stance_context(slug="never-seen", kind="person", repo_root=tmp_path)
    assert result == ""


def test_load_stance_context_strips_frontmatter_and_wraps_with_header(tmp_path):
    target = tmp_path / "wiki" / "people" / "test-author-stance.md"
    target.parent.mkdir(parents=True)
    target.write_text(
        "---\n"
        "id: stance-test-author\n"
        "type: note\n"
        "---\n"
        "# Test Author — Current Stance\n"
        "\n"
        "## Core beliefs\n"
        "- believes in iteration\n",
        encoding="utf-8",
    )
    result = load_stance_context(slug="test-author", kind="person", repo_root=tmp_path)
    assert result.startswith("## What this author believed last time you read them")
    assert "## Core beliefs" in result
    assert "id: stance-test-author" not in result, "frontmatter should be stripped"


def test_load_stance_context_channel_uses_channel_directory(tmp_path):
    target = tmp_path / "wiki" / "channels" / "test-podcast-stance.md"
    target.parent.mkdir(parents=True)
    target.write_text(
        "---\n"
        "id: stance-test-podcast\n"
        "type: note\n"
        "---\n"
        "# Test Podcast — Current Stance\n"
        "\n"
        "## Core beliefs\n"
        "- on-mic position is X\n",
        encoding="utf-8",
    )
    result = load_stance_context(slug="test-podcast", kind="channel", repo_root=tmp_path)
    assert "on-mic position is X" in result


def test_load_stance_context_person_prefers_canonical_person_page_sections(tmp_path):
    person = tmp_path / "wiki" / "people" / "test-author.md"
    person.parent.mkdir(parents=True)
    person.write_text(
        "---\n"
        "id: test-author\n"
        "type: person\n"
        "---\n"
        "# Test Author\n\n"
        "Intro.\n\n"
        "## Core beliefs\n\n"
        "- believes in iteration\n\n"
        "## Changelog\n\n"
        "- seeded\n",
        encoding="utf-8",
    )
    legacy = tmp_path / "wiki" / "people" / "test-author-stance.md"
    legacy.write_text(
        "---\n"
        "id: stance-test-author\n"
        "type: note\n"
        "---\n"
        "# Test Author — Current Stance\n\n"
        "## Core beliefs\n\n"
        "- stale legacy copy\n",
        encoding="utf-8",
    )

    result = load_stance_context(slug="test-author", kind="person", repo_root=tmp_path)
    assert "believes in iteration" in result
    assert "stale legacy copy" not in result
    assert "Intro." not in result


def test_load_stance_context_no_frontmatter_passes_body_through(tmp_path):
    target = tmp_path / "wiki" / "people" / "no-fm-stance.md"
    target.parent.mkdir(parents=True)
    target.write_text("# No frontmatter author\n\n## Core beliefs\n- yes\n", encoding="utf-8")
    result = load_stance_context(slug="no-fm", kind="person", repo_root=tmp_path)
    assert "Core beliefs" in result
    assert "- yes" in result


def test_load_stance_snapshot_caps_bullets_and_omits_changelog(tmp_path):
    target = tmp_path / "wiki" / "people" / "test-author.md"
    target.parent.mkdir(parents=True)
    target.write_text(
        "---\n"
        "id: test-author\n"
        "type: person\n"
        "---\n"
        "# Test Author\n\n"
        "## Core beliefs\n\n"
        "- belief 1\n\n"
        "- belief 2\n\n"
        "- belief 3\n\n"
        "## Changelog\n\n"
        "- seeded\n",
        encoding="utf-8",
    )
    result = load_stance_snapshot(
        slug="test-author",
        kind="person",
        repo_root=tmp_path,
        max_bullets_per_section=2,
        max_chars=400,
        include_changelog=False,
    )
    assert "belief 1" in result
    assert "belief 2" in result
    assert "belief 3" not in result
    assert "omitted for brevity" in result
    assert "seeded" not in result
