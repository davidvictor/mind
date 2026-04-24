"""Tests for scripts.substack.stance — read-only and write/stub helpers."""
from pathlib import Path

import pytest

from scripts.substack.stance import (
    apply_stance_delta,
    load_stance_context,
    migrate_legacy_stance_pages,
    read_stance_body,
    stance_cache_path,
    stance_page_path,
    stub_stance_doc,
    write_stance_doc,
)
from scripts.substack.parse import SubstackRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(**overrides) -> SubstackRecord:
    defaults = dict(
        id="140000001",
        title="On Trust",
        subtitle="Why the internet runs on it",
        slug="on-trust",
        published_at="2026-03-15T09:00:00Z",
        saved_at="2026-04-02T18:00:00Z",
        url="https://thegeneralist.substack.com/p/on-trust",
        author_name="Mario Gabriele",
        author_id="9001",
        publication_name="The Generalist",
        publication_slug="thegeneralist",
        body_html="<p>Trust is the root.</p>",
        is_paywalled=False,
    )
    defaults.update(overrides)
    return SubstackRecord(**defaults)


def _write_stance(tmp_path: Path, author_slug: str, content: str) -> Path:
    """Helper: write a canonical author page into the correct location."""
    people_dir = tmp_path / "wiki" / "people"
    people_dir.mkdir(parents=True, exist_ok=True)
    path = people_dir / f"{author_slug}.md"
    path.write_text(content, encoding="utf-8")
    return path


_SAMPLE_UPDATED_BODY = """\
## Core beliefs

- Trust is the foundation of all durable networks.
- Credibility compounds over time.

## Open questions

- How do you rebuild trust once broken?

## Recent shifts

- (no shifts yet)

## Contradictions observed

- (none yet)
"""


# ---------------------------------------------------------------------------
# stance_page_path
# ---------------------------------------------------------------------------


def test_stance_page_path_returns_correct_location(tmp_path):
    path = stance_page_path(tmp_path, "dan-luu")
    assert path == tmp_path / "wiki" / "people" / "dan-luu.md"


def test_stance_page_path_uses_provided_slug_verbatim(tmp_path):
    path = stance_page_path(tmp_path, "mario-gabriele")
    assert path.name == "mario-gabriele.md"


# ---------------------------------------------------------------------------
# stance_cache_path
# ---------------------------------------------------------------------------


def test_stance_cache_path_correct_location(tmp_path):
    path = stance_cache_path(tmp_path, "140000001")
    assert path == tmp_path / "raw" / "transcripts" / "substack" / "140000001.stance.json"


# ---------------------------------------------------------------------------
# load_stance_context — missing file
# ---------------------------------------------------------------------------


def test_load_stance_context_missing_file_returns_empty_string(tmp_path):
    result = load_stance_context("no-such-author", tmp_path)
    assert result == ""


# ---------------------------------------------------------------------------
# load_stance_context — present file
# ---------------------------------------------------------------------------


def test_load_stance_context_returns_wrapped_body_with_header(tmp_path):
    _write_stance(
        tmp_path,
        "dan-luu",
        "---\nid: dan-luu\ntype: person\n---\n# Dan Luu\n\n## Core beliefs\n\n- Software simplicity matters.\n",
    )
    result = load_stance_context("dan-luu", tmp_path)
    assert result.startswith("## What this author believed last time you read them\n\n")
    assert "## Core beliefs" in result
    assert "Software simplicity matters." in result


def test_load_stance_context_strips_frontmatter(tmp_path):
    _write_stance(
        tmp_path,
        "mario-gabriele",
        "---\nid: mario-gabriele\ntype: person\ntitle: Mario Gabriele\n---\n# Mario Gabriele\n\n## Core beliefs\n\nBullish on trust networks.\n",
    )
    result = load_stance_context("mario-gabriele", tmp_path)
    # Frontmatter keys must NOT appear in the output
    assert "id: mario-gabriele" not in result
    assert "type: person" not in result
    assert "title:" not in result
    # Body content must appear
    assert "Bullish on trust networks." in result


def test_load_stance_context_body_preserved_verbatim(tmp_path):
    body = "## Core beliefs\n\n- Decentralisation is good.\n- Incumbents are fragile.\n"
    _write_stance(
        tmp_path,
        "packy-mccormick",
        f"---\nid: packy-mccormick\ntype: person\n---\n# Packy McCormick\n\n{body}",
    )
    result = load_stance_context("packy-mccormick", tmp_path)
    assert "## Core beliefs" in result
    assert "Decentralisation is good." in result
    assert "Incumbents are fragile." in result


def test_load_stance_context_file_without_frontmatter(tmp_path):
    """A canonical author page with no frontmatter still returns stance sections."""
    _write_stance(
        tmp_path,
        "plain-author",
        "# Plain Author\n\n## Core beliefs\n\nThis author argues that plain text is underrated.\n",
    )
    result = load_stance_context("plain-author", tmp_path)
    assert "## What this author believed last time you read them" in result
    assert "## Core beliefs" in result
    assert "plain text is underrated" in result


# ---------------------------------------------------------------------------
# stub_stance_doc
# ---------------------------------------------------------------------------


def test_stub_stance_doc_returns_correct_frontmatter():
    record = _make_record()
    frontmatter, _body = stub_stance_doc(record)
    assert frontmatter["id"] == "mario-gabriele"
    assert frontmatter["type"] == "person"
    assert frontmatter["title"] == "Mario Gabriele"
    assert frontmatter["status"] == "active"
    assert "function/reference" in frontmatter["tags"]
    assert "domain/relationships" in frontmatter["tags"]
    assert "signal/canon" in frontmatter["tags"]
    assert frontmatter["domains"] == ["relationships"]
    assert frontmatter["sources"] == []
    assert frontmatter["created"] == "2026-04-02"
    assert frontmatter["last_updated"] == "2026-04-02"
    assert frontmatter["name"] == "Mario Gabriele"
    assert frontmatter["substack_author_id"] == "9001"


def test_stub_stance_doc_body_has_all_four_sections_plus_changelog():
    record = _make_record()
    _frontmatter, body = stub_stance_doc(record)
    assert "Substack author at [[thegeneralist|The Generalist]]." in body
    assert "## Core beliefs" in body
    assert "## Open questions" in body
    assert "## Recent shifts" in body
    assert "## Contradictions observed" in body
    assert "## Changelog" in body


# ---------------------------------------------------------------------------
# read_stance_body
# ---------------------------------------------------------------------------


def test_read_stance_body_missing_file_returns_empty(tmp_path):
    result = read_stance_body(tmp_path, "no-such-author")
    assert result == ""


def test_read_stance_body_strips_frontmatter(tmp_path):
    _write_stance(
        tmp_path,
        "dan-luu",
        "---\nid: dan-luu\ntype: person\n---\n# Dan Luu\n\n## Core beliefs\n\n- Simplicity matters.\n",
    )
    body = read_stance_body(tmp_path, "dan-luu")
    assert "id: dan-luu" not in body
    assert "type: person" not in body
    assert "## Core beliefs" in body
    assert "Simplicity matters." in body


# ---------------------------------------------------------------------------
# write_stance_doc — first ingest (Mode A)
# ---------------------------------------------------------------------------


def test_write_stance_doc_first_ingest_creates_file(tmp_path):
    record = _make_record()
    post_slug = "2026-03-15-on-trust"
    write_stance_doc(
        record=record,
        updated_body=_SAMPLE_UPDATED_BODY,
        change_note="Extended Core beliefs with trust foundation claim.",
        post_slug=post_slug,
        repo_root=tmp_path,
    )
    stance_path = stance_page_path(tmp_path, "mario-gabriele")
    assert stance_path.exists()
    text = stance_path.read_text(encoding="utf-8")
    assert "## Core beliefs" in text
    assert "## Open questions" in text
    assert "## Recent shifts" in text
    assert "## Contradictions observed" in text
    assert "## Changelog" in text
    assert "Extended Core beliefs with trust foundation claim." in text
    assert "Trust is the foundation" in text


def test_write_stance_doc_first_ingest_appends_source(tmp_path):
    record = _make_record()
    post_slug = "2026-03-15-on-trust"
    write_stance_doc(
        record=record,
        updated_body=_SAMPLE_UPDATED_BODY,
        change_note="First ingest.",
        post_slug=post_slug,
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    assert "[[2026-03-15-on-trust]]" in text


# ---------------------------------------------------------------------------
# write_stance_doc — subsequent ingest (Mode B)
# ---------------------------------------------------------------------------


def _first_ingest(tmp_path, record, post_slug, updated_body, change_note):
    write_stance_doc(
        record=record,
        updated_body=updated_body,
        change_note=change_note,
        post_slug=post_slug,
        repo_root=tmp_path,
    )


def test_write_stance_doc_subsequent_ingest_preserves_existing_sources(tmp_path):
    record = _make_record()
    _first_ingest(tmp_path, record, "2026-03-15-on-trust", _SAMPLE_UPDATED_BODY, "First.")

    record2 = _make_record(
        id="140000002",
        title="On Capital",
        slug="on-capital",
        published_at="2026-04-01T09:00:00Z",
        saved_at="2026-04-10T18:00:00Z",
    )
    updated_body2 = """\
## Core beliefs

- Trust and capital are intertwined.

## Open questions

- How do you rebuild trust once broken?

## Recent shifts

- Now believes capital allocation reflects trust hierarchies (previously no stated view).

## Contradictions observed

- (none yet)
"""
    write_stance_doc(
        record=record2,
        updated_body=updated_body2,
        change_note="Added capital-trust connection to Core beliefs.",
        post_slug="2026-04-01-on-capital",
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    assert "[[2026-03-15-on-trust]]" in text
    assert "[[2026-04-01-on-capital]]" in text


def test_write_stance_doc_subsequent_ingest_updates_last_updated(tmp_path):
    record = _make_record(saved_at="2026-04-02T18:00:00Z")
    _first_ingest(tmp_path, record, "2026-03-15-on-trust", _SAMPLE_UPDATED_BODY, "First.")

    record2 = _make_record(
        id="140000002",
        title="On Capital",
        slug="on-capital",
        published_at="2026-04-01T09:00:00Z",
        saved_at="2026-05-01T18:00:00Z",
    )
    write_stance_doc(
        record=record2,
        updated_body=_SAMPLE_UPDATED_BODY,
        change_note="Second ingest.",
        post_slug="2026-04-01-on-capital",
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    assert "2026-05-01" in text


def test_write_stance_doc_subsequent_ingest_appends_changelog(tmp_path):
    record = _make_record()
    _first_ingest(tmp_path, record, "2026-03-15-on-trust", _SAMPLE_UPDATED_BODY, "First entry.")

    record2 = _make_record(
        id="140000002",
        title="On Capital",
        slug="on-capital",
        published_at="2026-04-01T09:00:00Z",
        saved_at="2026-04-10T18:00:00Z",
    )
    write_stance_doc(
        record=record2,
        updated_body=_SAMPLE_UPDATED_BODY,
        change_note="Second entry.",
        post_slug="2026-04-01-on-capital",
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    assert "First entry." in text
    assert "Second entry." in text


def test_write_stance_doc_subsequent_ingest_replaces_section_bodies(tmp_path):
    record = _make_record()
    old_body = """\
## Core beliefs

- OLD belief about trust.

## Open questions

- OLD question.

## Recent shifts

- (none)

## Contradictions observed

- (none)
"""
    _first_ingest(tmp_path, record, "2026-03-15-on-trust", old_body, "First.")

    record2 = _make_record(
        id="140000002",
        title="On Capital",
        slug="on-capital",
        published_at="2026-04-01T09:00:00Z",
        saved_at="2026-04-10T18:00:00Z",
    )
    new_body = """\
## Core beliefs

- NEW belief about trust networks.

## Open questions

- NEW question about capital.

## Recent shifts

- (none)

## Contradictions observed

- (none)
"""
    write_stance_doc(
        record=record2,
        updated_body=new_body,
        change_note="Updated core beliefs.",
        post_slug="2026-04-01-on-capital",
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    assert "NEW belief about trust networks." in text
    assert "OLD belief about trust." not in text


def test_apply_stance_delta_appends_new_bullets_without_rewriting_existing_sections(tmp_path):
    record = _make_record()
    _first_ingest(tmp_path, record, "2026-03-15-on-trust", _SAMPLE_UPDATED_BODY, "First.")

    record2 = _make_record(
        id="140000002",
        title="On Trust Again",
        slug="on-trust-again",
        published_at="2026-04-01T09:00:00Z",
        saved_at="2026-04-10T18:00:00Z",
    )
    delta_body = """\
## Core beliefs

- Trust compounds through repetition.

## Recent shifts

- Now treats trust as a distribution moat (previously framed it as a network effect).
"""
    apply_stance_delta(
        record=record2,
        delta_body=delta_body,
        change_note="Added a new trust-compounding belief.",
        post_slug="2026-04-01-on-trust-again",
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    assert "Trust is the foundation of all durable networks." in text
    assert "Trust compounds through repetition." in text
    assert "distribution moat" in text
    assert "Added a new trust-compounding belief." in text


def test_write_stance_doc_preserves_changelog_order(tmp_path):
    record = _make_record()
    _first_ingest(tmp_path, record, "2026-03-15-on-trust", _SAMPLE_UPDATED_BODY, "First entry.")

    record2 = _make_record(
        id="140000002",
        title="On Capital",
        slug="on-capital",
        published_at="2026-04-01T09:00:00Z",
        saved_at="2026-04-10T18:00:00Z",
    )
    write_stance_doc(
        record=record2,
        updated_body=_SAMPLE_UPDATED_BODY,
        change_note="Second entry.",
        post_slug="2026-04-01-on-capital",
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    first_pos = text.index("First entry.")
    second_pos = text.index("Second entry.")
    assert first_pos < second_pos, "First changelog entry must appear before second"


def test_write_stance_doc_subsequent_ingest_deduplicates_sources(tmp_path):
    """Writing the same post_slug twice must not add duplicate sources."""
    record = _make_record()
    post_slug = "2026-03-15-on-trust"
    _first_ingest(tmp_path, record, post_slug, _SAMPLE_UPDATED_BODY, "First.")
    # Write again with same post_slug
    write_stance_doc(
        record=record,
        updated_body=_SAMPLE_UPDATED_BODY,
        change_note="Duplicate call.",
        post_slug=post_slug,
        repo_root=tmp_path,
    )
    text = stance_page_path(tmp_path, "mario-gabriele").read_text(encoding="utf-8")
    count = text.count("[[2026-03-15-on-trust]]")
    # Should appear in sources list only once (may also appear in changelog entries)
    sources_section = text.split("---")[1] if "---" in text else ""
    assert sources_section.count("[[2026-03-15-on-trust]]") == 1


def test_migrate_legacy_stance_pages_merges_into_author_page_and_deletes_legacy(tmp_path):
    people_dir = tmp_path / "wiki" / "people"
    people_dir.mkdir(parents=True, exist_ok=True)
    author_page = people_dir / "mario-gabriele.md"
    author_page.write_text(
        "---\n"
        "id: mario-gabriele\n"
        "type: person\n"
        "title: Mario Gabriele\n"
        "sources: []\n"
        "---\n\n"
        "# Mario Gabriele\n\n"
        "Substack author at [[thegeneralist|The Generalist]].\n",
        encoding="utf-8",
    )
    legacy = people_dir / "mario-gabriele-stance.md"
    legacy.write_text(
        "---\n"
        "id: mario-gabriele-stance\n"
        "type: note\n"
        "title: Mario Gabriele — Current Stance\n"
        "last_updated: 2026-04-10\n"
        "sources:\n"
        "  - \"[[2026-03-15-on-trust]]\"\n"
        "---\n\n"
        "# Mario Gabriele — Current Stance\n\n"
        "## Core beliefs\n\n"
        "- Trust compounds.\n\n"
        "## Open questions\n\n"
        "- How do you rebuild it?\n\n"
        "## Recent shifts\n\n"
        "- (none)\n\n"
        "## Contradictions observed\n\n"
        "- (none)\n\n"
        "## Changelog\n\n"
        "- 2026-04-10 — Seeded from stance doc.\n",
        encoding="utf-8",
    )

    migrated = migrate_legacy_stance_pages(tmp_path)
    assert migrated == [author_page]
    assert not legacy.exists()
    content = author_page.read_text(encoding="utf-8")
    assert "## Core beliefs" in content
    assert "Trust compounds." in content
    assert "## Changelog" in content
    assert "Seeded from stance doc." in content
    assert "[[2026-03-15-on-trust]]" in content
