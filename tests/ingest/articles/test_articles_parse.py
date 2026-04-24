from pathlib import Path

import pytest

from scripts.articles import parse
from tests.paths import FIXTURES_ROOT


FIXTURE = FIXTURES_ROOT / "articles" / "drop-smoketest.jsonl"


def test_parses_jsonl_into_drop_entries():
    entries = list(parse.parse_drop_file(FIXTURE))
    # 4 lines but 2 share a URL, so dedupe → 3
    assert len(entries) == 3


def test_first_entry_fields():
    entries = list(parse.parse_drop_file(FIXTURE))
    e = entries[0]
    assert e.url == "https://stratechery.com/2024/aggregators-and-jobs-to-be-done"
    assert e.source_post_id == "190000001"
    assert e.source_post_url == "https://thegeneralist.substack.com/p/on-trust"
    assert e.source_page_id == ""
    assert e.anchor_text == "aggregators"
    assert e.context_snippet == "see aggregators for context"
    assert e.category == "business"
    assert e.discovered_at == "2026-04-02T18:00:00Z"
    assert e.source_type == "substack-link"


def test_dedupe_keeps_first_occurrence(tmp_path):
    """When the same URL appears twice, the first entry wins (its source_post_id)."""
    entries = list(parse.parse_drop_file(FIXTURE))
    stratechery = [e for e in entries if "stratechery" in e.url]
    assert len(stratechery) == 1
    assert stratechery[0].source_post_id == "190000001"


def test_skips_malformed_lines(tmp_path):
    """A bad JSON line should be skipped, not crash the whole file."""
    bad = tmp_path / "bad.jsonl"
    bad.write_text(
        '{"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u", "anchor_text": "a", "context_snippet": "c", "category": "business", "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"}\n'
        'not valid json\n'
        '{"url": "https://b.com/y", "source_post_id": "2", "source_post_url": "u", "anchor_text": "b", "context_snippet": "c", "category": "personal", "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"}\n'
    )
    entries = list(parse.parse_drop_file(bad))
    assert len(entries) == 2
    assert entries[0].url == "https://a.com/x"
    assert entries[1].url == "https://b.com/y"


def test_ignores_blank_lines(tmp_path):
    blank = tmp_path / "blank.jsonl"
    blank.write_text(
        '\n\n{"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "u", "anchor_text": "a", "context_snippet": "c", "category": "business", "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"}\n\n'
    )
    entries = list(parse.parse_drop_file(blank))
    assert len(entries) == 1


def test_empty_file_returns_empty(tmp_path):
    empty = tmp_path / "empty.jsonl"
    empty.write_text("")
    assert list(parse.parse_drop_file(empty)) == []


def test_parses_optional_source_page_id(tmp_path):
    path = tmp_path / "with-source-page-id.jsonl"
    path.write_text(
        '{"url": "https://a.com/x", "source_post_id": "1", "source_post_url": "https://example.com/source", "source_page_id": "2026-04-02-on-trust", "anchor_text": "a", "context_snippet": "c", "category": "business", "discovered_at": "2026-04-07T00:00:00Z", "source_type": "substack-link"}\n',
        encoding="utf-8",
    )
    entries = list(parse.parse_drop_file(path))
    assert len(entries) == 1
    assert entries[0].source_page_id == "2026-04-02-on-trust"
