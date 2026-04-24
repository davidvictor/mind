from __future__ import annotations

from pathlib import Path

from scripts.links.importer import append_links_drop, load_links
from tests.paths import FIXTURES_ROOT


FIXTURE = FIXTURES_ROOT / "links-sample.json"


def test_load_links_extracts_nested_bookmarks():
    links = load_links(FIXTURE)
    assert len(links) == 2
    assert links[0].url == "https://example.com/posts/latency"
    assert links[0].folder == "AI"
    assert links[1].notes == "Useful for the brain architecture work"


def test_append_links_drop_writes_links_queue(tmp_path: Path):
    links = load_links(FIXTURE)
    target = append_links_drop(tmp_path, links=links, today_str="2026-04-08")
    assert target == tmp_path / "raw" / "drops" / "articles-from-links-2026-04-08.jsonl"
    text = target.read_text(encoding="utf-8")
    assert "links-import" in text
    assert "https://example.com/posts/latency" in text
