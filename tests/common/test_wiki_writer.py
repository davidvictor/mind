from pathlib import Path

import pytest

from scripts.common.frontmatter import split_frontmatter
from scripts.common.wiki_writer import write_page


def test_writes_v2_frontmatter_then_body(tmp_path: Path):
    target = tmp_path / "wiki" / "sources" / "youtube" / "abc123-test-video.md"
    write_page(
        target,
        frontmatter={
            "id": "abc123-test-video",
            "type": "video",
            "title": "Test Video",
            "status": "active",
            "created": "2026-04-06",
            "last_updated": "2026-04-06",
            "tags": ["learning", "reference", "compounding"],
            "domains": ["learning"],
            "youtube_id": "abc12345678",
            "channel": "Test Channel",
            "duration_minutes": 12,
            "topics": ["ml", "ai"],
        },
        body="## TL;DR\n\nThis is a test.\n",
    )
    text = target.read_text()
    assert text.startswith("---\n")
    assert "id: abc123-test-video\n" in text
    assert "youtube_id: abc12345678\n" in text
    assert "topics:\n  - ml\n  - ai\n" in text
    assert "---\n\n## TL;DR" in text
    assert text.endswith("\n")


def test_creates_parent_directories(tmp_path: Path):
    target = tmp_path / "wiki" / "sources" / "youtube" / "deep" / "nested" / "page.md"
    write_page(target, frontmatter={"id": "page", "type": "note", "title": "x", "status": "active",
                                     "created": "2026-04-06", "last_updated": "2026-04-06",
                                     "tags": ["learning", "active", "nice-to-have"],
                                     "domains": ["learning"]}, body="body")
    assert target.exists()


def test_refuses_overwrite_without_force(tmp_path: Path):
    target = tmp_path / "page.md"
    target.write_text("existing content")
    with pytest.raises(FileExistsError):
        write_page(target, frontmatter={"id": "page", "type": "note", "title": "x",
                                         "status": "active", "created": "2026-04-06",
                                         "last_updated": "2026-04-06",
                                         "tags": ["learning", "active", "nice-to-have"],
                                         "domains": ["learning"]}, body="new")
    assert target.read_text() == "existing content"


def test_quotes_apostrophes_in_frontmatter_scalars(tmp_path: Path):
    target = tmp_path / "page.md"
    write_page(
        target,
        frontmatter={
            "id": "page",
            "type": "stance",
            "title": "'Too expensive' in exit interviews is a value-delivery signal",
            "status": "active",
            "created": "2026-04-18",
            "last_updated": "2026-04-18",
            "tags": ["domain/meta", "function/stance", "signal/working"],
            "domains": ["meta"],
        },
        body="body",
    )

    text = target.read_text(encoding="utf-8")
    assert 'title: "\'Too expensive\' in exit interviews is a value-delivery signal"\n' in text
    frontmatter, body = split_frontmatter(text)
    assert frontmatter["title"] == "'Too expensive' in exit interviews is a value-delivery signal"
    assert frontmatter["type"] == "stance"
    assert body == "\nbody\n"
