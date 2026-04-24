"""Tests for scripts/books/write_pages.py id alignment."""
from datetime import date as _date
import pytest

from scripts.common import env
from scripts.books.parse import BookRecord
from scripts.books import write_pages
from tests.support import write_repo_config


def _fake_cfg(tmp_path):
    class FakeCfg:
        gemini_api_key = "fake"
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path
        wiki_root = tmp_path / "memory"
        raw_root = tmp_path / "raw"
        substack_session_cookie = ""
    return FakeCfg()


@pytest.fixture(autouse=True)
def _configured_repo(tmp_path, monkeypatch):
    write_repo_config(tmp_path)
    monkeypatch.setattr(env, "load", lambda: _fake_cfg(tmp_path))


def _make_book(**overrides) -> BookRecord:
    defaults = dict(
        title="Designing Data-Intensive Applications",
        author=["Martin Kleppmann"],
        status="finished",
        finished_date="2026-03-15",
    )
    defaults.update(overrides)
    return BookRecord(**defaults)


FAKE_ENRICHED = {
    "tldr": "A thorough guide to building data systems.",
    "key_ideas": [{"idea": "Idempotency matters", "explanation": "..."}],
    "frameworks_introduced": [],
    "in_conversation_with": [],
    "notable_quotes": [],
    "takeaways": [],
    "topics": [],
}

FAKE_DEEP_ENRICHED = {
    "tldr": "A thorough guide to building data systems.",
    "core_argument": "Systems design is about explicit tradeoffs.",
    "key_frameworks": [],
    "memorable_stories": [],
    "counterarguments": [],
    "famous_quotes": [],
    "in_conversation_with": [],
    "topics": [],
}

FAKE_APPLIED = {
    "applied_paragraph": "This book sharpens Example Owner's systems instincts.",
    "applied_bullets": [{"claim": "Name the tradeoff", "why_it_matters": "It avoids cargo culting", "action": "Write the decision down"}],
    "thread_links": ["system-tradeoffs"],
}

EMPTY_APPLIED = {"applied_paragraph": "", "applied_bullets": [], "thread_links": []}

FAKE_STANCE_CHANGE = "The author is now more explicit about operability versus consistency tradeoffs."


def test_write_book_page_uses_filename_slug_as_id_no_asin(tmp_path):
    book = _make_book()
    path = write_pages.write_book_page(
        book,
        FAKE_ENRICHED,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert f"id: {path.stem}" in content
    # No asin → no external_id
    assert "external_id:" not in content
    assert "retention: keep" in content
    assert "domains:\n  - business" in content
    assert "synthesis_mode: deep" in content


def test_write_book_page_adds_external_id_when_asin_present(tmp_path):
    book = _make_book(asin="B00BOOK123")
    path = write_pages.write_book_page(
        book,
        FAKE_ENRICHED,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert f"id: {path.stem}" in content
    assert "external_id: audible-B00BOOK123" in content


def test_write_summary_page_uses_filename_slug_as_id(tmp_path):
    book = _make_book(asin="B00BOOK123")
    path = write_pages.write_summary_page(
        book,
        FAKE_ENRICHED,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert f"id: {path.stem}" in content
    assert "external_id: audible-B00BOOK123" in content
    assert "domains:\n  - business" in content


def test_write_book_page_emits_three_tag_axes(tmp_path):
    book = _make_book()
    enriched = dict(FAKE_ENRICHED)
    enriched["topics"] = ["distributed-systems"]
    path = write_pages.write_book_page(
        book,
        enriched,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content
    assert "  - distributed-systems" in content
    assert "  - book" not in content


def test_write_summary_book_page_emits_three_tag_axes(tmp_path):
    book = _make_book(asin="B00BOOK123")
    enriched = dict(FAKE_ENRICHED)
    enriched["topics"] = ["distributed-systems"]
    path = write_pages.write_summary_page(
        book,
        enriched,
        category="business",
        policy={"retention": "keep", "domains": ["business"], "synthesis_mode": "deep"},
    )
    content = path.read_text(encoding="utf-8")
    assert "domain/" in content
    assert "function/" in content
    assert "signal/" in content
    assert "  - distributed-systems" in content
    assert "  - book" not in content


def test_write_book_pages_follow_flattened_memory_layout(tmp_path):
    book = _make_book()

    book_path = write_pages.write_book_page(book, FAKE_ENRICHED, category="business")
    summary_path = write_pages.write_summary_page(book, FAKE_ENRICHED, category="business")

    assert book_path == tmp_path / "memory" / "sources" / "books" / "business" / "martin-kleppmann-designing-data-intensive-applications.md"
    assert summary_path == book_path

    book_content = book_path.read_text(encoding="utf-8")
    assert "source_path: ../../../../raw/research/books/martin-kleppmann-designing-data-intensive-applications.summary.json" in book_content
    assert "summary-martin-kleppmann-designing-data-intensive-applications" in book_content


def test_write_book_page_renders_phase3_sections_when_present(tmp_path):
    book = _make_book()
    path = write_pages.write_book_page(
        book,
        FAKE_DEEP_ENRICHED,
        category="business",
        applied=FAKE_APPLIED,
        stance_change_note=FAKE_STANCE_CHANGE,
    )
    content = path.read_text(encoding="utf-8")
    assert "## Applied to You" in content
    assert "This book sharpens Example Owner's systems instincts." in content
    assert "system-tradeoffs" in content
    assert "## Author Stance Update" in content
    assert FAKE_STANCE_CHANGE in content


def test_write_summary_page_renders_phase3_sections_when_present(tmp_path):
    book = _make_book()
    path = write_pages.write_summary_page(
        book,
        FAKE_DEEP_ENRICHED,
        category="business",
        applied=FAKE_APPLIED,
        stance_change_note=FAKE_STANCE_CHANGE,
    )
    assert path == write_pages.book_page_path(tmp_path, book, "business")
    content = path.read_text(encoding="utf-8")
    assert "## Applied to You" in content
    assert "## Author Stance Update" in content
    assert FAKE_STANCE_CHANGE in content


def test_write_book_pages_omit_empty_phase3_sections_cleanly(tmp_path):
    book = _make_book()
    book_path = write_pages.write_book_page(
        book,
        FAKE_DEEP_ENRICHED,
        category="business",
        applied=EMPTY_APPLIED,
        stance_change_note="",
    )
    summary_path = write_pages.write_summary_page(
        book,
        FAKE_DEEP_ENRICHED,
        category="business",
        applied=EMPTY_APPLIED,
        stance_change_note=None,
    )
    book_content = book_path.read_text(encoding="utf-8")
    summary_content = summary_path.read_text(encoding="utf-8")
    assert "## Applied to You" not in book_content
    assert "## Author Stance Update" not in book_content
    assert "## Applied to You" not in summary_content
    assert "## Author Stance Update" not in summary_content


def test_write_book_page_force_rewrite_preserves_created_and_ingested(tmp_path, monkeypatch):
    book = _make_book()

    class _OldDate(_date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 15)

    class _NewDate(_date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 18)

    monkeypatch.setattr(write_pages, "date", _OldDate)
    path = write_pages.write_book_page(
        book,
        FAKE_DEEP_ENRICHED,
        category="business",
    )
    monkeypatch.setattr(write_pages, "date", _NewDate)
    write_pages.write_book_page(
        book,
        {**FAKE_DEEP_ENRICHED, "core_argument": "Updated core argument."},
        category="business",
        force=True,
    )
    content = path.read_text(encoding="utf-8")
    assert "created: 2026-04-15" in content
    assert "ingested: 2026-04-15" in content
    assert "last_updated: 2026-04-18" in content
