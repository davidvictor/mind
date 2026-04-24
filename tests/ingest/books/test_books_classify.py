"""Tests for the books classifier wrapper.

Uses dependency injection (monkeypatch) to fake out the gemini.classify_book
call, so the tests don't need a real GEMINI_API_KEY or network.
"""
import json
from pathlib import Path

import pytest

from scripts.books.parse import BookRecord
from scripts.books import enrich


class _FakeIdentity:
    def to_dict(self):
        return {"provider": "test", "model": "test"}


def _fake_service(response):
    return type(
        "FakeService",
        (),
        {
            "cache_identities": staticmethod(lambda **kwargs: [_FakeIdentity()]),
            "classify_book": staticmethod(lambda **kwargs: response),
        },
    )()


def test_classify_business(tmp_path, monkeypatch):
    """Books classified as business should return the dict and cache to disk."""
    # Patch env.load() to return a Config with repo_root=tmp_path
    from scripts.common import env

    class FakeCfg:
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path

    monkeypatch.setattr(env, "load", lambda: FakeCfg())
    monkeypatch.setattr("scripts.books.enrich.get_llm_service", lambda: _fake_service({"category": "business", "confidence": "high", "reasoning": "tech book"}))

    book = BookRecord(
        title="Designing Data-Intensive Applications",
        author=["Martin Kleppmann"],
        status="finished",
    )
    result = enrich.classify(book)
    assert result["category"] == "business"
    assert result["retention"] == "keep"
    assert result["domains"] == ["business"]
    assert result["synthesis_mode"] == "deep"
    assert result["confidence"] == "high"

    # Verify it cached
    cache = enrich.classification_path(tmp_path, book)
    assert cache.exists()
    assert json.loads(cache.read_text())["data"]["category"] == "business"


def test_classify_personal(tmp_path, monkeypatch):
    from scripts.common import env

    class FakeCfg:
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path

    monkeypatch.setattr(env, "load", lambda: FakeCfg())
    monkeypatch.setattr("scripts.books.enrich.get_llm_service", lambda: _fake_service({"category": "personal", "confidence": "medium", "reasoning": "history"}))

    book = BookRecord(title="The Power Broker", author=["Robert Caro"], status="finished")
    result = enrich.classify(book)
    assert result["category"] == "personal"
    assert result["retention"] == "keep"
    assert result["domains"] == ["personal"]
    assert result["synthesis_mode"] == "light"


def test_classify_ignore(tmp_path, monkeypatch):
    from scripts.common import env

    class FakeCfg:
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path

    monkeypatch.setattr(env, "load", lambda: FakeCfg())
    monkeypatch.setattr("scripts.books.enrich.get_llm_service", lambda: _fake_service({"category": "ignore", "confidence": "high", "reasoning": "self-help"}))

    book = BookRecord(title="The Subtle Art", author=["Mark Manson"], status="finished")
    result = enrich.classify(book)
    assert result["category"] == "ignore"
    assert result["retention"] == "exclude"
    assert result["domains"] == ["personal"]
    assert result["synthesis_mode"] == "none"


def test_classify_uses_cache_on_second_call(tmp_path, monkeypatch):
    """Second call should NOT invoke the gemini function — cache should be used."""
    from scripts.common import env

    class FakeCfg:
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path

    monkeypatch.setattr(env, "load", lambda: FakeCfg())

    call_count = {"n": 0}
    def fake_classify(**kwargs):
        call_count["n"] += 1
        return {"category": "business", "confidence": "high", "reasoning": "ok"}

    monkeypatch.setattr(
        "scripts.books.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities": staticmethod(lambda **kwargs: [_FakeIdentity()]),
                "classify_book": staticmethod(fake_classify),
            },
        )(),
    )

    book = BookRecord(title="Test", author=["Author"], status="finished")
    enrich.classify(book)
    enrich.classify(book)  # second call should use cache
    assert call_count["n"] == 1
