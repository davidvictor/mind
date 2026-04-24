"""Smoke tests for scripts.books.enrich.apply_to_you after the profile helper move.

Verifies the wiring: apply_to_you -> load_profile_context (from new location)
-> gemini.applied_to_you. Does NOT call real Gemini.
"""
from __future__ import annotations

import pytest

import scripts.common.profile as profile_module
from scripts.books import enrich
from scripts.books.parse import BookRecord


class _FakeIdentity:
    def to_dict(self):
        return {"provider": "test", "model": "test"}


@pytest.fixture(autouse=True)
def reset_profile_cache():
    profile_module._PROFILE_CACHE = None
    yield
    profile_module._PROFILE_CACHE = None


@pytest.fixture()
def fake_env(tmp_path, monkeypatch):
    from scripts.common import env

    class FakeCfg:
        gemini_api_key = "fake"
        llm_model = "fake"
        browser_for_cookies = "chrome"
        repo_root = tmp_path
        substack_session_cookie = ""

    monkeypatch.setattr(env, "load", lambda: FakeCfg())
    return tmp_path


def test_apply_to_you_smoke(fake_env, monkeypatch):
    """Happy path: profile present + stubbed Gemini -> returns the stub dict."""
    me_dir = fake_env / "wiki" / "me"
    me_dir.mkdir(parents=True)
    (me_dir / "profile.md").write_text("I am Example Owner.")
    (me_dir / "positioning.md").write_text("Founder, builder.")

    stub = {
        "applied_paragraph": "This book matters because X.",
        "applied_bullets": [],
        "thread_links": [],
    }
    call_count = {"n": 0}

    def fake_applied_to_you(title, author, profile_ctx, deep_research):
        call_count["n"] += 1
        # Sanity check: profile context made it through
        assert "I am Example Owner." in profile_ctx
        assert "Founder, builder." in profile_ctx
        return stub

    monkeypatch.setattr(
        "scripts.books.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities": staticmethod(lambda **kwargs: [_FakeIdentity()]),
                "applied_to_you": staticmethod(lambda **kwargs: fake_applied_to_you(
                    kwargs["title"],
                    kwargs["author"],
                    kwargs["profile_context"],
                    kwargs["research"],
                )),
            },
        )(),
    )

    book = BookRecord(title="Test Book", author=["Test Author"], status="finished")
    result = enrich.apply_to_you(
        book,
        deep_research={"tldr": "t", "core_argument": "c", "key_frameworks": [], "topics": []},
    )

    assert result == stub
    assert call_count["n"] == 1
    # And it cached to disk under tmp_path (not the real raw/ tree)
    assert enrich.applied_path(fake_env, book).exists()


def test_apply_to_you_empty_profile_short_circuits(fake_env, monkeypatch):
    """No wiki/me/ files -> returns empty stub WITHOUT calling Gemini."""
    # Intentionally do not create wiki/me/

    call_count = {"n": 0}

    def fake_applied_to_you(*args, **kwargs):
        call_count["n"] += 1
        return {"should": "not be called"}

    monkeypatch.setattr(
        "scripts.books.enrich.get_llm_service",
        lambda: type(
            "FakeService",
            (),
            {
                "cache_identities": staticmethod(lambda **kwargs: [_FakeIdentity()]),
                "applied_to_you": staticmethod(fake_applied_to_you),
            },
        )(),
    )

    book = BookRecord(title="Test Book", author=["Test Author"], status="finished")
    result = enrich.apply_to_you(
        book,
        deep_research={"tldr": "t", "core_argument": "c", "key_frameworks": [], "topics": []},
    )

    assert result == {"applied_paragraph": "", "applied_bullets": [], "thread_links": []}
    assert call_count["n"] == 0
