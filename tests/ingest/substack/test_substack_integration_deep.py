"""Phase 7 — deep enrichment integration smoketest.

Runs the full deep-enrichment pipeline (Pass A+B+C + entities + quote verification
+ writer) against 3 fixture posts with different shapes, all in a tmp_path repo.
Verifies caches, stance docs, entity inbox, and rendered pages.

Mocks all Gemini calls — no network, no API key required.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mind.services.llm_cache import LLMCacheIdentity
from scripts.substack import enrich, stance, write_pages
from scripts.substack.parse import SubstackRecord
from tests.paths import FIXTURES_ROOT


_FAKE_IDENTITY = LLMCacheIdentity(
    task_class="summary",
    provider="google",
    model="gemini-test",
    transport="direct",
    api_family="genai",
    input_mode="text",
    prompt_version="test.v1",
)


FIXTURES = FIXTURES_ROOT / "substack" / "deep"


# ---------------------------------------------------------------------------
# Fixture loaders
# ---------------------------------------------------------------------------


def _load_record(name: str) -> SubstackRecord:
    data = json.loads((FIXTURES / f"{name}.json").read_text())
    return SubstackRecord(**data)


def _load_body(post_num: str) -> str:
    return (FIXTURES / f"post_{post_num}_body.md").read_text()


def _load_response(pass_letter: str, post_num: str) -> dict:
    return json.loads((FIXTURES / f"post_{post_num}_pass_{pass_letter}_response.json").read_text())


# ---------------------------------------------------------------------------
# Fake Gemini builder
# ---------------------------------------------------------------------------


def _build_fake_gemini(responses_by_post: dict[str, dict]) -> dict:
    """Build fake Gemini callables dispatching by post title.

    responses_by_post: {
        "a": {post_key: {**response, "_for_title": title}, ...},
        "b": {post_key: {**response, "_for_title": title}, ...},
        "c": {post_key: {**response, "_for_title": title}, ...},
    }
    Returns a dict with fake callables and per-pass call counters.
    """
    call_counts = {"summarize": 0, "applied": 0, "stance": 0}

    def fake_summarize(
        title,
        publication,
        author,
        body_markdown,
        prior_posts_context="",
        stance_context="",
    ):
        call_counts["summarize"] += 1
        for resp in responses_by_post["a"].values():
            if resp.get("_for_title") == title:
                return {k: v for k, v in resp.items() if not k.startswith("_")}
        raise KeyError(f"no fake summarize response for title={title!r}")

    def fake_applied(title, publication, author, profile_context, summary):
        call_counts["applied"] += 1
        for resp in responses_by_post["b"].values():
            if resp.get("_for_title") == title:
                return {k: v for k, v in resp.items() if not k.startswith("_")}
        raise KeyError(f"no fake applied response for title={title!r}")

    def fake_stance(author, title, post_slug, current_stance, summary):
        call_counts["stance"] += 1
        for resp in responses_by_post["c"].values():
            if resp.get("_for_title") == title:
                return {k: v for k, v in resp.items() if not k.startswith("_")}
        raise KeyError(f"no fake stance response for title={title!r}")

    return {
        "fake_summarize": fake_summarize,
        "fake_applied": fake_applied,
        "fake_stance": fake_stance,
        "call_counts": call_counts,
    }


# ---------------------------------------------------------------------------
# Profile seeding + cache reset
# ---------------------------------------------------------------------------


def _seed_profile(repo_root: Path) -> None:
    """Write minimal wiki/me/ files so apply_post_to_you has profile context."""
    me = repo_root / "wiki" / "me"
    me.mkdir(parents=True, exist_ok=True)
    (me / "profile.md").write_text("# Example Owner\n\nDesign engineer, Example Health App founder.\n")
    (me / "positioning.md").write_text("# Positioning\n\nBuilding the Brain wiki.\n")
    (me / "values.md").write_text("# Values\n\n- Craft\n- Honesty\n")
    (me / "open-inquiries.md").write_text("# Open Inquiries\n\n- Brain wiki\n- Example Health App\n")


def _reset_profile_cache() -> None:
    import scripts.common.profile as profile_mod
    profile_mod._PROFILE_CACHE = None


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _run_pipeline(
    record: SubstackRecord,
    body_md: str,
    repo_root: Path,
    fakes: dict,
    classified_links: dict | None = None,
    today: str = "2026-04-07",
) -> dict:
    """Run the full deep enrichment pipeline for one record under mocked Gemini."""
    if classified_links is None:
        classified_links = {"external_classified": [], "substack_internal": []}

    svc = MagicMock()
    svc.cache_identities.return_value = [_FAKE_IDENTITY]
    svc.summarize_substack_post.side_effect = fakes["fake_summarize"]
    svc.applied_to_post.side_effect = fakes["fake_applied"]
    svc.update_author_stance.side_effect = fakes["fake_stance"]

    with patch("scripts.substack.enrich._get_llm_service", return_value=svc):
        prior_context = enrich.get_prior_posts_context(record, repo_root)
        author_slug = write_pages.slugify(record.author_name)
        stance_ctx = stance.load_stance_context(author_slug, repo_root)

        summary = enrich.summarize_post(
            record,
            body_markdown=body_md,
            repo_root=repo_root,
            prior_posts_context=prior_context,
            stance_context=stance_ctx,
        )
        summary = enrich.verify_quotes(
            summary, body_md, record, repo_root, _now="2026-04-07T12:00:00Z"
        )
        applied = enrich.apply_post_to_you(record, summary=summary, repo_root=repo_root)
        stance_change_note = enrich.update_author_stance(
            record, summary=summary, repo_root=repo_root
        )
        logged_entities = enrich.log_entities(
            record,
            summary=summary,
            body_markdown=body_md,
            repo_root=repo_root,
            today=today,
        )

        write_pages.write_article_page(
            record,
            summary=summary,
            classified_links=classified_links,
            body_markdown=body_md,
            repo_root=repo_root,
            applied=applied,
            stance_change_note=stance_change_note,
        )
        write_pages.write_summary_page(
            record,
            summary=summary,
            repo_root=repo_root,
            applied=applied,
            stance_change_note=stance_change_note,
        )

    return {
        "summary": summary,
        "applied": applied,
        "stance_change_note": stance_change_note,
        "logged_entities": logged_entities,
    }


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """Temp repo with wiki/me profile, env.load patched to point to tmp_path."""
    _reset_profile_cache()
    _seed_profile(tmp_path)

    import scripts.common.env as env_mod

    class _FakeCfg:
        repo_root = tmp_path
        gemini_api_key = "fake"
        substack_session_cookie = "fake"

    monkeypatch.setattr(env_mod, "load", lambda: _FakeCfg())
    yield tmp_path
    _reset_profile_cache()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDeepEnrichmentPipeline:

    def test_post_1_first_ingest_full_pipeline(self, repo):
        """Post 1: simple first-ingest case with no prior context or stance doc."""
        record = _load_record("post_1_first_ingest")
        body = _load_body("1")
        fakes = _build_fake_gemini(
            {
                "a": {"p1": {**_load_response("a", "1"), "_for_title": record.title}},
                "b": {"p1": {**_load_response("b", "1"), "_for_title": record.title}},
                "c": {"p1": {**_load_response("c", "1"), "_for_title": record.title}},
            }
        )

        _run_pipeline(record, body, repo, fakes)

        # All three passes called exactly once
        assert fakes["call_counts"]["summarize"] == 1
        assert fakes["call_counts"]["applied"] == 1
        assert fakes["call_counts"]["stance"] == 1

        # All three caches written
        cache_dir = repo / "raw" / "transcripts" / "substack"
        assert (cache_dir / f"{record.id}.json").exists()
        assert (cache_dir / f"{record.id}.applied.json").exists()
        assert (cache_dir / f"{record.id}.stance.json").exists()

        # Canonical author page created for Dan Luu
        author_slug = write_pages.slugify(record.author_name)
        assert (repo / "wiki" / "people" / f"{author_slug}.md").exists()

        # Entity inbox has at least one entry
        entity_inbox = repo / "wiki" / "inbox" / "substack-entities-2026-04-07.md"
        assert entity_inbox.exists()
        content = entity_inbox.read_text()
        assert "referenced by" in content

        # Article page rendered with all key sections
        article_path = write_pages.article_page_path(repo, record)
        assert article_path.exists()
        article = article_path.read_text()
        assert "## TL;DR" in article
        assert "## Core Argument" in article
        assert "## Key Claims (with receipts)" in article
        assert "## Applied to You" in article
        assert "## Questions This Raises for You" in article
        assert "## Author Stance Update" in article

        # Canonical article page is the only durable source page
        assert not write_pages.summary_page_path(repo, record).exists()

        # Quote verification passed for Post 1 — no sidecar
        warnings_path = cache_dir / f"{record.id}.quote-warnings.json"
        assert not warnings_path.exists()

    def test_post_2_cross_post_weave_after_post_1(self, repo):
        """Post 2: run AFTER Post 1, verify stance update + quote flagging."""
        # --- First, ingest Post 1 ---
        rec1 = _load_record("post_1_first_ingest")
        body1 = _load_body("1")
        fakes1 = _build_fake_gemini(
            {
                "a": {"p1": {**_load_response("a", "1"), "_for_title": rec1.title}},
                "b": {"p1": {**_load_response("b", "1"), "_for_title": rec1.title}},
                "c": {"p1": {**_load_response("c", "1"), "_for_title": rec1.title}},
            }
        )
        _run_pipeline(rec1, body1, repo, fakes1)

        # --- Then ingest Post 2 ---
        rec2 = _load_record("post_2_cross_post_weave")
        body2 = _load_body("2")
        fakes2 = _build_fake_gemini(
            {
                "a": {"p2": {**_load_response("a", "2"), "_for_title": rec2.title}},
                "b": {"p2": {**_load_response("b", "2"), "_for_title": rec2.title}},
                "c": {"p2": {**_load_response("c", "2"), "_for_title": rec2.title}},
            }
        )
        _run_pipeline(rec2, body2, repo, fakes2)

        # Canonical author page should now have 2 changelog entries (one from each post)
        author_slug = write_pages.slugify(rec2.author_name)
        stance_path = repo / "wiki" / "people" / f"{author_slug}.md"
        assert stance_path.exists()
        stance_text = stance_path.read_text()
        assert "## Changelog" in stance_text
        changelog_section = stance_text.split("## Changelog", 1)[1]
        # Two entries: one from Post 1, one from Post 2
        assert changelog_section.count("\n- ") >= 2

        # Quote verification should have flagged at least one claim in Post 2
        # (the paraphrased evidence_quote that is not a substring of the body)
        cache_dir = repo / "raw" / "transcripts" / "substack"
        warnings_path = cache_dir / f"{rec2.id}.quote-warnings.json"
        assert warnings_path.exists()
        warnings = json.loads(warnings_path.read_text())
        assert len(warnings["unverified_claims"]) >= 1

        # Article page should contain ⚠️ for the unverified claim
        article = write_pages.article_page_path(repo, rec2).read_text()
        assert "⚠️" in article

        # relates_to_prior from Post 2's Pass A response should appear in article
        assert "In Conversation With" in article or "Prior posts" in article or "why-benchmarks-lie" in article.lower() or "Why Benchmarks Lie" in article

    def test_post_3_different_author_independent_stance(self, repo):
        """Post 3: different author — new stance doc independent of Posts 1/2."""
        rec3 = _load_record("post_3_entity_heavy")
        body3 = _load_body("3")
        fakes3 = _build_fake_gemini(
            {
                "a": {"p3": {**_load_response("a", "3"), "_for_title": rec3.title}},
                "b": {"p3": {**_load_response("b", "3"), "_for_title": rec3.title}},
                "c": {"p3": {**_load_response("c", "3"), "_for_title": rec3.title}},
            }
        )
        _run_pipeline(rec3, body3, repo, fakes3)

        # Canonical author page created for Gergely Orosz
        author_slug = write_pages.slugify(rec3.author_name)
        stance_path = repo / "wiki" / "people" / f"{author_slug}.md"
        assert stance_path.exists()
        # Dan Luu's author page was NOT created (no Post 1 run)
        dan_slug = write_pages.slugify("Dan Luu")
        assert not (repo / "wiki" / "people" / f"{dan_slug}.md").exists()

        # Entity inbox should have many entries (Post 3 has 15 entities)
        entity_inbox = repo / "wiki" / "inbox" / "substack-entities-2026-04-07.md"
        assert entity_inbox.exists()
        content = entity_inbox.read_text()
        # Should have at least 5 bold entity lines
        assert content.count("- **") >= 5

        # All three caches written
        cache_dir = repo / "raw" / "transcripts" / "substack"
        assert (cache_dir / f"{rec3.id}.json").exists()
        assert (cache_dir / f"{rec3.id}.applied.json").exists()
        assert (cache_dir / f"{rec3.id}.stance.json").exists()

        # Article page written
        assert write_pages.article_page_path(repo, rec3).exists()

        # No quote warnings for Post 3 (all quotes are verbatim substrings)
        warnings_path = cache_dir / f"{rec3.id}.quote-warnings.json"
        assert not warnings_path.exists()

    def test_rerun_is_no_op(self, repo):
        """Second pipeline run on the same post should produce zero Gemini calls."""
        rec = _load_record("post_1_first_ingest")
        body = _load_body("1")

        # First run
        fakes1 = _build_fake_gemini(
            {
                "a": {"p1": {**_load_response("a", "1"), "_for_title": rec.title}},
                "b": {"p1": {**_load_response("b", "1"), "_for_title": rec.title}},
                "c": {"p1": {**_load_response("c", "1"), "_for_title": rec.title}},
            }
        )
        _run_pipeline(rec, body, repo, fakes1)
        assert fakes1["call_counts"]["summarize"] == 1
        assert fakes1["call_counts"]["applied"] == 1
        assert fakes1["call_counts"]["stance"] == 1

        # Second run — all caches exist, zero Gemini calls expected
        _reset_profile_cache()
        _seed_profile(repo)  # re-seed so profile context is available

        fakes2 = _build_fake_gemini(
            {
                "a": {"p1": {**_load_response("a", "1"), "_for_title": rec.title}},
                "b": {"p1": {**_load_response("b", "1"), "_for_title": rec.title}},
                "c": {"p1": {**_load_response("c", "1"), "_for_title": rec.title}},
            }
        )
        _run_pipeline(rec, body, repo, fakes2)
        assert fakes2["call_counts"]["summarize"] == 0, "summarize cache should have been hit"
        assert fakes2["call_counts"]["applied"] == 0, "applied cache should have been hit"
        assert fakes2["call_counts"]["stance"] == 0, "stance cache should have been hit"

    def test_force_rebuild_single_post_regenerates_all_caches(self, repo):
        """Delete caches + pages for one post, re-run, verify full regeneration."""
        rec = _load_record("post_1_first_ingest")
        body = _load_body("1")

        fakes1 = _build_fake_gemini(
            {
                "a": {"p1": {**_load_response("a", "1"), "_for_title": rec.title}},
                "b": {"p1": {**_load_response("b", "1"), "_for_title": rec.title}},
                "c": {"p1": {**_load_response("c", "1"), "_for_title": rec.title}},
            }
        )
        _run_pipeline(rec, body, repo, fakes1)

        # Delete all caches and rendered pages
        cache_dir = repo / "raw" / "transcripts" / "substack"
        (cache_dir / f"{rec.id}.json").unlink()
        (cache_dir / f"{rec.id}.applied.json").unlink()
        (cache_dir / f"{rec.id}.stance.json").unlink()
        write_pages.article_page_path(repo, rec).unlink()
        if write_pages.summary_page_path(repo, rec).exists():
            write_pages.summary_page_path(repo, rec).unlink()

        # Reset profile cache so apply_post_to_you re-reads profile from disk
        _reset_profile_cache()

        # Re-run — everything should be regenerated
        fakes2 = _build_fake_gemini(
            {
                "a": {"p1": {**_load_response("a", "1"), "_for_title": rec.title}},
                "b": {"p1": {**_load_response("b", "1"), "_for_title": rec.title}},
                "c": {"p1": {**_load_response("c", "1"), "_for_title": rec.title}},
            }
        )
        _run_pipeline(rec, body, repo, fakes2)

        # All three passes called again
        assert fakes2["call_counts"]["summarize"] == 1, "summarize should re-run after cache delete"
        assert fakes2["call_counts"]["applied"] == 1, "applied should re-run after cache delete"
        assert fakes2["call_counts"]["stance"] == 1, "stance should re-run after cache delete"

        # All caches and pages regenerated
        assert (cache_dir / f"{rec.id}.json").exists()
        assert (cache_dir / f"{rec.id}.applied.json").exists()
        assert (cache_dir / f"{rec.id}.stance.json").exists()
        assert write_pages.article_page_path(repo, rec).exists()
        assert not write_pages.summary_page_path(repo, rec).exists()
