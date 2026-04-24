from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import patch

from mind.cli import main
from scripts import lint
from scripts.articles.fetch import ArticleFetchFailure, ArticleFetchResult
from scripts.common.vault import Vault
from tests.paths import EXAMPLES_ROOT, REPO_ROOT
from tests.support import fake_env_config, patch_onboarding_llm


BLOCKER_ARTIFACT_RELATIVE = Path("raw") / "reports" / "phase6-first-real-ingest-blockers.md"
MANIFEST_PATH = REPO_ROOT / "tests" / "fixtures" / "final-readiness" / "manifest.json"


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    return target


def _enable_dream(root: Path) -> None:
    cfg = root / "config.yaml"
    text = cfg.read_text(encoding="utf-8")
    text = text.replace("enabled: false", "enabled: true", 1)
    cfg.write_text(text, encoding="utf-8")


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def _reset_onboarding_surface(root: Path) -> None:
    for name in ("profile.md", "values.md", "positioning.md", "open-inquiries.md"):
        target = root / "memory" / "me" / name
        if target.exists():
            target.unlink()


def _load_manifest() -> dict[str, object]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    fixtures = {
        key: REPO_ROOT / value
        for key, value in manifest["fixtures"].items()
    }
    manifest["fixtures"] = fixtures
    return manifest


def _write_blocker_artifact(path: Path, blockers: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not blockers:
        path.write_text("", encoding="utf-8")
        return

    lines: list[str] = []
    for blocker in blockers:
        lines.extend(
            [
                f"## Gate {blocker['gate']}",
                "",
                f"- severity: {blocker['severity']}",
                f"- symptom: {blocker['symptom']}",
                f"- repro: {blocker['repro']}",
                f"- owner: {blocker['owner']}",
                f"- next_action: {blocker['next_action']}",
                "",
            ]
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _fake_article_fetch(entry, repo_root):
    if "paywalled" in entry.url:
        return ArticleFetchFailure(
            failure_kind="paywalled",
            detail="member-only",
            url=entry.url,
        )
    cache = repo_root / "raw" / "transcripts" / "articles"
    cache.mkdir(parents=True, exist_ok=True)
    html_path = cache / "phase6-article.html"
    html_path.write_text("article body", encoding="utf-8")
    return ArticleFetchResult(
        body_text=f"{entry.anchor_text} shows how trust compounds in user owned ai systems.",
        title=f"Phase 6 article for {entry.anchor_text}",
        author="Phase Six Author",
        sitename="Phase Six Outlet",
        published="2026-04-07",
        raw_html_path=html_path,
    )


def _fake_article_summary(*_args, **_kwargs) -> dict[str, object]:
    return {
        "tldr": "Trust compounds when user owned ai systems stay local first.",
        "key_claims": ["Trust is an architecture choice."],
        "notable_quotes": ["Trust compounds in local systems."],
        "takeaways": ["Keep the graph local first."],
        "topics": ["trust", "local-first-systems"],
        "article": "However, user owned ai still needs stronger synthesis loops.",
    }


def _fake_substack_summary(record, *, body_markdown, **_kwargs) -> dict[str, object]:
    title = record.title
    if "Trust" in title:
        return {
            "tldr": "User owned ai however still depends on trust and local first systems.",
            "core_argument": "Trust is the root of durable local-first knowledge systems.",
            "argument_graph": {"premises": ["Trust compounds"], "inferences": [], "conclusion": "Trust shapes system design."},
            "key_claims": [{"claim": "Trust is the root", "evidence_quote": "trust is the root", "evidence_context": "", "quote_unverified": False}],
            "memorable_examples": [],
            "notable_quotes": ["Trust is the root."],
            "steelman": "",
            "strongest_rebuttal": "",
            "would_change_mind_if": "",
            "in_conversation_with": [],
            "relates_to_prior": [],
            "topics": ["trust", "local-first-systems"],
            "article": body_markdown,
            "schema_version": 2,
        }
    return {
        "tldr": "Aggregators still matter for local-first distribution.",
        "core_argument": "Distribution shapes leverage.",
        "argument_graph": {"premises": [], "inferences": [], "conclusion": ""},
        "key_claims": [{"claim": "Aggregators win", "evidence_quote": "", "evidence_context": "", "quote_unverified": False}],
        "memorable_examples": [],
        "notable_quotes": [],
        "steelman": "",
        "strongest_rebuttal": "",
        "would_change_mind_if": "",
        "in_conversation_with": [],
        "relates_to_prior": [],
        "topics": ["aggregators"],
        "article": body_markdown,
        "schema_version": 2,
    }


def _fake_substack_classify(record, *, body_html, repo_root) -> dict[str, list[dict[str, str]]]:
    if "Trust" not in record.title:
        return {"external_classified": [], "substack_internal": []}
    return {
        "external_classified": [
            {
                "url": "https://stratechery.com/2024/aggregators",
                "anchor_text": "aggregators",
                "context_snippet": "trust and aggregation",
                "category": "business",
                "reason": "analysis",
            },
            {
                "url": "https://twitter.com/patrickc",
                "anchor_text": "twitter",
                "context_snippet": "ignored social link",
                "category": "ignore",
                "reason": "social",
            },
        ],
        "substack_internal": [],
    }


def _fake_youtube_summary(*_args, **_kwargs) -> dict[str, object]:
    return {
        "tldr": "Trust and local first systems reinforce user owned ai.",
        "key_claims": ["Local-first systems preserve operator trust."],
        "notable_quotes": ["Local-first is a trust decision."],
        "takeaways": ["Preserve local context."],
        "topics": ["trust", "local-first-systems"],
        "article": "User owned ai becomes more legible when the stack stays local first.",
    }


def _fake_book_research() -> dict[str, object]:
    return {
        "tldr": "Distributed systems work depends on explicit trust boundaries.",
        "topics": ["distributed systems", "trust"],
        "key_frameworks": [],
        "memorable_stories": [],
        "counterarguments": [],
        "famous_quotes": [],
        "in_conversation_with": [],
    }


def _fake_books_ingest_export(root: Path):
    def _run(*_args, **_kwargs):
        from mind.commands.ingest import BooksIngestResult
        from scripts.books.parse import BookRecord
        from scripts.books.write_pages import write_book_page

        book = BookRecord(
            title="Atomic Habits",
            author=[],
            format="ebook",
            status="finished",
        )
        page = write_book_page(
            book,
            enriched=_fake_book_research(),
            category="business",
            summary={
                "tldr": "Trust compounds through small repeated behaviors.",
                "topics": ["trust", "habits"],
            },
            force=True,
        )
        return BooksIngestResult(
            pages_written=1,
            page_ids=[page.stem],
            selected_count=1,
            skipped_materialized=0,
            resumable=0,
            blocked=0,
            stale=0,
            executed=1,
            failed=0,
            blocked_samples=[],
            failed_items=[],
        )

    return _run


def test_phase6_final_synthetic_rehearsal(tmp_path: Path, monkeypatch, capsys) -> None:
    manifest = _load_manifest()
    fixtures = manifest["fixtures"]
    today = str(manifest["today"])
    blockers: list[dict[str, str]] = []
    root = _copy_harness(tmp_path)
    blocker_artifact = root / BLOCKER_ARTIFACT_RELATIVE

    try:
        _enable_dream(root)
        _patch_roots(monkeypatch, root)
        _reset_onboarding_surface(root)
        monkeypatch.chdir(root)
        patch_onboarding_llm(monkeypatch)
        monkeypatch.setattr("scripts.common.env.load", lambda: fake_env_config(root, substack_session_cookie="phase6-cookie"))
        monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
        monkeypatch.setattr("scripts.substack.enrich.fetch_body", lambda *args, **kwargs: "<div class='body markup'><p>Trust is the root.</p></div>")
        monkeypatch.setattr("scripts.substack.html_to_markdown.convert", lambda _html: "# On Trust\n\nTrust is the root. However, user owned ai still needs local first systems.")
        monkeypatch.setattr("scripts.substack.enrich.classify_post_links", _fake_substack_classify)
        monkeypatch.setattr("scripts.substack.enrich.get_prior_posts_context", lambda *args, **kwargs: "")
        monkeypatch.setattr("scripts.substack.stance.load_stance_context", lambda *args, **kwargs: "")
        monkeypatch.setattr("scripts.substack.enrich.summarize_post", _fake_substack_summary)
        monkeypatch.setattr("scripts.substack.enrich.verify_quotes", lambda summary, *_args, **_kwargs: summary)
        monkeypatch.setattr("scripts.substack.enrich.apply_post_to_you", lambda *args, **kwargs: {"applied_paragraph": "", "applied_bullets": [], "thread_links": []})
        monkeypatch.setattr("scripts.substack.enrich.update_author_stance", lambda *args, **kwargs: None)
        monkeypatch.setattr("scripts.substack.enrich.run_pass_d_for_substack", lambda *args, **kwargs: {})

        monkeypatch.setattr("scripts.articles.pipeline.fetch_article", _fake_article_fetch)
        monkeypatch.setattr("scripts.articles.pipeline.summarize_article", _fake_article_summary)
        monkeypatch.setattr("scripts.articles.enrich.apply_article_to_you", lambda *args, **kwargs: {"applied_paragraph": "", "applied_bullets": [], "thread_links": []})
        monkeypatch.setattr("scripts.articles.enrich.build_article_attribution", lambda *args, **kwargs: {"status": "empty", "stance_change_note": "", "stance_context": ""})
        monkeypatch.setattr("scripts.articles.enrich.run_pass_d_for_article", lambda *args, **kwargs: {})

        monkeypatch.setattr("scripts.youtube.enrich.classify", lambda record: {"category": "business"})
        monkeypatch.setattr(
            "scripts.youtube.enrich.fetch_transcription_result",
            lambda record, repo_root: {
                "transcript": "Trust and local first systems reinforce user owned ai.",
                "transcription_path": "captions",
                "multimodal_error": "",
                "fallback_attempts": [],
            },
        )
        monkeypatch.setattr("scripts.youtube.enrich.summarize", lambda *args, **kwargs: _fake_youtube_summary())
        monkeypatch.setattr("scripts.youtube.enrich.apply_video_to_you", lambda *args, **kwargs: {"applied_paragraph": "", "applied_bullets": [], "thread_links": []})
        monkeypatch.setattr("scripts.youtube.enrich.build_channel_attribution", lambda *args, **kwargs: {"status": "empty", "stance_change_note": "", "stance_context": ""})
        monkeypatch.setattr("scripts.youtube.enrich.run_pass_d_for_youtube", lambda *args, **kwargs: {})

        monkeypatch.setattr("scripts.books.enrich.classify", lambda book: {"category": "business"})
        monkeypatch.setattr("scripts.books.enrich.enrich_deep", lambda book: _fake_book_research())
        monkeypatch.setattr("scripts.books.enrich.summarize_research", lambda book, research: _fake_book_research())
        monkeypatch.setattr("scripts.books.enrich.apply_to_you", lambda *args, **kwargs: {"applied_paragraph": "", "applied_bullets": [], "thread_links": []})
        monkeypatch.setattr("scripts.books.enrich.update_author_memory", lambda *args, **kwargs: {"status": "empty", "stance_change_note": "", "stance_context": ""})
        monkeypatch.setattr("scripts.books.enrich.run_pass_d_for_book", lambda *args, **kwargs: {})
        monkeypatch.setattr("mind.commands.ingest.ingest_books_export", _fake_books_ingest_export(root))

        copied_substack_export = root / "raw" / "exports" / "substack-saved-smoketest.json"
        copied_substack_export.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(fixtures["substack_export"], copied_substack_export)

        assert main(["onboard", "--from-json", str(fixtures["onboarding"])]) == 0
        onboard_out = capsys.readouterr().out
        assert "onboard: created" in onboard_out

        harness = Vault.load(root)
        assert harness.wiki == root / "memory"
        assert harness.raw == root / "raw"
        assert (harness.raw / "onboarding" / "current.json").exists()
        assert any((harness.raw / "onboarding" / "bundles").glob("*"))
        for path in (
            harness.wiki / "me" / "profile.md",
            harness.wiki / "me" / "values.md",
            harness.wiki / "me" / "positioning.md",
            harness.wiki / "me" / "open-inquiries.md",
        ):
            assert path.exists(), path

        assert main(["ingest", "substack", "raw/exports/substack-saved-smoketest.json", "--today", today]) == 0
        substack_out = capsys.readouterr().out
        assert "ingest-substack:" in substack_out

        copied_articles_drop = root / "raw" / "drops" / f"articles-from-fixture-{today}.jsonl"
        copied_articles_drop.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(fixtures["articles_drop"], copied_articles_drop)

        assert main(["ingest", "articles", "--today", today]) == 0
        articles_out = capsys.readouterr().out
        assert "ingest-articles:" in articles_out

        assert main(["ingest", "youtube", str(fixtures["youtube_export"])]) == 0
        youtube_out = capsys.readouterr().out
        assert "ingest-youtube:" in youtube_out

        assert main(["ingest", "books", str(fixtures["books_export"])]) == 0
        books_out = capsys.readouterr().out
        assert "ingest-books:" in books_out

        assert (harness.wiki / "sources" / "substack" / "thegeneralist" / "2026-03-15-on-trust.md").exists()
        assert any((harness.wiki / "sources" / "articles").glob("*.md"))
        assert any((harness.wiki / "sources" / "youtube" / "business").glob("*.md"))
        assert any((harness.wiki / "sources" / "books").rglob("*.md"))

        assert main(["dream", "light"]) == 0
        light_out = capsys.readouterr().out
        assert "Light Dream processed" in light_out
        stance_text = (harness.wiki / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
        assert "# User-owned AI" in stance_text

        assert main(["dream", "deep"]) == 0
        deep_out = capsys.readouterr().out
        assert "Deep Dream processed" in deep_out
        assert list((harness.wiki / "me" / "digests").glob("*.md"))
        assert "[[how-to-balance-depth-and-speed]]" in (harness.wiki / "me" / "open-inquiries.md").read_text(encoding="utf-8")

        assert main(["dream", "rem"]) == 0
        rem_out = capsys.readouterr().out
        assert "REM Dream processed" in rem_out
        assert list((harness.wiki / "dreams" / "rem").glob("*.md"))
        assert list((harness.wiki / "me" / "reflections").glob("*.md"))
        assert not (harness.wiki / "me" / "timeline.md").exists()
        assert not (root / "skills").exists()

        report = lint.run(harness)
        assert report.failing_pages == 0
        assert report.schema_violations == 0
        assert report.broken_links == 0
        assert report.stale_pages == 0
        assert report.exit_code == 0

        assert main(["query", str(manifest["query_prompt"])]) == 0
        query_out = capsys.readouterr().out
        assert "Question:" in query_out
        assert "Relevant pages:" in query_out
        assert "Answer:" in query_out
        assert "Confidence:" in query_out
        assert "No relevant wiki pages found." not in query_out
    except Exception as exc:
        blockers.append(
            {
                "gate": "G",
                "severity": "blocker",
                "symptom": f"Final copied-harness rehearsal failed: {type(exc).__name__}: {exc}",
                "repro": ".venv/bin/pytest -q tests/readiness/test_phase6_final_synthetic_rehearsal.py",
                "owner": "tests/readiness + copied-harness CLI rehearsal",
                "next_action": "Fix the failing rehearsal assertion or command path, then rerun Gate G.",
            }
        )
        raise
    finally:
        _write_blocker_artifact(blocker_artifact, blockers)

    assert blocker_artifact.exists()
    assert blocker_artifact.read_text(encoding="utf-8") == ""
