from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scripts.articles import enrich as articles_enrich
from scripts.articles.parse import ArticleDropEntry
from scripts.books import enrich as books_enrich
from scripts.books.parse import BookRecord
from scripts.substack import enrich as substack_enrich
from scripts.substack.parse import SubstackRecord
from scripts.youtube import enrich as youtube_enrich
from scripts.youtube.parse import YouTubeRecord


@dataclass
class _FakePassDResult:
    q1_matches: list[object]
    q2_candidates: list[object]


def _patch_pass_d_stack(monkeypatch, captured: dict[str, object]) -> None:
    monkeypatch.setattr("scripts.atoms.working_set.load_for_source", lambda **kwargs: ["working-set"])
    monkeypatch.setattr("scripts.atoms.evidence_writer.append_evidence", lambda **kwargs: None)
    monkeypatch.setattr("scripts.atoms.probationary.create_or_extend", lambda **kwargs: None)

    def fake_run_pass_d(**kwargs):
        captured.update(kwargs)
        return _FakePassDResult(q1_matches=[], q2_candidates=[])

    monkeypatch.setattr("scripts.atoms.pass_d.run_pass_d", fake_run_pass_d)


def test_substack_pass_d_helper_uses_shared_argument_categories(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    _patch_pass_d_stack(monkeypatch, captured)

    record = SubstackRecord(
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

    substack_enrich.run_pass_d_for_substack(
        record,
        body_markdown="# On Trust\n\nTrust matters.",
        summary={"topics": ["trust"]},
        applied={"applied_paragraph": "x", "applied_bullets": [], "thread_links": []},
        stance_change_note="Author shifted toward systems trust.",
        stance_context_text="Prior stance",
        prior_context="Earlier post",
        repo_root=tmp_path,
        today="2026-04-09",
    )

    assert captured["source_kind"] == "substack"
    assert captured["applied"]["applied_paragraph"] == "x"  # type: ignore[index]
    assert captured["pass_c_delta"] == "Author shifted toward systems trust."
    assert captured["stance_context"] == "Prior stance"
    assert captured["prior_source_context"] == "Earlier post"


def test_article_pass_d_helper_uses_shared_argument_categories(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    _patch_pass_d_stack(monkeypatch, captured)

    entry = ArticleDropEntry(
        url="https://example.com/article",
        source_post_id="1",
        source_post_url="https://example.com/source",
        anchor_text="article",
        context_snippet="ctx",
        category="business",
        discovered_at="2026-04-09T00:00:00Z",
        source_type="substack-link",
    )
    body_text = "Body text"

    articles_enrich.run_pass_d_for_article(
        entry,
        body_text=body_text,
        summary={"topics": ["media"]},
        applied={"applied_paragraph": "x", "applied_bullets": [], "thread_links": []},
        attribution={"stance_change_note": "Author now stresses leverage.", "stance_context": "Prior stance"},
        repo_root=tmp_path,
        today="2026-04-09",
    )

    assert captured["source_kind"] == "article"
    assert captured["body_or_transcript"] == body_text
    assert captured["applied"]["applied_paragraph"] == "x"  # type: ignore[index]
    assert captured["pass_c_delta"] == "Author now stresses leverage."
    assert captured["stance_context"] == "Prior stance"
    assert captured["prior_source_context"] == ""


def test_youtube_pass_d_helper_uses_shared_argument_categories(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    _patch_pass_d_stack(monkeypatch, captured)

    record = YouTubeRecord(
        video_id="abc123xyz00",
        title="Test Video",
        channel="Test Channel",
        watched_at="2026-04-01T10:00:00Z",
    )

    youtube_enrich.run_pass_d_for_youtube(
        record,
        transcript="hello world",
        summary={"topics": ["systems"]},
        applied={"applied_paragraph": "x", "applied_bullets": [], "thread_links": []},
        attribution={"stance_change_note": "Channel is focusing on systems thinking.", "stance_context": "Prior channel stance"},
        repo_root=tmp_path,
        today="2026-04-09",
    )

    assert captured["source_kind"] == "youtube"
    assert captured["body_or_transcript"] == "hello world"
    assert captured["applied"]["applied_paragraph"] == "x"  # type: ignore[index]
    assert captured["pass_c_delta"] == "Channel is focusing on systems thinking."
    assert captured["stance_context"] == "Prior channel stance"
    assert captured["prior_source_context"] == ""


def test_book_pass_d_helper_uses_normalized_primary_content(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    _patch_pass_d_stack(monkeypatch, captured)

    book = BookRecord(
        title="Designing Data-Intensive Applications",
        author=["Martin Kleppmann"],
        status="finished",
        finished_date="2026-03-15",
        format="ebook",
    )
    source = books_enrich.normalize_book_source(
        book,
        classification={"category": "business"},
        research={"tldr": "x", "topics": ["distributed systems"]},
    )

    books_enrich.run_pass_d_for_book(
        book,
        body_or_transcript=source.primary_content,
        summary_artifact={"topics": ["distributed systems"]},
        applied={"applied_paragraph": "x", "applied_bullets": [], "thread_links": []},
        attribution={"stance_change_note": "Author is more explicit about tradeoffs.", "stance_context": "Prior author stance"},
        repo_root=tmp_path,
        today="2026-04-09",
    )

    assert captured["source_kind"] == "book"
    assert captured["body_or_transcript"] == source.primary_content
    assert captured["applied"]["applied_paragraph"] == "x"  # type: ignore[index]
    assert captured["pass_c_delta"] == "Author is more explicit about tradeoffs."
    assert captured["stance_context"] == "Prior author stance"
    assert captured["prior_source_context"] == ""
