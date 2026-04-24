from __future__ import annotations

from pathlib import Path

from scripts.substack import enrich
from scripts.substack.parse import SubstackRecord


def _record(**overrides) -> SubstackRecord:
    data = {
        "id": "140000001",
        "title": "On Trust",
        "subtitle": "Why the internet runs on it",
        "slug": "on-trust",
        "published_at": "2026-03-15T09:00:00Z",
        "saved_at": "2026-04-02T18:00:00Z",
        "url": "https://thegeneralist.substack.com/p/on-trust",
        "author_name": "Mario Gabriele",
        "author_id": "9001",
        "publication_name": "The Generalist",
        "publication_slug": "thegeneralist",
        "body_html": "<p>Trust is the root.</p>",
        "is_paywalled": False,
    }
    data.update(overrides)
    return SubstackRecord(**data)


def test_normalize_substack_source_builds_primary_targets() -> None:
    source = enrich.normalize_substack_source(
        _record(),
        body_markdown="# On Trust\n\nTrust is the root.",
        body_html="<p>Trust is the root.</p>",
    )
    assert source.source_id == "substack-140000001"
    assert source.source_kind == "substack"
    assert len(source.creator_candidates) == 2
    roles = {candidate["role"] for candidate in source.creator_candidates}
    assert roles == {"creator", "publisher"}


def test_write_pages_use_selected_targets_authoritatively(tmp_path: Path) -> None:
    from mind.services.materialization import MaterializationCandidate
    from scripts.substack import write_pages

    record = _record()
    creator = MaterializationCandidate(
        page_type="person",
        name="Canonical Author",
        role="creator",
        confidence=0.99,
        deterministic=True,
        source="substack",
        page_id="canonical-author",
    )
    publisher = MaterializationCandidate(
        page_type="company",
        name="Canonical Publication",
        role="publisher",
        confidence=0.99,
        deterministic=True,
        source="substack",
        page_id="canonical-publication",
    )
    summary = {
        "tldr": "Trust matters",
        "core_argument": "",
        "argument_graph": {},
        "key_claims": [],
        "memorable_examples": [],
        "notable_quotes": [],
        "steelman": "",
        "strongest_rebuttal": "",
        "would_change_mind_if": "",
        "in_conversation_with": [],
        "relates_to_prior": [],
        "topics": [],
    }
    classified = {"external_classified": [], "substack_internal": []}
    article_path = write_pages.write_article_page(
        record,
        summary=summary,
        classified_links=classified,
        body_markdown="# On Trust\n\nTrust matters.",
        repo_root=tmp_path,
        creator_target=creator,
        publisher_target=publisher,
    )
    author_path = write_pages.ensure_author_page(
        record,
        repo_root=tmp_path,
        creator_target=creator,
        publisher_target=publisher,
    )
    publication_path = write_pages.ensure_publication_page(
        record,
        repo_root=tmp_path,
        publisher_target=publisher,
    )

    article_text = article_path.read_text(encoding="utf-8")
    author_text = author_path.read_text(encoding="utf-8")
    publication_text = publication_path.read_text(encoding="utf-8")

    assert 'author: "[[canonical-author]]"' in article_text
    assert publication_path.name == "canonical-publication.md"
    assert author_path.name == "canonical-author.md"
    assert 'outlet: "[[canonical-publication]]"' in article_text
    assert "[[canonical-publication]]" in author_text
    assert "# Canonical Publication" in publication_text
