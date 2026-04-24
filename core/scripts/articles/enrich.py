"""Articles lifecycle orchestration.

This module normalizes fetched article input into the shared ingestion
contract, runs the article summary step, dispatches Pass D, and hands the
materialization boundary enough information to rewrite flat pages safely.
Fetch remains in ``scripts.articles.fetch``.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from mind.services.ingest_contract import IngestionLifecycleResult, NormalizedSource, run_ingestion_lifecycle
from mind.services.quality_receipts import write_quality_receipt
from mind.services.llm_cache import load_llm_cache, write_llm_cache
from mind.services.llm_service import get_llm_service
from mind.services.llm_service import get_llm_service as _get_llm_service
from mind.services.materialization import MaterializationCandidate, select_primary_targets
from mind.services.prompt_builders import APPLIED_TO_YOU_PROMPT_VERSION, UPDATE_AUTHOR_STANCE_PROMPT_VERSION
from scripts.articles.parse import ArticleDropEntry
from scripts.articles.fetch import ArticleFetchResult
from scripts.articles.write_pages import slugify_url
from scripts.common.drop_queue import append_article_links_to_drop_queue, extract_urls_with_context
from scripts.common.entity_log import log_entities as log_source_entities
from scripts.common.profile import load_profile_context
from scripts.common.quote_verify import verify_quotes as verify_source_quotes
from scripts.common.slugify import slugify
from scripts.common.source_context import build_prior_context, strip_wiki_link
from scripts.common.stance import load_stance_context
from scripts.common.vault import raw_path, wiki_path


_ORG_AUTHOR_HINTS = (
    "inc",
    "llc",
    "media",
    "news",
    "press",
    "team",
    "staff",
    "corp",
    "company",
    "magazine",
    "journal",
)


def summary_cache_path(repo_root: Path, entry: ArticleDropEntry) -> Path:
    slug = slugify_url(entry.url, entry.discovered_at)
    return raw_path(repo_root, "transcripts", "articles", f"{slug}.json")


def applied_cache_path(repo_root: Path, entry: ArticleDropEntry) -> Path:
    slug = slugify_url(entry.url, entry.discovered_at)
    return raw_path(repo_root, "transcripts", "articles", f"{slug}.applied.json")


def attribute_cache_path(repo_root: Path, entry: ArticleDropEntry) -> Path:
    slug = slugify_url(entry.url, entry.discovered_at)
    return raw_path(repo_root, "transcripts", "articles", f"{slug}.stance.json")


def _empty_applied_payload() -> dict[str, Any]:
    return {
        "applied_paragraph": "",
        "applied_bullets": [],
        "thread_links": [],
    }


def summarize_article(
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult,
    repo_root: Path,
    stance_context: str = "",
    prior_sources_context: str = "",
) -> dict[str, Any]:
    """Summarize an article. Cached at raw/transcripts/articles/<slug>.json."""
    target = summary_cache_path(repo_root, entry)
    identities = get_llm_service().cache_identities(task_class="summary", prompt_version="articles.summary.v1")
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    title = fetch_result.title or entry.anchor_text or "Untitled"
    result = _get_llm_service().summarize_article_text(
        title=title,
        url=entry.url,
        body_markdown=fetch_result.body_text,
        sitename=fetch_result.sitename,
        stance_context=stance_context,
        prior_sources_context=prior_sources_context,
    )
    if isinstance(result, tuple):
        response, identity = result
    else:
        response = result
    write_llm_cache(target, identity=identity, data=response)
    return response


def _looks_like_org(name: str | None, sitename: str | None) -> bool:
    text = (name or "").strip().lower()
    if not text:
        return False
    if sitename and text == sitename.strip().lower():
        return True
    return any(token in text for token in _ORG_AUTHOR_HINTS)


def _primary_author_name(name: str | None) -> str:
    text = (name or "").strip()
    if not text:
        return ""
    for separator in (",", " and ", " & "):
        if separator in text:
            return text.split(separator, 1)[0].strip()
    return text


def _additional_author_hints(name: str | None) -> list[str]:
    text = (name or "").strip()
    if not text:
        return []
    for separator in (",", " and ", " & "):
        if separator in text:
            parts = [part.strip() for part in text.split(separator) if part.strip()]
            return parts[1:]
    return []


def normalize_article_source(
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult,
) -> NormalizedSource:
    """Normalize an article into the shared source boundary."""
    slug = slugify_url(entry.url, entry.discovered_at)
    title = fetch_result.title or entry.anchor_text or "Untitled"
    sitename = (fetch_result.sitename or "").strip()
    author = (fetch_result.author or "").strip()
    creator_candidates: list[MaterializationCandidate] = []

    if author and not _looks_like_org(author, sitename):
        creator_candidates.append(
            MaterializationCandidate(
                page_type="person",
                name=_primary_author_name(author),
                role="creator",
                confidence=0.95,
                deterministic=True,
                source="article",
            )
        )
    elif sitename:
        creator_candidates.append(
            MaterializationCandidate(
                page_type="company",
                name=sitename,
                role="creator",
                confidence=0.99,
                deterministic=True,
                source="article",
            )
        )

    if sitename:
        creator_candidates.append(
            MaterializationCandidate(
                page_type="company",
                name=sitename,
                role="publisher",
                confidence=0.99,
                deterministic=True,
                source="article",
            )
        )

    return NormalizedSource(
        source_id=f"article-{slug}",
        source_kind="article",
        external_id="",
        canonical_url=entry.url,
        title=title,
        creator_candidates=[asdict(candidate) for candidate in creator_candidates],
        published_at=fetch_result.published or "",
        discovered_at=entry.discovered_at,
        source_metadata={
            "entry": entry,
            "fetch_result": fetch_result,
            "additional_author_hints": _additional_author_hints(author),
        },
        discovered_links=[
            {
                **link,
                "category": entry.category,
            }
            for link in extract_urls_with_context(fetch_result.body_text)
        ],
        provenance={
            "adapter": "articles",
            "source_type": entry.source_type,
            "source_label": entry.source_label,
        },
        raw_text=fetch_result.body_text,
    )


def _materialization_targets_from_source(source: NormalizedSource):
    candidates = [MaterializationCandidate(**candidate) for candidate in source.creator_candidates]
    return select_primary_targets(candidates)


def apply_article_to_you(
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult,
    summary: dict[str, Any],
    repo_root: Path,
) -> dict[str, Any]:
    """Pass B: create an applied-to-you note for an article."""
    target = applied_cache_path(repo_root, entry)
    identities = get_llm_service().cache_identities(
        task_class="personalization",
        prompt_version=APPLIED_TO_YOU_PROMPT_VERSION,
    )
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    profile_ctx = load_profile_context(repo_root=repo_root)
    if not profile_ctx:
        return _empty_applied_payload()

    creator_name = _primary_author_name(fetch_result.author) or fetch_result.sitename or "unknown"
    result_or_tuple = _get_llm_service().applied_to_you(
        title=fetch_result.title or entry.anchor_text or "Untitled",
        author=creator_name,
        profile_context=profile_ctx,
        research=summary,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    write_llm_cache(target, identity=identity, data=response)
    return response


def build_article_attribution(
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult,
    source: NormalizedSource,
    summary: dict[str, Any],
    repo_root: Path,
) -> dict[str, Any]:
    """Pass C: derive an article author-memory delta when supported."""
    targets = _materialization_targets_from_source(source)
    creator_target = targets.creator_target
    if creator_target is None:
        return {
            "status": "unsupported",
            "reason": "article creator target could not be resolved",
            "stance_change_note": "",
            "stance_context": "",
        }
    if creator_target.page_type != "person":
        return {
            "status": "unsupported",
            "reason": "article company creators intentionally skip Pass C in Phase 3",
            "stance_change_note": "",
            "stance_context": "",
        }

    stance_context = load_stance_context(
        slug=creator_target.resolved_page_id(),
        kind="person",
        repo_root=repo_root,
    )
    if not stance_context:
        return {
            "status": "empty",
            "reason": "no prior author stance context exists yet",
            "stance_change_note": "",
            "stance_context": "",
        }

    target = attribute_cache_path(repo_root, entry)
    identities = get_llm_service().cache_identities(
        task_class="stance",
        prompt_version=UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
    )
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    author_name = _primary_author_name(fetch_result.author) or creator_target.name
    post_slug = __import__("scripts.articles.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, entry)
    result_or_tuple = _get_llm_service().update_author_stance(
        author=author_name,
        title=fetch_result.title or entry.anchor_text or "Untitled",
        post_slug=post_slug,
        current_stance=stance_context,
        summary=summary,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple

    payload = {
        "status": "implemented" if (response.get("change_note") or "").strip() else "empty",
        "stance_change_note": response.get("change_note", ""),
        "stance_context": stance_context,
    }
    write_llm_cache(target, identity=identity, data=payload)
    return payload


def run_pass_d_for_article(
    entry: ArticleDropEntry,
    *,
    body_text: str,
    summary: dict,
    applied: dict[str, Any] | None,
    attribution: dict[str, Any] | None,
    repo_root: Path,
    today: str,
    prior_source_context: str = "",
    cache_mode: str = "default",
    evidence_date: str | None = None,
    force_refresh: bool = False,
) -> dict:
    """Run shared Pass D for an article source and dispatch evidence/probationary writes."""
    from scripts.atoms import pass_d, working_set
    from scripts.atoms.replay import apply_pass_d_result

    ws = working_set.load_for_source(
        source_topics=list(summary.get("topics") or []),
        source_domains=["learning"],
        cap=300,
        repo_root=repo_root,
    )
    slug = slugify_url(entry.url, entry.discovered_at)
    cache_reused = pass_d.pass_d_cache_exists(
        repo_root=repo_root,
        source_kind="article",
        source_id=f"article-{slug}",
        cache_mode=cache_mode,
    ) and not force_refresh
    try:
        result = pass_d.run_pass_d(
            source_id=f"article-{slug}",
            source_link=f"[[{__import__('scripts.articles.write_pages', fromlist=['canonical_page_id']).canonical_page_id(repo_root, entry)}]]",
            source_kind="article",
            body_or_transcript=body_text,
            summary=summary,
            applied=applied,
            pass_c_delta=(attribution or {}).get("stance_change_note") or None,
            stance_context=(attribution or {}).get("stance_context", ""),
            prior_source_context=prior_source_context,
            working_set=ws,
            repo_root=repo_root,
            today_str=today,
            cache_mode=cache_mode,
            force_refresh=force_refresh,
        )
    except Exception as exc:
        return {
            "cache_reused": cache_reused,
            "error": f"{type(exc).__name__}: {exc}",
            "error_stage": "pass_d.parse",
            "evidence_updates": 0,
            "probationary_updates": 0,
            "missing_atoms": [],
        }

    payload = {
        "q1_matches": [asdict(match) for match in result.q1_matches],
        "q2_candidates": [asdict(candidate) for candidate in result.q2_candidates],
        "warnings": list(getattr(result, "warnings", [])),
        "dropped_q1_matches": int(getattr(result, "dropped_q1_matches", 0)),
        "dropped_q2_candidates": int(getattr(result, "dropped_q2_candidates", 0)),
        "cache_reused": cache_reused,
    }
    try:
        dispatch = apply_pass_d_result(
            result,
            dedupe_by_source=cache_mode != "default",
            evidence_date=evidence_date or today,
            recorded_on=today,
            source_link=f"[[{__import__('scripts.articles.write_pages', fromlist=['canonical_page_id']).canonical_page_id(repo_root, entry)}]]",
            repo_root=repo_root,
            source_id=f"article-{slug}",
            source_kind="article",
            source_date=evidence_date or today,
            topics=[str(item) for item in list((summary or {}).get("topics") or [])],
            entities=[str(item) for item in list((summary or {}).get("entities") or [])],
        )
    except Exception as exc:
        payload.update(
            {
                "error": f"{type(exc).__name__}: {exc}",
                "error_stage": "pass_d.dispatch",
                "evidence_updates": 0,
                "probationary_updates": 0,
                "missing_atoms": [],
            }
        )
        return payload

    payload.update(
        {
            "evidence_updates": dispatch.evidence_updates,
            "probationary_updates": dispatch.probationary_updates,
            "missing_atoms": dispatch.missing_atoms,
        }
    )
    return payload


def run_article_entry_lifecycle(
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult,
    repo_root: Path,
    today: str,
    summarize_override=None,
) -> IngestionLifecycleResult:
    """Run one article entry through the shared ingestion lifecycle."""

    source = normalize_article_source(entry, fetch_result=fetch_result)

    def understand(article_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        summarizer = summarize_override or summarize_article
        prior_context = _get_prior_article_context(fetch_result, repo_root=repo_root)
        author_name = fetch_result.author or ""
        author_stance = load_stance_context(
            slug=slugify(author_name) if author_name else "", kind="person", repo_root=repo_root,
        ) if author_name else ""
        summary = summarizer(
            entry, fetch_result=fetch_result, repo_root=repo_root,
            stance_context=author_stance,
            prior_sources_context=prior_context,
        )
        summary = verify_source_quotes(
            summary=summary,
            body_text=article_source.primary_content,
            source_id=slugify_url(entry.url, entry.discovered_at),
            source_kind="article",
            repo_root=repo_root,
        )
        return {
            "summary": summary,
            "prior_context": prior_context,
            "materialization_hints": {
                "additional_author_hints": list(article_source.source_metadata.get("additional_author_hints") or []),
                "discovered_links": list(article_source.discovered_links or []),
            },
        }

    def personalize(_article_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        applied = apply_article_to_you(
            entry,
            fetch_result=fetch_result,
            summary=summary,
            repo_root=repo_root,
        )
        status = "implemented" if any(applied.get(key) for key in ("applied_paragraph", "applied_bullets", "thread_links")) else "empty"
        return {
            "status": status,
            "applied": applied,
        }

    def attribute(article_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        return build_article_attribution(
            entry,
            fetch_result=fetch_result,
            source=article_source,
            summary=summary,
            repo_root=repo_root,
        )

    def distill(article_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        try:
            return run_pass_d_for_article(
                entry,
                body_text=article_source.primary_content,
                summary=summary,
                applied=pass_b.get("applied"),
                attribution=pass_c,
                repo_root=repo_root,
                today=today,
                prior_source_context=str((envelope.get("pass_a") or {}).get("prior_context") or ""),
            )
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    def materialize(article_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        from scripts.articles import write_pages
        from scripts.substack import write_pages as substack_write_pages

        summary = (envelope.get("pass_a") or {}).get("summary", {})
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        targets = _materialization_targets_from_source(article_source)
        article = write_pages.write_article_page(
            entry,
            fetch_result=fetch_result,
            summary=summary,
            repo_root=repo_root,
            applied=pass_b.get("applied"),
            stance_change_note=(pass_c.get("stance_change_note") or "").strip() or None,
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            force=True,
        )
        if entry.source_page_id:
            substack_write_pages.add_materialized_link_to_source_page(
                repo_root=repo_root,
                source_page_id=entry.source_page_id,
                target_page_id=write_pages.canonical_page_id(repo_root, entry),
                target_kind="article",
            )
        materialized = {"article": str(article)}
        source_link = __import__("scripts.articles.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, entry)
        author_page = write_pages.ensure_author_page(
            fetch_result=fetch_result,
            repo_root=repo_root,
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            source_link=source_link,
        )
        if author_page is not None:
            materialized["author"] = str(author_page)
        publisher_page = write_pages.ensure_outlet_page(
            fetch_result=fetch_result,
            repo_root=repo_root,
            publisher_target=targets.publisher_target,
            source_link=source_link,
        )
        if publisher_page is not None:
            materialized["publisher"] = str(publisher_page)
        return materialized

    def propagate(_article_source: NormalizedSource, envelope: dict[str, object], _materialized: dict[str, str]) -> dict[str, object]:
        from scripts.atoms import pass_d

        pass_a = envelope.get("pass_a") or {}
        drop_path = append_article_links_to_drop_queue(
            repo_root=repo_root,
            today=today,
            source_id=slugify_url(entry.url, entry.discovered_at),
            source_url=entry.url,
            source_type="article-link",
            source_label="article-link",
            discovered_at=entry.discovered_at,
            links=list((pass_a.get("materialization_hints") or {}).get("discovered_links") or []),
        )
        logged_entities = log_source_entities(
            summary=pass_a.get("summary", {}),
            body_text=_article_source.primary_content,
            repo_root=repo_root,
            today=today,
            source_link=__import__("scripts.articles.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, entry),
            inbox_kind="article-entities",
            stopwords=_article_stopwords(fetch_result),
        )
        return {
            "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
            "drop_path": str(drop_path),
            "logged_entities": logged_entities,
            "logged_entity_count": len(logged_entities),
            "propagate_discovered_count": len(list((pass_a.get("materialization_hints") or {}).get("discovered_links") or [])),
            "propagate_queued_count": len(list((pass_a.get("materialization_hints") or {}).get("discovered_links") or [])),
        }

    result = run_ingestion_lifecycle(
        source=source,
        understand=understand,
        personalize=personalize,
        attribute=attribute,
        distill=distill,
        materialize=materialize,
        propagate=propagate,
    )
    write_quality_receipt(repo_root=repo_root, result=result, executed_at=today)
    return result


def _article_stopwords(fetch_result: ArticleFetchResult) -> set[str]:
    values = {
        (fetch_result.author or "").strip(),
        (fetch_result.sitename or "").strip(),
        (fetch_result.title or "").strip(),
    }
    for text in (fetch_result.author or "", fetch_result.sitename or "", fetch_result.title or ""):
        values.update(token.strip() for token in text.split() if len(token.strip()) >= 2)
    return {value for value in values if value}


def _get_prior_article_context(fetch_result: ArticleFetchResult, *, repo_root: Path) -> str:
    author = (fetch_result.author or "").strip()
    outlet = (fetch_result.sitename or "").strip()
    author_lc = author.lower()
    outlet_lc = outlet.lower()
    return build_prior_context(
        root=wiki_path(repo_root, "sources", "articles"),
        heading="## Prior article sources in your wiki",
        matcher=lambda fm: (
            (author and strip_wiki_link(str(fm.get("author", ""))).lower() == author_lc)
            or (outlet and strip_wiki_link(str(fm.get("outlet", ""))).lower() == outlet_lc)
        ),
    )
