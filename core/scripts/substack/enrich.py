"""Substack enrichment and lifecycle orchestration.

Per-record cache stages:
  1. fetch_body          -> raw/transcripts/substack/<id>.html
  2. classify_post_links -> raw/transcripts/substack/<id>.links.json
  3. summarize_post      -> raw/transcripts/substack/<id>.json

The shared lifecycle later composes those stage outputs with Pass B, Pass C,
Pass D, materialization, and propagate.
"""
from __future__ import annotations

from dataclasses import asdict
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List

from bs4 import BeautifulSoup
import requests

from mind.services.ingest_contract import NormalizedSource, run_ingestion_lifecycle, IngestionLifecycleResult
from mind.services.llm_cache import load_llm_cache, write_llm_cache
from mind.services.llm_service import get_llm_service as _get_llm_service
from mind.services.materialization import MaterializationCandidate, select_primary_targets
from mind.services.quality_receipts import write_quality_receipt
from mind.services.prompt_builders import (
    APPLIED_TO_POST_PROMPT_VERSION,
    CLASSIFY_LINKS_PROMPT_VERSION,
    SUMMARIZE_SUBSTACK_PROMPT_VERSION,
    UPDATE_AUTHOR_STANCE_PROMPT_VERSION,
)
from scripts.common.inbox_log import append_to_inbox_log
from scripts.common.anti_sales import is_sales_chrome
from scripts.common.profile import load_profile_context
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import raw_path, wiki_path
from scripts.substack import link_extractor, stance
from scripts.substack.parse import SubstackRecord

# ---------------------------------------------------------------------------
# Schema version for summary cache files
# ---------------------------------------------------------------------------

SUMMARY_CACHE_SCHEMA_VERSION = 2


class Paywalled(Exception):
    """Raised when the post body is not accessible (401/403) — caller logs to inbox."""


class FetchFailed(Exception):
    """Raised when body fetch fails for reasons other than paywall."""


# ---------------------------------------------------------------------------
# Frontmatter helpers
# ---------------------------------------------------------------------------

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(text: str) -> str:
    """Minimal slug normalizer matching the canonical ASCII-safe slugifier."""
    return normalize_identifier(text) or "untitled"


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    """Split a markdown page into (frontmatter_dict, body_str).

    Frontmatter must be enclosed in leading/trailing ``---`` fences.
    Returns ({}, full_text) when no valid frontmatter is found.
    Keys and scalar values are parsed from ``key: value`` lines.
    Multi-line values (indented bullet lists) are concatenated and stripped.
    """
    if not text.startswith("---"):
        return {}, text
    parts = text.split("---\n", 2)
    if len(parts) < 3:
        return {}, text
    fm_raw, body = parts[1], parts[2]
    fm: dict = {}
    for line in fm_raw.splitlines():
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        val = val.strip()
        if key and not key.startswith(" "):
            fm[key] = val
    return fm, body


def _extract_tldr(body: str) -> str:
    """Extract a short tldr string from the body text of a substack wiki page.

    Looks for a ``## TL;DR`` heading and takes the next non-empty paragraph.
    Falls back to the first non-empty paragraph of the body if the heading
    isn't present. Trims to 200 characters.
    """
    # Try TL;DR section first
    tldr_match = re.search(r"^## TL;DR\s*\n(.*)", body, re.MULTILINE | re.DOTALL)
    if tldr_match:
        rest = tldr_match.group(1)
        for para in rest.split("\n"):
            para = para.strip()
            if para and not para.startswith("#"):
                return para[:200]

    # Fallback: first non-empty paragraph anywhere in body
    for para in body.split("\n"):
        para = para.strip()
        if para and not para.startswith("#") and not para.startswith("---"):
            return para[:200]

    return ""


# ---------------------------------------------------------------------------
# Prior-post retrieval (Phase 1b)
# ---------------------------------------------------------------------------

_WIKI_LINK_RE = re.compile(r"^\[\[(.+)\]\]$")


def _strip_wiki_link(value: str) -> str:
    """Strip ``[[...]]`` wrapper from a frontmatter value, e.g. ``[[dan-luu]]`` → ``dan-luu``."""
    m = _WIKI_LINK_RE.match(value.strip())
    return m.group(1) if m else value.strip()


PRIOR_POSTS_BUDGET = 2000


def get_prior_posts_context(record: SubstackRecord, repo_root: Path) -> str:
    """Return a formatted markdown block listing up to 5 prior wiki posts by the
    same author or from the same outlet.

    Globs ``wiki/sources/substack/**/*.md``, filters by author slug OR
    publication_slug, sorts by ``last_updated`` descending, takes top 5,
    extracts a tldr from each, and formats the block.

    Returns empty string when no matches exist.
    Budget guard: if the block exceeds 2000 chars, truncate to top 3; if still
    over, truncate to top 2.
    """
    author_slug = _slugify(record.author_name)
    outlet_slug = record.publication_slug

    substack_dir = wiki_path(repo_root, "sources", "substack")
    if not substack_dir.exists():
        return ""

    candidates = []
    for md_path in substack_dir.glob("**/*.md"):
        text = md_path.read_text(encoding="utf-8")
        fm, body = _parse_frontmatter(text)
        fm_author = _strip_wiki_link(fm.get("author", ""))
        fm_outlet = _strip_wiki_link(fm.get("outlet", ""))
        if fm_author != author_slug and fm_outlet != outlet_slug:
            continue
        last_updated = fm.get("last_updated", "")
        title = fm.get("title", md_path.stem)
        tldr = _extract_tldr(body)
        candidates.append((last_updated, title, md_path.stem, tldr))

    if not candidates:
        return ""

    # Sort by last_updated descending; missing dates sort last (empty string < any date)
    candidates.sort(key=lambda x: x[0], reverse=True)

    def _build_block(items: list) -> str:
        lines = ["## Prior posts in your wiki\n"]
        for _lu, title, slug, tldr in items:
            entry = f'- [[{slug}]] "{title}" — {tldr}' if tldr else f'- [[{slug}]] "{title}"'
            lines.append(entry)
        return "\n".join(lines) + "\n"

    for limit in (5, 3, 2):
        block = _build_block(candidates[:limit])
        if len(block) <= PRIOR_POSTS_BUDGET or limit == 2:
            return block

    return _build_block(candidates[:2])


# ---------------------------------------------------------------------------
# Cache paths
# ---------------------------------------------------------------------------


def _cache_dir(repo_root: Path) -> Path:
    return raw_path(repo_root, "transcripts", "substack")


def html_cache_path(repo_root: Path, post_id: str) -> Path:
    return _cache_dir(repo_root) / f"{post_id}.html"


def summary_cache_path(repo_root: Path, post_id: str) -> Path:
    return _cache_dir(repo_root) / f"{post_id}.json"


def links_cache_path(repo_root: Path, post_id: str) -> Path:
    return _cache_dir(repo_root) / f"{post_id}.links.json"


def quote_warnings_path(repo_root: Path, post_id: str) -> Path:
    return _cache_dir(repo_root) / f"{post_id}.quote-warnings.json"


def applied_cache_path(repo_root: Path, post_id: str) -> Path:
    return _cache_dir(repo_root) / f"{post_id}.applied.json"


# stance cache path is in scripts.substack.stance to avoid circular imports


# ---------------------------------------------------------------------------
# Stage 1 — fetch body
# ---------------------------------------------------------------------------


def _extract_body_from_post_page(full_html: str, url: str) -> str:
    """Return the <div class='body markup'> content from a post page.

    Raises FetchFailed if the body selector doesn't match — usually means
    Substack changed their DOM, or the response is an auth interstitial
    instead of the post. Silent fallback to full-page HTML would pollute
    the cache and blow up Gemini tokens.
    """
    soup = BeautifulSoup(full_html, "html.parser")
    body = soup.select_one("div.body.markup") or soup.select_one(".body.markup")
    if body is None:
        raise FetchFailed(f"body selector not found for {url}")
    return str(body)


def fetch_body(
    record: SubstackRecord,
    *,
    client: requests.Session,
    repo_root: Path,
) -> str:
    """Get the post body HTML. Uses record.body_html if present, else fetches.

    Cached at raw/transcripts/substack/<id>.html.
    Raises Paywalled on 401/403. Raises FetchFailed on other HTTP errors or network issues.
    """
    target = html_cache_path(repo_root, record.id)
    if target.exists():
        return target.read_text(encoding="utf-8")

    target.parent.mkdir(parents=True, exist_ok=True)

    if record.body_html:
        target.write_text(record.body_html, encoding="utf-8")
        return record.body_html

    try:
        resp = client.get(record.url, timeout=30, allow_redirects=True)
        resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response.status_code in (401, 403):
            raise Paywalled(record.url) from e
        raise FetchFailed(f"{e.response.status_code} for {record.url}") from e
    except requests.RequestException as e:
        raise FetchFailed(f"{type(e).__name__} for {record.url}") from e

    body_html = _extract_body_from_post_page(resp.text, record.url)
    target.write_text(body_html, encoding="utf-8")
    return body_html


# ---------------------------------------------------------------------------
# Stage 3 — extract + classify links (single combined cache file)
# ---------------------------------------------------------------------------


def classify_post_links(
    record: SubstackRecord,
    *,
    body_html: str,
    repo_root: Path,
) -> dict:
    """Extract links from body, classify external links, cache result.

    Returns a dict with:
      external_classified: list of {url, anchor_text, context_snippet, category, reason}
      substack_internal:   list of {url, anchor_text, context_snippet} (not classified)
    """
    target = links_cache_path(repo_root, record.id)
    identities = _get_llm_service().cache_identities(task_class="classification", prompt_version=CLASSIFY_LINKS_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    extracted = link_extractor.extract(body_html)
    external = extracted["external_links"]
    substack_internal = extracted["substack_links"]

    if not external:
        result = {"external_classified": [], "substack_internal": substack_internal}
    else:
        result_or_tuple = _get_llm_service().classify_links_batch(
            post_title=record.title,
            publication=record.publication_name,
            links=external,
        )
        if isinstance(result_or_tuple, tuple):
            classifications, identity = result_or_tuple
        else:
            classifications = result_or_tuple
        # Merge classifications onto extracted links by URL. classify_links_batch
        # already enforces same-order return, but merging by URL is defense in
        # depth: classifier hallucinations (URLs not in input) are dropped, and
        # missing entries get a sane default. Using `or` instead of dict.get
        # default collapses both missing-key and explicit-None into the fallback
        # (Gemini JSON mode returns null for empty fields).
        by_url = {c["url"]: c for c in classifications}
        merged = []
        for link in external:
            cls = by_url.get(link["url"]) or {}
            merged.append({
                **link,
                "category": cls.get("category") or "ignore",
                "reason": cls.get("reason") or "",
            })
        result = {"external_classified": merged, "substack_internal": substack_internal}

    write_llm_cache(target, identity=identity, data=result)
    return result


# ---------------------------------------------------------------------------
# Stage 4 — summarize
# ---------------------------------------------------------------------------


def summarize_post(
    record: SubstackRecord,
    *,
    body_markdown: str,
    repo_root: Path,
    prior_posts_context: str = "",
    stance_context: str = "",
) -> dict:
    """Summarize a post body into the structured dict. Cached on disk.

    Passes prior_posts_context and stance_context through to the Gemini call.
    Injects schema_version into the cached JSON (defense-in-depth — the prompt
    also asks Gemini to include it, but we guarantee it here).

    On cache load: if the cached file is missing schema_version or has a value
    below SUMMARY_CACHE_SCHEMA_VERSION, the stale file is deleted and the post
    is re-summarised.
    """
    target = summary_cache_path(repo_root, record.id)
    identities = _get_llm_service().cache_identities(task_class="summary", prompt_version=SUMMARIZE_SUBSTACK_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        if cached.get("schema_version", 1) >= SUMMARY_CACHE_SCHEMA_VERSION:
            return cached

    result_or_tuple = _get_llm_service().summarize_substack_post(
        title=record.title,
        publication=record.publication_name,
        author=record.author_name,
        body_markdown=body_markdown,
        prior_posts_context=prior_posts_context,
        stance_context=stance_context,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    response["schema_version"] = SUMMARY_CACHE_SCHEMA_VERSION
    write_llm_cache(target, identity=identity, data=response)
    return response


# ---------------------------------------------------------------------------
# Phase 1e — post-hoc evidence quote verification
# ---------------------------------------------------------------------------


def verify_quotes(
    summary: dict,
    body_markdown: str,
    record: SubstackRecord,
    repo_root: Path,
    *,
    _now: str | None = None,
) -> dict:
    """Walk summary["key_claims"], verify each evidence_quote against body_markdown.

    Unmatched claims are marked with ``quote_unverified: True`` (mutated in place).
    When at least one claim fails verification, a sidecar JSON file is written to
    ``raw/transcripts/substack/<id>.quote-warnings.json``.

    Returns the (possibly mutated) summary dict (same object).

    Thin wrapper around scripts.common.quote_verify.verify_quotes — preserved
    here so existing callers continue to use enrich.verify_quotes(...). The
    legacy ``post_id`` sidecar field is preserved alongside the new
    ``source_id``/``source_kind`` fields for backward compatibility.
    """
    from scripts.common.quote_verify import (
        verify_quotes as _verify_quotes,
        _quote_warnings_path,
    )

    result = _verify_quotes(
        summary=summary,
        body_text=body_markdown,
        source_id=record.id,
        source_kind="substack",
        repo_root=repo_root,
        _now=_now,
    )
    # Backward-compat: existing callers expect the sidecar to have a `post_id`
    # field. Re-write the sidecar with the legacy key alongside the new keys.
    sidecar_path = _quote_warnings_path(repo_root, "substack", record.id)
    if sidecar_path.exists():
        data = json.loads(sidecar_path.read_text(encoding="utf-8"))
        if "post_id" not in data:
            data["post_id"] = record.id
            sidecar_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
            )
    return result


# ---------------------------------------------------------------------------
# Phase 2 — apply post to the owner personally (Pass B)
# ---------------------------------------------------------------------------


def apply_post_to_you(
    record: SubstackRecord,
    *,
    summary: dict,
    repo_root: Path,
) -> dict:
    """Pass B: write a personal advisory note tying the essay to the owner.

    Cached on disk. Returns empty stub when no profile context is available.
    Empty-profile path does NOT write a cache file — next run will retry if
    the user has since added their profile.
    """
    target = applied_cache_path(repo_root, record.id)
    identities = _get_llm_service().cache_identities(task_class="personalization", prompt_version=APPLIED_TO_POST_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(target, expected=identities)
    if isinstance(cached, dict):
        return cached

    profile_ctx = load_profile_context()
    if not profile_ctx:
        return {
            "applied_paragraph": "",
            "applied_bullets": [],
            "socratic_questions": [],
            "thread_links": [],
        }

    result_or_tuple = _get_llm_service().applied_to_post(
        title=record.title,
        publication=record.publication_name,
        author=record.author_name,
        profile_context=profile_ctx,
        summary=summary,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple
    write_llm_cache(target, identity=identity, data=response)
    return response


# ---------------------------------------------------------------------------
# Phase 3 — update author stance doc (Pass C)
# ---------------------------------------------------------------------------


def _post_slug_from_record(record: SubstackRecord, repo_root: Path) -> str:
    """Derive the wiki-link slug (article page basename without .md) for a post."""
    from scripts.substack.write_pages import article_slug as _article_slug
    return _article_slug(repo_root, record)


def update_author_stance(
    record: SubstackRecord,
    *,
    summary: dict,
    repo_root: Path,
) -> str:
    """Pass C: update the per-author stance doc with learnings from a new post.

    Returns the change_note string. Cached on disk at <id>.stance.json.
    On cache hit this is a no-op — the stance doc as it existed when the
    original run wrote it was correct for that post, and later runs don't
    rewind it.
    """
    cache_target = stance.stance_cache_path(repo_root, record.id)
    identities = _get_llm_service().cache_identities(task_class="stance", prompt_version=UPDATE_AUTHOR_STANCE_PROMPT_VERSION)
    identity = identities[0]
    cached = load_llm_cache(cache_target, expected=identities)
    if isinstance(cached, dict):
        data = cached
        return data.get("change_note", "")

    from scripts.substack.write_pages import slugify
    author_slug = slugify(record.author_name)

    # Read current stance body (empty string if file doesn't exist)
    current_stance_text = stance.read_stance_update_snapshot(repo_root, author_slug)

    post_slug = _post_slug_from_record(record, repo_root)

    # Call Gemini for updated stance sections + change note
    result_or_tuple = _get_llm_service().update_author_stance(
        author=record.author_name,
        title=record.title,
        post_slug=post_slug,
        current_stance=current_stance_text,
        summary=summary,
    )
    if isinstance(result_or_tuple, tuple):
        response, identity = result_or_tuple
    else:
        response = result_or_tuple

    updated_body = response.get("stance_delta_md") or response.get("updated_stance_md", "")
    change_note = response.get("change_note", "")

    # Append only the new stance delta onto the canonical author page.
    if change_note.strip():
        stance.apply_stance_delta(
            record=record,
            delta_body=updated_body,
            change_note=change_note,
            post_slug=post_slug,
            repo_root=repo_root,
        )

    write_llm_cache(
        cache_target,
        identity=identity,
        data={
            "post_id": record.id,
            "author_slug": author_slug,
            "change_note": change_note,
            "stance_delta_md": updated_body,
            "cached_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )

    return change_note


# ---------------------------------------------------------------------------
# Phase 4 — entity extraction to wiki inbox
# ---------------------------------------------------------------------------

_STATIC_STOPWORDS: frozenset = frozenset({
    "the", "a", "an", "this", "that", "these", "those",
    "i", "me", "my", "we", "us", "our", "you", "your",
    "he", "she", "it", "they", "them", "their",
    "is", "are", "was", "were", "be", "been", "being",
    "and", "or", "but", "if", "then", "so",
    "to", "of", "in", "on", "at", "by", "for", "with", "from",
    "as", "about", "into", "through", "during", "before", "after",
    # substack-specific noise
    "substack", "newsletter", "post", "essay", "blog",
})

# Entity type → wiki subdirectory mapping
_ENTITY_WIKI_DIRS = {
    "people": "people",
    "companies": "companies",
    "tools": "tools",
    "concepts": "concepts",
}

# Iteration order for dedupe: first-seen category wins
_ENTITY_CATEGORY_ORDER = ("people", "companies", "tools", "concepts")

_CAP = 30
_ENTITY_SALES_CONTEXT_FRAGMENTS = (
    "sponsor",
    "sponsored",
    "use code",
    "discount",
    "subscribe",
    "upgrade to paid",
    "support the show",
    "join the waitlist",
    "follow ",
    "affiliate",
    "register now",
    "membership",
)


def _extract_context_sentence(body_markdown: str, entity_name: str) -> str:
    """Find the first occurrence of entity_name (case-insensitive) in body_markdown
    and return the enclosing sentence (delimited by . ! ? or paragraph break).
    Caps at 200 chars. Returns '(no direct quote in body)' if not found.
    """
    pattern = re.compile(re.escape(entity_name), re.IGNORECASE)
    m = pattern.search(body_markdown)
    if not m:
        return "(no direct quote in body)"

    start = m.start()

    # Find the start of the sentence: scan backwards for . ! ? or \n\n
    sentence_start = 0
    for i in range(start - 1, -1, -1):
        ch = body_markdown[i]
        if ch in ".!?":
            sentence_start = i + 1
            break
        # Paragraph break (two consecutive newlines)
        if body_markdown[i:i+2] == "\n\n":
            sentence_start = i + 2
            break

    # Find the end of the sentence: scan forwards for . ! ? or \n\n
    sentence_end = len(body_markdown)
    i = start
    while i < len(body_markdown):
        ch = body_markdown[i]
        if ch in ".!?":
            sentence_end = i + 1
            break
        if body_markdown[i:i+2] == "\n\n":
            sentence_end = i
            break
        i += 1

    sentence = body_markdown[sentence_start:sentence_end].strip()
    return sentence[:200]


def log_entities(
    record: SubstackRecord,
    *,
    summary: dict,
    body_markdown: str,
    repo_root: Path,
    today: str,
) -> List[str]:
    """Log new entities to wiki/inbox/substack-entities-{today}.md.

    Returns the list of entity names that were logged (not skipped as
    existing or deduped or filtered). This is so the orchestrator can
    include the count in CHANGELOG.
    """
    from scripts.substack.write_pages import slugify, article_slug as _article_slug

    entities_block = summary.get("entities")
    if not entities_block:
        return []

    # Build effective stopwords from static list + author/publication tokens
    effective_stopwords: set = set(_STATIC_STOPWORDS)
    effective_stopwords.add(record.author_name.strip().lower())
    effective_stopwords.add(record.publication_name.strip().lower())
    for token in record.author_name.split():
        if len(token) >= 2:
            effective_stopwords.add(token.strip().lower())

    # Collect all entities in iteration order, deduping case-insensitively
    seen_lower: dict = {}  # lower-cased name → (original name, category)
    for category in _ENTITY_CATEGORY_ORDER:
        items = entities_block.get(category) or []
        for entity in items:
            if not isinstance(entity, str):
                continue
            entity = entity.strip()
            if len(entity) < 2:
                continue
            lower = entity.lower()
            if lower in effective_stopwords:
                continue
            if lower not in seen_lower:
                seen_lower[lower] = (entity, category)

    # Apply cap
    surviving = list(seen_lower.values())[:_CAP]

    if not surviving:
        return []

    # Determine post slug for wiki-links
    post_slug = _article_slug(repo_root, record)

    # Check which entities already have wiki pages; skip them
    to_log = []
    filtered_sales_entities: list[tuple[str, str, str]] = []
    for entity_name, category in surviving:
        context = _extract_context_sentence(body_markdown, entity_name)
        if _is_sales_entity(entity_name, context):
            filtered_sales_entities.append((entity_name, category, context))
            continue
        wiki_dir = _ENTITY_WIKI_DIRS.get(category, category)
        slug = slugify(entity_name)
        existing_page = wiki_path(repo_root, wiki_dir, f"{slug}.md")
        if existing_page.exists():
            continue
        to_log.append((entity_name, category, context))

    if not to_log:
        return []

    # Build inbox lines
    lines = []
    for entity_name, category, context in to_log:
        line = (
            f'- **{entity_name}** ({category}) — '
            f'referenced by [[{post_slug}]] — '
            f'"{context}"'
        )
        lines.append(line)

    # Write to inbox file via the frontmatter-aware appender
    inbox_path = wiki_path(repo_root, "inbox", f"substack-entities-{today}.md")
    entry_text = "\n".join(lines) + "\n"
    append_to_inbox_log(
        target=inbox_path,
        kind="substack-entities",
        entry=entry_text,
        date=today,
    )

    _append_anti_sales_audit(
        repo_root=repo_root,
        today=today,
        lines=[
            (
                f'- entity: **{entity_name}** ({category}) — '
                f'referenced by [[{post_slug}]] — '
                f'reason: filtered sales chrome — '
                f'"{context}"'
            )
            for entity_name, category, context in filtered_sales_entities
        ],
    )

    return [name for name, _cat, _context in to_log]


def normalize_substack_source(
    record: SubstackRecord,
    *,
    body_markdown: str,
    body_html: str,
) -> NormalizedSource:
    """Normalize a Substack record into the shared source boundary."""

    creator_candidates = [
        MaterializationCandidate(
            page_type="person",
            name=record.author_name,
            role="creator",
            confidence=0.99,
            deterministic=True,
            source="substack",
            page_id=_slugify(record.author_name),
        ),
        MaterializationCandidate(
            page_type="company",
            name=record.publication_name,
            role="publisher",
            confidence=0.99,
            deterministic=True,
            source="substack",
            page_id=record.publication_slug,
        ),
    ]
    return NormalizedSource(
        source_id=f"substack-{record.id}",
        source_kind="substack",
        external_id=f"substack-{record.id}",
        canonical_url=record.url,
        title=record.title,
        creator_candidates=[asdict(candidate) for candidate in creator_candidates],
        published_at=record.published_at,
        discovered_at=record.saved_at,
        source_metadata={
            "record": record,
            "body_html": body_html,
        },
        discovered_links=[],
        provenance={
            "adapter": "substack",
            "publication_slug": record.publication_slug,
        },
        body_markdown=body_markdown,
    )


def _materialization_targets_from_source(source: NormalizedSource):
    candidates = [MaterializationCandidate(**candidate) for candidate in source.creator_candidates]
    return select_primary_targets(candidates)


def run_pass_d_for_substack(
    record: SubstackRecord,
    *,
    body_markdown: str,
    summary: dict,
    applied: dict | None,
    stance_change_note: str | None,
    stance_context_text: str,
    prior_context: str,
    repo_root: Path,
    today: str,
    cache_mode: str = "default",
    evidence_date: str | None = None,
    force_refresh: bool = False,
) -> dict:
    """Run shared Pass D for a Substack source and dispatch evidence/probationary writes."""
    from scripts.atoms import pass_d, working_set
    from scripts.atoms.replay import apply_pass_d_result

    ws = working_set.load_for_source(
        source_topics=list(summary.get("topics") or []),
        source_domains=["learning"],
        cap=300,
        repo_root=repo_root,
    )
    source_page_id = __import__("scripts.substack.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, record)
    cache_reused = pass_d.pass_d_cache_exists(
        repo_root=repo_root,
        source_kind="substack",
        source_id=f"substack-{record.id}",
        cache_mode=cache_mode,
    ) and not force_refresh
    try:
        result = pass_d.run_pass_d(
            source_id=f"substack-{record.id}",
            source_link=f"[[{source_page_id}]]",
            source_kind="substack",
            body_or_transcript=body_markdown,
            summary=summary,
            applied=applied,
            pass_c_delta=stance_change_note,
            stance_context=stance_context_text,
            prior_source_context=prior_context,
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
            source_link=f"[[{source_page_id}]]",
            repo_root=repo_root,
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


def _unsaved_substack_links_payload(
    *,
    record: SubstackRecord,
    links: list[dict[str, str]],
    repo_root: Path,
) -> list[dict[str, str]]:
    source_page_id = __import__("scripts.substack.write_pages", fromlist=["canonical_page_id"]).canonical_page_id(repo_root, record)
    return [
        {
            "url": _normalize_substack_post_url(link["url"]),
            "anchor_text": str(link.get("anchor_text") or ""),
            "source_page_id": source_page_id,
            "source_post_id": record.id,
            "source_post_url": record.url,
            "discovered_at": record.saved_at,
        }
        for link in links
    ]


def log_unsaved_substack_refs(
    refs: list[dict[str, str]],
    *,
    repo_root: Path,
    today: str,
) -> None:
    for ref in refs:
        append_to_inbox_log(
            target=wiki_path(repo_root, "inbox", f"substack-referenced-unsaved-{today}.md"),
            kind="substack-referenced-unsaved",
            entry=f"- {ref['source_post_id']} — {ref['url']} — {ref.get('anchor_text', '')}\n",
            date=today,
        )


def run_substack_record_lifecycle(
    record: SubstackRecord,
    *,
    client: requests.Session,
    repo_root: Path,
    today: str,
    saved_urls: set[str],
    discovered_via_page_id: str | None = None,
    discovered_via_url: str | None = None,
    log_unsaved_refs: bool = True,
) -> IngestionLifecycleResult:
    """Run one Substack record through the shared ingestion lifecycle."""

    body_html = fetch_body(record, client=client, repo_root=repo_root)
    body_md = __import__("scripts.substack.html_to_markdown", fromlist=["convert"]).convert(body_html)
    source = normalize_substack_source(record, body_markdown=body_md, body_html=body_html)

    def understand(substack_source: NormalizedSource, _envelope: dict[str, object]) -> dict[str, object]:
        substack_record = substack_source.source_metadata["record"]
        html = substack_source.source_metadata["body_html"]
        classified = classify_post_links(substack_record, body_html=html, repo_root=repo_root)
        prior_context = get_prior_posts_context(substack_record, repo_root)
        stance_context_text = stance.load_stance_context(_slugify(substack_record.author_name), repo_root)
        summary = summarize_post(
            substack_record,
            body_markdown=substack_source.primary_content,
            repo_root=repo_root,
            prior_posts_context=prior_context,
            stance_context=stance_context_text,
        )
        summary = verify_quotes(summary, substack_source.primary_content, substack_record, repo_root)
        return {
            "summary": summary,
            "classified_links": classified,
            "body_markdown": substack_source.primary_content,
            "prior_context": prior_context,
            "stance_context": stance_context_text,
        }

    def personalize(substack_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        substack_record = substack_source.source_metadata["record"]
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        try:
            applied = apply_post_to_you(substack_record, summary=summary, repo_root=repo_root)
        except Exception:
            applied = None
        return {"applied": applied}

    def attribute(substack_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        substack_record = substack_source.source_metadata["record"]
        summary = (envelope.get("pass_a") or {}).get("summary", {})
        try:
            stance_change_note = update_author_stance(substack_record, summary=summary, repo_root=repo_root)
            return {
                "status": "implemented" if (stance_change_note or "").strip() else "empty",
                "stance_change_note": stance_change_note,
            }
        except Exception as exc:
            return {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "stance_change_note": None,
            }

    def distill(substack_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, object]:
        substack_record = substack_source.source_metadata["record"]
        pass_a = envelope.get("pass_a") or {}
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        try:
            return run_pass_d_for_substack(
                substack_record,
                body_markdown=pass_a.get("body_markdown", substack_source.primary_content),
                summary=pass_a.get("summary", {}),
                applied=pass_b.get("applied"),
                stance_change_note=pass_c.get("stance_change_note"),
                stance_context_text=pass_a.get("stance_context", ""),
                prior_context=pass_a.get("prior_context", ""),
                repo_root=repo_root,
                today=today,
            )
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}

    def materialize(substack_source: NormalizedSource, envelope: dict[str, object]) -> dict[str, str]:
        from scripts.substack import write_pages

        substack_record = substack_source.source_metadata["record"]
        pass_a = envelope.get("pass_a") or {}
        pass_b = envelope.get("pass_b") or {}
        pass_c = envelope.get("pass_c") or {}
        targets = _materialization_targets_from_source(substack_source)
        if targets.creator_target is None or targets.publisher_target is None:
            raise ValueError("substack materialization requires creator and publisher targets")
        article = write_pages.write_article_page(
            substack_record,
            summary=pass_a.get("summary", {}),
            classified_links=pass_a.get("classified_links", {}),
            body_markdown=pass_a.get("body_markdown", substack_source.primary_content),
            repo_root=repo_root,
            applied=pass_b.get("applied"),
            stance_change_note=pass_c.get("stance_change_note"),
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
            discovered_via_page_id=discovered_via_page_id,
            discovered_via_url=discovered_via_url,
            force=True,
        )
        if discovered_via_page_id:
            write_pages.add_materialized_link_to_source_page(
                repo_root=repo_root,
                source_page_id=discovered_via_page_id,
                target_page_id=write_pages.canonical_page_id(repo_root, substack_record),
                target_kind="substack",
            )
        author = write_pages.ensure_author_page(
            substack_record,
            repo_root=repo_root,
            creator_target=targets.creator_target,
            publisher_target=targets.publisher_target,
        )
        publication = write_pages.ensure_publication_page(
            substack_record,
            repo_root=repo_root,
            publisher_target=targets.publisher_target,
            source_link=write_pages.canonical_page_id(repo_root, substack_record),
        )
        return {
            "article": str(article),
            "author": str(author),
            "publication": str(publication),
        }

    def propagate(substack_source: NormalizedSource, envelope: dict[str, object], _materialized: dict[str, str]) -> dict[str, object]:
        from scripts.atoms import pass_d

        substack_record = substack_source.source_metadata["record"]
        pass_a = envelope.get("pass_a") or {}
        classified = pass_a.get("classified_links", {})
        logged_entities = log_entities(
            substack_record,
            summary=pass_a.get("summary", {}),
            body_markdown=pass_a.get("body_markdown", substack_source.primary_content),
            repo_root=repo_root,
            today=today,
        )
        external_classified = classified.get("external_classified") or []
        audited_links = _audit_sales_chrome_links(
            record=substack_record,
            classified_links=external_classified,
            repo_root=repo_root,
            today=today,
        )
        audit_path = wiki_path(repo_root, "inbox", f"substack-anti-sales-audit-{today}.md")
        return {
            "pass_d": pass_d.stage_outcomes_from_payload(envelope.get("pass_d") or {}),
            "drop_path": "",
            "drop_candidates": 0,
            "unsaved_refs": 0,
            "unsaved_substack_links": [],
            "logged_entities": logged_entities,
            "logged_entity_count": len(logged_entities),
            "propagate_discovered_count": len(external_classified),
            "propagate_queued_count": 0,
            "anti_sales_audit_path": str(audit_path) if audit_path.exists() else "",
            "audited_sales_links": audited_links,
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


def _is_sales_entity(entity_name: str, context: str) -> bool:
    combined = f"{entity_name} {context}".lower()
    return any(fragment in combined for fragment in _ENTITY_SALES_CONTEXT_FRAGMENTS)


def _is_audited_sales_link(link: dict) -> bool:
    reason = str(link.get("reason") or "").lower()
    category = str(link.get("category") or "").lower()
    if category not in {"ignore", "ignored"}:
        return False
    if is_sales_chrome(
        str(link.get("url") or ""),
        str(link.get("anchor_text") or ""),
        str(link.get("context_snippet") or ""),
    ):
        return True
    return any(token in reason for token in ("sales", "sponsor", "promo", "promotion", "subscribe", "social"))


def _append_anti_sales_audit(*, repo_root: Path, today: str, lines: list[str]) -> None:
    if not lines:
        return
    append_to_inbox_log(
        target=wiki_path(repo_root, "inbox", f"substack-anti-sales-audit-{today}.md"),
        kind="substack-anti-sales-audit",
        entry="\n".join(lines) + "\n",
        date=today,
    )


def _audit_sales_chrome_links(
    *,
    record: SubstackRecord,
    classified_links: list[dict],
    repo_root: Path,
    today: str,
) -> list[str]:
    lines: list[str] = []
    audited_urls: list[str] = []
    for link in classified_links:
        if not _is_audited_sales_link(link):
            continue
        audited_urls.append(str(link.get("url") or ""))
        lines.append(
            f'- link: [{link.get("anchor_text") or link.get("url")}]({link.get("url")})'
            f" — source={record.id}"
            f" — reason={link.get('reason') or 'filtered sales chrome'}"
            f" — context={link.get('context_snippet') or ''}"
        )
    _append_anti_sales_audit(repo_root=repo_root, today=today, lines=lines)
    return audited_urls
