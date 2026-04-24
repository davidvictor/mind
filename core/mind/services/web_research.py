from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

from scripts.articles.enrich import run_article_entry_lifecycle
from scripts.articles.fetch import fetch_article
from scripts.articles.parse import ArticleDropEntry


@dataclass(frozen=True)
class WebSearchResult:
    title: str
    url: str


@dataclass(frozen=True)
class GroundedArticleResult:
    query: str
    url: str
    article_page_id: str


def search_web(query: str, *, limit: int = 3) -> list[WebSearchResult]:
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    response = httpx.get(url, timeout=20.0, headers={"user-agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    results: list[WebSearchResult] = []
    for anchor in soup.select("a.result__a"):
        href = anchor.get("href", "").strip()
        title = anchor.get_text(" ", strip=True)
        if not href.startswith("http"):
            continue
        results.append(WebSearchResult(title=title, url=href))
        if len(results) >= limit:
            break
    return results


def ingest_web_articles(
    *,
    repo_root: Path,
    queries: list[str],
    source_label: str,
    today: str,
    results_per_query: int,
) -> list[GroundedArticleResult]:
    ingested: list[GroundedArticleResult] = []
    seen_urls: set[str] = set()
    for query in queries:
        for result in search_web(query, limit=results_per_query):
            if result.url in seen_urls:
                continue
            seen_urls.add(result.url)
            entry = ArticleDropEntry(
                url=result.url,
                source_post_id=source_label,
                source_post_url="",
                anchor_text=result.title,
                context_snippet=query,
                category="business",
                discovered_at=_today_timestamp(),
                source_type="web-grounding",
                source_label=source_label,
            )
            fetch_result = fetch_article(entry, repo_root=repo_root)
            if not hasattr(fetch_result, "body_text"):
                continue
            lifecycle = run_article_entry_lifecycle(
                entry,
                fetch_result=fetch_result,
                repo_root=repo_root,
                today=today,
            )
            article_path = Path(lifecycle.materialized["article"])
            ingested.append(
                GroundedArticleResult(
                    query=query,
                    url=result.url,
                    article_page_id=article_path.stem,
                )
            )
    return ingested


def _today_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_atom_queries(
    *,
    title: str,
    tldr: str,
    typed_neighbors: list[dict[str, str]],
    max_queries: int,
) -> list[str]:
    base = _squash(f"{title} {tldr}".strip())
    queries: list[str] = [base] if base else []
    if typed_neighbors:
        neighbor_text = " ".join(item["atom_id"].replace("-", " ") for item in typed_neighbors[:2])
        blended = _squash(f"{title} {neighbor_text}")
        if blended and blended not in queries:
            queries.append(blended)
    return queries[:max_queries]


def _squash(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned[:240]
