"""Drain entry point for the articles queue.

``drain_drop_queue()`` walks article drop files, fetches each entry, runs the
shared article lifecycle, logs paywall and lifecycle failures, and writes the
drop marker only after the file has been processed. The Substack inline drain,
standalone article ingest, and links-import path all call through this seam.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from scripts.articles import write_pages
from scripts.articles.enrich import run_article_entry_lifecycle
from scripts.articles.enrich import summarize_article  # compatibility patch target for existing tests
from scripts.articles.fetch import ArticleFetchFailure, fetch_article, is_supported_article_url
from scripts.articles.parse import ArticleDropEntry, parse_drop_file
from scripts.common.inbox_log import append_to_inbox_log
from scripts.common.vault import raw_path, wiki_path


@dataclass
class DrainResult:
    drop_files_processed: int = 0
    urls_in_queue: int = 0
    skipped_existing: int = 0
    fetched_summarized: int = 0
    paywalled: int = 0
    failed: int = 0
    new_pages_written: int = 0
    new_page_paths: List[Path] = field(default_factory=list)


def _append_inbox(repo_root: Path, filename: str, line: str) -> None:
    inbox = wiki_path(repo_root, "inbox", filename)
    stem = Path(filename).stem
    date = stem[-10:]
    kind = stem[:-11]
    append_to_inbox_log(
        target=inbox,
        kind=kind,
        entry=line + "\n",
        date=date,
    )


def iter_drop_entries(
    *,
    repo_root: Path,
    path: Path | None = None,
    today: str | None = None,
) -> list[tuple[Path, ArticleDropEntry]]:
    drops_dir = raw_path(repo_root, "drops")
    if path is not None:
        drop_files = [path.resolve()]
    elif not drops_dir.exists():
        drop_files = []
    elif today:
        drop_files = sorted(drops_dir.glob(f"articles-from-*-{today}.jsonl"))
        if not drop_files:
            drop_files = sorted(drops_dir.glob(f"articles-from-*{today}*.jsonl"))
    else:
        drop_files = sorted(drops_dir.glob("articles-from-*.jsonl"))

    entries: list[tuple[Path, ArticleDropEntry]] = []
    for drop_file in drop_files:
        for entry in parse_drop_file(drop_file):
            entries.append((drop_file, entry))
    return entries


def drain_drop_queue(
    *,
    today_str: str,
    repo_root: Path,
) -> DrainResult:
    """Process all unprocessed article drop files. Returns counters."""
    result = DrainResult()
    drops_dir = raw_path(repo_root, "drops")
    if not drops_dir.exists():
        return result

    for drop_file in sorted(drops_dir.glob("articles-from-*.jsonl")):
        marker = (
            wiki_path(repo_root, "sources", "articles")
            / f".ingested-{drop_file.name}"
        )
        if marker.exists():
            continue
        result.drop_files_processed += 1

        for entry in parse_drop_file(drop_file):
            result.urls_in_queue += 1

            if not is_supported_article_url(entry.url):
                result.skipped_existing += 1
                continue

            fr = fetch_article(entry, repo_root=repo_root)
            if isinstance(fr, ArticleFetchFailure):
                if fr.failure_kind == "paywalled":
                    result.paywalled += 1
                    _append_inbox(
                        repo_root,
                        f"articles-paywalled-{today_str}.md",
                        f"- [{entry.url}]({entry.url}) — discovered via {entry.source_type}:{entry.source_post_id or entry.source_label} "
                        f"({entry.category})",
                    )
                else:
                    result.failed += 1
                    _append_inbox(
                        repo_root,
                        f"articles-failures-{today_str}.md",
                        (
                            f"- [{entry.url}]({entry.url})"
                            f" — stage=fetch"
                            f" — kind={fr.failure_kind}"
                            f" — {fr.detail}"
                        ),
                    )
                continue

            try:
                lifecycle = run_article_entry_lifecycle(
                    entry,
                    fetch_result=fr,
                    repo_root=repo_root,
                    today=today_str,
                    summarize_override=summarize_article,
                )
            except Exception as e:
                result.failed += 1
                _append_inbox(
                    repo_root,
                    f"articles-failures-{today_str}.md",
                    f"- [{entry.url}]({entry.url}) — discovered via {entry.source_type}:{entry.source_post_id or entry.source_label} "
                    f"— summarize failed: {type(e).__name__}: {e}",
                )
                continue

            page = Path(lifecycle.materialized["article"])
            result.fetched_summarized += 1
            result.new_pages_written += 1
            result.new_page_paths.append(page)
            for pass_d_outcome in (lifecycle.propagate or {}).get("pass_d") or []:
                _append_inbox(
                    repo_root,
                    f"articles-failures-{today_str}.md",
                    (
                        f"- [{entry.url}]({entry.url})"
                        f" — stage={pass_d_outcome['stage']}"
                        f" — {pass_d_outcome['summary']}"
                    ),
                )
            for fanout_outcome in (lifecycle.propagate or {}).get("fanout_outcomes") or []:
                _append_inbox(
                    repo_root,
                    f"articles-failures-{today_str}.md",
                    (
                        f"- [{entry.url}]({entry.url})"
                        f" — stage={fanout_outcome['stage']}"
                        f" — {fanout_outcome['summary']}"
                    ),
                )

        # Write marker for this drop file (only once we've processed every entry)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()

    return result
