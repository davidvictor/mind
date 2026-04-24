from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

from mind.services.durable_write import write_contract_page
from scripts.common.frontmatter import split_frontmatter as _split_frontmatter
from scripts.common.vault import raw_path, wiki_path
from scripts.search_signals.contracts import SearchSignal


@dataclass(frozen=True)
class SearchSignalsIngestResult:
    drop_files_processed: int = 0
    signals_materialized: int = 0
    pages_written: int = 0


def _load_signals(path: Path) -> list[SearchSignal]:
    signals: list[SearchSignal] = []
    if not path.exists():
        return signals
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        data = json.loads(line)
        signals.append(SearchSignal(**data))
    return signals


def _month_slug(searched_at: str) -> str:
    if not searched_at:
        return datetime.now(timezone.utc).strftime("%Y-%m")
    return searched_at[:7]


def _rollup_page_path(repo_root: Path, month_slug: str) -> Path:
    return wiki_path(repo_root, "me", "search-patterns", f"{month_slug}.md")


def _write_rollup_page(repo_root: Path, month_slug: str, signals: list[SearchSignal]) -> Path:
    target = _rollup_page_path(repo_root, month_slug)
    existing_ids: set[str] = set()
    existing_body = ""
    if target.exists():
        frontmatter, existing_body = _split_frontmatter(target.read_text(encoding="utf-8"))
        existing_ids = set(frontmatter.get("query_ids") or [])
    ordered = sorted(
        [signal for signal in signals if signal.query_id not in existing_ids],
        key=lambda item: (item.searched_at, item.query_text.lower(), item.query_id),
    )
    if target.exists() and existing_body.strip():
        body_lines = [existing_body.rstrip(), ""]
    else:
        body_lines = [f"# Search Patterns — {month_slug}\n", "## Queries\n"]
    for signal in ordered:
        topics = f" ({', '.join(signal.topics)})" if signal.topics else ""
        clicks = ""
        if signal.clicked_canonical_urls:
            clicks = f" — clicked: {', '.join(signal.clicked_canonical_urls[:3])}"
        body_lines.append(f"- {signal.query_text}{topics}{clicks}")
    body_lines.append("")
    write_contract_page(
        target,
        page_type="note",
        title=f"Search Patterns — {month_slug}",
        body="\n".join(body_lines),
        created=f"{month_slug}-01",
        last_updated=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        domains=["learning"],
        extra_frontmatter={
            "month": month_slug,
            "query_ids": sorted(existing_ids | {signal.query_id for signal in signals}),
        },
        force=True,
    )
    return target


def ingest_search_signal_drop_files(
    repo_root: Path,
    *,
    today_str: str,
) -> SearchSignalsIngestResult:
    result = SearchSignalsIngestResult()
    drops_dir = raw_path(repo_root, "drops")
    if not drops_dir.exists():
        return result
    for drop_file in sorted(drops_dir.glob("search-signals-from-*.jsonl")):
        marker = wiki_path(repo_root, "me", "search-patterns") / f".ingested-{drop_file.name}"
        if marker.exists():
            continue
        signals = _load_signals(drop_file)
        result = SearchSignalsIngestResult(
            drop_files_processed=result.drop_files_processed + 1,
            signals_materialized=result.signals_materialized + len(signals),
            pages_written=result.pages_written,
        )
        grouped: dict[str, list[SearchSignal]] = defaultdict(list)
        for signal in signals:
            grouped[_month_slug(signal.searched_at)].append(signal)
        for month_slug, month_signals in grouped.items():
            _write_rollup_page(repo_root, month_slug, month_signals)
            result = SearchSignalsIngestResult(
                drop_files_processed=result.drop_files_processed,
                signals_materialized=result.signals_materialized,
                pages_written=result.pages_written + 1,
            )
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
    return result
