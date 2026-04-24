from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from urllib.parse import urlparse

from scripts.chrome.contracts import ChromeEvent, discovery_key_for_url
from scripts.common.config import BrainConfig
from scripts.common.inbox_log import append_to_inbox_log
from scripts.common.vault import raw_path, wiki_path
from scripts.search_signals.contracts import SearchSignal, build_search_signals
from scripts.web_discovery.contracts import DiscoveryEventEdge, WebCandidate
from scripts.web_discovery.firecrawl import FirecrawlClient, FirecrawlError
from scripts.web_discovery.materialize import load_existing_record, merge_candidate, write_web_discovery_page
from scripts.web_discovery.triage import build_candidate_seed, is_query_private, triage_candidate


@dataclass(frozen=True)
class WebDiscoveryIngestResult:
    raw_events_seen: int = 0
    candidates_written: int = 0
    search_signals_written: int = 0


@dataclass(frozen=True)
class WebDiscoveryDrainResult:
    drop_files_processed: int = 0
    candidates_processed: int = 0
    pages_written: int = 0
    crawled: int = 0
    deferred: int = 0
    failed: int = 0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cooldown_until(days: int) -> str:
    return (_utc_now() + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_web_candidates(events: list[ChromeEvent], *, repo_root: Path) -> list[WebCandidate]:
    cfg = BrainConfig.load(repo_root)
    query_ids_by_key: dict[tuple[str, str, str, str], str] = {}
    for signal in build_search_signals(events):
        query_ids_by_key[(signal.chrome_profile, signal.searched_at, signal.query_text, signal.engine_domain)] = signal.query_id

    grouped: dict[str, list[DiscoveryEventEdge]] = defaultdict(list)
    titles: dict[str, str] = {}
    urls: dict[str, str] = {}
    for event in events:
        if event.event_type not in {"bookmark", "history_visit", "query_click"} or not event.url:
            continue
        seed = build_candidate_seed(event)
        if seed is None:
            continue
        canonical = seed.canonical_url
        query_id = ""
        if event.query_text:
            query_id = query_ids_by_key.get((event.chrome_profile, event.occurred_at, event.query_text, event.engine_domain), "")
        grouped[canonical].append(
            DiscoveryEventEdge(
                edge_type="history" if event.event_type == "history_visit" else event.event_type.replace("_visit", ""),
                event_id=event.event_id,
                occurred_at=event.occurred_at,
                bookmark_folder_path=event.bookmark_folder_path,
                query_id=query_id,
            )
        )
        titles.setdefault(canonical, event.title or canonical)
        urls.setdefault(canonical, event.url)

    candidates: list[WebCandidate] = []
    for canonical, edges in grouped.items():
        candidate = WebCandidate(
            candidate_id=discovery_key_for_url(canonical),
            canonical_url=canonical,
            url=urls[canonical],
            title=titles[canonical],
            domain=(urlparse(canonical).hostname or "").lower(),
            evidence_edges=edges,
            triage=build_candidate_seed(
                ChromeEvent(
                    event_id="",
                    event_type="bookmark",
                    chrome_profile="",
                    occurred_at="",
                    url=canonical,
                    title=titles[canonical],
                )
            ).triage,
            crawl={
                "status": "not_attempted",
                "last_crawled_at": "",
                "cooldown_until": _cooldown_until(cfg.chrome.firecrawl.crawl_cooldown_days),
            },
        )
        triage = triage_candidate(candidate, confidence_threshold=cfg.chrome.triage_confidence_threshold)
        candidate = WebCandidate(
            candidate_id=candidate.candidate_id,
            canonical_url=candidate.canonical_url,
            url=candidate.url,
            title=candidate.title,
            domain=candidate.domain,
            evidence_edges=candidate.evidence_edges,
            triage=triage,
            crawl=candidate.crawl,
        )
        if candidate.triage.decision != "drop":
            candidates.append(candidate)
    return candidates


def _write_jsonl(path: Path, payloads: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(item, sort_keys=True) + "\n" for item in payloads), encoding="utf-8")
    return path


def write_web_discovery_drop(repo_root: Path, *, candidates: list[WebCandidate], today_str: str) -> Path:
    target = raw_path(repo_root, "drops", f"web-discovery-candidates-from-chrome-{today_str}.jsonl")
    return _write_jsonl(target, [candidate.to_dict() for candidate in candidates])


def write_search_signal_drop(repo_root: Path, *, search_signals: list[SearchSignal], today_str: str) -> Path:
    target = raw_path(repo_root, "drops", f"search-signals-from-chrome-{today_str}.jsonl")
    return _write_jsonl(target, [signal.to_dict() for signal in search_signals])


def build_retained_search_signals(events: list[ChromeEvent]) -> list[SearchSignal]:
    retained: list[SearchSignal] = []
    for signal in build_search_signals(events):
        if is_query_private(signal.query_text):
            continue
        retained.append(signal)
    return retained


def _load_candidates(path: Path) -> list[WebCandidate]:
    candidates: list[WebCandidate] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        candidates.append(WebCandidate.from_dict(json.loads(line)))
    return candidates


def drain_web_discovery_drop_queue(
    *,
    repo_root: Path,
    today_str: str | None = None,
) -> WebDiscoveryDrainResult:
    cfg = BrainConfig.load(repo_root)
    firecrawl_cap = int(cfg.chrome.firecrawl.max_requests_per_run)
    drops_dir = raw_path(repo_root, "drops")
    if not drops_dir.exists():
        return WebDiscoveryDrainResult()
    crawls_used = 0
    result = WebDiscoveryDrainResult()
    firecrawl_client: FirecrawlClient | None = None
    for drop_file in sorted(drops_dir.glob("web-discovery-candidates-from-chrome-*.jsonl")):
        marker = wiki_path(repo_root, "sources", "web-discovery") / f".ingested-{drop_file.name}"
        if marker.exists():
            continue
        try:
            candidates = _load_candidates(drop_file)
        except Exception as exc:
            append_to_inbox_log(
                target=wiki_path(repo_root, "inbox", f"web-discovery-failures-{today_str or date.today().isoformat()}.md"),
                kind="web-discovery-failures",
                entry=f"- {drop_file.name} — malformed drop file — {type(exc).__name__}: {exc}\n",
                date=today_str or date.today().isoformat(),
            )
            result = WebDiscoveryDrainResult(
                drop_files_processed=result.drop_files_processed + 1,
                candidates_processed=result.candidates_processed,
                pages_written=result.pages_written,
                crawled=result.crawled,
                deferred=result.deferred,
                failed=result.failed + 1,
            )
            continue
        file_failed = False
        result = WebDiscoveryDrainResult(
            drop_files_processed=result.drop_files_processed + 1,
            candidates_processed=result.candidates_processed,
            pages_written=result.pages_written,
            crawled=result.crawled,
            deferred=result.deferred,
            failed=result.failed,
        )
        for candidate in candidates:
            try:
                crawl_markdown = ""
                if candidate.triage.decision == "crawl":
                    if crawls_used >= firecrawl_cap or firecrawl_cap <= 0:
                        result = WebDiscoveryDrainResult(
                            drop_files_processed=result.drop_files_processed,
                            candidates_processed=result.candidates_processed + 1,
                            pages_written=result.pages_written,
                            crawled=result.crawled,
                            deferred=result.deferred + 1,
                            failed=result.failed,
                        )
                    else:
                        firecrawl_client = firecrawl_client or FirecrawlClient()
                        crawl_markdown = firecrawl_client.scrape(candidate.canonical_url).markdown
                        crawls_used += 1
                        result = WebDiscoveryDrainResult(
                            drop_files_processed=result.drop_files_processed,
                            candidates_processed=result.candidates_processed + 1,
                            pages_written=result.pages_written,
                            crawled=result.crawled + 1,
                            deferred=result.deferred,
                            failed=result.failed,
                        )
                else:
                    result = WebDiscoveryDrainResult(
                        drop_files_processed=result.drop_files_processed,
                        candidates_processed=result.candidates_processed + 1,
                        pages_written=result.pages_written,
                        crawled=result.crawled,
                        deferred=result.deferred,
                        failed=result.failed,
                    )
                existing = load_existing_record(repo_root, merge_candidate({}, candidate))
                record = merge_candidate(existing, candidate, crawl_markdown=crawl_markdown)
                write_web_discovery_page(repo_root, record)
                result = WebDiscoveryDrainResult(
                    drop_files_processed=result.drop_files_processed,
                    candidates_processed=result.candidates_processed,
                    pages_written=result.pages_written + 1,
                    crawled=result.crawled,
                    deferred=result.deferred,
                    failed=result.failed,
                )
            except FirecrawlError as exc:
                append_to_inbox_log(
                    target=wiki_path(repo_root, "inbox", f"web-discovery-failures-{today_str or date.today().isoformat()}.md"),
                    kind="web-discovery-failures",
                    entry=f"- {candidate.canonical_url} — firecrawl — {exc}\n",
                    date=today_str or date.today().isoformat(),
                )
                file_failed = True
                result = WebDiscoveryDrainResult(
                    drop_files_processed=result.drop_files_processed,
                    candidates_processed=result.candidates_processed,
                    pages_written=result.pages_written,
                    crawled=result.crawled,
                    deferred=result.deferred,
                    failed=result.failed + 1,
                )
            except Exception as exc:
                append_to_inbox_log(
                    target=wiki_path(repo_root, "inbox", f"web-discovery-failures-{today_str or date.today().isoformat()}.md"),
                    kind="web-discovery-failures",
                    entry=f"- {candidate.canonical_url} — {type(exc).__name__}: {exc}\n",
                    date=today_str or date.today().isoformat(),
                )
                file_failed = True
                result = WebDiscoveryDrainResult(
                    drop_files_processed=result.drop_files_processed,
                    candidates_processed=result.candidates_processed,
                    pages_written=result.pages_written,
                    crawled=result.crawled,
                    deferred=result.deferred,
                    failed=result.failed + 1,
                )
        if not file_failed:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
    return result
