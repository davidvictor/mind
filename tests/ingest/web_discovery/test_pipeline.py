from __future__ import annotations
from pathlib import Path

import pytest

from scripts.chrome.scan import scan_chrome_profiles
from scripts.search_signals.materialize import ingest_search_signal_drop_files
from scripts.web_discovery.pipeline import (
    build_retained_search_signals,
    build_web_candidates,
    drain_web_discovery_drop_queue,
    write_search_signal_drop,
    write_web_discovery_drop,
)
from scripts.web_discovery.triage import should_exclude_url
from tests.paths import FIXTURES_ROOT
from tests.support import write_repo_config


FIXTURE_ROOT = FIXTURES_ROOT / "chrome"


class _StubTriageLLM:
    """Deterministic triage for tests — avoids calling the real LLM."""

    def generate_json_prompt(self, prompt: str) -> dict[str, object]:
        return {
            "decision": "signal_only",
            "confidence": 0.85,
            "reason": "stubbed triage for tests",
            "object_type": "tool",
            "topics": ["design", "mcp"],
            "why_it_matters": "repeated research signal",
            "cost_sensitivity": "medium",
        }


@pytest.fixture(autouse=True)
def _stub_triage_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "scripts.web_discovery.triage.get_llm_service",
        lambda: _StubTriageLLM(),
    )


def _make_chrome_root(tmp_path: Path) -> Path:
    chrome_root = tmp_path / "Chrome"
    chrome_root.mkdir(parents=True, exist_ok=True)
    (chrome_root / "Local State").write_text((FIXTURE_ROOT / "local-state-sample.json").read_text(encoding="utf-8"), encoding="utf-8")
    profile = chrome_root / "Profile 1"
    profile.mkdir(parents=True, exist_ok=True)
    (profile / "Bookmarks").write_text((FIXTURE_ROOT / "bookmarks-sample.json").read_text(encoding="utf-8"), encoding="utf-8")
    (profile / "History").write_bytes((FIXTURE_ROOT / "history-sample.sqlite3").read_bytes())
    return chrome_root


def test_build_web_candidates_excludes_noisy_urls_and_merges_canonical_variants(tmp_path: Path) -> None:
    chrome_root = _make_chrome_root(tmp_path)
    result = scan_chrome_profiles(chrome_root=chrome_root, selected_profiles=["Research"], since_days=3650)

    candidates = build_web_candidates(result.events, repo_root=tmp_path)

    canonical_urls = {candidate.canonical_url for candidate in candidates}
    assert "https://example.com/tools/design-mcp" in canonical_urls
    assert "http://localhost:3000/dashboard" not in canonical_urls
    merged = next(candidate for candidate in candidates if candidate.canonical_url == "https://example.com/tools/design-mcp")
    assert len(merged.evidence_edges) >= 3


def test_should_exclude_common_www_search_result_pages() -> None:
    assert should_exclude_url("https://www.google.com/search?q=design+mcp").excluded is True
    assert should_exclude_url("https://www.bing.com/search?q=design+mcp").excluded is True


def test_search_signals_rollup_writes_marker_and_is_idempotent(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_me=True)
    chrome_root = _make_chrome_root(tmp_path)
    scan_result = scan_chrome_profiles(chrome_root=chrome_root, selected_profiles=["Research"], since_days=3650)
    search_signals = build_retained_search_signals(scan_result.events)

    drop_path = write_search_signal_drop(tmp_path, search_signals=search_signals, today_str="2026-04-09")
    result = ingest_search_signal_drop_files(tmp_path, today_str="2026-04-09")

    assert result.signals_materialized == len(search_signals)
    month_slug = search_signals[0].searched_at[:7]
    rollup = tmp_path / "memory" / "me" / "search-patterns" / f"{month_slug}.md"
    assert rollup.exists()
    marker = tmp_path / "memory" / "me" / "search-patterns" / f".ingested-{drop_path.name}"
    assert marker.exists()

    rerun = ingest_search_signal_drop_files(tmp_path, today_str="2026-04-09")
    assert rerun.drop_files_processed == 0


def test_web_discovery_drain_signal_only_writes_page_and_marker(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_me=True)
    chrome_root = _make_chrome_root(tmp_path)
    scan_result = scan_chrome_profiles(chrome_root=chrome_root, selected_profiles=["Research"], since_days=3650)
    candidates = build_web_candidates(scan_result.events, repo_root=tmp_path)
    drop_path = write_web_discovery_drop(tmp_path, candidates=candidates, today_str="2026-04-09")

    result = drain_web_discovery_drop_queue(repo_root=tmp_path, today_str="2026-04-09")

    assert result.pages_written >= 1
    pages = list((tmp_path / "memory" / "sources" / "web-discovery").glob("*.md"))
    assert pages
    assert "canonical_url" in pages[0].read_text(encoding="utf-8")
    marker = tmp_path / "memory" / "sources" / "web-discovery" / f".ingested-{drop_path.name}"
    assert marker.exists()


def test_web_discovery_drain_malformed_drop_logs_failure_without_marker(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_me=True)
    drops = tmp_path / "raw" / "drops"
    drops.mkdir(parents=True, exist_ok=True)
    bad_drop = drops / "web-discovery-candidates-from-chrome-2026-04-09.jsonl"
    bad_drop.write_text('{"candidate_id": "oops"}\n', encoding="utf-8")

    result = drain_web_discovery_drop_queue(repo_root=tmp_path, today_str="2026-04-09")

    assert result.failed == 1
    marker = tmp_path / "memory" / "sources" / "web-discovery" / f".ingested-{bad_drop.name}"
    assert not marker.exists()
    failure_log = tmp_path / "memory" / "inbox" / "web-discovery-failures-2026-04-09.md"
    assert failure_log.exists()


def test_web_discovery_drain_is_idempotent_for_existing_evidence(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_me=True)
    chrome_root = _make_chrome_root(tmp_path)
    scan_result = scan_chrome_profiles(chrome_root=chrome_root, selected_profiles=["Research"], since_days=3650)
    candidates = build_web_candidates(scan_result.events, repo_root=tmp_path)
    first_drop = write_web_discovery_drop(tmp_path, candidates=candidates, today_str="2026-04-09")

    first = drain_web_discovery_drop_queue(repo_root=tmp_path, today_str="2026-04-09")
    assert first.pages_written >= 1

    second_drop = write_web_discovery_drop(tmp_path, candidates=candidates, today_str="2026-04-10")
    second = drain_web_discovery_drop_queue(repo_root=tmp_path, today_str="2026-04-10")
    assert second.pages_written >= 1

    pages = list((tmp_path / "memory" / "sources" / "web-discovery").glob("*.md"))
    assert pages
    text = pages[0].read_text(encoding="utf-8")
    assert "evidence_edge_count: 4" in text
    assert "visit_count_total: 2" in text
    assert (tmp_path / "memory" / "sources" / "web-discovery" / f".ingested-{first_drop.name}").exists()
    assert (tmp_path / "memory" / "sources" / "web-discovery" / f".ingested-{second_drop.name}").exists()
