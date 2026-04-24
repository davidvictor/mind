from __future__ import annotations

import json
import shutil
from pathlib import Path

from scripts.chrome.contracts import canonicalize_url
from scripts.chrome.scan import discover_profiles, scan_chrome_profiles
from tests.paths import FIXTURES_ROOT


FIXTURE_ROOT = FIXTURES_ROOT / "chrome"


def _make_chrome_root(tmp_path: Path) -> Path:
    chrome_root = tmp_path / "Chrome"
    chrome_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(FIXTURE_ROOT / "local-state-sample.json", chrome_root / "Local State")
    for profile_dir in ("Default", "Profile 1"):
        profile = chrome_root / profile_dir
        profile.mkdir(parents=True, exist_ok=True)
        shutil.copy2(FIXTURE_ROOT / "bookmarks-sample.json", profile / "Bookmarks")
        shutil.copy2(FIXTURE_ROOT / "history-sample.sqlite3", profile / "History")
    return chrome_root


def test_discover_profiles_reads_local_state_and_selection(tmp_path: Path) -> None:
    chrome_root = _make_chrome_root(tmp_path)

    profiles = discover_profiles(chrome_root=chrome_root, selected_profiles=["Research"])

    assert [profile.profile_name for profile in profiles] == ["Research"]


def test_scan_chrome_profiles_extracts_bookmarks_visits_queries_and_clicks(tmp_path: Path) -> None:
    chrome_root = _make_chrome_root(tmp_path)

    result = scan_chrome_profiles(chrome_root=chrome_root, selected_profiles=["Research"], since_days=3650)

    event_types = {event.event_type for event in result.events}
    assert {"bookmark", "history_visit", "search_query", "query_click"} <= event_types
    bookmark = next(event for event in result.events if event.event_type == "bookmark" and event.title == "Design MCP")
    assert bookmark.bookmark_folder_path == "Bookmarks Bar/Tools"
    query = next(event for event in result.events if event.event_type == "search_query")
    assert query.query_text == "design mcp"
    click = next(event for event in result.events if event.event_type == "query_click")
    assert click.url.startswith("https://example.com/tools/design-mcp")


def test_scan_chrome_profiles_snapshot_copy_tolerates_wal_sidecars(tmp_path: Path) -> None:
    chrome_root = _make_chrome_root(tmp_path)
    history = chrome_root / "Profile 1" / "History"
    (chrome_root / "Profile 1" / "History-wal").write_bytes(b"wal")
    (chrome_root / "Profile 1" / "History-shm").write_bytes(b"shm")

    result = scan_chrome_profiles(chrome_root=chrome_root, selected_profiles=["Research"], since_days=3650)

    assert any(event.event_type == "history_visit" for event in result.events)
    assert history.exists()


def test_canonicalize_url_collapses_tracking_and_www_variants() -> None:
    assert canonicalize_url("https://www.example.com/tools/design-mcp?utm_source=x#top") == "https://example.com/tools/design-mcp"
