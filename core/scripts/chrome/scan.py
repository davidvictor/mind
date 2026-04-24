from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile

from scripts.chrome.contracts import ChromeEvent, ChromeProfile, chrome_timestamp_to_iso, stable_hash
from scripts.common.config import BrainConfig
from scripts.common.vault import raw_path


DEFAULT_CHROME_ROOT = Path.home() / "Library" / "Application Support" / "Google" / "Chrome"


@dataclass(frozen=True)
class ChromeScanResult:
    profiles: list[ChromeProfile] = field(default_factory=list)
    events: list[ChromeEvent] = field(default_factory=list)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _should_keep_iso(occurred_at: str, *, since_days: int | None) -> bool:
    if since_days is None or not occurred_at:
        return True
    try:
        parsed = datetime.strptime(occurred_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return parsed >= (_utc_now() - timedelta(days=since_days))


def _load_local_state(chrome_root: Path) -> dict[str, object]:
    path = chrome_root / "Local State"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def discover_profiles(
    *,
    chrome_root: Path | None = None,
    selected_profiles: list[str] | None = None,
) -> list[ChromeProfile]:
    root = chrome_root or DEFAULT_CHROME_ROOT
    local_state = _load_local_state(root)
    info_cache = ((local_state.get("profile") or {}).get("info_cache") or {})
    selected = {item.strip() for item in (selected_profiles or []) if item.strip()}
    profiles: list[ChromeProfile] = []
    for dir_name, payload in info_cache.items():
        path = root / dir_name
        if not path.exists():
            continue
        profile_name = str((payload or {}).get("name") or dir_name)
        if selected and dir_name not in selected and profile_name not in selected:
            continue
        profiles.append(
            ChromeProfile(
                profile_dir_name=dir_name,
                profile_name=profile_name,
                user_name=str((payload or {}).get("user_name") or ""),
                root=path,
            )
        )
    if not profiles and (root / "Default").exists() and not selected:
        profiles.append(ChromeProfile(profile_dir_name="Default", profile_name="Default", root=root / "Default"))
    return profiles


def _walk_bookmark_tree(node: dict[str, object], *, profile_name: str, folder: str = "") -> list[ChromeEvent]:
    events: list[ChromeEvent] = []
    name = str(node.get("name") or "").strip()
    next_folder = folder
    if name and node.get("type") != "url":
        next_folder = f"{folder}/{name}".strip("/")
    if node.get("type") == "url" and node.get("url"):
        url = str(node.get("url") or "")
        occurred_at = chrome_timestamp_to_iso(node.get("date_added"))
        events.append(
            ChromeEvent(
                event_id=stable_hash(profile_name, "bookmark", str(node.get("id") or occurred_at or url), url),
                event_type="bookmark",
                chrome_profile=profile_name,
                occurred_at=occurred_at,
                url=url,
                title=str(node.get("name") or ""),
                bookmark_folder_path=next_folder,
                native_ref={"bookmark_id": str(node.get("id") or "")},
            )
        )
    for child in list(node.get("children") or []):
        if isinstance(child, dict):
            events.extend(_walk_bookmark_tree(child, profile_name=profile_name, folder=next_folder))
    return events


def _read_bookmark_events(profile: ChromeProfile) -> list[ChromeEvent]:
    path = profile.root / "Bookmarks"
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    roots = payload.get("roots") or {}
    events: list[ChromeEvent] = []
    for key in ("bookmark_bar", "other", "synced"):
        node = roots.get(key)
        if isinstance(node, dict):
            events.extend(_walk_bookmark_tree(node, profile_name=profile.profile_name))
    return events


def _snapshot_history_db(path: Path) -> Path:
    tmpdir = Path(tempfile.mkdtemp(prefix="brain-chrome-history-"))
    target = tmpdir / path.name
    shutil.copy2(path, target)
    for suffix in ("-wal", "-shm"):
        sidecar = path.with_name(path.name + suffix)
        if sidecar.exists():
            shutil.copy2(sidecar, tmpdir / sidecar.name)
    return target


def _read_history_events(profile: ChromeProfile) -> list[ChromeEvent]:
    path = profile.root / "History"
    if not path.exists():
        return []
    snapshot = _snapshot_history_db(path)
    try:
        conn = sqlite3.connect(snapshot)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                visits.id AS visit_id,
                urls.url AS url,
                urls.title AS title,
                urls.visit_count AS visit_count,
                urls.typed_count AS typed_count,
                visits.visit_time AS visit_time
            FROM visits
            JOIN urls ON urls.id = visits.url
            ORDER BY visits.visit_time ASC
            """
        ).fetchall()
        events: list[ChromeEvent] = []
        for row in rows:
            occurred_at = chrome_timestamp_to_iso(row["visit_time"])
            url = str(row["url"] or "")
            events.append(
                ChromeEvent(
                    event_id=stable_hash(profile.profile_name, "history_visit", str(row["visit_id"]), url),
                    event_type="history_visit",
                    chrome_profile=profile.profile_name,
                    occurred_at=occurred_at,
                    url=url,
                    title=str(row["title"] or ""),
                    visit_count_hint=int(row["visit_count"] or 0),
                    typed_count_hint=int(row["typed_count"] or 0),
                    native_ref={"visit_id": int(row["visit_id"])},
                )
            )

        query_rows = conn.execute(
            """
            SELECT
                kst.term AS term,
                urls.url AS url,
                urls.last_visit_time AS last_visit_time,
                urls.title AS title
            FROM keyword_search_terms AS kst
            JOIN urls ON urls.id = kst.url_id
            ORDER BY urls.last_visit_time ASC
            """
        ).fetchall()
        for idx, row in enumerate(query_rows):
            occurred_at = chrome_timestamp_to_iso(row["last_visit_time"])
            query_text = str(row["term"] or "")
            url = str(row["url"] or "")
            title = str(row["title"] or "")
            engine_domain = ""
            if url:
                from urllib.parse import urlparse

                engine_domain = (urlparse(url).hostname or "").lower()
            query_event_id = stable_hash(profile.profile_name, "search_query", occurred_at or str(idx), query_text, engine_domain)
            events.append(
                ChromeEvent(
                    event_id=query_event_id,
                    event_type="search_query",
                    chrome_profile=profile.profile_name,
                    occurred_at=occurred_at,
                    query_text=query_text,
                    engine_domain=engine_domain,
                    native_ref={"query_index": idx},
                )
            )
            if url:
                events.append(
                    ChromeEvent(
                        event_id=stable_hash(profile.profile_name, "query_click", occurred_at or str(idx), query_text, url),
                        event_type="query_click",
                        chrome_profile=profile.profile_name,
                        occurred_at=occurred_at,
                        url=url,
                        title=title,
                        query_text=query_text,
                        engine_domain=engine_domain,
                        native_ref={"query_index": idx},
                    )
                )
        conn.close()
    finally:
        shutil.rmtree(snapshot.parent, ignore_errors=True)
    return events


def scan_chrome_profiles(
    *,
    repo_root: Path | None = None,
    chrome_root: Path | None = None,
    selected_profiles: list[str] | None = None,
    since_days: int | None = None,
) -> ChromeScanResult:
    root = chrome_root
    if root is None and repo_root is not None:
        cfg = BrainConfig.load(repo_root)
        if cfg.chrome.profile_root:
            root = Path(cfg.chrome.profile_root)
        if selected_profiles is None and cfg.chrome.profiles:
            selected_profiles = list(cfg.chrome.profiles)
        if since_days is None:
            since_days = int(cfg.chrome.history_days)
    root = root or DEFAULT_CHROME_ROOT
    profiles = discover_profiles(chrome_root=root, selected_profiles=selected_profiles)
    events: list[ChromeEvent] = []
    for profile in profiles:
        for event in _read_bookmark_events(profile) + _read_history_events(profile):
            if event.event_type != "bookmark" and not _should_keep_iso(event.occurred_at, since_days=since_days):
                continue
            events.append(event)
    return ChromeScanResult(profiles=profiles, events=events)


def _prune_old_query_dirs(repo_root: Path, *, retention_days: int) -> None:
    queries_root = raw_path(repo_root, "chrome", "search-queries")
    if not queries_root.exists():
        return
    cutoff = (_utc_now() - timedelta(days=retention_days)).date()
    for child in queries_root.iterdir():
        if not child.is_dir():
            continue
        try:
            stamp = datetime.strptime(child.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if stamp < cutoff:
            shutil.rmtree(child, ignore_errors=True)


def write_scan_outputs(
    repo_root: Path,
    result: ChromeScanResult,
    *,
    today_str: str,
    raw_query_retention_days: int = 90,
) -> tuple[list[Path], list[Path]]:
    events_dir = raw_path(repo_root, "chrome", "events", today_str)
    queries_dir = raw_path(repo_root, "chrome", "search-queries", today_str)
    events_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)
    event_paths: list[Path] = []
    query_paths: list[Path] = []
    by_profile: dict[str, list[ChromeEvent]] = {}
    query_by_profile: dict[str, list[ChromeEvent]] = {}
    for event in result.events:
        by_profile.setdefault(event.chrome_profile, []).append(event)
        if event.event_type == "search_query":
            query_by_profile.setdefault(event.chrome_profile, []).append(event)
    for profile_name, events in by_profile.items():
        safe_name = profile_name.lower().replace(" ", "-")
        target = events_dir / f"{safe_name}.jsonl"
        target.write_text(
            "".join(json.dumps(item.to_dict(), sort_keys=True) + "\n" for item in events),
            encoding="utf-8",
        )
        event_paths.append(target)
    for profile_name, events in query_by_profile.items():
        safe_name = profile_name.lower().replace(" ", "-")
        target = queries_dir / f"{safe_name}.jsonl"
        target.write_text(
            "".join(json.dumps(item.to_dict(), sort_keys=True) + "\n" for item in events),
            encoding="utf-8",
        )
        query_paths.append(target)
    _prune_old_query_dirs(repo_root, retention_days=raw_query_retention_days)
    return event_paths, query_paths
