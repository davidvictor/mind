"""Weekly YouTube watch-history puller.

Uses yt-dlp's --cookies-from-browser to read the user's authenticated session
and fetch their watch history. Writes a normalized JSON file to raw/exports/.

Run modes:
  python scripts/youtube/pull.py            # full fetch, write to raw/exports/
  python scripts/youtube/pull.py --dry-run  # fetch but don't write — print count

Limits the fetch to the most recent N videos via --limit (default 200) so a
single run is bounded.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime
import json
import shutil
import subprocess
import sys
from pathlib import Path

from scripts.common import env


WATCH_HISTORY_URL = "https://www.youtube.com/feed/history"
YT_DLP_PULL_TIMEOUT_SECONDS = 180
_AUTH_FAILURE_TOKENS = (
    "sign in to confirm",
    "not a bot",
    "http error 403",
    "cookies",
    "login required",
)


@dataclass(frozen=True)
class FetchResult:
    entries: list[dict]
    exit_code: int
    stderr: str
    timed_out: bool = False


@dataclass(frozen=True)
class PullRunResult:
    exit_code: int
    detail: str
    records: list[dict]
    export_path: Path | None = None


def _yt_dlp_binary() -> str:
    """Resolve yt-dlp from the venv first, then PATH."""
    venv_bin = Path(sys.executable).parent / "yt-dlp"
    if venv_bin.exists():
        return str(venv_bin)
    found = shutil.which("yt-dlp")
    if found:
        return found
    return "yt-dlp"


def _parse_entries(stdout: str) -> list[dict]:
    entries: list[dict] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


def _stderr_summary(stderr: str) -> str:
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    if not lines:
        return ""
    return lines[-1][:240]


def _looks_like_auth_failure(stderr: str) -> bool:
    lowered = stderr.lower()
    return any(token in lowered for token in _AUTH_FAILURE_TOKENS)


def _watch_item_phrase(count: int) -> str:
    suffix = "" if count == 1 else "s"
    return f"{count} watch item{suffix}"


def _coerce_iso8601_timestamp(value: object) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC).isoformat().replace("+00:00", "Z")
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _published_at(entry: dict) -> str:
    for key in ("release_timestamp", "timestamp", "upload_date"):
        published = _coerce_iso8601_timestamp(entry.get(key))
        if published:
            return published
    return ""


def _thumbnail_url(entry: dict) -> str:
    thumbnail = str(entry.get("thumbnail") or "").strip()
    if thumbnail:
        return thumbnail
    thumbnails = entry.get("thumbnails") or []
    if thumbnails and isinstance(thumbnails[-1], dict):
        return str(thumbnails[-1].get("url") or "").strip()
    return ""


def fetch_via_yt_dlp(browser: str, limit: int) -> FetchResult:
    """Invoke yt-dlp to dump full per-video metadata for the watch history feed.

    Drops --flat-playlist so we get channel name, full description, tags, and
    categories (not just video_id and title). Slower (~1s/video) but the
    description is critical for accurate LLM classification.

    Uses --ignore-no-formats-error so the call succeeds even though we never
    actually want media — yt-dlp normally errors on videos with no available
    formats; we just want the metadata.

    Output is JSON-lines (one JSON object per video, NOT a single wrapping
    array), so we parse line by line.
    """
    cmd = [
        _yt_dlp_binary(),
        "--cookies-from-browser", browser,
        "--dump-json",
        "--skip-download",
        "--ignore-no-formats-error",
        "--playlist-end", str(limit),
        WATCH_HISTORY_URL,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=YT_DLP_PULL_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        return FetchResult(
            entries=[],
            exit_code=1,
            stderr=f"yt-dlp timed out after {YT_DLP_PULL_TIMEOUT_SECONDS}s",
            timed_out=True,
        )
    return FetchResult(
        entries=_parse_entries(result.stdout),
        exit_code=int(result.returncode),
        stderr=result.stderr,
    )


def normalize_entries(entries: list[dict]) -> list[dict]:
    """Convert yt-dlp entries to the shape the classifier and ingest expect."""
    normalized: list[dict] = []
    for entry in entries:
        video_id = entry.get("id") or ""
        title = entry.get("title") or ""
        channel = entry.get("uploader") or entry.get("channel") or ""
        duration = entry.get("duration")  # seconds
        description = entry.get("description") or ""
        tags = entry.get("tags") or []
        categories = [str(category).strip() for category in (entry.get("categories") or []) if str(category).strip()]
        category = categories[0] if categories else ""
        channel_url = str(entry.get("uploader_url") or entry.get("channel_url") or "").strip()
        channel_id = str(entry.get("channel_id") or entry.get("uploader_id") or "").strip()
        title_url = str(entry.get("webpage_url") or entry.get("original_url") or "").strip()
        if not video_id or not title:
            continue
        normalized.append({
            "video_id": video_id,
            "title": title,
            "channel": channel,
            "watched_at": "",
            "published_at": _published_at(entry),
            "duration_seconds": duration,
            "description": description,
            "tags": tags,
            "category": category,
            "categories": categories,
            "title_url": title_url or f"https://www.youtube.com/watch?v={video_id}",
            "url": title_url or f"https://www.youtube.com/watch?v={video_id}",
            "channel_url": channel_url,
            "channel_id": channel_id,
            "thumbnail_url": _thumbnail_url(entry),
        })
    return normalized


def run(*, browser: str, raw_root: Path, limit: int, dry_run: bool, today: date | None = None) -> PullRunResult:
    fetch_result = fetch_via_yt_dlp(browser, limit)
    records = normalize_entries(fetch_result.entries)

    if fetch_result.exit_code != 0:
        detail = _stderr_summary(fetch_result.stderr) or "yt-dlp exited non-zero"
        if records:
            detail = f"{detail} ({_watch_item_phrase(len(records))} parsed before failure)"
        return PullRunResult(exit_code=1, detail=detail, records=records)

    if not records and _looks_like_auth_failure(fetch_result.stderr):
        detail = _stderr_summary(fetch_result.stderr) or "YouTube auth failed while reading watch history"
        return PullRunResult(exit_code=1, detail=detail, records=records)

    detail = f"found {_watch_item_phrase(len(records))}"
    if dry_run:
        return PullRunResult(exit_code=0, detail=detail, records=records)

    target_dir = raw_root / "exports"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_day = today or date.today()
    target = target_dir / f"youtube-recent-{target_day.isoformat()}.json"
    target.write_text(json.dumps(records, indent=2, ensure_ascii=False))
    return PullRunResult(exit_code=0, detail=str(target), records=records, export_path=target)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args(argv)

    cfg = env.load()
    result = run(
        browser=cfg.browser_for_cookies,
        raw_root=cfg.raw_root,
        limit=args.limit,
        dry_run=bool(args.dry_run),
    )

    if result.exit_code != 0:
        print(result.detail)
        return result.exit_code

    if args.dry_run:
        print(f"dry-run: {len(result.records)} videos in your watch history")
        return 0

    print(f"wrote {len(result.records)} records to {result.export_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
