from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "source",
}

CANONICAL_QUERY_ALLOWLIST: dict[str, set[str]] = {
    "github.com": {"tab"},
    "youtube.com": {"v", "list"},
    "www.youtube.com": {"v", "list"},
}

_CHROME_EPOCH = datetime(1601, 1, 1, tzinfo=timezone.utc)


def stable_hash(*parts: str) -> str:
    payload = "||".join(part.strip() for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def chrome_timestamp_to_iso(value: int | str | None) -> str:
    if value in (None, "", 0, "0"):
        return ""
    try:
        micros = int(value)
    except (TypeError, ValueError):
        return ""
    try:
        dt = _CHROME_EPOCH + timedelta(microseconds=micros)
    except OverflowError:
        return ""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def canonicalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    scheme = (parsed.scheme or "https").lower()
    host = parsed.hostname.lower() if parsed.hostname else ""
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return ""

    port = parsed.port
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    else:
        netloc = host

    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    allowed = CANONICAL_QUERY_ALLOWLIST.get(host, set())
    query_items: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        lowered = key.lower()
        if lowered in TRACKING_QUERY_KEYS or any(lowered.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        if allowed and lowered not in allowed:
            continue
        if not allowed and lowered in {"q", "query", "search", "s"}:
            continue
        query_items.append((key, value))
    query = urlencode(sorted(query_items))
    return urlunparse((scheme, netloc, path, "", query, ""))


def discovery_key_for_url(url: str) -> str:
    canonical = canonicalize_url(url)
    return stable_hash(canonical)


@dataclass(frozen=True)
class ChromeProfile:
    profile_dir_name: str
    profile_name: str
    user_name: str = ""
    root: Path = Path()


@dataclass(frozen=True)
class ChromeEvent:
    event_id: str
    event_type: str
    chrome_profile: str
    occurred_at: str
    url: str = ""
    title: str = ""
    query_text: str = ""
    bookmark_folder_path: str = ""
    visit_count_hint: int = 0
    typed_count_hint: int = 0
    engine_domain: str = ""
    native_ref: dict[str, Any] = field(default_factory=dict)
    privacy_class: str = "public"

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "chrome_profile": self.chrome_profile,
            "occurred_at": self.occurred_at,
            "url": self.url,
            "title": self.title,
            "query_text": self.query_text,
            "bookmark_folder_path": self.bookmark_folder_path,
            "visit_count_hint": self.visit_count_hint,
            "typed_count_hint": self.typed_count_hint,
            "engine_domain": self.engine_domain,
            "native_ref": self.native_ref,
            "privacy_class": self.privacy_class,
        }
