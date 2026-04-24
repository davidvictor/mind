"""Substack session cookie authentication for the reader API.

Substack has no public API. We authenticate by replaying the session cookie
from a logged-in browser. The cookie is loaded from the SUBSTACK_SESSION_COOKIE
env var via scripts.common.env.

See README.md for how to export the cookie.
"""
from __future__ import annotations

import requests

from scripts.common import env


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _normalize_cookie_header(raw_value: str) -> str:
    value = (raw_value or "").strip().strip('"').strip("'")
    if not value:
        return ""
    if value.lower().startswith("cookie:"):
        value = value.split(":", 1)[1].strip()
    if "substack.sid=" in value.lower():
        return value
    if "=" in value and ";" in value:
        return value
    if "=" in value and ";" not in value:
        return value
    return f"substack.sid={value}"


def build_client() -> requests.Session:
    """Build a requests.Session with Substack session auth headers baked in.

    Raises RuntimeError if SUBSTACK_SESSION_COOKIE is missing.
    """
    cfg = env.load()
    if not cfg.substack_session_cookie:
        raise RuntimeError(
            "SUBSTACK_SESSION_COOKIE is missing. Follow README.md to export "
            "your session cookie into .env."
        )
    cookie_header = _normalize_cookie_header(cfg.substack_session_cookie)
    headers = {
        "cookie": cookie_header,
        "user-agent": USER_AGENT,
        "accept": "application/json, text/plain, */*",
    }
    session = requests.Session()
    session.headers.update(headers)
    return session
