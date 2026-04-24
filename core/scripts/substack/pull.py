"""Pull the owner's saved Substack posts via the private reader API.

Writes a dated export file to raw/exports/substack-saved-YYYY-MM-DD.json with
all pages concatenated into a single {"posts": [...], "next_cursor": null}
shape. Idempotent at the day level — re-running overwrites that day's export.

Atomic: writes to a tempfile and renames on success, so a mid-pagination
failure leaves no partial file behind.

Endpoint: `/api/v1/posts/saved` returns a payload of the form:
    {
      "posts": [...],            # post objects WITHOUT embedded publication
      "publications": [...],     # publication objects, joined via post.publication_id
      "savedPosts": [...],       # {user_id, post_id, created_at}, drives pagination cursor
      "more": true|false,
    }

Pagination is cursor-based by `before=<savedPosts[-1].created_at>`. Max page
size observed in production is ~30; we use 25 to stay safely under any cap.
The endpoint silently truncates rather than rejecting oversized limits, so
keep this conservative.

Each post in the response has `publication: None` and `publication_id: int`.
The parser (parse.py) and the rest of the pipeline expect an embedded
`post.publication` object (matching the smoketest fixture and the older shape).
We bridge that here by joining `publications[]` on `publication_id` and
injecting `post["publication"]` into each post before writing the export.
We also stamp `post["saved_at"]` from `savedPosts[].created_at` as a fallback,
since the post itself may already carry `saved_at` but we want canonical timing.

Auth expiration detection: if the session cookie expires, Substack may
redirect to an HTML login page. requests follows the redirect (see
scripts/substack/auth.py) so we would otherwise get HTML where we expect
JSON. We detect this by checking the response content-type header before
calling .json(), and raise SubstackAuthError with a clear message.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


BASE_URL = "https://substack.com"
PATH = "/api/v1/posts/saved"
PAGE_SIZE = 25  # observed cap is ~30; stay conservative


class SubstackAuthError(RuntimeError):
    """Raised when Substack returns 401/403 or redirects to a login HTML page."""


def _auth_error_detail(resp: requests.Response) -> str:
    headers = resp.headers
    if hasattr(headers, "get_list"):
        set_cookie_values = headers.get_list("set-cookie")
    else:
        raw = headers.get("set-cookie", "")
        set_cookie_values = [raw] if raw else []
    set_cookie = " ".join(set_cookie_values).lower()
    server = resp.headers.get("server", "").lower()
    if "__cf_bm=" in set_cookie or "cloudflare" in server:
        return (
            "Substack returned a Cloudflare-protected 403. A lone substack.sid may no longer be "
            "sufficient; export the full browser Cookie header for substack.com (including any "
            "Cloudflare/session cookies) into SUBSTACK_SESSION_COOKIE."
        )
    return "Refresh your cookie per README.md."


def _today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _is_json_response(resp: requests.Response) -> bool:
    ct = resp.headers.get("content-type", "")
    return "json" in ct.lower()


def _enrich_posts(body: dict[str, Any]) -> list[dict[str, Any]]:
    """Inject embedded `publication` and canonical `saved_at` into each post.

    Mutates posts in-place to match the historical export shape that parse.py
    and the smoketest fixture both expect.
    """
    posts = list(body.get("posts") or [])
    pubs_by_id: dict[int, dict[str, Any]] = {}
    for pub in body.get("publications") or []:
        pid = pub.get("id")
        if pid is not None:
            pubs_by_id[int(pid)] = pub

    saved_by_post: dict[int, str] = {}
    for sp in body.get("savedPosts") or []:
        pid = sp.get("post_id")
        if pid is not None:
            saved_by_post[int(pid)] = sp.get("created_at") or ""

    for post in posts:
        # Join publication
        if not post.get("publication"):
            pid = post.get("publication_id")
            if pid is not None:
                pub = pubs_by_id.get(int(pid))
                if pub is not None:
                    # Map publication shape: publications[] uses base_url +
                    # author_name + author_id; the parser expects {name, subdomain}.
                    # We pass the original through but also normalize the shape
                    # so parse.py's `pub.get("name")` / `pub.get("subdomain")`
                    # work without modification.
                    base_url = pub.get("base_url") or ""
                    subdomain = ""
                    if base_url.startswith("https://"):
                        host = base_url[len("https://"):].split("/", 1)[0]
                        subdomain = host.split(".", 1)[0]
                    post["publication"] = {
                        **pub,
                        "name": pub.get("name") or "",
                        "subdomain": pub.get("subdomain") or subdomain,
                    }

        # Stamp saved_at from savedPosts if missing on the post itself
        if not post.get("saved_at"):
            sa = saved_by_post.get(int(post.get("id") or 0))
            if sa:
                post["saved_at"] = sa

    return posts


def pull_saved(
    *,
    client: requests.Session,
    out_dir: Path,
    today: str | None = None,
) -> Path:
    """Pull all saved posts, write dated export, return the path.

    Pagination: uses `before=<savedPosts[-1].created_at>` until `more` is false
    or no posts come back. Each page is enriched with embedded publication and
    canonical saved_at fields before being added to the combined export.
    """
    today = today or _today_utc()
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"substack-saved-{today}.json"

    all_posts: list[dict[str, Any]] = []
    seen_ids: set[Any] = set()
    before: str | None = None
    while True:
        params: dict[str, Any] = {"limit": PAGE_SIZE}
        if before:
            params["before"] = before
        try:
            resp = client.get(BASE_URL + PATH, params=params, timeout=30, allow_redirects=True)
            resp.raise_for_status()
        except requests.HTTPError as e:
            if e.response.status_code in (401, 403):
                raise SubstackAuthError(
                    "Substack auth failed (%d). %s"
                    % (e.response.status_code, _auth_error_detail(e.response))
                ) from e
            raise

        if not _is_json_response(resp):
            raise SubstackAuthError(
                "Substack returned HTML where JSON was expected. Your session "
                "cookie likely expired and the request was redirected to a "
                "login page. Refresh your cookie per README.md."
            )

        body = resp.json()
        page_posts = _enrich_posts(body)
        if not page_posts:
            break

        # Dedupe across pages just in case the cursor returns overlap
        new_posts = [p for p in page_posts if p.get("id") not in seen_ids]
        for p in new_posts:
            seen_ids.add(p.get("id"))
        all_posts.extend(new_posts)

        if not body.get("more"):
            break
        # Compute next-page cursor from this response's savedPosts.
        sp = body.get("savedPosts") or []
        if not sp:
            break
        new_before = sp[-1].get("created_at")
        if not new_before or new_before == before:
            # Cursor not advancing — guard against an infinite loop.
            break
        before = new_before

    combined = {"posts": all_posts, "next_cursor": None}

    # Atomic write: tempfile + rename
    fd, tmp_path = tempfile.mkstemp(dir=out_dir, prefix=".substack-saved-", suffix=".json.tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(combined, fh, indent=2, ensure_ascii=False)
        os.replace(tmp_path, target)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
    return target
