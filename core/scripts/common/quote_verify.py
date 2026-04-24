"""Source-kind-agnostic quote verifier.

Walks summary['key_claims'] and flags any evidence_quote that isn't found
verbatim (case-insensitive, whitespace-normalized) in the source body.
Mutates the summary dict in place. Writes a sidecar JSON file when at
least one claim fails.

Originally extracted from scripts/substack/enrich.py::verify_quotes — the
substack version takes a SubstackRecord, this version takes source_id and
source_kind so it can serve substack, articles, youtube, and books.

The substack-flavored caller in scripts/substack/enrich.py is now a thin
wrapper that calls into this module so existing tests stay green.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.common.vault import raw_path


def _normalize_for_quote_match(text: str) -> str:
    """Collapse whitespace to single spaces and lowercase for quote matching."""
    return " ".join(text.lower().split())


def _quote_warnings_path(repo_root: Path, source_kind: str, source_id: str) -> Path:
    return raw_path(repo_root, "transcripts", source_kind, f"{source_id}.quote-warnings.json")


def verify_quotes(
    *,
    summary: dict[str, Any],
    body_text: str,
    source_id: str,
    source_kind: str,
    repo_root: Path,
    _now: str | None = None,
) -> dict[str, Any]:
    """Verify each key_claim's evidence_quote against body_text.

    Unmatched claims are mutated in place with ``quote_unverified: True``.
    When at least one claim fails verification, a sidecar JSON file is
    written to ``raw/transcripts/<source_kind>/<source_id>.quote-warnings.json``.

    Returns the (possibly mutated) summary dict (the same object).

    Args:
        summary: The structured summary dict (mutated in place).
        body_text: The full text body of the source.
        source_id: The canonical id of the source (e.g. substack post id, video id, book slug).
        source_kind: One of 'substack', 'article', 'youtube', 'book'.
        repo_root: Repo root path (used to resolve sidecar path).
        _now: Optional ISO-8601 UTC timestamp string for deterministic testing.
              When None, the real current time is used.

    Returns:
        The same summary object passed in (mutated in place).
    """
    key_claims = summary.get("key_claims")
    if not key_claims:
        return summary

    normalized_body = _normalize_for_quote_match(body_text)
    unverified_claims: list[dict[str, Any]] = []

    for i, claim in enumerate(key_claims):
        if not isinstance(claim, dict):
            continue
        evidence_quote = claim.get("evidence_quote", "")
        if not evidence_quote:
            claim["quote_unverified"] = True
            unverified_claims.append({
                "index": i,
                "claim": claim.get("claim", ""),
                "evidence_quote": evidence_quote,
            })
            continue

        normalized_quote = _normalize_for_quote_match(evidence_quote)
        if normalized_quote not in normalized_body:
            claim["quote_unverified"] = True
            unverified_claims.append({
                "index": i,
                "claim": claim.get("claim", ""),
                "evidence_quote": evidence_quote,
            })

    if unverified_claims:
        verified_at = _now if _now is not None else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sidecar = {
            "source_id": source_id,
            "source_kind": source_kind,
            "verified_at": verified_at,
            "unverified_claims": unverified_claims,
        }
        target = _quote_warnings_path(repo_root, source_kind, source_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(sidecar, indent=2, ensure_ascii=False), encoding="utf-8")

    return summary
