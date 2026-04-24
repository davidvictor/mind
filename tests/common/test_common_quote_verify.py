"""Tests for scripts.common.quote_verify — source-kind-agnostic quote verifier.

The verifier walks summary['key_claims'] and flags any evidence_quote that
isn't found verbatim (case-insensitive, whitespace-normalized) in the source
body. When at least one claim fails, a sidecar JSON file is written.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.common.quote_verify import verify_quotes


_BODY = (
    "The cursed kingdom was ruled by a bear named Olek. He had a fondness "
    "for honeyed bread. \"All things in moderation,\" he often said, "
    "\"except moderation itself.\""
)
_NOW = "2026-04-08T12:00:00Z"


def test_all_verified(tmp_path):
    summary = {
        "key_claims": [
            {"claim": "Olek liked bread", "evidence_quote": "He had a fondness for honeyed bread"},
            {"claim": "Olek was paradoxical",
             "evidence_quote": '"All things in moderation," he often said'},
        ],
    }
    result = verify_quotes(
        summary=summary,
        body_text=_BODY,
        source_id="test-001",
        source_kind="substack",
        repo_root=tmp_path,
        _now=_NOW,
    )
    assert all(not c.get("quote_unverified") for c in result["key_claims"])
    sidecar = tmp_path / "raw" / "transcripts" / "substack" / "test-001.quote-warnings.json"
    assert not sidecar.exists()


def test_some_unverified_writes_sidecar(tmp_path):
    summary = {
        "key_claims": [
            {"claim": "Olek liked bread", "evidence_quote": "He had a fondness for honeyed bread"},
            {"claim": "Olek had a daughter", "evidence_quote": "his daughter Anya was wise"},
        ],
    }
    result = verify_quotes(
        summary=summary,
        body_text=_BODY,
        source_id="test-002",
        source_kind="substack",
        repo_root=tmp_path,
        _now=_NOW,
    )
    assert result["key_claims"][0].get("quote_unverified") is None
    assert result["key_claims"][1].get("quote_unverified") is True

    sidecar = tmp_path / "raw" / "transcripts" / "substack" / "test-002.quote-warnings.json"
    assert sidecar.exists()
    data = json.loads(sidecar.read_text())
    assert data["source_id"] == "test-002"
    assert data["source_kind"] == "substack"
    assert data["verified_at"] == _NOW
    assert len(data["unverified_claims"]) == 1
    assert data["unverified_claims"][0]["index"] == 1


def test_empty_evidence_quote_marked_unverified(tmp_path):
    summary = {
        "key_claims": [
            {"claim": "Something", "evidence_quote": ""},
        ],
    }
    result = verify_quotes(
        summary=summary,
        body_text=_BODY,
        source_id="test-003",
        source_kind="article",
        repo_root=tmp_path,
        _now=_NOW,
    )
    assert result["key_claims"][0]["quote_unverified"] is True


def test_no_key_claims_returns_summary_unchanged(tmp_path):
    summary = {"tldr": "test"}
    result = verify_quotes(
        summary=summary,
        body_text=_BODY,
        source_id="test-004",
        source_kind="youtube",
        repo_root=tmp_path,
        _now=_NOW,
    )
    assert result == {"tldr": "test"}


def test_case_insensitive_and_whitespace_normalized(tmp_path):
    summary = {
        "key_claims": [
            {"claim": "Bread", "evidence_quote": "HE HAD A   FONDNESS\nFOR\thoneyed bread"},
        ],
    }
    result = verify_quotes(
        summary=summary,
        body_text=_BODY,
        source_id="test-005",
        source_kind="substack",
        repo_root=tmp_path,
        _now=_NOW,
    )
    assert result["key_claims"][0].get("quote_unverified") is None


def test_sidecar_written_under_correct_source_kind(tmp_path):
    """Verifier writes the sidecar under raw/transcripts/<source_kind>/<source_id>.quote-warnings.json."""
    summary = {
        "key_claims": [{"claim": "x", "evidence_quote": "nonexistent"}],
    }
    verify_quotes(
        summary=summary,
        body_text=_BODY,
        source_id="vid-abc",
        source_kind="youtube",
        repo_root=tmp_path,
        _now=_NOW,
    )
    sidecar = tmp_path / "raw" / "transcripts" / "youtube" / "vid-abc.quote-warnings.json"
    assert sidecar.exists()


def test_returns_same_object_mutated_in_place(tmp_path):
    summary = {"key_claims": [{"claim": "x", "evidence_quote": "nope"}]}
    result = verify_quotes(
        summary=summary,
        body_text=_BODY,
        source_id="test-006",
        source_kind="substack",
        repo_root=tmp_path,
        _now=_NOW,
    )
    assert result is summary
    assert summary["key_claims"][0]["quote_unverified"] is True
