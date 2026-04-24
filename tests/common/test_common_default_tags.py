"""Tests for scripts.common.default_tags — controlled tag axes per page type."""
from __future__ import annotations

import pytest

from scripts.common.default_tags import default_tags, validate_axes


def test_article_source_page_has_three_axes():
    tags = default_tags("article")
    domains = [t for t in tags if t.startswith("domain/")]
    functions = [t for t in tags if t.startswith("function/")]
    signals = [t for t in tags if t.startswith("signal/")]
    assert len(domains) >= 1
    assert len(functions) >= 1
    assert len(signals) >= 1


@pytest.mark.parametrize("page_type", [
    "article", "summary", "person", "company", "channel",
    "concept", "playbook", "stance", "inquiry", "skill",
    "video", "book", "note",
])
def test_default_tags_returns_three_axes_for_every_page_type(page_type):
    tags = default_tags(page_type)
    axes = {t.split("/")[0] for t in tags if "/" in t}
    assert "domain" in axes, f"{page_type} missing domain axis: {tags}"
    assert "function" in axes, f"{page_type} missing function axis: {tags}"
    assert "signal" in axes, f"{page_type} missing signal axis: {tags}"


def test_default_tags_returns_a_list_not_a_set():
    """Order matters for git-friendly diffs — frontmatter tags should
    serialize in deterministic order."""
    result = default_tags("article")
    assert isinstance(result, list)
    # Two calls should return the same order
    assert default_tags("article") == default_tags("article")


def test_validate_axes_accepts_valid_tags():
    assert validate_axes(["domain/work", "function/source", "signal/canon"]) == []


def test_validate_axes_reports_missing_axes():
    errors = validate_axes(["domain/work"])
    assert any("function" in e for e in errors)
    assert any("signal" in e for e in errors)


def test_validate_axes_rejects_unknown_axis_value():
    """If the axis value isn't in the controlled vocabulary, validate
    returns a warning. (Not an error — Topic axis is open vocabulary,
    but Domain/Function/Signal are controlled.)"""
    errors = validate_axes(["domain/madeupthing", "function/source", "signal/canon"])
    assert any("madeupthing" in e for e in errors)


def test_validate_axes_accepts_legacy_thread_function_tag():
    assert validate_axes(["domain/meta", "function/thread", "signal/working"]) == []
