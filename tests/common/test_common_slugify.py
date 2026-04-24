"""Tests for scripts.common.slugify — single canonical slugifier."""
from __future__ import annotations

import pytest

from scripts.common.slugify import normalize_identifier, slugify


@pytest.mark.parametrize("input_text,expected", [
    ("Founder vs. Employee", "founder-vs-employee"),
    ("founder vs. employee", "founder-vs-employee"),
    ("FOUNDER VS EMPLOYEE", "founder-vs-employee"),
    ("Founder  vs.  Employee", "founder-vs-employee"),
    ("founder--vs--employee", "founder-vs-employee"),
    ("  spaces around  ", "spaces-around"),
    ("Hello, World!", "hello-world"),
    ("Lütke Eval", "lutke-eval"),
    ("Test (with parens)", "test-with-parens"),
    ("emoji 🚀 inline", "emoji-inline"),
    ("café résumé", "cafe-resume"),
    ("multi/slash/path", "multi-slash-path"),
    ("trailing.period.", "trailing-period"),
    ("", ""),
    ("---", ""),
    ("a", "a"),
])
def test_slugify_canonical(input_text, expected):
    assert slugify(input_text) == expected


def test_slugify_max_length():
    long_input = "a" * 200
    result = slugify(long_input, max_len=80)
    assert len(result) <= 80
    assert result == "a" * 80


def test_slugify_max_length_does_not_split_word_at_dash():
    """When truncation lands inside a multi-character run, prefer trimming
    the trailing dash so we don't get 'foo-bar-' as a result."""
    result = slugify("foo bar baz qux quux corge", max_len=12)
    assert not result.endswith("-")
    assert len(result) <= 12


def test_slugify_idempotent():
    once = slugify("Hello World")
    twice = slugify(once)
    assert once == twice


def test_normalize_identifier_strips_control_chars_and_transliterates() -> None:
    assert normalize_identifier("l\x00ütke_eval methodology") == "lutke-eval-methodology"
