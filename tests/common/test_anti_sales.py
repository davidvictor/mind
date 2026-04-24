"""Tests for scripts.common.anti_sales — the cross-cutting anti-sales rule.

The rule + heuristic are the single source of truth for dropping sales chrome
from every extraction surface (link classification, entity logging, Pass D
candidates, applied-to-you recommendations).
"""
from __future__ import annotations

import pytest

from scripts.common.anti_sales import ANTI_SALES_RULE_PROMPT, is_sales_chrome


# Each row: (url, anchor_text, surrounding_context, expected_is_sales_chrome, comment)
CASES = [
    ("https://substack.com/subscribe?next=foo", "Subscribe", "", True, "subscribe CTA"),
    ("https://athleticgreens.com/lex", "AG1", "use code LEX20 for 20% off", True, "sponsor with code"),
    ("https://amzn.to/abc123", "buy on Amazon", "", True, "amazon affiliate short link"),
    ("https://www.amazon.com/Becoming-Michelle-Obama/dp/1524763136",
     "Becoming",
     "Obama's memoir, which I found genuinely moving",
     False,
     "book that IS the subject"),
    ("https://arxiv.org/abs/2510.01395", "the paper", "see arxiv:2510.01395", False, "academic citation"),
    ("https://doi.org/10.1234/foo.bar", "DOI", "the original study", False, "DOI citation"),
    ("https://example-author.com/course-waitlist", "join the waitlist", "", True, "course waitlist"),
    ("https://patreon.com/lexfridman", "Patreon", "support the show", True, "patreon support"),
    ("https://twitter.com/balajis", "Balaji on Twitter", "follow Balaji", True, "follow social CTA"),
    ("https://twitter.com/balajis/status/1234567890",
     "this thread",
     "Balaji argued in this thread",
     False,
     "linking to substantive tweet"),
    ("https://github.com/anthropics/claude-code", "claude-code repo", "", False, "github repo reference"),
    ("https://nordvpn.com/lex30", "NordVPN", "use code LEX30", True, "sponsor with code"),
    ("https://abundance360.com/membership", "A360 membership", "apply now", True, "conference upsell"),
    ("https://abundance360.com/livestream", "A360 livestream", "watch the livestream", True, "conference upsell"),
    ("https://www.theguardian.com/technology/2025-dec-18-ai-co2",
     "Guardian piece",
     "as the Guardian reported",
     False,
     "substantive citation"),
]


@pytest.mark.parametrize("url,anchor,context,expected,comment", CASES, ids=[c[4] for c in CASES])
def test_is_sales_chrome(url, anchor, context, expected, comment):
    assert is_sales_chrome(url, anchor, context) is expected, comment


def test_anti_sales_rule_prompt_is_nonempty_string():
    assert isinstance(ANTI_SALES_RULE_PROMPT, str)
    assert len(ANTI_SALES_RULE_PROMPT) > 200, "rule should be substantive"
    # The rule should mention both keep and drop semantics
    assert "drop" in ANTI_SALES_RULE_PROMPT.lower()
    assert "keep" in ANTI_SALES_RULE_PROMPT.lower()


def test_anti_sales_rule_prompt_mentions_unless_clause():
    """The 'unless the product/service IS the subject' clause is critical
    — it's what keeps book reviews from dropping the book."""
    assert "unless" in ANTI_SALES_RULE_PROMPT.lower()
