"""Cross-cutting anti-sales rule.

This module is the single source of truth for the rule that drops sales
chrome (sponsor reads, subscribe CTAs, affiliate links, course signups)
from the brain. It's imported by every Pass A through D prompt template
via the %%ANTI_SALES%% substitution and by the link classification
helpers via direct call.

Historical design notes are kept outside the public release tree.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse


ANTI_SALES_RULE_PROMPT = """\
ANTI-SALES RULE (applies to URL extraction, entity logging, and atom candidates):

Drop any URL, entity, or claim whose primary purpose is to convert the reader/viewer
into a paying customer of the source's author, publisher, sponsor, or affiliate,
UNLESS the product/service is itself the substantive subject of the source.

Drop these:
- "Subscribe to my Substack" / "Upgrade to paid" CTAs
- Sponsor reads (Athletic Greens, Squarespace, Manscaped, NordVPN, etc.) inside videos/podcasts
- "Buy my book" links by the source's author when the source is not about that book
- Affiliate Amazon links / discount codes
- "Apply to my course / cohort / waitlist" CTAs
- "Follow me on Twitter / Instagram / TikTok" links
- "Join my Patreon / Substack / OnlyFans"
- Newsletter signup widgets
- Conference registration / membership upsells

Keep these:
- Citations to academic papers, DOIs, arXiv links, primary sources
- Substantive references in show notes / references / bibliography sections
- Links to other essays, videos, books being discussed as the topic
- Government/NGO data sources, dataset URLs, GitHub repositories
- The author's OWN prior work when it's being discussed substantively (not promoted)
- Sponsor brands when the video/article IS about that brand

When in doubt, drop. The brain prefers a clean knowledge base over a comprehensive one.
"""


# Domains that are almost always sales chrome regardless of context.
_SALES_DOMAINS = frozenset({
    "patreon.com",
    "www.patreon.com",
    "amzn.to",
    "athleticgreens.com",
    "www.athleticgreens.com",
    "drinkag1.com",
    "www.drinkag1.com",
    "nordvpn.com",
    "www.nordvpn.com",
    "expressvpn.com",
    "www.expressvpn.com",
    "squarespace.com",
    "www.squarespace.com",
    "manscaped.com",
    "www.manscaped.com",
    "betterhelp.com",
    "www.betterhelp.com",
    "magicspoon.com",
    "www.magicspoon.com",
})

# URL path patterns that indicate sales intent.
_SALES_PATH_PATTERNS = (
    re.compile(r"/subscribe(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/membership(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/livestream(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/waitlist(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/course(?:s)?(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/upgrade(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/checkout(?:[/?]|$)", re.IGNORECASE),
    re.compile(r"/buy(?:[/?]|$)", re.IGNORECASE),
)

# Anchor text fragments that signal sales chrome (case-insensitive substring match).
_SALES_ANCHOR_FRAGMENTS = (
    "subscribe",
    "join the waitlist",
    "buy on amazon",
    "patreon",
    "follow",
    "support the show",
    "use code",
    "ag1",
    "apply now",
    "membership",
    "watch the livestream",
)

# Anchor text fragments that override sales detection — these signal the URL
# is being cited substantively even if it lives at a sales-y location.
_SUBSTANTIVE_OVERRIDE_FRAGMENTS = (
    "study",
    "paper",
    "the original",
    "doi",
    "arxiv",
    "github",
    "repository",
    "the article",
    "the report",
    "this thread",  # twitter status URLs cited as content
    "as reported",
)


def is_sales_chrome(url: str, anchor_text: str, surrounding_context: str) -> bool:
    """Cheap heuristic check used by link classifiers as a pre-filter before
    asking Gemini. Returns True if the URL is *obviously* sales chrome and
    can be dropped without an LLM call.

    The Gemini-based classification (in scripts.<ingestor>.enrich) handles
    the harder cases. This function is a fast path for the obvious ones.

    Substantive overrides: if the anchor text or context indicates the URL
    is being cited as evidence (an academic paper, a github repo, a quoted
    tweet, a news article), the URL is kept regardless of where it lives.

    Args:
        url: Full URL string.
        anchor_text: The visible link text.
        surrounding_context: Surrounding sentence(s) from the source body.

    Returns:
        True if the URL is sales chrome and should be dropped.
    """
    anchor_lc = (anchor_text or "").lower()
    context_lc = (surrounding_context or "").lower()
    combined_lc = f"{anchor_lc} {context_lc}"

    # Substantive overrides win regardless of where the URL lives.
    for override in _SUBSTANTIVE_OVERRIDE_FRAGMENTS:
        if override in combined_lc:
            return False

    # Domain-level: known sales domains drop unconditionally.
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in _SALES_DOMAINS:
        return True

    # Path-level: subscribe/membership/etc. paths drop.
    path = parsed.path or ""
    for pattern in _SALES_PATH_PATTERNS:
        if pattern.search(path):
            return True

    # Anchor-level: salesy anchor fragments drop.
    for fragment in _SALES_ANCHOR_FRAGMENTS:
        if fragment in combined_lc:
            return True

    return False
