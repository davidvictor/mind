"""Controlled tag axes derived from the machine-readable contract."""
from __future__ import annotations

import re
from typing import Final

from scripts.common.contract import canonicalize_page_type, default_tag_triples, tag_taxonomy


_TAXONOMY = tag_taxonomy()
DOMAIN_VALUES: Final[set[str]] = set(_TAXONOMY.get("domain") or [])
FUNCTION_VALUES: Final[set[str]] = set(_TAXONOMY.get("function") or [])
SIGNAL_VALUES: Final[set[str]] = set(_TAXONOMY.get("signal") or [])
LEGACY_FUNCTION_ALIASES: Final[dict[str, str]] = dict(
    ((_TAXONOMY.get("legacy_aliases") or {}).get("function")) or {}
)
TOPIC_TAG_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_RESERVED_NON_TOPIC_TAGS: Final[set[str]] = {
    "article",
    "book",
    "books-failures",
    "business",
    "channel",
    "compounding",
    "contradiction",
    "fiction",
    "generated",
    "personal",
    "publication",
    "publisher",
    "search-patterns",
    "substack",
    "web",
    "writer",
    "youtube",
    "youtube-no-captions",
}
_LEGACY_FLAT_TAGS: Final[set[str]] = DOMAIN_VALUES | FUNCTION_VALUES | SIGNAL_VALUES | {
    "active",
    "archived",
    "reference",
    "template",
    "inbox",
    "connector",
    "north-star",
    "urgent",
    "important",
    "compounding",
}


# Per-page-type defaults. Each entry is the (domain, function, signal)
# triple for that page type's baseline tags.
_DEFAULTS: Final[dict[str, tuple[str, str, str]]] = {
    key: tuple(value)  # type: ignore[arg-type]
    for key, value in default_tag_triples().items()
}


def default_tags(page_type: str) -> list[str]:
    """Return the baseline three-axis tag list for a page type.

    Args:
        page_type: One of the schema-v2 page types.

    Returns:
        A list like ['domain/learning', 'function/source', 'signal/canon'].
        Order is deterministic (domain, function, signal) for git-friendly diffs.

    Raises:
        KeyError: if page_type is not in the controlled set.
    """
    page_type = canonicalize_page_type(page_type)
    if page_type not in _DEFAULTS:
        raise KeyError(
            f"Unknown page type {page_type!r}. Add an entry to "
            f"scripts/common/default_tags.py::_DEFAULTS."
        )
    domain, function, signal = _DEFAULTS[page_type]
    return [f"domain/{domain}", f"function/{function}", f"signal/{signal}"]


def default_domains(page_type: str) -> list[str]:
    """Return the canonical default domain list for a page type."""

    page_type = canonicalize_page_type(page_type)
    if page_type not in _DEFAULTS:
        raise KeyError(
            f"Unknown page type {page_type!r}. Add an entry to "
            f"scripts/common/default_tags.py::_DEFAULTS."
        )
    domain, _function, _signal = _DEFAULTS[page_type]
    return [domain]


def is_valid_topic_tag(tag: str) -> bool:
    """Return True when a tag is an allowed open-vocabulary topic tag."""

    cleaned = str(tag).strip().lower()
    if not cleaned:
        return False
    if cleaned.startswith(("domain/", "function/", "signal/")):
        return False
    if cleaned in _RESERVED_NON_TOPIC_TAGS:
        return False
    if cleaned in _LEGACY_FLAT_TAGS:
        return False
    return bool(TOPIC_TAG_RE.fullmatch(cleaned))


def normalize_topic_tags(tags: list[str] | tuple[str, ...] | None) -> list[str]:
    """Return deterministic, de-duplicated topic tags."""

    ordered: list[str] = []
    seen: set[str] = set()
    for tag in tags or []:
        cleaned = str(tag).strip().lower()
        if not is_valid_topic_tag(cleaned) or cleaned in seen:
            continue
        ordered.append(cleaned)
        seen.add(cleaned)
    return ordered


def validate_topic_tags(tags: list[str]) -> list[str]:
    """Validate optional topic tags in a complete tag list."""

    errors: list[str] = []
    for tag in tags:
        cleaned = str(tag).strip().lower()
        if cleaned.startswith(("domain/", "function/", "signal/")):
            continue
        if not is_valid_topic_tag(cleaned):
            errors.append(
                f"invalid topic tag {tag!r} (must be lowercase-hyphenated and not reserved metadata)"
            )
    return errors


def validate_axes(tags: list[str]) -> list[str]:
    """Validate that a tag list contains all three required axes with
    values from the controlled vocabularies.

    Returns a list of error strings. Empty list means valid.
    """
    errors: list[str] = []
    domain_tags = [t for t in tags if t.startswith("domain/")]
    function_tags = [t for t in tags if t.startswith("function/")]
    signal_tags = [t for t in tags if t.startswith("signal/")]

    if not domain_tags:
        errors.append("missing required axis: domain/<value>")
    else:
        for t in domain_tags:
            value = t.split("/", 1)[1]
            if value not in DOMAIN_VALUES:
                errors.append(f"unknown domain value {value!r} (allowed: {sorted(DOMAIN_VALUES)})")

    if not function_tags:
        errors.append("missing required axis: function/<value>")
    else:
        for t in function_tags:
            value = t.split("/", 1)[1]
            value = LEGACY_FUNCTION_ALIASES.get(value, value)
            if value not in FUNCTION_VALUES:
                errors.append(f"unknown function value {value!r} (allowed: {sorted(FUNCTION_VALUES)})")

    if not signal_tags:
        errors.append("missing required axis: signal/<value>")
    else:
        for t in signal_tags:
            value = t.split("/", 1)[1]
            if value not in SIGNAL_VALUES:
                errors.append(f"unknown signal value {value!r} (allowed: {sorted(SIGNAL_VALUES)})")

    return errors
