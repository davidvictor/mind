"""Source-kind-agnostic entity logging to inbox files."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from scripts.common.inbox_log import append_to_inbox_log
from scripts.common.vault import wiki_path

_STATIC_STOPWORDS: frozenset[str] = frozenset({
    "the", "a", "an", "this", "that", "these", "those",
    "i", "me", "my", "we", "us", "our", "you", "your",
    "he", "she", "it", "they", "them", "their",
    "is", "are", "was", "were", "be", "been", "being",
    "and", "or", "but", "if", "then", "so",
    "to", "of", "in", "on", "at", "by", "for", "with", "from",
    "as", "about", "into", "through", "during", "before", "after",
})

_ENTITY_WIKI_DIRS = {
    "people": "people",
    "companies": "companies",
    "tools": "tools",
    "concepts": "concepts",
}
_ENTITY_CATEGORY_ORDER = ("people", "companies", "tools", "concepts")
_CAP = 30


def _extract_context_sentence(body_text: str, entity_name: str) -> str:
    pattern = re.compile(re.escape(entity_name), re.IGNORECASE)
    match = pattern.search(body_text)
    if not match:
        return "(no direct quote in body)"

    start = match.start()
    sentence_start = 0
    for index in range(start - 1, -1, -1):
        if body_text[index] in ".!?":
            sentence_start = index + 1
            break
        if body_text[index:index + 2] == "\n\n":
            sentence_start = index + 2
            break

    sentence_end = len(body_text)
    index = start
    while index < len(body_text):
        if body_text[index] in ".!?":
            sentence_end = index + 1
            break
        if body_text[index:index + 2] == "\n\n":
            sentence_end = index
            break
        index += 1

    return body_text[sentence_start:sentence_end].strip()[:200] or "(no direct quote in body)"


def log_entities(
    *,
    summary: dict[str, Any],
    body_text: str,
    repo_root: Path,
    today: str,
    source_link: str,
    inbox_kind: str,
    stopwords: set[str] | None = None,
) -> list[str]:
    """Log newly discovered entities to an inbox file."""
    entities_block = summary.get("entities")
    if not isinstance(entities_block, dict):
        return []

    effective_stopwords = {word.lower() for word in _STATIC_STOPWORDS}
    if stopwords:
        effective_stopwords.update(word.lower() for word in stopwords if word)

    seen_lower: dict[str, tuple[str, str]] = {}
    for category in _ENTITY_CATEGORY_ORDER:
        for entity in entities_block.get(category) or []:
            if not isinstance(entity, str):
                continue
            text = entity.strip()
            if len(text) < 2:
                continue
            lowered = text.lower()
            if lowered in effective_stopwords:
                continue
            if lowered not in seen_lower:
                seen_lower[lowered] = (text, category)

    surviving = list(seen_lower.values())[:_CAP]
    if not surviving:
        return []

    to_log: list[tuple[str, str]] = []
    for entity_name, category in surviving:
        wiki_dir = _ENTITY_WIKI_DIRS.get(category, category)
        from scripts.common.slugify import slugify

        if wiki_path(repo_root, wiki_dir, f"{slugify(entity_name)}.md").exists():
            continue
        to_log.append((entity_name, category))

    if not to_log:
        return []

    lines = []
    for entity_name, category in to_log:
        context = _extract_context_sentence(body_text, entity_name)
        lines.append(
            f'- **{entity_name}** ({category}) — referenced by [[{source_link}]] — "{context}"'
        )

    append_to_inbox_log(
        target=wiki_path(repo_root, "inbox", f"{inbox_kind}-{today}.md"),
        kind=inbox_kind,
        entry="\n".join(lines) + "\n",
        date=today,
    )
    return [name for name, _ in to_log]
