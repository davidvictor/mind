from __future__ import annotations

from pathlib import Path
import re

import yaml

from scripts.common.slugify import slugify
from scripts.common.vault import SYSTEM_SKIP_NAMES, Vault
from scripts.common.wikilinks import NESTED_WIKILINK_RE, WIKILINK_RE


def _normalize_lookup_key(value: str) -> str:
    return slugify(value.strip().replace("_", "-"), max_len=120)


def _valid_target_map(repo_root: Path) -> dict[str, str | None]:
    memory = Vault.load(repo_root).wiki
    normalized: dict[str, set[str]] = {}
    for path in sorted(memory.rglob("*.md")):
        rel = path.relative_to(memory)
        if rel.parts and rel.parts[0] in {"templates", ".archive"}:
            continue
        if path.name in SYSTEM_SKIP_NAMES:
            continue
        page_id = path.stem
        text = path.read_text(encoding="utf-8")
        if text.startswith("---\n"):
            end = text.find("\n---\n", 4)
            if end != -1:
                try:
                    frontmatter = yaml.safe_load(text[4:end]) or {}
                except yaml.YAMLError:
                    frontmatter = {}
                page_id = str(frontmatter.get("id") or path.stem).strip()
        if not page_id:
            continue
        for key in {page_id, path.stem}:
            norm = _normalize_lookup_key(key)
            if not norm:
                continue
            normalized.setdefault(norm, set()).add(page_id)
    return {
        key: next(iter(values)) if len(values) == 1 else None
        for key, values in normalized.items()
    }


def _extract_target_and_rest(inner: str) -> tuple[str, str]:
    for index, char in enumerate(inner):
        if char in {"|", "#"}:
            return inner[:index], inner[index:]
    return inner, ""


def _downgrade(inner: str) -> str:
    target, rest = _extract_target_and_rest(inner)
    if "|" in rest:
        return rest.split("|", 1)[1] or target
    return target


def sanitize_wikilinks(text: str, *, repo_root: Path) -> str:
    """Collapse malformed wikilinks, resolve known targets, and downgrade unknowns."""

    target_map = _valid_target_map(repo_root)
    updated = text
    while True:
        newer, count = NESTED_WIKILINK_RE.subn(r"[[\1]]", updated)
        if count == 0:
            break
        updated = newer

    def _replace(match: re.Match[str]) -> str:
        inner = match.group(1)
        target, rest = _extract_target_and_rest(inner)
        canonical = target_map.get(_normalize_lookup_key(target))
        if canonical is None:
            return _downgrade(inner)
        return f"[[{canonical}{rest}]]"

    return WIKILINK_RE.sub(_replace, updated)
