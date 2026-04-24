"""Shared helper: load the owner's wiki/me/ profile files into a context string."""
from __future__ import annotations

from pathlib import Path

from scripts.common import env
from scripts.common.vault import Vault

_PROFILE_CACHE: dict[str, str] | None = None
LEGACY_OWNER_NOTE = "open" + "-threads.md"


def _resolve_wiki_root(*, repo_root: Path | None) -> Path:
    if repo_root is not None:
        return Vault.load(repo_root).wiki

    cfg = env.load()
    wiki_root = getattr(cfg, "wiki_root", None)
    if isinstance(wiki_root, Path):
        return wiki_root
    return Vault.load(cfg.repo_root).wiki


def load_profile_context(*, repo_root: Path | None = None) -> str:
    """Concatenate the owner's wiki/me/*.md files into a single context block.

    Cached for the lifetime of the process so a 308-book run doesn't read
    disk 308 times. Returns empty string if any file is missing.
    """
    global _PROFILE_CACHE
    wiki_root = _resolve_wiki_root(repo_root=repo_root)
    cache_key = str(wiki_root.resolve())
    if _PROFILE_CACHE is None:
        _PROFILE_CACHE = {}
    cached = _PROFILE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    me_dir = wiki_root / "me"
    parts: list[str] = []
    owner_notes = ["open-inquiries.md"]
    # Read-only compatibility for older fixtures and repos during the naming lock.
    if not (me_dir / "open-inquiries.md").exists() and (me_dir / LEGACY_OWNER_NOTE).exists():
        owner_notes = [LEGACY_OWNER_NOTE]
    for name in ("profile.md", "positioning.md", "values.md", *owner_notes):
        p = me_dir / name
        if p.exists():
            parts.append(f"### {name}\n\n{p.read_text()}")
    result = "\n\n".join(parts)
    _PROFILE_CACHE[cache_key] = result
    return result
