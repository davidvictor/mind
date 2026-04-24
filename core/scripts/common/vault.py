"""Vault — the typed handle for Brain's private memory, raw inputs, and state.

The public repository contains code and synthetic fixtures. Real memory/raw
roots are resolved from config/env and may live under ignored `local_data/`,
legacy root directories, or an external private path.
"""
from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from scripts.common.config import (
    BRAIN_DROPBOX_ROOT_ENV,
    BRAIN_LOCAL_DATA_ROOT_ENV,
    BRAIN_MEMORY_ROOT_ENV,
    BRAIN_RAW_ROOT_ENV,
    BRAIN_STATE_ROOT_ENV,
    BrainConfig,
)

logger = logging.getLogger(__name__)
LEGACY_OWNER_NOTE = "open" + "-threads.md"
SYSTEM_SKIP_NAMES: set[str] = {"INDEX.md", "CHANGELOG.md", ".brain-state.json", ".brain-lock"}
WIKI_LOGICAL_PREFIXES: set[str] = {
    "companies",
    "concepts",
    "dreams",
    "events",
    "goals",
    "inbox",
    "inquiries",
    "meetings",
    "me",
    "notes",
    "people",
    "places",
    "playbooks",
    "podcasts",
    "projects",
    "quotes",
    "skills",
    "sources",
    "stances",
    "summaries",
    "templates",
    "tools",
}


@dataclass(frozen=True)
class Vault:
    root: Path
    config: BrainConfig

    def _resolve_path(self, value: str | Path) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.root / path

    @property
    def local_data_root(self) -> Path:
        override = os.environ.get(BRAIN_LOCAL_DATA_ROOT_ENV, "").strip()
        value = override or self.config.paths.local_data_root
        return self._resolve_path(value)

    @property
    def wiki(self) -> Path:
        override = os.environ.get(BRAIN_MEMORY_ROOT_ENV, "").strip()
        value = override or self.config.paths.memory_root or self.config.vault.wiki_dir
        return self._resolve_path(value)

    @property
    def raw(self) -> Path:
        override = os.environ.get(BRAIN_RAW_ROOT_ENV, "").strip()
        value = override or self.config.paths.raw_root or self.config.vault.raw_dir
        return self._resolve_path(value)

    @property
    def dropbox(self) -> Path:
        override = os.environ.get(BRAIN_DROPBOX_ROOT_ENV, "").strip()
        value = override or self.config.paths.dropbox_root or self.config.vault.dropbox_dir
        if value:
            return self._resolve_path(value)
        if self.raw.parent == self.root:
            return self.root / "dropbox"
        return self.local_data_root / "dropbox"

    @property
    def state_root(self) -> Path:
        override = os.environ.get(BRAIN_STATE_ROOT_ENV, "").strip()
        value = override or self.config.paths.state_root or self.config.vault.state_dir
        return self._resolve_path(value or self.root)

    @property
    def reports_root(self) -> Path:
        """Private root for operator reports and rebuild manifests."""

        if self.state_root.resolve() == self.root.resolve():
            return self.raw / "reports"
        return self.state_root / "reports"

    @property
    def memory_root(self) -> Path:
        """Common parent for wiki/ and raw/ when they live together."""
        if self.wiki.parent == self.raw.parent:
            return self.wiki.parent
        return self.root

    def logical_path(self, path: str | Path) -> str:
        """Return a stable public path for a private filesystem path.

        Public artifacts should refer to configured private roots as
        `memory/...`, `raw/...`, `dropbox/...`, or `state/...` instead of
        leaking absolute local paths or assuming the roots live under the repo.
        """

        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = self.root / candidate
        candidate = candidate.resolve()

        logical_roots = [
            ("memory", self.wiki),
            ("raw", self.raw),
            ("dropbox", self.dropbox),
        ]
        if self.state_root.resolve() != self.root.resolve():
            logical_roots.append(("state", self.state_root))

        for prefix, root in logical_roots:
            try:
                relative = candidate.relative_to(root.resolve())
            except ValueError:
                continue
            if relative.as_posix() == ".":
                return prefix
            return Path(prefix).joinpath(relative).as_posix()

        try:
            return candidate.relative_to(self.root.resolve()).as_posix()
        except ValueError:
            return candidate.as_posix()

    def resolve_logical_path(self, path: str | Path) -> Path:
        """Resolve a public logical path back into the configured private roots."""

        raw = Path(path)
        if raw.is_absolute():
            return raw
        parts = raw.parts
        if not parts:
            return self.root

        roots = {
            "memory": self.wiki,
            "wiki": self.wiki,
            "raw": self.raw,
            "dropbox": self.dropbox,
            "state": self.state_root,
        }
        root = roots.get(parts[0])
        if root is not None:
            return root.joinpath(*parts[1:])
        if parts[0] in WIKI_LOGICAL_PREFIXES:
            return self.wiki / raw
        return self.root / raw

    @property
    def owner_profile(self) -> Path:
        return self.wiki / "me" / Path(self.config.vault.owner_profile).name

    @property
    def values_path(self) -> Path:
        return self.wiki / "me" / "values.md"

    @property
    def positioning_path(self) -> Path:
        return self.wiki / "me" / "positioning.md"

    @property
    def timeline_path(self) -> Path:
        return self.wiki / "me" / "timeline.md"

    @property
    def open_inquiries_path(self) -> Path:
        open_inquiries = self.wiki / "me" / "open-inquiries.md"
        if open_inquiries.exists():
            return open_inquiries
        # Read-only compatibility fallback while legacy owner notes still exist.
        return self.wiki / "me" / LEGACY_OWNER_NOTE

    @property
    def brain_state(self) -> Path:
        return self.wiki / ".brain-state.json"

    @property
    def brain_lock(self) -> Path:
        return self.wiki / ".brain-lock"

    @property
    def runtime_db(self) -> Path:
        if self.config.state.runtime_db:
            return self._resolve_path(self.config.state.runtime_db)
        if self.state_root == self.root:
            return self.root / ".brain-runtime.sqlite3"
        return self.state_root / "brain-runtime.sqlite3"

    @property
    def sources_db(self) -> Path:
        if self.config.state.sources_db:
            return self._resolve_path(self.config.state.sources_db)
        if self.state_root == self.root:
            return self.root / ".brain-sources.sqlite3"
        return self.state_root / "brain-sources.sqlite3"

    @property
    def graph_db(self) -> Path:
        if self.config.state.graph_db:
            return self._resolve_path(self.config.state.graph_db)
        if self.state_root == self.root:
            return self.root / ".brain-graph.sqlite3"
        return self.state_root / "brain-graph.sqlite3"

    @property
    def vector_db(self) -> Path:
        if self.config.retrieval.vector_db:
            return self._resolve_path(self.config.retrieval.vector_db)
        if self.state_root == self.root:
            return self.raw / "cache" / "graph-vectors" / "graph-vectors.sqlite3"
        return self.state_root / "graph-vectors.sqlite3"

    @property
    def onboarding_root(self) -> Path:
        return self.raw / "onboarding"

    @property
    def onboarding_bundles_root(self) -> Path:
        return self.onboarding_root / "bundles"

    @property
    def onboarding_current_path(self) -> Path:
        return self.onboarding_root / "current.json"

    @property
    def changelog(self) -> Path:
        return self.wiki / "CHANGELOG.md"

    @property
    def index(self) -> Path:
        return self.wiki / "INDEX.md"

    def owner_profile_text(self) -> Optional[str]:
        """Return the owner profile contents, or None with a warning if missing.

        Pass B 'applied-to-you' calls hit this. If a user runs the engine
        without filling out their profile, those passes skip with a warning
        rather than crashing.
        """
        if not self.owner_profile.exists():
            logger.warning(
                "owner profile not found at %s — Pass B applied-to-you calls "
                "will be skipped. Run /onboard or create the file manually.",
                self.owner_profile,
            )
            return None
        return self.owner_profile.read_text()

    @classmethod
    def load(cls, root: Path) -> "Vault":
        cfg = BrainConfig.load(root)
        return cls(root=root, config=cfg)

    @classmethod
    def from_repo_root(cls, root: Path) -> "Vault":
        """Compat shim for legacy call sites that have a `repo_root: Path`.

        Equivalent to `Vault.load(root)`. Provided so callers can migrate
        incrementally without a giant refactor PR.
        """
        return cls.load(root)


def wiki_path(root: Path, *parts: str) -> Path:
    """Resolve a path under the configured durable memory tree."""

    return Vault.from_repo_root(root).wiki.joinpath(*parts)


def raw_path(root: Path, *parts: str) -> Path:
    """Resolve a path under the configured raw input tree."""

    return Vault.from_repo_root(root).raw.joinpath(*parts)


def relative_markdown_path(from_file: Path, to_file: Path) -> str:
    """Return a stable relative path string for frontmatter links."""

    return Path(os.path.relpath(to_file, start=from_file.parent)).as_posix()


def project_root() -> Path:
    """Return the Brain repository root."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / ".git").exists() or (parent / "contracts" / "brain-contract.yaml").exists():
            return parent
    return current.parents[2]
