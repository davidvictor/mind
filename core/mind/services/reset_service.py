from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

from scripts.atoms import cache as atoms_cache
from scripts.common.contract import atom_collection_dirs
from scripts.common.vault import Vault


WIKI_SCAFFOLD_DIRS = (
    "channels",
    "companies",
    "concepts",
    "decisions",
    "inbox",
    "inquiries",
    "me",
    "people",
    "playbooks",
    "projects",
    "sources",
    "stances",
    "summaries",
)
RAW_SCAFFOLD_DIRS = (
    "drops",
    "exports",
    "onboarding/bundles",
    "reports",
)
DROPBOX_SCAFFOLD_DIRS = (
    ".processed",
    ".failed",
    ".reports",
    ".review",
)
SCAFFOLD_FILE_CONTENT = {
    "INDEX.md": "# INDEX\n",
    "CHANGELOG.md": "# CHANGELOG\n",
}


@dataclass(frozen=True)
class ResetPlan:
    repo_root: Path
    wiki_root: Path
    raw_root: Path
    dropbox_root: Path
    wiki_entries: list[Path]
    raw_entries: list[Path]
    dropbox_entries: list[Path]
    runtime_db_exists: bool
    graph_db_exists: bool
    sources_db_exists: bool

    @property
    def removable_paths(self) -> list[Path]:
        paths = [*self.wiki_entries, *self.raw_entries, *self.dropbox_entries]
        if self.runtime_db_exists:
            paths.append(Vault.load(self.repo_root).runtime_db)
        if self.graph_db_exists:
            paths.append(Vault.load(self.repo_root).graph_db)
        if self.sources_db_exists:
            paths.append(Vault.load(self.repo_root).sources_db)
        return paths


@dataclass(frozen=True)
class ResetResult:
    mode: str
    plan: ResetPlan
    recreated_paths: list[Path]

    def render(self) -> str:
        lines = [
            "reset:",
            f"- mode={self.mode}",
            f"- repo_root={self.plan.repo_root}",
            f"- removed_paths={len(self.plan.removable_paths)}",
            f"- memory_entries={len(self.plan.wiki_entries)}",
            f"- raw_entries={len(self.plan.raw_entries)}",
            f"- dropbox_entries={len(self.plan.dropbox_entries)}",
            f"- runtime_db={'yes' if self.plan.runtime_db_exists else 'no'}",
            f"- graph_db={'yes' if self.plan.graph_db_exists else 'no'}",
            f"- sources_db={'yes' if self.plan.sources_db_exists else 'no'}",
            f"- recreated_paths={len(self.recreated_paths)}",
            f"- memory_root={self.plan.wiki_root}",
            f"- raw_root={self.plan.raw_root}",
            f"- dropbox_root={self.plan.dropbox_root}",
        ]
        if self.mode == "dry-run":
            lines.append("- next_step=rerun with --apply to perform the reset")
        else:
            lines.append("- result=brain reset to an empty starter layout")
        return "\n".join(lines)


def _child_entries(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.iterdir(), key=lambda path: path.as_posix())


def build_reset_plan(repo_root: Path) -> ResetPlan:
    vault = Vault.load(repo_root)
    return ResetPlan(
        repo_root=repo_root,
        wiki_root=vault.wiki,
        raw_root=vault.raw,
        dropbox_root=vault.dropbox,
        wiki_entries=_child_entries(vault.wiki),
        raw_entries=_child_entries(vault.raw),
        dropbox_entries=_child_entries(vault.dropbox),
        runtime_db_exists=vault.runtime_db.exists(),
        graph_db_exists=vault.graph_db.exists(),
        sources_db_exists=vault.sources_db.exists(),
    )


def _remove_path(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink()


def _ensure_scaffold(repo_root: Path) -> list[Path]:
    vault = Vault.load(repo_root)
    created: list[Path] = []

    vault.wiki.mkdir(parents=True, exist_ok=True)
    vault.raw.mkdir(parents=True, exist_ok=True)
    vault.dropbox.mkdir(parents=True, exist_ok=True)

    for dirname in WIKI_SCAFFOLD_DIRS:
        path = vault.wiki / dirname
        path.mkdir(parents=True, exist_ok=True)
        created.append(path)

    # Canonical atom families should always exist after reset.
    for dirname in atom_collection_dirs().values():
        path = vault.wiki / dirname
        path.mkdir(parents=True, exist_ok=True)
        if path not in created:
            created.append(path)

    for dirname, content in SCAFFOLD_FILE_CONTENT.items():
        path = vault.wiki / dirname
        path.write_text(content, encoding="utf-8")
        created.append(path)

    for dirname in RAW_SCAFFOLD_DIRS:
        path = vault.raw / dirname
        path.mkdir(parents=True, exist_ok=True)
        created.append(path)

    dropbox_root = vault.dropbox
    root_gitkeep = dropbox_root / ".gitkeep"
    root_gitkeep.write_text("", encoding="utf-8")
    created.append(root_gitkeep)
    for dirname in DROPBOX_SCAFFOLD_DIRS:
        path = dropbox_root / dirname
        path.mkdir(parents=True, exist_ok=True)
        created.append(path)
        gitkeep = path / ".gitkeep"
        gitkeep.write_text("", encoding="utf-8")
        created.append(gitkeep)

    atoms_cache.rebuild(repo_root)
    created.append(vault.brain_state)
    return created


def reset_brain(repo_root: Path, *, apply: bool) -> ResetResult:
    plan = build_reset_plan(repo_root)
    if not apply:
        return ResetResult(mode="dry-run", plan=plan, recreated_paths=[])

    for path in plan.removable_paths:
        _remove_path(path)

    recreated_paths = _ensure_scaffold(repo_root)
    return ResetResult(mode="apply", plan=plan, recreated_paths=sorted(set(recreated_paths), key=lambda path: path.as_posix()))
