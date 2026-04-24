"""Tests for scripts/common/vault.py Vault dataclass."""
from __future__ import annotations

from pathlib import Path
import pytest


def _write_minimal_config(root: Path) -> None:
    (root / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
    )


def test_vault_load_resolves_paths(tmp_path: Path):
    from scripts.common.vault import Vault

    _write_minimal_config(tmp_path)
    (tmp_path / "memory" / "me").mkdir(parents=True)
    v = Vault.load(tmp_path)
    assert v.root == tmp_path
    assert v.wiki == tmp_path / "memory"
    assert v.raw == tmp_path / "raw"
    assert v.owner_profile == tmp_path / "memory" / "me" / "profile.md"
    assert v.brain_state == tmp_path / "memory" / ".brain-state.json"
    assert v.brain_lock == tmp_path / "memory" / ".brain-lock"
    assert v.runtime_db == tmp_path / ".brain-runtime.sqlite3"


def test_vault_owner_profile_missing_returns_none_with_warning(tmp_path: Path, caplog):
    from scripts.common.vault import Vault

    _write_minimal_config(tmp_path)
    v = Vault.load(tmp_path)
    # Owner profile file doesn't exist — vault still loads, but owner_profile_text() warns
    text = v.owner_profile_text()
    assert text is None
    assert "owner profile not found" in caplog.text.lower()


def test_vault_from_repo_root_compat_shim(tmp_path: Path):
    from scripts.common.vault import Vault

    _write_minimal_config(tmp_path)
    v = Vault.from_repo_root(tmp_path)
    assert v.root == tmp_path


def test_vault_load_resolves_flattened_memory_and_raw_layout(tmp_path: Path):
    from scripts.common.vault import Vault, relative_markdown_path

    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
    )
    (tmp_path / "memory" / "me").mkdir(parents=True)
    (tmp_path / "raw" / "transcripts" / "youtube").mkdir(parents=True)
    target = tmp_path / "memory" / "summaries" / "summary-yt-abc.md"
    source = tmp_path / "raw" / "transcripts" / "youtube" / "abc.json"

    v = Vault.load(tmp_path)

    assert v.wiki == tmp_path / "memory"
    assert v.raw == tmp_path / "raw"
    assert v.owner_profile == tmp_path / "memory" / "me" / "profile.md"
    assert relative_markdown_path(target, source) == "../../raw/transcripts/youtube/abc.json"


def test_vault_memory_root_prefers_split_parent(tmp_path: Path):
    from scripts.common.vault import Vault

    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
    )
    (tmp_path / "memory" / "me").mkdir(parents=True)
    (tmp_path / "raw").mkdir(parents=True)
    v = Vault.load(tmp_path)
    assert v.wiki == tmp_path / "memory"
    assert v.raw == tmp_path / "raw"
    assert v.memory_root == tmp_path
    assert v.runtime_db == tmp_path / ".brain-runtime.sqlite3"


def test_vault_logical_paths_hide_external_private_roots(tmp_path: Path):
    from scripts.common.vault import Vault

    private_root = tmp_path / "private-store"
    memory = private_root / "memory"
    raw = private_root / "raw"
    dropbox = private_root / "dropbox"
    state = private_root / "state"
    memory.mkdir(parents=True)
    raw.mkdir()
    dropbox.mkdir()
    state.mkdir()
    (tmp_path / "config.yaml").write_text(
        "paths:\n"
        f"  memory_root: {memory.as_posix()}\n"
        f"  raw_root: {raw.as_posix()}\n"
        f"  dropbox_root: {dropbox.as_posix()}\n"
        f"  state_root: {state.as_posix()}\n"
        "vault:\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
    )

    v = Vault.load(tmp_path)
    concept = memory / "concepts" / "local-first.md"
    concept.parent.mkdir(parents=True)
    concept.write_text("# Local First\n", encoding="utf-8")

    assert v.logical_path(concept) == "memory/concepts/local-first.md"
    assert v.resolve_logical_path("memory/concepts/local-first.md") == concept
    assert v.resolve_logical_path("concepts/local-first.md") == concept
    assert v.logical_path(raw / "reports" / "run.json") == "raw/reports/run.json"
    assert v.logical_path(dropbox / "note.md") == "dropbox/note.md"
    assert v.logical_path(state / "brain-runtime.sqlite3") == "state/brain-runtime.sqlite3"
