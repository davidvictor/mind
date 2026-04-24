"""Tests for scripts.common.brain_lock — SQLite-backed brain lock context manager."""
from __future__ import annotations

from pathlib import Path

import pytest

from scripts.common.brain_lock import (
    BrainLockBusy,
    brain_lock,
    is_locked,
    read_lock_holder,
)


def _write_config(root: Path) -> None:
    (root / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n",
        encoding="utf-8",
    )


def test_lock_creates_and_releases(tmp_path):
    _write_config(tmp_path)
    db_path = tmp_path / ".brain-runtime.sqlite3"

    assert not db_path.exists()
    with brain_lock(holder="test-holder", repo_root=tmp_path):
        assert db_path.exists()
        assert is_locked(tmp_path) is True
        assert read_lock_holder(tmp_path) == "test-holder"

    assert is_locked(tmp_path) is False, "lock should be released on context exit"


def test_lock_released_on_exception(tmp_path):
    _write_config(tmp_path)

    with pytest.raises(RuntimeError, match="boom"):
        with brain_lock(holder="test", repo_root=tmp_path):
            assert is_locked(tmp_path) is True
            raise RuntimeError("boom")

    assert is_locked(tmp_path) is False, "lock should be released even if body raises"


def test_lock_busy_raises_when_already_held(tmp_path):
    _write_config(tmp_path)
    with brain_lock(holder="other-holder", repo_root=tmp_path):
        with pytest.raises(BrainLockBusy, match="other-holder"):
            with brain_lock(holder="test", repo_root=tmp_path):
                pass  # pragma: no cover

        assert is_locked(tmp_path) is True
        assert read_lock_holder(tmp_path) == "other-holder"


def test_is_locked_returns_false_when_no_lock(tmp_path):
    _write_config(tmp_path)
    assert is_locked(tmp_path) is False


def test_is_locked_returns_true_when_locked(tmp_path):
    _write_config(tmp_path)
    with brain_lock(holder="h", repo_root=tmp_path):
        assert is_locked(tmp_path) is True


def test_read_lock_holder_returns_holder_name(tmp_path):
    _write_config(tmp_path)
    with brain_lock(holder="ingest-substack", repo_root=tmp_path):
        assert read_lock_holder(tmp_path) == "ingest-substack"


def test_read_lock_holder_returns_none_when_no_lock(tmp_path):
    _write_config(tmp_path)
    assert read_lock_holder(tmp_path) is None


def test_legacy_lockfile_is_not_authoritative(tmp_path):
    _write_config(tmp_path)
    legacy_lock = tmp_path / "wiki" / ".brain-lock"
    legacy_lock.parent.mkdir(parents=True, exist_ok=True)
    legacy_lock.write_text("legacy-holder\n2026-04-08T01:23:45Z\n", encoding="utf-8")

    with brain_lock(holder="test", repo_root=tmp_path):
        assert read_lock_holder(tmp_path) == "test"
    assert legacy_lock.exists()
