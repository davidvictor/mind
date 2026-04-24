"""SQLite-backed brain lock context manager.

The runtime lock is now authoritative in the root operational-state database.
Legacy `wiki/.brain-lock` files may still exist in fixtures or old repos, but
their presence no longer governs runtime coordination.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from mind.runtime_state import DEFAULT_LOCK_NAME, RuntimeState, RuntimeStateLockBusy


class BrainLockBusy(Exception):
    """Raised when attempting to acquire the brain lock while another
    workflow already holds it. The exception message includes the current
    holder's name so the caller can decide whether to retry or skip."""


def is_locked(repo_root: Path) -> bool:
    """Return True if the SQLite runtime lock is currently held."""
    state = RuntimeState.for_repo_root(repo_root)
    return state.read_lock(name=DEFAULT_LOCK_NAME) is not None


def read_lock_holder(repo_root: Path) -> str | None:
    """Return the active lock holder from SQLite, or None if unlocked."""
    state = RuntimeState.for_repo_root(repo_root)
    lock = state.read_lock(name=DEFAULT_LOCK_NAME)
    if lock is None:
        return None
    return lock.holder


@contextmanager
def brain_lock(*, holder: str, repo_root: Path) -> Iterator[None]:
    """Acquire the SQLite-backed brain lock for the duration of a with-block."""
    state = RuntimeState.for_repo_root(repo_root)
    try:
        state.acquire_lock(holder=holder, name=DEFAULT_LOCK_NAME)
    except RuntimeStateLockBusy as exc:
        raise BrainLockBusy(str(exc)) from exc

    try:
        yield
    finally:
        state.release_lock(holder=holder, name=DEFAULT_LOCK_NAME)
