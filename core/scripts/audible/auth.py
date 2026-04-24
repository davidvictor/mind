"""Audible auth helper.

Wraps audible.Authenticator.from_file() with a friendly error message
when the auth file is missing or expired.
"""
from __future__ import annotations

from pathlib import Path

import audible


AUDIBLE_HOME = Path.home() / ".audible"


def find_auth_file() -> Path:
    """Locate the audible-cli auth file. Returns the first .json found in ~/.audible/."""
    if not AUDIBLE_HOME.exists():
        raise RuntimeError(
            f"~/.audible/ does not exist. Run `.venv/bin/audible quickstart` first. "
            "See README.md for the full setup."
        )
    candidates = sorted(AUDIBLE_HOME.glob("*.json"))
    if not candidates:
        raise RuntimeError(
            f"No audible auth files in {AUDIBLE_HOME}. "
            f"Run `.venv/bin/audible quickstart`."
        )
    return candidates[0]


def load_authenticator() -> audible.Authenticator:
    """Load the persisted Audible auth file. Re-raises with a clearer message on failure."""
    path = find_auth_file()
    try:
        return audible.Authenticator.from_file(path)
    except Exception as e:
        raise RuntimeError(
            f"Failed to load Audible auth from {path}: {e}. "
            f"Tokens may have expired. Re-run `.venv/bin/audible quickstart`."
        ) from e
