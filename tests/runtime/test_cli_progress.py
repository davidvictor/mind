from __future__ import annotations

import io
import time
from types import SimpleNamespace

import pytest

from mind.services.cli_progress import CliProgress, progress_enabled_for_args


class _TTYBuffer(io.StringIO):
    def __init__(self, *, tty: bool):
        super().__init__()
        self._tty = tty

    def isatty(self) -> bool:
        return self._tty


def test_progress_enabled_only_for_tty(monkeypatch):
    stderr = _TTYBuffer(tty=True)
    monkeypatch.setattr("mind.services.cli_progress.sys.stderr", stderr)
    args = SimpleNamespace(progress_enabled=True, quiet=False, json=False, print_json=False)

    assert progress_enabled_for_args(args) is True

    monkeypatch.setattr("mind.services.cli_progress.sys.stderr", _TTYBuffer(tty=False))
    assert progress_enabled_for_args(args) is False


def test_progress_disabled_for_json_and_env_opt_out(monkeypatch):
    stderr = _TTYBuffer(tty=True)
    monkeypatch.setattr("mind.services.cli_progress.sys.stderr", stderr)
    args = SimpleNamespace(progress_enabled=True, quiet=False, json=True, print_json=False)
    assert progress_enabled_for_args(args) is False

    args = SimpleNamespace(progress_enabled=True, quiet=False, json=False, print_json=False)
    monkeypatch.setenv("BRAIN_NO_PROGRESS", "1")
    assert progress_enabled_for_args(args) is False


def test_delayed_spinner_does_not_emit_for_fast_commands(monkeypatch):
    stderr = _TTYBuffer(tty=True)
    monkeypatch.setattr("mind.services.cli_progress.sys.stderr", stderr)

    with CliProgress(enabled=True, message="working", delay_seconds=0.25) as progress:
        progress.phase("working")

    assert stderr.getvalue() == ""


def test_spinner_cleanup_happens_on_exception(monkeypatch):
    stderr = _TTYBuffer(tty=True)
    monkeypatch.setattr("mind.services.cli_progress.sys.stderr", stderr)

    with pytest.raises(RuntimeError):
        with CliProgress(enabled=True, message="working", delay_seconds=0.0) as progress:
            progress.phase("phase one")
            time.sleep(0.12)
            raise RuntimeError("boom")

    assert "phase one" in stderr.getvalue()


def test_spinner_cleanup_leaves_newline_when_spinner_visible(monkeypatch):
    stderr = _TTYBuffer(tty=True)
    monkeypatch.setattr("mind.services.cli_progress.sys.stderr", stderr)

    with CliProgress(enabled=True, message="working", delay_seconds=0.0):
        time.sleep(0.12)

    assert stderr.getvalue().endswith("\n")
