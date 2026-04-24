from __future__ import annotations

from contextlib import contextmanager
import os
import sys
import threading
import time
from typing import Iterator


_FRAMES = ("|", "/", "-", "\\")
_CLEAR_LINE = "\r\x1b[2K"


def progress_enabled_for_args(args, *, default: bool = False) -> bool:
    if not bool(getattr(args, "progress_enabled", default)):
        return False
    if bool(getattr(args, "quiet", False)):
        return False
    if os.environ.get("BRAIN_NO_PROGRESS", "").strip():
        return False
    if any(bool(getattr(args, key, False)) for key in ("json", "print_json")):
        return False
    try:
        return bool(sys.stderr.isatty())
    except Exception:
        return False


class CliProgress:
    def __init__(self, *, enabled: bool, message: str = "working", delay_seconds: float = 0.35):
        self.enabled = enabled
        self.message = message
        self.delay_seconds = delay_seconds
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._started_at = 0.0
        self._spinner_visible = False
        self._last_phase: str | None = None

    def __enter__(self) -> "CliProgress":
        if self.enabled:
            self._started_at = time.monotonic()
            self._thread = threading.Thread(target=self._run_spinner, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self.enabled:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=0.5)
        self.clear(newline=True)

    def update(self, message: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            self.message = message

    def phase(self, message: str) -> None:
        if not self.enabled:
            return
        with self._lock:
            self.message = message
            if self._last_phase == message:
                return
            if self._spinner_visible:
                sys.stderr.write(_CLEAR_LINE)
                sys.stderr.write(f"{message}\n")
                sys.stderr.flush()
                self._spinner_visible = False
            self._last_phase = message

    def clear(self, *, newline: bool = False) -> None:
        if not self.enabled:
            return
        with self._lock:
            if not self._spinner_visible:
                return
            sys.stderr.write(_CLEAR_LINE)
            if newline:
                sys.stderr.write("\n")
            sys.stderr.flush()
            self._spinner_visible = False

    def _run_spinner(self) -> None:
        frame_index = 0
        while not self._stop.is_set():
            elapsed = time.monotonic() - self._started_at
            if elapsed < self.delay_seconds:
                time.sleep(min(0.05, self.delay_seconds - elapsed))
                continue
            with self._lock:
                sys.stderr.write(_CLEAR_LINE)
                sys.stderr.write(f"{_FRAMES[frame_index % len(_FRAMES)]} {self.message}")
                sys.stderr.flush()
                self._spinner_visible = True
            frame_index += 1
            time.sleep(0.1)


@contextmanager
def progress_for_args(args, *, message: str, default: bool = False, delay_seconds: float = 0.35) -> Iterator[CliProgress]:
    progress = CliProgress(
        enabled=progress_enabled_for_args(args, default=default),
        message=message,
        delay_seconds=delay_seconds,
    )
    with progress:
        yield progress
