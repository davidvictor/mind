"""Compatibility shim for the public core layout.

The real `mind` package lives in `core/mind`. Keeping this tiny package at the
repo root preserves `python -m mind` and existing import paths without putting
implementation code back in the public root.
"""
from __future__ import annotations

from pathlib import Path

_CORE_PACKAGE = Path(__file__).resolve().parents[1] / "core" / "mind"
__path__ = [str(_CORE_PACKAGE)]
