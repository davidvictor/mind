"""Compatibility shim for the public core layout.

The real `scripts` package lives in `core/scripts`. This keeps imports and
`python -m scripts.<module>` working from a source checkout.
"""
from __future__ import annotations

from pathlib import Path

_CORE_PACKAGE = Path(__file__).resolve().parents[1] / "core" / "scripts"
__path__ = [str(_CORE_PACKAGE)]
