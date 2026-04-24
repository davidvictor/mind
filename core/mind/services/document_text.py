"""Helpers for extracting source-grounded text from local documents."""
from __future__ import annotations

from pathlib import Path
import subprocess


def extract_document_text(path: Path) -> str:
    """Return best-effort source text for a local document path."""
    if path.suffix.lower() == ".pdf":
        return _extract_pdf_text(path)
    return path.read_text(encoding="utf-8")


def _extract_pdf_text(path: Path) -> str:
    result = subprocess.run(
        ["pdftotext", str(path), "-"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pdftotext exited {result.returncode}: {result.stderr.strip()[:200]}")
    return result.stdout.strip()
