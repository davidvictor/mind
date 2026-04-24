"""Weekly Audible library puller.

Current behavior exports Audible library metadata only.

The old per-book annotations/clips fetch path is intentionally disabled until
the Audible annotations API is reliable again. Compatibility flags like
``--library-only`` and ``--sleep`` remain accepted so existing wrappers do not
break, but they no longer affect runtime behavior.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

from scripts.common import env
from scripts.audible.auth import load_authenticator


def _audible_cli_binary() -> str:
    venv_bin = Path(sys.executable).parent / "audible"
    if venv_bin.exists():
        return str(venv_bin)
    return "audible"


def export_library(target: Path) -> int:
    """Run `audible library export` and write the JSON to target. Returns count."""
    target.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        _audible_cli_binary(),
        "library",
        "export",
        "--format", "json",
        "--output", str(target),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"audible library export failed: {result.stderr.strip()[:300]}")
    if not target.exists():
        raise RuntimeError(f"audible library export wrote no file at {target}")
    data = json.loads(target.read_text())
    return len(data) if isinstance(data, list) else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--library-only", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.0, help="deprecated compatibility flag; ignored")
    args = parser.parse_args(argv)

    cfg = env.load()  # we don't strictly need GEMINI_API_KEY here, but env.load enforces .env exists

    if args.dry_run:
        try:
            load_authenticator()
            print("dry-run: audible auth file present and loadable")
            return 0
        except Exception as e:
            print(f"dry-run FAILED: {e}")
            return 1

    today = date.today().isoformat()
    library_target = cfg.raw_root / "exports" / f"audible-library-{today}.json"

    print(f"exporting library → {library_target}")
    count = export_library(library_target)
    print(f"  {count} books in library")
    print("  clip fetch disabled; library export only")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
