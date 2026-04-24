"""Fail when private Brain data is staged or tracked.

This guard is intentionally conservative. It checks path classes first, then a
small set of high-signal content markers that should never appear in public
code, docs, examples, or fixtures.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


PRIVATE_ROOTS = {
    "local_data",
    "memory",
    "raw",
    "dropbox",
    ".obsidian",
    ".logs",
    ".omc",
    ".omx",
}
PRIVATE_EXACT = {
    ".env",
    "config.yaml",
    ".brain-runtime.sqlite3",
    ".brain-graph.sqlite3",
    ".brain-sources.sqlite3",
}
PRIVATE_SUFFIXES = (
    ".sqlite3",
    ".sqlite3-shm",
    ".sqlite3-wal",
    ".db",
)
CONTENT_MARKERS = (
    "private-" + "owner.invalid",
    "github.com/" + "private-owner",
    "linkedin.com/in/" + "private-owner",
    "twitter.com/" + "private-owner",
    "(555) " + "000-0000",
    "".join(("d", "av", "id", "vic", "tor")),
    "relationship_to_" + "".join(("d", "av", "id")),
    "applied_by_" + "".join(("d", "av", "id")),
    "".join(("d", "av", "id")) + "_alignment",
)
CONTENT_PATTERNS = (
    re.compile(r"/Users/[A-Za-z0-9._-]+/"),
    re.compile(r"-Users-[A-Za-z0-9._-]+"),
    re.compile(r"BEGIN [A-Z ]*PRIVATE KEY"),
    re.compile(r"substack\.sid=ey"),
    re.compile(r"/raw/onboarding/bundles/20\d{6}"),
)
PUBLIC_DATABASE_FIXTURES = (
    "tests/fixtures/chrome/history-sample.sqlite3",
)


def _git(args: list[str]) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return [line for line in result.stdout.splitlines() if line]


def _staged_paths() -> list[str]:
    rows = _git(["diff", "--cached", "--name-only", "--diff-filter=ACMR"])
    return sorted(set(rows))


def _tracked_paths() -> list[str]:
    return sorted(set(_git(["ls-files"])))


def _path_reason(path: str) -> str | None:
    parts = Path(path).parts
    if not parts:
        return None
    first = parts[0]
    name = Path(path).name
    if first in PRIVATE_ROOTS:
        return f"private root `{first}/`"
    if any(part in {".logs", ".obsidian", ".omc", ".omx"} for part in parts):
        return "private tool/runtime artifact path"
    if parts[:2] in {("docs", "archive"), ("docs", "plans"), ("docs", "spikes")}:
        return f"local planning/archive docs `{parts[0]}/{parts[1]}/`"
    if parts[:2] in {(".claude", "settings.local.json"), (".claude", "worktrees")}:
        return f"private Claude local state `{parts[0]}/{parts[1]}`"
    if path in PRIVATE_EXACT:
        return f"private file `{name}`"
    if re.fullmatch(r"20\d{2}-\d{2}-\d{2}\.md", name):
        return f"private dated note `{name}`"
    if name == ".env" or (name.startswith(".env.") and name != ".env.example"):
        return f"private env file `{name}`"
    if name.startswith(".brain-") and name.endswith(".sqlite3"):
        return "private Brain sqlite database"
    if any(name.endswith(suffix) for suffix in PRIVATE_SUFFIXES) and path not in PUBLIC_DATABASE_FIXTURES:
        return f"private database-like file `{name}`"
    if first == "tests" and len(parts) > 2 and parts[1] == "fixtures" and parts[2] == "onboarding":
        return "private onboarding fixture path; use tests/fixtures/synthetic/"
    return None


def _content_reason(path: str) -> str | None:
    candidate = Path(path)
    if not candidate.is_file():
        return None
    try:
        text = candidate.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None
    for marker in CONTENT_MARKERS:
        if marker in text:
            return f"private content marker `{marker}`"
    for pattern in CONTENT_PATTERNS:
        if pattern.search(text):
            return f"private content pattern `{pattern.pattern}`"
    return None


def check(paths: list[str], *, scan_content: bool) -> list[str]:
    failures: list[str] = []
    for path in paths:
        reason = _path_reason(path)
        if reason is None and scan_content:
            reason = _content_reason(path)
        if reason:
            failures.append(f"{path}: {reason}")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--staged", action="store_true", help="scan staged public changes")
    group.add_argument("--tracked", action="store_true", help="scan the full tracked tree")
    parser.add_argument("--no-content", action="store_true", help="skip content marker checks")
    args = parser.parse_args(argv)

    paths = _tracked_paths() if args.tracked else _staged_paths()
    failures = check(paths, scan_content=not args.no_content)
    if failures:
        print("private-data guard failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print(f"private-data guard passed: scanned {len(paths)} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
