"""Brain lint — health check for a vault.

Usage:
    .venv/bin/python -m scripts.lint                  # current dir as vault
    .venv/bin/python -m scripts.lint /path/to/vault   # specified vault root

Replaces .claude/commands/lint.md as the canonical implementation.
The slash command is now a thin wrapper that calls this module.
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from scripts.common.default_tags import validate_axes, validate_topic_tags
from scripts.common.schema import KNOWN_PAGE_TYPES, is_system_file, resolve_page_type
from scripts.common.vault import Vault
from scripts.common.wikilinks import WIKILINK_FULL_RE as WIKILINK_RE

logger = logging.getLogger(__name__)
_ATOM_PAGE_TYPES = {"concept", "playbook", "stance", "inquiry"}
_ATOM_REQUIRED_HEADINGS = {
    "concept": ("## TL;DR", "## Evidence log"),
    "playbook": ("## TL;DR", "## Steps", "## Evidence log"),
    "stance": ("## TL;DR", "## Evidence log", "## Contradictions"),
    "inquiry": ("## TL;DR", "## Evidence log"),
}
_MATURE_ATOM_REQUIRED_HEADINGS = {
    "concept": ("## TL;DR", "## Why It Matters", "## Mechanism", "## Examples", "## In Conversation With", "## Evidence log"),
    "playbook": ("## TL;DR", "## When To Use", "## Prerequisites", "## Steps", "## Failure Modes", "## Evidence log"),
    "stance": ("## TL;DR", "## Position", "## Why", "## Best Evidence For", "## Strongest Counterevidence", "## What Would Change My Mind", "## Evidence log", "## Contradictions"),
    "inquiry": ("## TL;DR", "## Question", "## Why This Matters", "## Current Hypotheses", "## What Would Resolve It", "## Evidence log"),
}


@dataclass
class LintReport:
    failing_pages: int = 0
    schema_violations: int = 0
    broken_links: int = 0
    orphans: int = 0
    stale_pages: int = 0
    exit_code: int = 0
    details: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"Pages failing: {self.failing_pages}\n"
            f"Schema violations: {self.schema_violations}\n"
            f"Broken links: {self.broken_links}\n"
            f"Orphans: {self.orphans}\n"
            f"Stale pages: {self.stale_pages}\n"
        )


def _frontmatter_block(text: str) -> str | None:
    if not text.startswith("---\n"):
        return None
    end = text.find("\n---\n", 4)
    if end == -1:
        return None
    return text[4:end]


def _parse_frontmatter(text: str) -> dict | None:
    block = _frontmatter_block(text)
    if block is None:
        return None
    try:
        return yaml.safe_load(block) or {}
    except yaml.YAMLError:
        return None


def _duplicate_frontmatter_keys(text: str) -> dict[str, int]:
    block = _frontmatter_block(text)
    if block is None:
        return {}
    counts: dict[str, int] = {}
    for line in block.splitlines():
        if not line or line.startswith((" ", "\t")) or ":" not in line:
            continue
        key = line.split(":", 1)[0].strip()
        counts[key] = counts.get(key, 0) + 1
    return {key: count for key, count in counts.items() if count > 1}


def _collect_wikilinks(value: object) -> list[str]:
    links: list[str] = []
    if isinstance(value, str):
        links.extend(WIKILINK_RE.findall(value))
        return links
    if isinstance(value, list):
        for item in value:
            links.extend(_collect_wikilinks(item))
        return links
    if isinstance(value, dict):
        for item in value.values():
            links.extend(_collect_wikilinks(item))
    return links


def _body_text(text: str) -> str:
    if not text.startswith("---\n"):
        return text
    end = text.find("\n---\n", 4)
    if end == -1:
        return text
    return text[end + 5 :]


def _leading_blank_lines(text: str) -> int:
    count = 0
    for line in text.splitlines():
        if line.strip():
            return count
        count += 1
    return count


def _section_body(body: str, heading: str) -> str:
    pattern = re.compile(rf"^{re.escape(heading)}\s*$", re.MULTILINE)
    match = pattern.search(body)
    if not match:
        return ""
    rest = body[match.end() :].lstrip("\n")
    next_heading = re.search(r"^## ", rest, re.MULTILINE)
    return rest[: next_heading.start()] if next_heading else rest


def _has_section(body: str, heading: str) -> bool:
    return bool(re.search(rf"^{re.escape(heading)}\s*$", body, re.MULTILINE))


def _walk_pages(vault: Vault):
    for path in vault.wiki.rglob("*.md"):
        if is_system_file(path):
            continue
        if "/.archive/" in str(path):
            continue
        yield path


def run(vault: Vault) -> LintReport:
    """Run all lint checks against `vault`. Returns a LintReport."""
    report = LintReport()

    # Warn about missing owner profile (skip-with-warning per Plan 02 Task 0).
    vault.owner_profile_text()

    all_page_ids: set[str] = set()
    all_page_paths: list[Path] = []
    page_links: dict[Path, list[str]] = {}

    for path in _walk_pages(vault):
        all_page_paths.append(path)
        text = path.read_text()
        duplicate_keys = _duplicate_frontmatter_keys(text)
        if duplicate_keys:
            report.failing_pages += 1
            report.schema_violations += len(duplicate_keys)
            report.details.append(f"{path}: duplicate frontmatter keys {sorted(duplicate_keys)}")
        fm = _parse_frontmatter(text)
        if fm is None:
            report.failing_pages += 1
            report.schema_violations += 1
            report.details.append(f"{path}: missing or invalid frontmatter")
            continue

        page_id = fm.get("id")
        if page_id:
            all_page_ids.add(str(page_id))

        page_type = fm.get("type")
        if not page_type:
            report.failing_pages += 1
            report.schema_violations += 1
            report.details.append(f"{path}: missing `type`")
            continue

        page_type = resolve_page_type(str(page_type))

        if page_type not in KNOWN_PAGE_TYPES:
            report.failing_pages += 1
            report.schema_violations += 1
            report.details.append(f"{path}: unknown type `{page_type}`")
            continue

        pt = KNOWN_PAGE_TYPES[page_type]
        missing = [f for f in pt.required_fields if f not in fm]
        if missing:
            report.failing_pages += 1
            report.schema_violations += len(missing)
            report.details.append(f"{path}: missing required fields {missing}")

        tags = fm.get("tags")
        if not isinstance(tags, list):
            report.failing_pages += 1
            report.schema_violations += 1
            report.details.append(f"{path}: tags must be a list")
        else:
            tag_errors = validate_axes([str(tag) for tag in tags]) + validate_topic_tags([str(tag) for tag in tags])
            if tag_errors:
                report.failing_pages += 1
                report.schema_violations += len(tag_errors)
                report.details.extend(f"{path}: {error}" for error in tag_errors)

        # Collect wikilinks for the broken-link pass
        body = _body_text(text)
        if page_type in _ATOM_PAGE_TYPES:
            if not body.strip():
                report.failing_pages += 1
                report.schema_violations += 1
                report.details.append(f"{path}: atom page body is blank")
            leading_blanks = _leading_blank_lines(body)
            if leading_blanks > 2:
                report.failing_pages += 1
                report.schema_violations += 1
                report.details.append(f"{path}: excessive leading blank lines in atom body ({leading_blanks})")
            required_headings = (
                _MATURE_ATOM_REQUIRED_HEADINGS[page_type]
                if str(fm.get("last_synthesized_at") or "").strip()
                else _ATOM_REQUIRED_HEADINGS[page_type]
            )
            for heading in required_headings:
                if not _has_section(body, heading):
                    report.failing_pages += 1
                    report.schema_violations += 1
                    report.details.append(f"{path}: missing required section {heading}")
            evidence_body = _section_body(body, "## Evidence log")
            evidence_leading = _leading_blank_lines(evidence_body)
            if evidence_body or _has_section(body, "## Evidence log"):
                if evidence_leading > 2:
                    report.failing_pages += 1
                    report.schema_violations += 1
                    report.details.append(f"{path}: excessive blank lines under ## Evidence log ({evidence_leading})")
            if page_type == "stance":
                position = str(fm.get("position") or "").strip()
                if not position or position.lower() == "null":
                    report.failing_pages += 1
                    report.schema_violations += 1
                    report.details.append(f"{path}: stance pages must have non-empty position")
            if page_type == "inquiry":
                question = str(fm.get("question") or "").strip()
                title = str(fm.get("title") or path.stem).strip()
                if not question:
                    report.failing_pages += 1
                    report.schema_violations += 1
                    report.details.append(f"{path}: inquiry pages must have non-empty question")
                elif question == title:
                    report.failing_pages += 1
                    report.schema_violations += 1
                    report.details.append(f"{path}: inquiry question should not simply duplicate the title")
        body_links = WIKILINK_RE.findall(body)
        frontmatter_links = _collect_wikilinks(fm)
        page_links[path] = body_links + frontmatter_links

    # Broken-link detection
    # Build a slug-name index from filenames + ids
    valid_targets: set[str] = set(all_page_ids)
    for p in all_page_paths:
        valid_targets.add(p.stem)

    for path, links in page_links.items():
        for link in links:
            target = link.strip()
            if target and target not in valid_targets:
                report.broken_links += 1
                report.details.append(f"{path}: broken wikilink [[{target}]]")

    # Orphan detection — pages with no inbound links and no `relates_to` parent.
    #
    # Subtask 11b (deferred from Plan 02 Task 11): system files and the
    # inbox append-only log are NOT content pages. Exclude them from orphan
    # detection to eliminate false positives. System files are already
    # filtered by _walk_pages via is_system_file(); this block adds the
    # inbox/ directory skip and is defensive about system files in case
    # _walk_pages is ever bypassed.
    ORPHAN_SKIP_DIRS = {"inbox", "digests"}
    ORPHAN_SKIP_NAMES = {"timeline.md"}

    referenced: set[str] = set()
    for links in page_links.values():
        for link in links:
            referenced.add(link.strip())

    for path in all_page_paths:
        # Defensive: system files should already be filtered by _walk_pages.
        if is_system_file(path):
            continue
        try:
            rel_parts = path.relative_to(vault.wiki).parts
        except ValueError:
            rel_parts = path.parts
        if any(part in ORPHAN_SKIP_DIRS for part in rel_parts):
            continue  # inbox is exempt
        if path.name in ORPHAN_SKIP_NAMES:
            continue
        if path.stem not in referenced:
            text = path.read_text()
            fm = _parse_frontmatter(text) or {}
            relates = fm.get("relates_to") or []
            if not relates:
                report.orphans += 1

    if report.failing_pages > 0 or report.broken_links > 0:
        report.exit_code = 1

    return report


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Lint a Brain vault")
    parser.add_argument("vault_root", nargs="?", default=".", type=Path)
    args = parser.parse_args(argv)

    vault = Vault.load(args.vault_root.resolve())
    report = run(vault)
    print(report.summary())
    if report.details and "-v" in sys.argv:
        for line in report.details[:50]:
            print(f"  {line}")
    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
