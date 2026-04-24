"""Runtime page-type registry derived from the machine-readable contract."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scripts.common.contract import canonicalize_page_type, page_types


@dataclass
class PageType:
    name: str
    required_fields: list[str]
    optional_fields: list[str]


KNOWN_PAGE_TYPES: dict[str, PageType] = {
    name: PageType(
        name=name,
        required_fields=list(spec.get("required_fields") or []),
        optional_fields=list(spec.get("optional_fields") or []),
    )
    for name, spec in page_types().items()
}


SYSTEM_FILE_NAMES = {
    "CHANGELOG.md", "INDEX.md", ".lint-report.md", ".lint-report.json",
    ".brain-state.json", ".brain-lock",
}


def is_system_file(path: Path) -> bool:
    return path.name in SYSTEM_FILE_NAMES or path.name.startswith(".lint-report")


def resolve_page_type(page_type: str) -> str:
    """Return the canonical type name, accepting legacy aliases during migration."""
    return canonicalize_page_type(page_type)
