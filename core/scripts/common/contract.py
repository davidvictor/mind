"""Machine-readable contract loader for phase-0 architecture freeze.

This centralizes canonical type names, tag vocabularies, legacy aliases,
and skill metadata so runtime code stops drifting away from the docs.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from scripts.common.vault import project_root

CONTRACT_PATH = project_root() / "contracts" / "brain-contract.yaml"
_ATOM_COLLECTION_DIRS = {
    "concept": "concepts",
    "playbook": "playbooks",
    "stance": "stances",
    "inquiry": "inquiries",
}


@lru_cache(maxsize=1)
def load_contract() -> dict[str, Any]:
    return yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8")) or {}


def canonical_family() -> list[str]:
    return list(load_contract().get("canonical_family") or [])


def page_types() -> dict[str, dict[str, Any]]:
    return dict(load_contract().get("page_types") or {})


def legacy_type_aliases() -> dict[str, str]:
    return dict(load_contract().get("legacy_type_aliases") or {})


def canonicalize_page_type(page_type: str) -> str:
    return legacy_type_aliases().get(page_type, page_type)


def atom_collection_dirs() -> dict[str, str]:
    return dict(_ATOM_COLLECTION_DIRS)


def atom_collection_dir(page_type: str) -> str:
    canonical = canonicalize_page_type(page_type)
    if canonical not in _ATOM_COLLECTION_DIRS:
        raise KeyError(f"Unknown atom collection for page type {page_type!r}")
    return _ATOM_COLLECTION_DIRS[canonical]


def tag_taxonomy() -> dict[str, Any]:
    return dict(load_contract().get("tag_taxonomy") or {})


def default_tag_triples() -> dict[str, list[str]]:
    return {
        key: list(value)
        for key, value in (load_contract().get("default_tag_triples") or {}).items()
    }


def skill_metadata() -> dict[str, Any]:
    return dict(load_contract().get("skill_metadata") or {})
