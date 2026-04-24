from __future__ import annotations

import pytest
from pydantic import ValidationError

from mind.dream.v2.contracts import KeneRenderPackage


def test_kene_render_package_targets_reviewable_map_path() -> None:
    package = KeneRenderPackage(
        run_id="run-1",
        markdown_target_path="memory/dreams/kene/2026-04-24-run-1.md",
        title="Kene Map 2026-04-24",
        sections=[
            {
                "heading": "Groups",
                "items": [{"group_id": "group-meta-concept", "member_atom_ids": ["local-first-systems"]}],
            }
        ],
    )

    assert package.stage == "kene"
    assert package.markdown_target_path.startswith("memory/dreams/kene/")
    assert package.sections[0]["heading"] == "Groups"


def test_kene_render_package_rejects_absolute_or_parent_paths() -> None:
    with pytest.raises(ValidationError):
        KeneRenderPackage(
            run_id="run-1",
            markdown_target_path="/tmp/kene.md",
            title="Bad",
        )
    with pytest.raises(ValidationError):
        KeneRenderPackage(
            run_id="run-1",
            markdown_target_path="memory/dreams/../kene.md",
            title="Bad",
        )
