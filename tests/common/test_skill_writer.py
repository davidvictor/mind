from __future__ import annotations

from pathlib import Path

from scripts.common.skill_writer import SkillArtifact, render_skill_markdown, write_skill
from tests.paths import SKILLS_ROOT


def _sample_skill() -> SkillArtifact:
    return SkillArtifact(
        name="artifact-smith",
        description="Creates skill artifacts from structured metadata\nand repeated workflow evidence",
        title="Artifact Smith",
        status="draft",
        created="2026-04-08",
        last_updated="2026-04-08",
        tags=["domain/meta", "function/skill", "signal/working"],
        domains=["meta"],
        relates_to=['"[[weekly-review-loop]]"'],
        sources=['"[[summary-example-seed]]"'],
        body="Instructions Claude follows when this skill is invoked.\n\n## Purpose\n\nCreate a compliant skill artifact.",
    )


def test_render_skill_markdown_starts_with_name_and_description():
    md = render_skill_markdown(_sample_skill()).splitlines()
    assert md[:4] == [
        "---",
        "name: artifact-smith",
        "description: Creates skill artifacts from structured metadata and repeated workflow evidence",
        "id: artifact-smith",
    ]


def test_render_skill_markdown_has_title_heading_and_body():
    md = render_skill_markdown(_sample_skill())
    assert "\n# Artifact Smith\n" in md
    assert "Instructions Claude follows when this skill is invoked." in md
    assert "## Purpose" in md


def test_write_skill_writes_to_root_skills_folder(tmp_path: Path):
    target = write_skill(tmp_path, _sample_skill())
    assert target == tmp_path / "skills" / "artifact-smith" / "SKILL.md"
    assert target.exists()


def test_default_skill_creator_matches_required_minimum_shape():
    path = SKILLS_ROOT / "skill-creator" / "SKILL.md"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[:4] == [
        "---",
        "name: skill-creator",
        "description: Creates or updates reusable agent skills from repeated workflows or explicit user requests",
        "id: skill-creator",
    ]
