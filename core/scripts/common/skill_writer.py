"""Helpers for rendering root-level skill artifacts.

Phase 4 does not implement REM itself. It provides the artifact primitive that
future REM work can call to generate root `skills/<id>/SKILL.md` files in the
required minimum shape.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class SkillArtifact:
    name: str
    description: str
    title: str
    status: str
    created: str
    last_updated: str
    tags: list[str]
    domains: list[str]
    relates_to: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    aliases: list[str] = field(default_factory=list)
    artifact_id: str | None = None
    body: str = ""

    @property
    def id(self) -> str:
        return self.artifact_id or self.name


def _single_line(text: str) -> str:
    return " ".join(text.split())


def _yaml_list(items: list[str]) -> str:
    if not items:
        return " []"
    return "\n" + "\n".join(f"  - {item}" for item in items)


def render_skill_markdown(skill: SkillArtifact) -> str:
    """Render a skill artifact with the required frontmatter ordering."""
    description = _single_line(skill.description)
    body = skill.body.rstrip()
    lines = [
        "---",
        f"name: {skill.name}",
        f"description: {description}",
        f"id: {skill.id}",
        "type: skill",
        f"title: {skill.title}",
        f"status: {skill.status}",
        f"created: {skill.created}",
        f"last_updated: {skill.last_updated}",
        f"aliases:{_yaml_list(skill.aliases)}",
        f"tags:{_yaml_list(skill.tags)}",
        f"domains:{_yaml_list(skill.domains)}",
        f"relates_to:{_yaml_list(skill.relates_to)}",
        f"sources:{_yaml_list(skill.sources)}",
        "---",
        "",
        f"# {skill.title}",
        "",
    ]
    if body:
        lines.append(body)
    else:
        lines.append("Instructions Claude follows when this skill is invoked.")
    return "\n".join(lines).rstrip() + "\n"


def write_skill(root: Path, skill: SkillArtifact, *, force: bool = False) -> Path:
    """Write `skills/<id>/SKILL.md` under the given project root."""
    target = root / "skills" / skill.id / "SKILL.md"
    if target.exists() and not force:
        raise FileExistsError(f"Refusing to overwrite {target} (use force=True)")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_skill_markdown(skill), encoding="utf-8")
    return target
