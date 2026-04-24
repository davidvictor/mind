from __future__ import annotations

import argparse

from mind.services.llm_service import get_llm_service
from scripts.common.skill_writer import SkillArtifact, write_skill
from scripts.common.slugify import slugify

from .common import project_root, today_str


def cmd_skill_generate(args: argparse.Namespace) -> int:
    body = get_llm_service().generate_skill(
        task_description=args.prompt,
        context_text=args.context or "",
    )
    if args.stdout or not args.name:
        print(body)
        return 0
    skill = SkillArtifact(
        name=slugify(args.name) or "generated-skill",
        description=args.description or args.prompt[:120],
        title=args.name,
        status="active",
        created=today_str(),
        last_updated=today_str(),
        tags=["skill", "generated"],
        domains=["work"],
        body=body,
    )
    target = write_skill(project_root(), skill, force=args.force)
    print(target)
    return 0

