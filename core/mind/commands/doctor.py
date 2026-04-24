from __future__ import annotations

import argparse

from scripts.common import env
from mind.services.llm_validation import validate_routed_llm

from .common import project_root, vault
from .config import config_path


def cmd_doctor(_args: argparse.Namespace) -> int:
    issues: list[str] = []
    warnings: list[str] = []
    root = project_root()
    v = vault()
    print(f"Project root: {root}")
    print(f"Config path: {config_path()}")
    print(f"Wiki path: {v.wiki}")
    print(f"Raw path: {v.raw}")
    print(f"Runtime DB: {v.runtime_db}")
    try:
        cfg = env.load()
    except Exception as exc:
        issues.append(str(exc))
        cfg = None
    if cfg is not None:
        print(f"LLM base/default route model: {cfg.llm_model}")
        print(f"LLM transport: {getattr(cfg, 'llm_transport_mode', 'ai_gateway')}")
        print(f"AI Gateway key present: {bool(getattr(cfg, 'ai_gateway_api_key', ''))}")
        print(f"Substack cookie present: {bool(cfg.substack_session_cookie)}")
        validation = validate_routed_llm(cfg)
        print(f"LLM routing valid: {validation.ok}")
        for message in validation.errors:
            issues.append(message)
        for message in validation.warnings:
            warnings.append(message)
    if not v.wiki.exists():
        issues.append(f"wiki directory missing at {v.wiki}")
    if not v.raw.exists():
        warnings.append(f"raw directory missing at {v.raw}")
    if not (root / ".claude" / "commands").exists():
        warnings.append("wrapper directory .claude/commands is missing")
    if issues:
        print("\nIssues:")
        for issue in issues:
            print(f"- {issue}")
    if warnings:
        print("\nWarnings:")
        for warning in warnings:
            print(f"- {warning}")
    return 1 if issues else 0
