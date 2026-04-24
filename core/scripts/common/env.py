"""Centralized config loading for the Brain runtime.

Reads .env from the app root once, then resolves the active memory root from
`config.yaml` so existing pipeline code can stay mostly vault-root-oriented
during the split transition.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from scripts.common.config import BrainConfig
from scripts.common.vault import Vault, project_root

APP_ROOT = project_root()
load_dotenv(APP_ROOT / ".env")


@dataclass(frozen=True)
class Config:
    llm_model: str
    llm_transport_mode: str
    llm_routes: dict[str, dict[str, object]]
    llm_backup: dict[str, object] | None
    llm_min_balance_usd: float
    llm_concurrency: dict[str, int]
    ai_gateway_api_key: str
    browser_for_cookies: str
    repo_root: Path
    app_root: Path
    active_config_path: str
    wiki_root: Path
    raw_root: Path
    substack_session_cookie: str

def load() -> Config:
    brain_cfg = BrainConfig.load(APP_ROOT)
    vault = Vault(root=APP_ROOT, config=brain_cfg)
    default_route = brain_cfg.llm.routes.get("default")
    model = (default_route.model if default_route and default_route.model else brain_cfg.llm.model).strip()
    transport_mode = brain_cfg.llm.transport.mode
    ai_gateway_api_key = os.environ.get("AI_GATEWAY_API_KEY", "").strip()
    llm_routes = {
        name: route.model_dump(mode="json", exclude_none=True)
        for name, route in brain_cfg.llm.routes.items()
    }
    llm_backup = brain_cfg.llm.backup.model_dump(mode="json", exclude_none=True) if brain_cfg.llm.backup else None
    return Config(
        llm_model=model,
        llm_transport_mode=transport_mode,
        llm_routes=llm_routes,
        llm_backup=llm_backup,
        llm_min_balance_usd=brain_cfg.llm.min_balance_usd,
        llm_concurrency={key: int(value) for key, value in brain_cfg.llm.concurrency.items()},
        ai_gateway_api_key=ai_gateway_api_key,
        browser_for_cookies=os.environ.get("BROWSER_FOR_COOKIES", "chrome").strip(),
        repo_root=APP_ROOT,
        app_root=APP_ROOT,
        active_config_path=BrainConfig.describe_active_config(APP_ROOT),
        wiki_root=vault.wiki,
        raw_root=vault.raw,
        substack_session_cookie=os.environ.get("SUBSTACK_SESSION_COOKIE", "").strip(),
    )
