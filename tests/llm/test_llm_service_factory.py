from __future__ import annotations

from pathlib import Path

from mind.services.llm_service import get_llm_service
from mind.services.providers.gateway import GatewayProviderClient


class _FakeEnv:
    def __init__(
        self,
        *,
        provider: str,
        model: str | None = None,
        repo_root: Path,
    ):
        self.llm_provider = provider
        self.llm_model = model or {
            "gemini": "google/gemini-3.1-flash-lite-preview",
            "openai": "openai/gpt-5.4",
            "anthropic": "anthropic/claude-sonnet-4.6",
        }[provider]
        self.llm_transport_mode = "ai_gateway"
        self.gemini_api_key = "gemini-key"
        self.openai_api_key = "openai-key"
        self.anthropic_api_key = "anthropic-key"
        self.ai_gateway_api_key = "gateway-key"
        self.repo_root = repo_root
        self.app_root = repo_root
        self.browser_for_cookies = "chrome"
        self.substack_session_cookie = ""


def test_get_llm_service_selects_openai_provider(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _FakeEnv(provider="openai", repo_root=tmp_path),
    )
    service = get_llm_service()
    assert service.runtime.provider == "openai"
    assert isinstance(service.provider_client, GatewayProviderClient)


def test_get_llm_service_selects_anthropic_provider(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _FakeEnv(provider="anthropic", repo_root=tmp_path),
    )
    service = get_llm_service()
    assert service.runtime.provider == "anthropic"
    assert isinstance(service.provider_client, GatewayProviderClient)


def test_get_llm_service_selects_gemini_provider(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _FakeEnv(provider="gemini", repo_root=tmp_path),
    )
    service = get_llm_service()
    assert service.runtime.provider == "gemini"
    assert isinstance(service.provider_client, GatewayProviderClient)
