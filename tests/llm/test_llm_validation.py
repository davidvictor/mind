from __future__ import annotations

import sys
import types
from types import SimpleNamespace

from mind.services.llm_validation import _validate_model_name, validate_routed_llm


def test_validate_routed_llm_accepts_gateway_key_for_anthropic_routes(monkeypatch):
    monkeypatch.setattr(
        "mind.services.llm_validation._validate_model_name",
        lambda provider, model, api_key, gateway_api_key="": ("ok", None),
    )
    cfg = SimpleNamespace(
        llm_provider="anthropic",
        llm_model="claude-sonnet-4.6",
        llm_routes={},
        llm_backup=None,
        ai_gateway_api_key="gateway-key",
        gemini_api_key="",
        openai_api_key="",
        anthropic_api_key="",
    )

    report = validate_routed_llm(cfg)

    assert report.ok is True
    assert report.errors == []
    assert report.routes["default"]["provider"] == "anthropic"


def test_validate_model_name_uses_gateway_endpoint_for_anthropic(monkeypatch):
    class FakeModels:
        def list(self):
            return types.SimpleNamespace(data=[types.SimpleNamespace(id="anthropic/claude-test")])

    class FakeOpenAI:
        def __init__(self, *, api_key: str, base_url: str):
            assert api_key == "gateway-key"
            assert base_url == "https://ai-gateway.vercel.sh/v1"
            self.models = FakeModels()

    monkeypatch.setattr("openai.OpenAI", FakeOpenAI)

    status, message = _validate_model_name(
        "anthropic",
        "claude-test",
        "gateway-key",
        gateway_api_key="gateway-key",
    )

    assert status == "ok"
    assert message is None


def test_validate_model_name_uses_gateway_endpoint_for_gemini(monkeypatch):
    class FakeModels:
        def list(self):
            return types.SimpleNamespace(data=[types.SimpleNamespace(id="google/gemini-3.1-flash-lite-preview")])

    class FakeOpenAI:
        def __init__(self, *, api_key: str, base_url: str):
            assert api_key == "gateway-key"
            assert base_url == "https://ai-gateway.vercel.sh/v1"
            self.models = FakeModels()

    monkeypatch.setattr("openai.OpenAI", FakeOpenAI)

    status, message = _validate_model_name(
        "gemini",
        "google/gemini-3.1-flash-lite-preview",
        "gateway-key",
        gateway_api_key="gateway-key",
    )

    assert status == "ok"
    assert message is None


def test_validate_routed_llm_reports_explicit_sources_for_all_phase1_task_classes(monkeypatch):
    monkeypatch.setattr(
        "mind.services.llm_validation._validate_model_name",
        lambda provider, model, api_key, gateway_api_key="": ("ok", None),
    )
    cfg = SimpleNamespace(
        llm_provider="anthropic",
        llm_model="anthropic/claude-sonnet-4.6",
        llm_routes={
            "default": {"model": "anthropic/claude-sonnet-4.6"},
            "classification": {"model": "google/gemini-3.1-flash-lite-preview"},
            "transcription": {"model": "google/gemini-3.1-pro-preview"},
            "document": {"model": "google/gemini-3.1-pro-preview"},
            "research": {"model": "google/gemini-3.1-pro-preview"},
            "summary": {"model": "anthropic/claude-sonnet-4.6"},
            "personalization": {"model": "anthropic/claude-sonnet-4.6"},
            "stance": {"model": "anthropic/claude-sonnet-4.6"},
            "dream": {"model": "anthropic/claude-sonnet-4.6"},
            "onboarding_synthesis": {"model": "anthropic/claude-sonnet-4.6"},
            "onboarding_merge": {"model": "anthropic/claude-sonnet-4.6"},
            "onboarding_verify": {"model": "anthropic/claude-sonnet-4.6"},
        },
        llm_backup=None,
        ai_gateway_api_key="gateway-key",
        gemini_api_key="",
        openai_api_key="",
        anthropic_api_key="",
    )

    report = validate_routed_llm(cfg)

    assert report.ok is True
    assert set(report.routes) == {
        "default",
        "embedding",
        "classification",
        "transcription",
        "document",
        "research",
        "summary",
        "personalization",
        "stance",
        "dream",
        "dream_pass_d",
        "dream_signal",
        "dream_decision",
        "dream_writer",
        "dream_reflection",
        "onboarding_synthesis",
        "onboarding_merge",
        "onboarding_verify",
        "onboarding_materialization",
    }
    explicit_routes = {
        "default",
        "embedding",
        "classification",
        "transcription",
        "document",
        "research",
        "summary",
        "personalization",
        "stance",
        "dream",
        "onboarding_synthesis",
        "onboarding_merge",
        "onboarding_verify",
    }
    for name, route in report.routes.items():
        if name in explicit_routes:
            assert route["source"] == "explicit"
        elif name in {"dream_signal", "dream_decision", "dream_writer", "dream_reflection"}:
            assert route["source"] == "policy-default"
        else:
            assert route["source"] == "inherited-from-default"


def test_validate_routed_llm_warns_when_onboarding_route_disables_strict_schema(monkeypatch):
    monkeypatch.setattr(
        "mind.services.llm_validation._validate_model_name",
        lambda provider, model, api_key, gateway_api_key="": ("ok", None),
    )
    cfg = SimpleNamespace(
        llm_provider="anthropic",
        llm_model="anthropic/claude-sonnet-4.6",
        llm_routes={
            "default": {"model": "anthropic/claude-sonnet-4.6"},
            "onboarding_verify": {
                "model": "google/gemini-3.1-pro-preview",
                "supports_strict_schema": False,
            },
        },
        llm_backup=None,
        ai_gateway_api_key="gateway-key",
        gemini_api_key="",
        openai_api_key="",
        anthropic_api_key="",
    )

    report = validate_routed_llm(cfg)

    assert report.ok is True
    assert any("onboarding_verify" in warning and "without strict-schema support" in warning for warning in report.warnings)
    assert report.routes["onboarding_verify"]["supports_strict_schema"] is False


def test_validate_routed_llm_warns_when_dream_route_disables_strict_schema(monkeypatch):
    monkeypatch.setattr(
        "mind.services.llm_validation._validate_model_name",
        lambda provider, model, api_key, gateway_api_key="": ("ok", None),
    )
    cfg = SimpleNamespace(
        llm_provider="anthropic",
        llm_model="anthropic/claude-sonnet-4.6",
        llm_routes={
            "default": {"model": "anthropic/claude-sonnet-4.6"},
            "dream_decision": {
                "model": "google/gemini-3.1-flash-lite-preview",
                "supports_strict_schema": False,
            },
        },
        llm_backup=None,
        ai_gateway_api_key="gateway-key",
        gemini_api_key="",
        openai_api_key="",
        anthropic_api_key="",
    )

    report = validate_routed_llm(cfg)

    assert report.ok is True
    assert any("dream_decision" in warning and "without strict-schema support" in warning for warning in report.warnings)
    assert report.routes["dream_decision"]["supports_strict_schema"] is False
