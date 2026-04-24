from __future__ import annotations

from types import SimpleNamespace

from mind.services.llm_routing import resolve_backup, resolve_route
from mind.services.llm_validation import validate_routed_llm


def _cfg(routes: dict[str, dict[str, object]] | None = None, backup: dict[str, object] | None = None):
    return SimpleNamespace(
        llm_model="google/gemini-2.5-pro",
        llm_routes=routes or {},
        llm_backup=backup,
        ai_gateway_api_key="gateway-key",
    )


def test_policy_defaults_use_strict_anthropic_routes_for_onboarding() -> None:
    cfg = _cfg()
    synth = resolve_route("onboarding_synthesis", runtime_cfg=cfg)
    merge = resolve_route("onboarding_merge", runtime_cfg=cfg)
    verify = resolve_route("onboarding_verify", runtime_cfg=cfg)

    assert synth.model == "anthropic/claude-haiku-4.5"
    assert merge.model == "anthropic/claude-haiku-4.5"
    assert verify.model == "anthropic/claude-sonnet-4.6"
    assert synth.supports_strict_schema is True
    assert merge.supports_strict_schema is True
    assert verify.supports_strict_schema is True


def test_policy_backups_apply_when_shared_backup_missing() -> None:
    cfg = _cfg()
    backup = resolve_backup(runtime_cfg=cfg, task_class="onboarding_synthesis")
    assert backup is not None
    assert backup.model == "anthropic/claude-sonnet-4.6"


def test_dream_pass_d_can_route_to_flash_lite_with_sonnet_backup() -> None:
    cfg = _cfg(
        routes={
            "dream_pass_d": {"model": "google/gemini-3.1-flash-lite-preview"},
        }
    )
    route = resolve_route("dream_pass_d", runtime_cfg=cfg)
    backup = resolve_backup(runtime_cfg=cfg, task_class="dream_pass_d")

    assert route.model == "google/gemini-3.1-flash-lite-preview"
    assert route.provider == "gemini"
    assert backup is not None
    assert backup.model == "anthropic/claude-sonnet-4.6"


def test_gemini_policy_routes_are_non_strict() -> None:
    cfg = _cfg()
    classification = resolve_route("classification", runtime_cfg=cfg)
    research = resolve_route("research", runtime_cfg=cfg)
    assert classification.provider == "gemini"
    assert classification.supports_strict_schema is False
    assert research.provider == "gemini"
    assert research.supports_strict_schema is False


def test_dream_v2_route_families_default_to_strict_anthropic_routes() -> None:
    cfg = _cfg()
    signal = resolve_route("dream_signal", runtime_cfg=cfg)
    decision = resolve_route("dream_decision", runtime_cfg=cfg)
    writer = resolve_route("dream_writer", runtime_cfg=cfg)
    reflection = resolve_route("dream_reflection", runtime_cfg=cfg)

    assert signal.model == "anthropic/claude-haiku-4.5"
    assert decision.model == "anthropic/claude-sonnet-4.6"
    assert writer.model == "anthropic/claude-sonnet-4.6"
    assert reflection.model == "anthropic/claude-sonnet-4.6"
    assert signal.supports_strict_schema is True
    assert decision.supports_strict_schema is True
    assert writer.supports_strict_schema is True
    assert reflection.supports_strict_schema is True


def test_explicit_route_can_force_strict_schema_on_gemini_model() -> None:
    cfg = _cfg(
        routes={
            "dream_decision": {
                "model": "google/gemini-3.1-flash-lite-preview",
                "supports_strict_schema": True,
            },
        }
    )

    route = resolve_route("dream_decision", runtime_cfg=cfg)

    assert route.model == "google/gemini-3.1-flash-lite-preview"
    assert route.supports_strict_schema is True


def test_explicit_route_can_disable_strict_schema_on_openai_model() -> None:
    cfg = _cfg(
        routes={
            "dream_writer": {
                "model": "openai/gpt-5.4-mini",
                "supports_strict_schema": False,
            },
        }
    )

    route = resolve_route("dream_writer", runtime_cfg=cfg)

    assert route.model == "openai/gpt-5.4-mini"
    assert route.supports_strict_schema is False


def test_route_surfaces_gateway_provider_and_generation_knobs() -> None:
    cfg = _cfg(
        routes={
            "dream_decision": {
                "model": "openai/gpt-5.4-mini",
                "supports_strict_schema": True,
                "top_p": 0.2,
                "truncation": "disabled",
                "gateway_options": {"only": ["vertex"]},
                "provider_options": {"openai": {"reasoningEffort": "high"}},
            },
        }
    )

    route = resolve_route("dream_decision", runtime_cfg=cfg)

    assert route.top_p == 0.2
    assert route.truncation == "disabled"
    assert route.gateway_options == {"only": ["vertex"]}
    assert route.provider_options == {"openai": {"reasoningEffort": "high"}}


def test_validate_routed_llm_warns_when_override_points_onboarding_to_gemini(monkeypatch) -> None:
    monkeypatch.setattr(
        "mind.services.llm_validation._validate_model_name",
        lambda provider, model, api_key, gateway_api_key="": ("ok", None),
    )
    report = validate_routed_llm(
        _cfg(
            routes={
                "onboarding_merge": {
                    "model": "google/gemini-3.1-pro-preview",
                    "supports_strict_schema": False,
                },
            }
        )
    )
    assert any("onboarding_merge" in warning and "without strict-schema support" in warning for warning in report.warnings)
