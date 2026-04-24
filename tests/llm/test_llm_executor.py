from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path
import time

import pytest

from mind.services.llm_executor import LLMExecutor
from mind.services.providers.base import LLMProviderResponse
from mind.services.providers.base import LLMConfigurationError, LLMInputPart, LLMStrictSchemaError, LLMTimeoutError


def _fake_env(*, routes: dict[str, dict[str, object]], backup: dict[str, object] | None):
    return SimpleNamespace(
        llm_provider="gemini",
        llm_model="google/gemini-3.1-flash-lite-preview",
        llm_transport_mode="ai_gateway",
        llm_routes=routes,
        llm_backup=backup,
        ai_gateway_api_key="gateway-key",
        repo_root=".",
        app_root=".",
        browser_for_cookies="chrome",
        substack_session_cookie="",
    )


@pytest.fixture(autouse=True)
def _stub_log_event(monkeypatch):
    events: list[dict[str, object]] = []

    def fake_log_event(repo_root, **kwargs):
        events.append({"repo_root": repo_root, **kwargs})

    monkeypatch.setattr("mind.services.llm_executor.log_event", fake_log_event)
    return events


def test_executor_fails_over_to_shared_backup_on_primary_structured_output_failure(monkeypatch, _stub_log_event):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "default": {"model": "google/gemini-default"},
                "summary": {"model": "openai/gpt-test"},
            },
            backup={"model": "google/gemini-backup"},
        ),
    )
    calls: list[tuple[str, str]] = []

    def fake_gateway_execute(self, request):
        calls.append((request.provider, request.model))
        if request.model == "openai/gpt-test":
            return "not-json"
        return '{"category":"business"}'

    monkeypatch.setattr("mind.services.providers.gateway.GatewayProviderClient.execute", fake_gateway_execute)

    result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
        task_class="summary",
        prompt="Output JSON only",
        prompt_version="summary.test.v1",
    )

    assert result.data == {"category": "business"}
    assert calls == [("openai", "openai/gpt-test"), ("openai", "openai/gpt-test"), ("gemini", "google/gemini-backup")]
    assert result.cache_identity.provider == "gemini"
    assert result.cache_identity.model == "google/gemini-backup"
    assert [event["attempt_role"] for event in _stub_log_event] == ["primary", "primary-retry", "backup"]


def test_executor_uses_policy_backup_for_research_when_primary_keeps_misformatting(monkeypatch, _stub_log_event):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "research": {"model": "google/gemini-3.1-pro-preview"},
            },
            backup=None,
        ),
    )
    calls: list[str] = []

    def fake_gateway_execute(self, request):
        calls.append(request.model)
        if request.model == "google/gemini-3.1-pro-preview":
            return "Not JSON"
        return '{"category":"business"}'

    monkeypatch.setattr("mind.services.providers.gateway.GatewayProviderClient.execute", fake_gateway_execute)

    result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
        task_class="research",
        prompt="Output JSON only",
        prompt_version="research.test.v1",
    )

    assert result.data == {"category": "business"}
    assert calls == [
        "google/gemini-3.1-pro-preview",
        "google/gemini-3.1-pro-preview",
        "anthropic/claude-haiku-4.5",
    ]
    assert result.cache_identity.provider == "anthropic"
    assert result.cache_identity.model == "anthropic/claude-haiku-4.5"
    assert [event["attempt_role"] for event in _stub_log_event] == ["primary", "primary-retry", "backup"]


def test_executor_does_not_retry_or_fail_over_on_invalid_config(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={"summary": {"model": "cohere/command-r"}},
            backup={"model": "google/gemini-backup"},
        ),
    )

    with pytest.raises(LLMConfigurationError):
        LLMExecutor(sleeper=lambda _seconds: None).execute_json(
            task_class="summary",
            prompt="Output JSON only",
            prompt_version="summary.test.v1",
        )


def test_executor_uses_gateway_client_for_anthropic_model(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: SimpleNamespace(
            llm_provider="anthropic",
            llm_model="anthropic/claude-sonnet-4.6",
            llm_transport_mode="ai_gateway",
            llm_routes={"default": {"provider": "anthropic", "model": "anthropic/claude-sonnet-4.6"}},
            llm_backup=None,
            ai_gateway_api_key="gateway-key",
            repo_root=".",
            app_root=".",
            browser_for_cookies="chrome",
            substack_session_cookie="",
        ),
    )
    captured: dict[str, str] = {}

    class FakeGatewayProviderClient:
        def __init__(self, *, api_key: str, model: str):
            captured["api_key"] = api_key
            captured["model"] = model

        def execute(self, request):
            captured["provider"] = request.provider
            return LLMProviderResponse(text='{"category":"personal"}', metadata={"generation_id": "gen_123"})

    monkeypatch.setattr("mind.services.llm_executor.GatewayProviderClient", FakeGatewayProviderClient)

    result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
        task_class="default",
        prompt="Output JSON only",
        prompt_version="default.test.v1",
    )

    assert result.data == {"category": "personal"}
    assert result.response_metadata["generation_id"] == "gen_123"
    assert captured == {
        "api_key": "gateway-key",
        "model": "anthropic/claude-sonnet-4.6",
        "provider": "anthropic",
    }


def test_executor_uses_gateway_client_for_gemini_model(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: SimpleNamespace(
            llm_provider="gemini",
            llm_model="google/gemini-3.1-flash-lite-preview",
            llm_transport_mode="ai_gateway",
            llm_routes={"default": {"provider": "gemini", "model": "google/gemini-3.1-flash-lite-preview"}},
            llm_backup=None,
            ai_gateway_api_key="gateway-key",
            repo_root=".",
            app_root=".",
            browser_for_cookies="chrome",
            substack_session_cookie="",
        ),
    )
    captured: dict[str, str] = {}

    class FakeGatewayProviderClient:
        def __init__(self, *, api_key: str, model: str):
            captured["api_key"] = api_key
            captured["model"] = model

        def execute(self, request):
            captured["provider"] = request.provider
            return LLMProviderResponse(text='{"category":"personal"}', metadata={})

    monkeypatch.setattr("mind.services.llm_executor.GatewayProviderClient", FakeGatewayProviderClient)

    result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
        task_class="default",
        prompt="Output JSON only",
        prompt_version="default.test.v1",
    )

    assert result.data == {"category": "personal"}
    assert captured == {
        "api_key": "gateway-key",
        "model": "google/gemini-3.1-flash-lite-preview",
        "provider": "gemini",
    }


def test_cache_identities_for_parts_diverge_by_multimodal_inputs_and_metadata(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "transcription": {"model": "google/gemini-3.1-pro-preview"},
            },
            backup=None,
        ),
    )
    executor = LLMExecutor(sleeper=lambda _seconds: None)

    url_identity = executor.cache_identities_for_parts(
        task_class="transcription",
        instructions="Transcribe this video",
        input_parts=(LLMInputPart.url_part("https://example.com/watch?v=abc"),),
        prompt_version="transcription.test.v1",
        input_mode="media",
        request_metadata={"source_kind": "youtube", "variant": "url-only"},
    )[0]
    audio_identity = executor.cache_identities_for_parts(
        task_class="transcription",
        instructions="Transcribe this video",
        input_parts=(
            LLMInputPart.url_part("https://example.com/watch?v=abc"),
            LLMInputPart.audio_part(b"audio-bytes", mime_type="audio/mpeg", file_name="clip.mp3"),
        ),
        prompt_version="transcription.test.v1",
        input_mode="media",
        request_metadata={"source_kind": "youtube", "variant": "url-plus-audio"},
    )[0]
    document_identity = executor.cache_identities_for_parts(
        task_class="transcription",
        instructions="Extract this document",
        input_parts=(LLMInputPart.pdf_part(b"%PDF-1.7", file_name="chapter.pdf"),),
        prompt_version="transcription.test.v1",
        input_mode="file",
        request_metadata={"source_kind": "book", "variant": "pdf"},
    )[0]

    assert url_identity.request_fingerprint != audio_identity.request_fingerprint
    assert url_identity.request_fingerprint != document_identity.request_fingerprint
    assert audio_identity.request_fingerprint != document_identity.request_fingerprint
    assert url_identity.input_mode == "media"
    assert document_identity.input_mode == "file"


def test_build_prompt_request_keeps_schema_when_route_supports_strict_schema(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "dream_decision": {
                    "model": "openai/gpt-5.4-mini",
                    "supports_strict_schema": True,
                },
            },
            backup=None,
        ),
    )

    request = LLMExecutor().build_prompt_request(
        task_class="dream_decision",
        prompt="Output JSON only",
        output_mode="json",
        response_schema={"type": "object"},
    )

    assert request.response_schema == {"type": "object"}


def test_build_prompt_request_strips_schema_when_route_disables_strict_schema(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "dream_decision": {
                    "model": "google/gemini-3.1-flash-lite-preview",
                    "supports_strict_schema": False,
                },
            },
            backup=None,
        ),
    )

    request = LLMExecutor().build_prompt_request(
        task_class="dream_decision",
        prompt="Output JSON only",
        output_mode="json",
        response_schema={"type": "object"},
    )

    assert request.response_schema is None


def test_build_prompt_request_forwards_route_generation_controls(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "dream_writer": {
                    "model": "openai/gpt-5.4-mini",
                    "supports_strict_schema": True,
                    "top_p": 0.2,
                    "truncation": "disabled",
                    "gateway_options": {"only": ["vertex"]},
                    "provider_options": {"openai": {"reasoningEffort": "high"}},
                },
            },
            backup=None,
        ),
    )

    request = LLMExecutor().build_prompt_request(
        task_class="dream_writer",
        prompt="Output JSON only",
        output_mode="json",
        response_schema={"type": "object"},
    )

    assert request.top_p == 0.2
    assert request.truncation == "disabled"
    assert request.gateway_options == {"only": ["vertex"]}
    assert request.provider_options == {"openai": {"reasoningEffort": "high"}}


def test_cache_identity_changes_when_route_provider_or_model_changes(monkeypatch):
    state = {"model": "openai/gpt-test"}

    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "summary": {"model": state["model"]},
            },
            backup=None,
        ),
    )
    executor = LLMExecutor(sleeper=lambda _seconds: None)

    openai_identity = executor.cache_identity(task_class="summary", prompt_version="summary.test.v1")
    state["model"] = "google/gemini-3.1-flash-lite-preview"
    gemini_identity = executor.cache_identity(task_class="summary", prompt_version="summary.test.v1")

    assert openai_identity.provider == "openai"
    assert gemini_identity.provider == "gemini"
    assert openai_identity.model != gemini_identity.model
    assert openai_identity.to_dict() != gemini_identity.to_dict()


def test_executor_strips_response_schema_for_non_strict_routes(monkeypatch, _stub_log_event):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "summary": {
                    "model": "google/gemini-3.1-pro-preview",
                    "supports_strict_schema": False,
                },
            },
            backup=None,
        ),
    )
    captured: list[object] = []

    def fake_gateway_execute(self, request):
        captured.append(request.response_schema)
        return '{"category":"personal"}'

    monkeypatch.setattr("mind.services.providers.gateway.GatewayProviderClient.execute", fake_gateway_execute)

    with pytest.warns(RuntimeWarning, match="without strict-schema support"):
        result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
            task_class="summary",
            prompt="Output JSON only",
            prompt_version="summary.test.v1",
            response_schema={"type": "object", "properties": {"category": {"type": "string"}}, "required": ["category"]},
        )

    assert result.data == {"category": "personal"}
    assert captured == [None]
    assert _stub_log_event[0]["attempt_role"] == "primary"
    assert _stub_log_event[0]["status"] == "success"


def test_executor_retries_once_without_schema_on_provider_schema_rejection(monkeypatch, _stub_log_event):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "summary": {"model": "openai/gpt-test"},
            },
            backup=None,
        ),
    )
    attempts: list[tuple[object, dict[str, object]]] = []

    def fake_gateway_execute(self, request):
        attempts.append((request.response_schema, dict(request.request_metadata)))
        if request.response_schema is not None:
            raise LLMStrictSchemaError("provider rejected strict json_schema payload")
        return '{"category":"business"}'

    monkeypatch.setattr("mind.services.providers.gateway.GatewayProviderClient.execute", fake_gateway_execute)

    result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
        task_class="summary",
        prompt="Output JSON only",
        prompt_version="summary.test.v1",
        response_schema={"type": "object", "properties": {"category": {"type": "string"}}, "required": ["category"]},
    )

    assert result.data == {"category": "business"}
    assert attempts == [
        (
            {"type": "object", "properties": {"category": {"type": "string"}}, "required": ["category"]},
            {},
        ),
        (
            None,
            {"attempt_role": "strict-schema-fallback"},
        ),
    ]
    assert [event["attempt_role"] for event in _stub_log_event] == ["primary", "strict-schema-fallback"]


def test_executor_logs_generation_metadata(monkeypatch, tmp_path: Path, _stub_log_event):
    cfg = _fake_env(
        routes={"summary": {"model": "openai/gpt-test"}},
        backup=None,
    )
    cfg.repo_root = tmp_path
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: cfg,
    )

    def fake_gateway_execute(self, request):
        return LLMProviderResponse(
            text='{"category":"business"}',
            metadata={
                "response_id": "resp_123",
                "generation_id": "gen_123",
                "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13},
            },
        )

    monkeypatch.setattr("mind.services.providers.gateway.GatewayProviderClient.execute", fake_gateway_execute)

    result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
        task_class="summary",
        prompt="Output JSON only",
        prompt_version="summary.test.v1",
    )

    assert result.response_metadata["generation_id"] == "gen_123"
    assert _stub_log_event[0]["generation_id"] == "gen_123"
    assert _stub_log_event[0]["tokens_in"] == 10
    assert _stub_log_event[0]["tokens_out"] == 3


def test_executor_ignores_telemetry_failures(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={"summary": {"model": "openai/gpt-test"}},
            backup=None,
        ),
    )

    monkeypatch.setattr("mind.services.llm_executor.log_event", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(
        "mind.services.providers.gateway.GatewayProviderClient.execute",
        lambda self, request: '{"category":"business"}',
    )

    result = LLMExecutor(sleeper=lambda _seconds: None).execute_json(
        task_class="summary",
        prompt="Output JSON only",
        prompt_version="summary.test.v1",
    )

    assert result.data == {"category": "business"}


def test_executor_uses_stage_specific_watchdog_timeouts(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={"summary": {"model": "openai/gpt-test"}},
            backup=None,
        ),
    )
    executor = LLMExecutor(sleeper=lambda _seconds: None)
    summary_request = executor.build_prompt_request(
        task_class="summary",
        prompt="Output JSON only",
        output_mode="json",
    )
    stance_request = executor.build_prompt_request(
        task_class="stance",
        prompt="Output JSON only",
        output_mode="json",
    )
    dream_request = executor.build_prompt_request(
        task_class="dream",
        prompt="Output JSON only",
        output_mode="json",
    )

    assert executor._wall_clock_timeout_seconds_for_request(summary_request, prompt_version="summary.test.v1") == 8 * 60
    assert executor._wall_clock_timeout_seconds_for_request(stance_request, prompt_version="stance.test.v1") == 12 * 60
    assert executor._wall_clock_timeout_seconds_for_request(dream_request, prompt_version="dream.pass-d.v3") == 8 * 60


def test_executor_caps_stage_watchdog_by_route_timeout(monkeypatch):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={
                "default": {"model": "openai/gpt-test", "timeout_seconds": 300},
            },
            backup=None,
        ),
    )
    executor = LLMExecutor(sleeper=lambda _seconds: None)
    summary_request = executor.build_prompt_request(
        task_class="summary",
        prompt="Output JSON only",
        output_mode="json",
    )
    stance_request = executor.build_prompt_request(
        task_class="stance",
        prompt="Output JSON only",
        output_mode="json",
    )

    assert executor._wall_clock_timeout_seconds_for_request(summary_request, prompt_version="summary.test.v1") == 300
    assert executor._wall_clock_timeout_seconds_for_request(stance_request, prompt_version="stance.test.v1") == 300


def test_executor_does_not_retry_or_fail_over_on_timeout(monkeypatch, _stub_log_event):
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: _fake_env(
            routes={"summary": {"model": "openai/gpt-test"}},
            backup={"model": "google/gemini-backup"},
        ),
    )
    calls: list[str] = []

    def fake_gateway_execute(self, request):
        calls.append(request.model)
        raise LLMTimeoutError("LLM wall-clock timeout after 480s during summary:summary.test.v1")

    monkeypatch.setattr("mind.services.providers.gateway.GatewayProviderClient.execute", fake_gateway_execute)

    with pytest.raises(LLMTimeoutError):
        LLMExecutor(sleeper=lambda _seconds: None).execute_json(
            task_class="summary",
            prompt="Output JSON only",
            prompt_version="summary.test.v1",
        )

    assert calls == ["openai/gpt-test"]
    assert [event["attempt_role"] for event in _stub_log_event] == ["primary"]


def test_wall_clock_timeout_interrupts_long_call():
    with pytest.raises(LLMTimeoutError, match="LLM wall-clock timeout after 1s during summary:slow-test"):
        with __import__("mind.services.llm_executor", fromlist=["_wall_clock_timeout"])._wall_clock_timeout(1, label="summary:slow-test"):
            time.sleep(1.1)
