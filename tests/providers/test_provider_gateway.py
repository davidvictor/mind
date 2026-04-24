from __future__ import annotations

import sys
import types

import pytest

from mind.services.providers.base import (
    LLMConfigurationError,
    LLMInputPart,
    LLMProviderResponse,
    LLMRequest,
    LLMStructuredOutputError,
    parse_first_json_object,
)
from mind.services.providers.gateway import GatewayProviderClient
from mind.services.embedding_service import EmbeddingRequest, EmbeddingService


def test_gateway_provider_parses_json(monkeypatch):
    captured: dict[str, object] = {}

    class FakeResponses:
        def create(self, **kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(output_text='{"category":"business"}')

    class FakeOpenAI:
        def __init__(self, *, api_key: str, base_url: str, timeout: int, max_retries: int):
            assert api_key == "gateway-key"
            assert base_url == "https://ai-gateway.vercel.sh/v1"
            assert timeout == 90
            assert max_retries == 2
            self.responses = FakeResponses()

    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI))

    client = GatewayProviderClient(api_key="gateway-key", model="google/gemini-3.1-flash-lite-preview")
    response = client.execute(
        LLMRequest(
            input_parts=(LLMInputPart.text_part("Output JSON only"),),
            output_mode="json",
            task_class="summary",
            model="google/gemini-3.1-flash-lite-preview",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
        )
    )

    assert parse_first_json_object(response.text) == {"category": "business"}
    assert response.metadata["response_id"] is None
    assert captured["model"] == "google/gemini-3.1-flash-lite-preview"
    assert captured["extra_body"] == {"caching": "auto"}


def test_parse_first_json_object_recovers_from_leading_chatter():
    parsed = parse_first_json_object('Here is the JSON you asked for:\n{"category":"business"}')
    assert parsed == {"category": "business"}


def test_parse_first_json_object_includes_payload_preview_on_failure():
    with pytest.raises(LLMStructuredOutputError, match="response starts with"):
        parse_first_json_object("Not JSON at all")


def test_gateway_provider_serializes_youtube_url_and_audio(monkeypatch):
    captured: dict[str, object] = {}

    class FakeResponses:
        def create(self, **kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(output_text='{"transcript":"hello"}')

    fake_client = types.SimpleNamespace(responses=FakeResponses())
    client = GatewayProviderClient(api_key="gateway-key", model="google/gemini-3.1-flash-lite-preview")
    monkeypatch.setattr(client, "_client_instance", lambda timeout_seconds=None: fake_client)

    response = client.execute(
        LLMRequest(
            instructions="Transcribe this source.",
            input_parts=(
                LLMInputPart.url_part("https://www.youtube.com/watch?v=abc123xyz00"),
                LLMInputPart.audio_part(
                    b"audio-bytes",
                    mime_type="audio/mpeg",
                    file_name="video.mp3",
                ),
            ),
            output_mode="json",
            task_class="transcription",
            model="google/gemini-3.1-flash-lite-preview",
            transport="ai_gateway",
            api_family="responses",
            input_mode="media",
        )
    )

    assert parse_first_json_object(response.text) == {"transcript": "hello"}
    content = captured["input"][0]["content"]
    assert content[0]["type"] == "input_file"
    assert content[0]["file_url"] == "https://www.youtube.com/watch?v=abc123xyz00"
    assert content[1]["type"] == "input_file"
    assert content[1]["filename"] == "video.mp3"


@pytest.mark.parametrize(
    ("mime_type", "file_name"),
    [("audio/mp4", "video.m4a"), ("audio/webm", "video.webm")],
)
def test_gateway_provider_normalizes_non_native_audio_formats(monkeypatch, mime_type: str, file_name: str):
    captured: dict[str, object] = {}

    class FakeResponses:
        def create(self, **kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(output_text='{"transcript":"hello"}')

    fake_client = types.SimpleNamespace(responses=FakeResponses())
    client = GatewayProviderClient(api_key="gateway-key", model="google/gemini-3.1-flash-lite-preview")
    monkeypatch.setattr(client, "_client_instance", lambda timeout_seconds=None: fake_client)
    monkeypatch.setattr(
        "mind.services.providers.gateway._normalize_audio_for_responses",
        lambda data, *, mime_type, file_name: (b"normalized-audio", "normalized.mp3"),
    )

    client.execute(
        LLMRequest(
            input_parts=(
                LLMInputPart.audio_part(
                    b"audio-bytes",
                    mime_type=mime_type,
                    file_name=file_name,
                ),
            ),
            output_mode="json",
            task_class="transcription",
            model="google/gemini-3.1-flash-lite-preview",
            transport="ai_gateway",
            api_family="responses",
            input_mode="media",
        )
    )

    content = captured["input"][0]["content"]
    assert content[0]["type"] == "input_file"
    assert content[0]["filename"] == "normalized.mp3"


def test_gateway_provider_requires_api_key():
    with pytest.raises(LLMConfigurationError):
        GatewayProviderClient(api_key="", model="google/gemini-3.1-flash-lite-preview")


def test_gateway_provider_extracts_generation_metadata(monkeypatch):
    class FakeResponses:
        def create(self, **kwargs):
            return types.SimpleNamespace(
                id="resp_123",
                output_text='{"category":"business"}',
                usage=types.SimpleNamespace(prompt_tokens=11, completion_tokens=7, total_tokens=18),
                provider_metadata={
                    "gateway": {
                        "generationId": "gen_abc",
                    }
                },
            )

    fake_client = types.SimpleNamespace(responses=FakeResponses())
    client = GatewayProviderClient(api_key="gateway-key", model="anthropic/claude-sonnet-4.6")
    monkeypatch.setattr(client, "_client_instance", lambda timeout_seconds=None: fake_client)

    response = client.execute(
        LLMRequest(
            input_parts=(LLMInputPart.text_part("Output JSON only"),),
            output_mode="json",
            task_class="summary",
            model="anthropic/claude-sonnet-4.6",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
        )
    )

    assert isinstance(response, LLMProviderResponse)
    assert response.metadata["response_id"] == "resp_123"
    assert response.metadata["generation_id"] == "gen_abc"
    assert response.metadata["usage"]["prompt_tokens"] == 11
    assert response.metadata["usage"]["completion_tokens"] == 7


def test_gateway_provider_forwards_generation_and_gateway_options(monkeypatch):
    captured: dict[str, object] = {}

    class FakeResponses:
        def create(self, **kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(output_text='{"category":"business"}')

    fake_client = types.SimpleNamespace(responses=FakeResponses())
    client = GatewayProviderClient(api_key="gateway-key", model="openai/gpt-5.4-mini")
    monkeypatch.setattr(client, "_client_instance", lambda timeout_seconds=None: fake_client)

    client.execute(
        LLMRequest(
            input_parts=(LLMInputPart.text_part("Output JSON only"),),
            output_mode="json",
            task_class="dream_decision",
            model="openai/gpt-5.4-mini",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
            top_p=0.2,
            truncation="disabled",
            gateway_options={"only": ["vertex"], "providerTimeouts": {"openai": 4}},
            provider_options={"openai": {"reasoningEffort": "high"}},
        )
    )

    assert captured["top_p"] == 0.2
    assert captured["truncation"] == "disabled"
    assert captured["extra_body"] == {
        "caching": "auto",
        "providerOptions": {
            "gateway": {"only": ["vertex"], "providerTimeouts": {"openai": 4}},
            "openai": {"reasoningEffort": "high"},
        },
    }


def test_gateway_provider_does_not_override_explicit_gateway_caching(monkeypatch):
    captured: dict[str, object] = {}

    class FakeResponses:
        def create(self, **kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(output_text='{"category":"business"}')

    fake_client = types.SimpleNamespace(responses=FakeResponses())
    client = GatewayProviderClient(api_key="gateway-key", model="openai/gpt-5.4-mini")
    monkeypatch.setattr(client, "_client_instance", lambda timeout_seconds=None: fake_client)

    client.execute(
        LLMRequest(
            input_parts=(LLMInputPart.text_part("Output JSON only"),),
            output_mode="json",
            task_class="dream_decision",
            model="openai/gpt-5.4-mini",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
            gateway_options={"caching": {"mode": "bypass"}},
        )
    )

    assert captured["extra_body"] == {
        "providerOptions": {
            "gateway": {"caching": {"mode": "bypass"}},
        }
    }


def test_gateway_provider_creates_embeddings(monkeypatch):
    captured: dict[str, object] = {}

    class FakeEmbeddings:
        def create(self, **kwargs):
            captured.update(kwargs)
            return types.SimpleNamespace(
                model="openai/text-embedding-3-small",
                data=[
                    types.SimpleNamespace(embedding=[0.1, 0.2, 0.3]),
                    types.SimpleNamespace(embedding=[0.4, 0.5, 0.6]),
                ],
                usage=types.SimpleNamespace(prompt_tokens=12, total_tokens=12),
            )

    fake_client = types.SimpleNamespace(embeddings=FakeEmbeddings())
    client = GatewayProviderClient(api_key="gateway-key", model="openai/text-embedding-3-small")
    monkeypatch.setattr(client, "_client_instance", lambda timeout_seconds=None: fake_client)

    vectors, metadata = client.embed(inputs=["Example Product", "Life Is Beautiful"])

    assert vectors == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
    assert captured["model"] == "openai/text-embedding-3-small"
    assert captured["input"] == ["Example Product", "Life Is Beautiful"]
    assert metadata["usage"]["prompt_tokens"] == 12


def test_gateway_provider_fetches_credits(monkeypatch):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"balance": 12.5}'

    captured = {}

    def fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["auth"] = req.headers["Authorization"]
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("mind.services.providers.gateway.urlrequest.urlopen", fake_urlopen)

    client = GatewayProviderClient(api_key="gateway-key", model="anthropic/claude-sonnet-4.6")
    payload = client.get_credits(timeout_seconds=7)

    assert payload["balance"] == 12.5
    assert captured["url"].endswith("/credits")
    assert captured["auth"] == "Bearer gateway-key"
    assert captured["timeout"] == 7


def test_embedding_service_rejects_partial_batch(monkeypatch):
    class FakeExecutor:
        def execute(self, *, inputs, dimensions=None):
            return __import__("types").SimpleNamespace(
                vectors=[[0.1, 0.2]],
                identity=__import__("types").SimpleNamespace(
                    model="openai/text-embedding-3-small",
                    provider="openai",
                    transport="ai_gateway",
                    api_family="responses",
                    input_mode="text",
                ),
                response_metadata={},
            )

    service = EmbeddingService(executor=FakeExecutor())
    with pytest.raises(Exception):
        service.embed_requests(
            [
                EmbeddingRequest(target_id="a", target_type="node", content="A", content_sha256="1"),
                EmbeddingRequest(target_id="b", target_type="node", content="B", content_sha256="2"),
            ]
        )
