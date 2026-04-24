"""Gateway-first provider client using the OpenAI-compatible Responses API."""
from __future__ import annotations

import base64
from copy import deepcopy
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from urllib import request as urlrequest
from urllib.parse import urlparse

from .base import (
    LLMConfigurationError,
    LLMInputPart,
    LLMProviderClient,
    LLMProviderResponse,
    LLMRateLimitError,
    LLMRequest,
    LLMStrictSchemaError,
    LLMUnavailableError,
    classify_transport_error,
)
from ..llm_rate_limiter import parse_retry_after_seconds

AI_GATEWAY_BASE_URL = "https://ai-gateway.vercel.sh/v1"


class GatewayProviderClient(LLMProviderClient):
    """Route all gateway traffic through the OpenAI-compatible Responses API."""

    def __init__(self, *, api_key: str, model: str):
        if not api_key:
            raise LLMConfigurationError("AI_GATEWAY_API_KEY is missing")
        self.api_key = api_key
        self.model = model
        self._clients: dict[int, object] = {}

    def _client_instance(self, *, timeout_seconds: int | None):
        from openai import OpenAI

        timeout_seconds = timeout_seconds or 90
        if timeout_seconds not in self._clients:
            # max_retries handles transient 5xx, 429, and connection
            # errors with exponential backoff inside the SDK. It does
            # NOT retry on request timeout — those are hard ceilings
            # and blind retries just reburn input tokens.
            self._clients[timeout_seconds] = OpenAI(
                api_key=self.api_key,
                base_url=AI_GATEWAY_BASE_URL,
                timeout=timeout_seconds,
                max_retries=2,
            )
        return self._clients[timeout_seconds]

    def _content_part(self, part: LLMInputPart) -> dict[str, object]:
        if part.kind == "text":
            return {"type": "input_text", "text": part.text or ""}
        if part.kind == "url":
            return {
                "type": "input_file",
                "file_url": part.url or "",
                "filename": part.file_name or _filename_from_url(part.url or ""),
            }
        if part.kind == "metadata":
            return {"type": "input_text", "text": f"Metadata: {part.metadata}"}
        if part.kind == "audio_bytes":
            audio_bytes, normalized_filename = _normalize_audio_for_responses(
                part.data or b"",
                mime_type=part.mime_type or "",
                file_name=part.file_name or _default_filename(part),
            )
            return {
                "type": "input_file",
                "file_data": base64.b64encode(audio_bytes).decode("utf-8"),
                "filename": normalized_filename,
            }
        if part.kind == "image_bytes":
            encoded = base64.b64encode(part.data or b"").decode("utf-8")
            mime_type = part.mime_type or "image/png"
            return {
                "type": "input_image",
                "detail": "auto",
                "image_url": f"data:{mime_type};base64,{encoded}",
            }
        encoded = base64.b64encode(part.data or b"").decode("utf-8")
        return {
            "type": "input_file",
            "file_data": encoded,
            "filename": part.file_name or _default_filename(part),
        }

    def execute(self, request: LLMRequest) -> LLMProviderResponse:
        try:
            extra_body = _build_extra_body(
                gateway_options=request.gateway_options,
                provider_options=request.provider_options,
            )
            create_kwargs: dict[str, object] = {
                "model": request.model,
                "input": [
                    {
                        "role": "user",
                        "content": [self._content_part(part) for part in request.input_parts],
                    }
                ],
                "extra_body": extra_body,
            }
            if request.instructions:
                create_kwargs["instructions"] = request.instructions
            if request.temperature is not None:
                create_kwargs["temperature"] = request.temperature
            if request.top_p is not None:
                create_kwargs["top_p"] = request.top_p
            if request.max_tokens is not None:
                create_kwargs["max_output_tokens"] = request.max_tokens
            if request.truncation is not None:
                create_kwargs["truncation"] = request.truncation
            if request.reasoning_effort is not None:
                create_kwargs["reasoning"] = {"effort": request.reasoning_effort}
            if request.tools:
                create_kwargs["tools"] = list(request.tools)
            if request.response_schema is not None:
                create_kwargs["text"] = {
                    "format": {
                        "type": "json_schema",
                        "name": "brain_response",
                        "strict": True,
                        "schema": request.response_schema,
                    }
                }
            response = self._client_instance(timeout_seconds=request.timeout_seconds).responses.create(**create_kwargs)
        except Exception as exc:  # pragma: no cover - covered by higher-level mocks
            if request.response_schema is not None and _looks_like_strict_schema_error(exc):
                raise LLMStrictSchemaError(f"AI Gateway strict-schema request failed: {exc}") from exc
            retry_after = _retry_after_from_exception(exc)
            transport_error = classify_transport_error(f"AI Gateway request failed: {exc}")
            if retry_after is not None and isinstance(transport_error, (LLMRateLimitError, LLMUnavailableError)):
                transport_error.retry_after_seconds = retry_after
            raise transport_error from exc
        output_text = getattr(response, "output_text", None)
        metadata = _extract_response_metadata(response)
        if output_text:
            return LLMProviderResponse(text=str(output_text), metadata=metadata)
        return LLMProviderResponse(text=str(response), metadata=metadata)

    def embed(
        self,
        *,
        inputs: list[str],
        dimensions: int | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[list[list[float]], dict[str, object]]:
        try:
            create_kwargs: dict[str, object] = {
                "model": self.model,
                "input": inputs,
                "encoding_format": "float",
            }
            if dimensions is not None:
                create_kwargs["dimensions"] = dimensions
            response = self._client_instance(timeout_seconds=timeout_seconds).embeddings.create(**create_kwargs)
        except Exception as exc:  # pragma: no cover - network/SDK dependent
            raise classify_transport_error(f"AI Gateway embedding request failed: {exc}") from exc
        data = list(getattr(response, "data", []) or [])
        vectors = [list(getattr(item, "embedding", []) or []) for item in data]
        usage = getattr(response, "usage", None)
        return vectors, {
            "model": getattr(response, "model", self.model),
            "usage": {
                "prompt_tokens": getattr(usage, "prompt_tokens", None) if usage is not None else None,
                "total_tokens": getattr(usage, "total_tokens", None) if usage is not None else None,
            },
        }

    def get_credits(self, *, timeout_seconds: int = 15) -> dict[str, object]:
        req = urlrequest.Request(
            f"{AI_GATEWAY_BASE_URL}/credits",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="GET",
        )
        with urlrequest.urlopen(req, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {}


def _build_extra_body(
    *,
    gateway_options: dict[str, object] | None,
    provider_options: dict[str, dict[str, object]] | None,
) -> dict[str, object]:
    extra_body: dict[str, object] = {}
    merged_provider_options: dict[str, dict[str, object]] = {}
    if provider_options:
        merged_provider_options = {
            str(key): deepcopy(value)
            for key, value in provider_options.items()
        }
    if gateway_options:
        gateway_payload = merged_provider_options.get("gateway", {})
        gateway_payload = {
            **gateway_payload,
            **deepcopy(gateway_options),
        }
        merged_provider_options["gateway"] = gateway_payload
    if merged_provider_options:
        extra_body["providerOptions"] = merged_provider_options
        gateway_payload = merged_provider_options.get("gateway", {})
        if isinstance(gateway_payload, dict) and "caching" not in gateway_payload:
            extra_body["caching"] = "auto"
    else:
        # Let AI Gateway add provider-appropriate prompt-cache controls.
        extra_body["caching"] = "auto"
    return extra_body


def _responses_audio_format(mime_type: str) -> str | None:
    if mime_type == "audio/mpeg":
        return "mp3"
    if mime_type == "audio/wav":
        return "wav"
    return None


def _normalize_audio_for_responses(data: bytes, *, mime_type: str, file_name: str) -> tuple[bytes, str]:
    audio_format = _responses_audio_format(mime_type)
    if audio_format is not None:
        return data, file_name or f"input.{audio_format}"
    if mime_type not in {"audio/mp4", "audio/webm"}:
        raise RuntimeError(f"unsupported audio mime type for Responses API: {mime_type or 'unknown'}")
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise RuntimeError("ffmpeg is required to normalize gateway audio input")
    suffix = Path(file_name).suffix or (".m4a" if mime_type == "audio/mp4" else ".webm")
    with tempfile.TemporaryDirectory() as tmp:
        source_path = Path(tmp) / f"input{suffix}"
        output_path = Path(tmp) / "normalized.mp3"
        source_path.write_bytes(data)
        result = subprocess.run(
            [
                ffmpeg,
                "-y",
                "-i",
                str(source_path),
                "-vn",
                "-acodec",
                "libmp3lame",
                str(output_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 or not output_path.exists():
            raise RuntimeError(f"ffmpeg audio normalization failed: {result.stderr.strip()[:200]}")
        return output_path.read_bytes(), "normalized.mp3"


def _default_filename(part: LLMInputPart) -> str:
    if part.kind == "pdf_bytes":
        return "input.pdf"
    if part.kind == "audio_bytes":
        return "input.mp3" if part.mime_type == "audio/mpeg" else "input.wav" if part.mime_type == "audio/wav" else "input.audio"
    return "input.bin"


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = parsed.path.rsplit("/", 1)[-1].strip()
    return name or "remote-input"


def _looks_like_strict_schema_error(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = (
        "json_schema",
        "json schema",
        "strict",
        "additionalproperties",
        "$ref",
        "$defs",
        "schema validation",
        "invalid schema",
    )
    return any(marker in text for marker in markers)


def _retry_after_from_exception(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None)
    value = None
    if isinstance(headers, dict):
        value = headers.get("Retry-After") or headers.get("retry-after")
    elif headers is not None:
        value = getattr(headers, "get", lambda *_args, **_kwargs: None)("Retry-After")
        if value is None:
            value = getattr(headers, "get", lambda *_args, **_kwargs: None)("retry-after")
    return parse_retry_after_seconds(value)


def _extract_response_metadata(response: object) -> dict[str, object]:
    payload = _response_payload(response)
    provider_metadata = _read_mapping(payload, "providerMetadata") or _read_mapping(payload, "provider_metadata")
    gateway_metadata = _read_mapping(provider_metadata, "gateway")
    gateway_cost = _read_mapping(gateway_metadata, "cost")
    usage = _read_mapping(payload, "usage")

    generation_id = (
        _read_scalar(gateway_metadata, "generationId")
        or _read_scalar(gateway_cost, "generationId")
        or _read_scalar(payload, "id")
    )
    response_id = _read_scalar(payload, "id")

    return {
        "response_id": response_id,
        "generation_id": generation_id,
        "usage": {
            "prompt_tokens": _read_scalar(usage, "prompt_tokens"),
            "completion_tokens": _read_scalar(usage, "completion_tokens"),
            "total_tokens": _read_scalar(usage, "total_tokens"),
        },
        "provider_metadata": provider_metadata or {},
        "gateway": gateway_metadata or {},
    }


def _response_payload(response: object) -> dict[str, object]:
    if isinstance(response, dict):
        return dict(response)
    model_dump = getattr(response, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python")
        if isinstance(dumped, dict):
            return dumped
    to_dict = getattr(response, "to_dict", None)
    if callable(to_dict):
        dumped = to_dict()
        if isinstance(dumped, dict):
            return dumped
    payload: dict[str, object] = {}
    for key in ("id", "usage", "provider_metadata", "providerMetadata"):
        value = getattr(response, key, None)
        if value is not None:
            payload[key] = value
    return payload


def _read_mapping(container: object, key: str) -> dict[str, object] | None:
    if isinstance(container, dict):
        value = container.get(key)
    else:
        value = getattr(container, key, None)
    if isinstance(value, dict):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python")
        if isinstance(dumped, dict):
            return dumped
    if value is None:
        return None
    result: dict[str, object] = {}
    for field in ("gateway", "cost", "generationId", "generation_id", "prompt_tokens", "completion_tokens", "total_tokens"):
        attr = getattr(value, field, None)
        if attr is not None:
            result[field] = attr
    return result or None


def _read_scalar(container: object, key: str) -> object | None:
    if isinstance(container, dict):
        return container.get(key)
    return getattr(container, key, None)
