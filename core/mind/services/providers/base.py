"""Shared provider primitives and errors for the Brain LLM seam."""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from json import JSONDecodeError
from typing import Any, Literal, Protocol


LLMTaskClass = Literal[
    "default",
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
]
LLMOutputMode = Literal["text", "json"]
LLMTransportMode = Literal["ai_gateway"]
LLMApiFamily = Literal["responses"]
LLMInputMode = Literal["text", "media", "file"]
LLMInputKind = Literal[
    "text",
    "url",
    "file_uri",
    "file_bytes",
    "audio_bytes",
    "video_bytes",
    "image_bytes",
    "pdf_bytes",
    "metadata",
]


class LLMServiceError(RuntimeError):
    """Base class for service-level LLM failures."""


class LLMConfigurationError(LLMServiceError):
    """Raised when provider configuration is missing or invalid."""


class LLMTransportError(LLMServiceError):
    """Raised when a provider request fails in transit."""

    def __init__(self, message: str, *, retry_after_seconds: int | None = None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class LLMStrictSchemaError(LLMTransportError):
    """Raised when the provider rejects a strict response schema."""


class LLMTimeoutError(LLMTransportError):
    """Raised when a provider request times out."""


class LLMRateLimitError(LLMTransportError):
    """Raised when a provider request is rate-limited."""


class LLMUnavailableError(LLMTransportError):
    """Raised when a provider is temporarily unavailable."""


class LLMStructuredOutputError(LLMServiceError):
    """Raised when a provider cannot produce the expected JSON payload."""


@dataclass(frozen=True)
class LLMProviderResponse:
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LLMInputPart:
    kind: LLMInputKind
    text: str | None = None
    url: str | None = None
    mime_type: str | None = None
    data: bytes | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    file_name: str | None = None

    @classmethod
    def text_part(cls, text: str) -> "LLMInputPart":
        return cls(kind="text", text=text)

    @classmethod
    def url_part(cls, url: str, *, metadata: dict[str, Any] | None = None) -> "LLMInputPart":
        return cls(kind="url", url=url, metadata=dict(metadata or {}))

    @classmethod
    def metadata_part(cls, metadata: dict[str, Any]) -> "LLMInputPart":
        return cls(kind="metadata", metadata=dict(metadata))

    @classmethod
    def file_bytes_part(
        cls,
        data: bytes,
        *,
        mime_type: str,
        file_name: str,
        kind: LLMInputKind = "file_bytes",
        metadata: dict[str, Any] | None = None,
    ) -> "LLMInputPart":
        return cls(
            kind=kind,
            data=data,
            mime_type=mime_type,
            file_name=file_name,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def pdf_part(cls, data: bytes, *, file_name: str, metadata: dict[str, Any] | None = None) -> "LLMInputPart":
        return cls.file_bytes_part(
            data,
            mime_type="application/pdf",
            file_name=file_name,
            kind="pdf_bytes",
            metadata=metadata,
        )

    @classmethod
    def audio_part(
        cls,
        data: bytes,
        *,
        mime_type: str,
        file_name: str,
        metadata: dict[str, Any] | None = None,
    ) -> "LLMInputPart":
        return cls.file_bytes_part(
            data,
            mime_type=mime_type,
            file_name=file_name,
            kind="audio_bytes",
            metadata=metadata,
        )

    def fingerprint(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "mime_type": self.mime_type,
            "file_name": self.file_name,
        }
        if self.text:
            payload["text_sha256"] = hashlib.sha256(self.text.encode("utf-8")).hexdigest()
        if self.url:
            payload["url"] = self.url
        if self.data is not None:
            payload["byte_size"] = len(self.data)
            payload["data_sha256"] = hashlib.sha256(self.data).hexdigest()
        if self.metadata:
            payload["metadata"] = self.metadata
        return payload


def provider_from_model(model: str) -> str:
    normalized = str(model or "").strip()
    if "/" in normalized:
        prefix = normalized.split("/", 1)[0].strip().lower()
        if prefix == "google":
            return "gemini"
        return prefix
    lowered = normalized.lower()
    if lowered.startswith(("claude", "anthropic")):
        return "anthropic"
    if lowered.startswith(("gpt", "o1", "o3", "o4", "openai")):
        return "openai"
    return "gemini"


@dataclass(frozen=True)
class LLMRequest:
    instructions: str = ""
    input_parts: tuple[LLMInputPart, ...] = ()
    output_mode: LLMOutputMode = "text"
    task_class: LLMTaskClass = "default"
    model: str = ""
    transport: LLMTransportMode = "ai_gateway"
    api_family: LLMApiFamily = "responses"
    input_mode: LLMInputMode = "text"
    tools: tuple[dict[str, Any], ...] = ()
    response_schema: dict[str, Any] | None = None
    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | None = None
    timeout_seconds: int | None = None
    truncation: Literal["auto", "disabled"] | None = None
    reasoning_effort: Literal["low", "medium", "high"] | None = None
    gateway_options: dict[str, Any] | None = None
    provider_options: dict[str, dict[str, Any]] | None = None
    request_metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.input_parts:
            object.__setattr__(self, "input_parts", (LLMInputPart.text_part(""),))

    @property
    def provider(self) -> str:
        return provider_from_model(self.model)

    @property
    def prompt(self) -> str:
        if self.instructions or len(self.input_parts) != 1:
            return ""
        part = self.input_parts[0]
        if part.kind != "text":
            return ""
        return part.text or ""

    @classmethod
    def from_prompt(
        cls,
        *,
        prompt: str,
        output_mode: LLMOutputMode,
        task_class: LLMTaskClass,
        model: str,
        transport: LLMTransportMode,
        api_family: LLMApiFamily,
        input_mode: LLMInputMode,
        temperature: float | None = None,
        top_p: float | None = None,
        max_tokens: int | None = None,
        timeout_seconds: int | None = None,
        truncation: Literal["auto", "disabled"] | None = None,
        reasoning_effort: Literal["low", "medium", "high"] | None = None,
        response_schema: dict[str, Any] | None = None,
        gateway_options: dict[str, Any] | None = None,
        provider_options: dict[str, dict[str, Any]] | None = None,
        request_metadata: dict[str, Any] | None = None,
    ) -> "LLMRequest":
        return cls(
            instructions="",
            input_parts=(LLMInputPart.text_part(prompt),),
            output_mode=output_mode,
            task_class=task_class,
            model=model,
            transport=transport,
            api_family=api_family,
            input_mode=input_mode,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            timeout_seconds=timeout_seconds,
            truncation=truncation,
            reasoning_effort=reasoning_effort,
            response_schema=response_schema,
            gateway_options=dict(gateway_options or {}) or None,
            provider_options={key: dict(value) for key, value in (provider_options or {}).items()} or None,
            request_metadata=dict(request_metadata or {}),
        )

    def request_fingerprint(self) -> dict[str, Any]:
        payload = {
            "instructions_sha256": hashlib.sha256(self.instructions.encode("utf-8")).hexdigest()
            if self.instructions
            else "",
            "input_parts": [part.fingerprint() for part in self.input_parts],
            "request_metadata": self.request_metadata,
            "has_tools": bool(self.tools),
            "has_response_schema": self.response_schema is not None,
        }
        if self.temperature is not None:
            payload["temperature"] = self.temperature
        if self.top_p is not None:
            payload["top_p"] = self.top_p
        if self.max_tokens is not None:
            payload["max_tokens"] = self.max_tokens
        if self.timeout_seconds is not None:
            payload["timeout_seconds"] = self.timeout_seconds
        if self.truncation is not None:
            payload["truncation"] = self.truncation
        if self.reasoning_effort is not None:
            payload["reasoning_effort"] = self.reasoning_effort
        if self.gateway_options:
            payload["gateway_options"] = self.gateway_options
        if self.provider_options:
            payload["provider_options"] = self.provider_options
        return payload


class LLMProviderClient(Protocol):
    """Minimal provider primitive surface used by the shared service layer."""

    def execute(self, request: LLMRequest) -> LLMProviderResponse:
        """Return raw text for a request."""


def parse_first_json_object(text: str) -> dict[str, Any]:
    """Parse the first JSON object from a provider response."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].lstrip()
    try:
        obj, _ = json.JSONDecoder().raw_decode(cleaned)
    except JSONDecodeError as exc:  # pragma: no cover - exercised by provider tests
        obj = _parse_embedded_json_object(cleaned)
        if obj is None:
            preview = cleaned[:160].replace("\n", "\\n")
            if len(cleaned) > 160:
                preview += "..."
            raise LLMStructuredOutputError(
                f"invalid JSON output: {exc}; response starts with: {preview!r}"
            ) from exc
    if not isinstance(obj, dict):
        raise LLMStructuredOutputError(f"expected JSON object, got {type(obj).__name__}")
    return obj


def _parse_embedded_json_object(text: str) -> dict[str, Any] | None:
    """Best-effort recovery for providers that prepend chatter before JSON."""
    decoder = json.JSONDecoder()
    start = text.find("{")
    while start != -1:
        try:
            obj, _ = decoder.raw_decode(text[start:])
        except JSONDecodeError:
            start = text.find("{", start + 1)
            continue
        if isinstance(obj, dict):
            return obj
        start = text.find("{", start + 1)
    return None


def classify_transport_error(message: str, *, default: type[LLMTransportError] = LLMTransportError) -> LLMTransportError:
    lowered = message.lower()
    if "timeout" in lowered:
        return LLMTimeoutError(message)
    if "rate limit" in lowered or "429" in lowered:
        return LLMRateLimitError(message)
    if "unavailable" in lowered or "503" in lowered or "overloaded" in lowered:
        return LLMUnavailableError(message)
    return default(message)
