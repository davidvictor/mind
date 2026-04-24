"""Route resolution for the routed LLM seam."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

from scripts.common import env


TaskClass = Literal[
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
]
TransportMode = Literal["ai_gateway"]
ApiFamily = Literal["responses"]
InputMode = Literal["text", "media", "file"]

VALID_TASK_CLASSES: tuple[TaskClass, ...] = (
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
)
SUPPORTED_PROVIDERS = {"gemini", "openai", "anthropic"}
SUPPORTED_TRANSPORTS = {"ai_gateway"}
SUPPORTED_API_FAMILIES = {"responses"}
SUPPORTED_INPUT_MODES = {"text", "media", "file"}

ROUTE_POLICY_DEFAULTS: dict[str, dict[str, object]] = {
    "classification": {"model": "google/gemini-3.1-flash-lite-preview", "supports_strict_schema": False},
    "document": {"model": "google/gemini-3.1-pro-preview", "supports_strict_schema": False},
    "research": {"model": "google/gemini-3.1-pro-preview", "supports_strict_schema": False},
    "dream_signal": {"model": "anthropic/claude-haiku-4.5", "supports_strict_schema": True},
    "dream_decision": {"model": "anthropic/claude-sonnet-4.6", "supports_strict_schema": True},
    "dream_writer": {"model": "anthropic/claude-sonnet-4.6", "supports_strict_schema": True},
    "dream_reflection": {"model": "anthropic/claude-sonnet-4.6", "supports_strict_schema": True},
    "onboarding_synthesis": {"model": "anthropic/claude-haiku-4.5", "supports_strict_schema": True},
    "onboarding_merge": {"model": "anthropic/claude-haiku-4.5", "supports_strict_schema": True},
    "onboarding_verify": {"model": "anthropic/claude-sonnet-4.6", "supports_strict_schema": True},
}

ROUTE_POLICY_BACKUPS: dict[str, dict[str, object]] = {
    "dream_pass_d": {"model": "anthropic/claude-sonnet-4.6", "supports_strict_schema": True},
    "research": {"model": "anthropic/claude-haiku-4.5", "supports_strict_schema": True},
    "onboarding_synthesis": {"model": "anthropic/claude-sonnet-4.6", "supports_strict_schema": True},
    "onboarding_merge": {"model": "anthropic/claude-sonnet-4.6", "supports_strict_schema": True},
    "onboarding_verify": {"model": "anthropic/claude-haiku-4.5", "supports_strict_schema": True},
}


@dataclass(frozen=True)
class ResolvedRoute:
    provider: str
    model: str
    transport: TransportMode
    api_family: ApiFamily
    input_mode: InputMode
    supports_strict_schema: bool = True
    temperature: float | None = None
    top_p: float | None = None
    max_tokens: int | None = None
    timeout_seconds: int | None = None
    truncation: Literal["auto", "disabled"] | None = None
    reasoning_effort: Literal["low", "medium", "high"] | None = None
    gateway_options: dict[str, Any] | None = None
    provider_options: dict[str, dict[str, Any]] | None = None
    source: str = "explicit"

    def to_public_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            "provider": self.provider,
            "model": self.model,
            "transport": self.transport,
            "api_family": self.api_family,
            "input_mode": self.input_mode,
            "supports_strict_schema": self.supports_strict_schema,
            "source": self.source,
        }
        if self.temperature is not None:
            data["temperature"] = self.temperature
        if self.top_p is not None:
            data["top_p"] = self.top_p
        if self.max_tokens is not None:
            data["max_tokens"] = self.max_tokens
        if self.timeout_seconds is not None:
            data["timeout_seconds"] = self.timeout_seconds
        if self.truncation is not None:
            data["truncation"] = self.truncation
        if self.reasoning_effort is not None:
            data["reasoning_effort"] = self.reasoning_effort
        if self.gateway_options:
            data["gateway_options"] = deepcopy(self.gateway_options)
        if self.provider_options:
            data["provider_options"] = deepcopy(self.provider_options)
        return data


def _route_overrides(runtime_cfg: env.Config) -> dict[str, dict[str, object]]:
    return getattr(runtime_cfg, "llm_routes", {}) or {}


def _normalize_model(model: str, provider: str, transport: str) -> str:
    normalized = str(model or "").strip()
    if not normalized:
        return ""
    if "/" not in normalized and provider in SUPPORTED_PROVIDERS:
        if provider == "gemini":
            return f"google/{normalized}"
        return f"{provider}/{normalized}"
    return normalized


def _provider_from_model(model: str) -> str:
    normalized = str(model or "").strip()
    if "/" in normalized:
        prefix = normalized.split("/", 1)[0].strip()
        if prefix == "google":
            return "gemini"
        return prefix
    return ""


def _route_provider(merged: dict[str, object]) -> str:
    model = str(merged.get("model", "") or "")
    derived = _provider_from_model(model)
    if derived:
        return derived
    lowered = model.lower()
    if lowered.startswith(("claude", "anthropic")):
        return "anthropic"
    if lowered.startswith(("gpt", "o1", "o3", "o4", "openai")):
        return "openai"
    return "gemini"


def _legacy_default(runtime_cfg: env.Config) -> dict[str, object]:
    model = getattr(runtime_cfg, "llm_model", "google/gemini-3.1-flash-lite-preview")
    transport = "ai_gateway"
    api_family = "responses"
    provider = _route_provider({"model": model, "transport": transport})
    return {
        "model": _normalize_model(model, provider, transport),
        "transport": transport,
        "api_family": api_family,
        "input_mode": "text",
    }


def _merge_route(base: dict[str, object], override: dict[str, object], *, source: str) -> ResolvedRoute:
    merged = dict(base)
    for key, value in override.items():
        if value is not None:
            merged[key] = value
    transport = "ai_gateway"
    provider = _route_provider({**merged, "transport": transport})
    model = _normalize_model(str(merged.get("model", "") or ""), provider, transport)
    api_family = "responses"
    input_mode = _coerce_input_mode(merged.get("input_mode")) or "text"
    supports_strict_schema = _coerce_bool(override.get("supports_strict_schema"))
    if supports_strict_schema is None and "model" not in override and "provider" not in override:
        supports_strict_schema = _coerce_bool(base.get("supports_strict_schema"))
    if supports_strict_schema is None:
        supports_strict_schema = _coerce_bool(base.get("supports_strict_schema"))
    if supports_strict_schema is None:
        supports_strict_schema = True
    return ResolvedRoute(
        provider=provider,
        model=model,
        transport=transport,
        api_family=api_family,
        input_mode=input_mode,
        supports_strict_schema=supports_strict_schema,
        temperature=_coerce_float(merged.get("temperature")),
        top_p=_coerce_float(merged.get("top_p")),
        max_tokens=_coerce_int(merged.get("max_tokens")),
        timeout_seconds=_coerce_int(merged.get("timeout_seconds")),
        truncation=_coerce_truncation(merged.get("truncation")),
        reasoning_effort=_coerce_reasoning_effort(merged.get("reasoning_effort")),
        gateway_options=_coerce_mapping(merged.get("gateway_options")),
        provider_options=_coerce_provider_options(merged.get("provider_options")),
        source=source,
    )


def _default_input_mode(task_class: TaskClass) -> InputMode:
    if task_class == "embedding":
        return "text"
    if task_class == "transcription":
        return "media"
    if task_class in {"document", "onboarding_synthesis", "onboarding_verify"}:
        return "file"
    return "text"


def _coerce_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_reasoning_effort(value: object) -> Literal["low", "medium", "high"] | None:
    if value in {"low", "medium", "high"}:
        return value
    return None


def _coerce_input_mode(value: object) -> InputMode | None:
    if value in SUPPORTED_INPUT_MODES:
        return value  # type: ignore[return-value]
    return None


def _coerce_truncation(value: object) -> Literal["auto", "disabled"] | None:
    if value in {"auto", "disabled"}:
        return value  # type: ignore[return-value]
    return None


def _coerce_mapping(value: object) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return deepcopy(value)
    return None


def _coerce_provider_options(value: object) -> dict[str, dict[str, Any]] | None:
    if not isinstance(value, dict):
        return None
    normalized: dict[str, dict[str, Any]] = {}
    for key, inner in value.items():
        if not isinstance(key, str) or not isinstance(inner, dict):
            return None
        normalized[key] = deepcopy(inner)
    return normalized


def _coerce_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def resolve_route(task_class: TaskClass, runtime_cfg: env.Config | None = None) -> ResolvedRoute:
    cfg = runtime_cfg or env.load()
    routes = _route_overrides(cfg)
    legacy = _legacy_default(cfg)
    if task_class == "embedding" and "embedding" not in routes:
        routes = dict(routes)
        routes["embedding"] = {
            "model": "openai/text-embedding-3-small",
            "transport": "ai_gateway",
            "api_family": "responses",
            "input_mode": "text",
        }
    if "default" in routes:
        default_route = _merge_route(legacy, routes["default"], source="explicit")
    else:
        default_route = _merge_route(legacy, {}, source="legacy")
    if task_class == "default":
        return default_route
    policy_specific = dict(ROUTE_POLICY_DEFAULTS.get(task_class, {}))
    explicit_specific = dict(routes.get(task_class, {}))
    specific = dict(policy_specific)
    specific.update(explicit_specific)
    if explicit_specific and "supports_strict_schema" not in explicit_specific and ("model" in explicit_specific or "provider" in explicit_specific):
        specific.pop("supports_strict_schema", None)
    explicit_source = bool(explicit_specific)
    specific.setdefault("input_mode", _default_input_mode(task_class))
    if explicit_source:
        source = "explicit"
    elif task_class in ROUTE_POLICY_DEFAULTS:
        source = "policy-default"
    else:
        source = "inherited-from-default"
    return _merge_route(default_route.to_public_dict(), specific, source=source)


def resolve_all_routes(runtime_cfg: env.Config | None = None) -> dict[str, ResolvedRoute]:
    cfg = runtime_cfg or env.load()
    return {task_class: resolve_route(task_class, runtime_cfg=cfg) for task_class in VALID_TASK_CLASSES}


def resolve_backup(runtime_cfg: env.Config | None = None, *, task_class: TaskClass | None = None) -> ResolvedRoute | None:
    cfg = runtime_cfg or env.load()
    backup = getattr(cfg, "llm_backup", None)
    if not backup:
        if task_class and task_class in ROUTE_POLICY_BACKUPS:
            route = resolve_route(task_class, runtime_cfg=cfg)
            return _merge_route(route.to_public_dict(), ROUTE_POLICY_BACKUPS[task_class], source="policy-backup")
        return None
    default_route = resolve_route(task_class or "default", runtime_cfg=cfg)
    return _merge_route(default_route.to_public_dict(), backup, source="shared-backup")
