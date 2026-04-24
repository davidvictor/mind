"""Additive AI Gateway route validation for config show and doctor."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple
import warnings

from scripts.common import env

from .llm_routing import SUPPORTED_API_FAMILIES, SUPPORTED_INPUT_MODES, SUPPORTED_PROVIDERS, SUPPORTED_TRANSPORTS, VALID_TASK_CLASSES, resolve_all_routes, resolve_backup

AI_GATEWAY_BASE_URL = "https://ai-gateway.vercel.sh/v1"
STRICT_SCHEMA_TASK_CLASSES = frozenset({
    "dream_decision",
    "dream_writer",
    "dream_reflection",
    "onboarding_synthesis",
    "onboarding_merge",
    "onboarding_verify",
})


@dataclass
class ValidationReport:
    ok: bool
    warnings: list[str] = field(default_factory=list)
    routes: dict[str, dict[str, object]] = field(default_factory=dict)
    backup: dict[str, object] = field(default_factory=lambda: {"enabled": False})
    errors: list[str] = field(default_factory=list)

    def to_public_dict(self) -> dict[str, object]:
        messages = list(self.errors)
        messages.extend(self.warnings)
        return {
            "ok": not self.errors,
            "warnings": messages,
            "routes": self.routes,
            "backup": self.backup,
        }


def validate_routed_llm(runtime_cfg: env.Config | None = None) -> ValidationReport:
    cfg = runtime_cfg or env.load()
    errors: list[str] = []
    warnings: list[str] = []
    gateway_api_key = getattr(cfg, "ai_gateway_api_key", "")
    raw_routes = getattr(cfg, "llm_routes", {}) or {}
    unknown = sorted(set(raw_routes) - set(VALID_TASK_CLASSES))
    if unknown:
        errors.append(f"unknown llm.routes task classes: {', '.join(unknown)}")

    resolved_routes = resolve_all_routes(cfg)
    public_routes: dict[str, dict[str, object]] = {}
    for name, route in resolved_routes.items():
        public_routes[name] = route.to_public_dict()
        _validate_route_shape(name, route.to_public_dict(), errors=errors)
        warning = strict_schema_route_warning(name, route.to_public_dict())
        if warning:
            warnings.append(warning)
        try:
            status, message = _validate_model_name(
                route.provider,
                route.model,
                getattr(cfg, "ai_gateway_api_key", ""),
                gateway_api_key=gateway_api_key,
            )
        except TypeError:
            status, message = _validate_model_name(
                route.provider,
                route.model,
                getattr(cfg, "ai_gateway_api_key", ""),
                gateway_api_key=gateway_api_key,
            )
        public_routes[name]["model_status"] = status
        if message:
            if status == "error":
                errors.append(f"route {name!r} model validation failed: {message}")
            elif status == "warning":
                warnings.append(f"route {name!r} model validation warning: {message}")

    backup = resolve_backup(cfg)
    backup_public = {"enabled": False}
    if backup is not None:
        backup_public = {"enabled": True, **backup.to_public_dict()}
        _validate_route_shape("backup", backup.to_public_dict(), errors=errors)
        try:
            status, message = _validate_model_name(
                backup.provider,
                backup.model,
                getattr(cfg, "ai_gateway_api_key", ""),
                gateway_api_key=gateway_api_key,
            )
        except TypeError:
            status, message = _validate_model_name(
                backup.provider,
                backup.model,
                getattr(cfg, "ai_gateway_api_key", ""),
                gateway_api_key=gateway_api_key,
            )
        backup_public["model_status"] = status
        if message:
            if status == "error":
                errors.append(f"backup model validation failed: {message}")
            elif status == "warning":
                warnings.append(f"backup model validation warning: {message}")

    if not gateway_api_key.strip():
        errors.append("AI_GATEWAY_API_KEY is missing for gateway-first execution")

    return ValidationReport(ok=not errors, warnings=warnings, routes=public_routes, backup=backup_public, errors=errors)


def strict_schema_route_warning(task_class: str, route: dict[str, object]) -> str | None:
    if task_class not in STRICT_SCHEMA_TASK_CLASSES:
        return None
    return schema_downgrade_warning(task_class, route)


def schema_downgrade_warning(task_class: str, route: dict[str, object]) -> str | None:
    if bool(route.get("supports_strict_schema", True)):
        return None
    provider = str(route.get("provider", "") or "unknown")
    model = str(route.get("model", "") or "unknown")
    return (
        f"route {task_class!r} resolves to provider {provider!r} model {model!r} "
        "without strict-schema support; schema-bearing calls will downgrade "
        "to post-validation repair behavior"
    )


def warn_strict_schema_route(task_class: str, route: dict[str, object]) -> None:
    message = schema_downgrade_warning(task_class, route)
    if message:
        warnings.warn(message, RuntimeWarning, stacklevel=2)


def _validate_route_shape(name: str, route: dict[str, object], *, errors: list[str]) -> None:
    provider = str(route.get("provider", "") or "")
    transport = str(route.get("transport", "") or "")
    api_family = str(route.get("api_family", "") or "")
    input_mode = str(route.get("input_mode", "") or "")
    model = str(route.get("model", "") or "")
    if provider not in SUPPORTED_PROVIDERS:
        errors.append(f"route {name!r} has unsupported provider {provider!r}")
    if transport not in SUPPORTED_TRANSPORTS:
        errors.append(f"route {name!r} has unsupported transport {transport!r}")
    if api_family not in SUPPORTED_API_FAMILIES:
        errors.append(f"route {name!r} has unsupported api_family {api_family!r}")
    if input_mode not in SUPPORTED_INPUT_MODES:
        errors.append(f"route {name!r} has unsupported input_mode {input_mode!r}")
    if not model.strip():
        errors.append(f"route {name!r} has an empty model")
    if "/" not in model:
        errors.append(f"route {name!r} must use provider-qualified model names in AI Gateway mode")


def _validate_model_name(
    provider: str,
    model: str,
    api_key: str,
    *,
    gateway_api_key: str = "",
) -> Tuple[str, str | None]:
    if not model.strip():
        return "error", "model is empty"
    if not _looks_like_gateway_key(api_key or gateway_api_key):
        return "warning", "skipping online route-model validation because no real-looking AI Gateway key is configured"
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key or gateway_api_key, base_url=AI_GATEWAY_BASE_URL)
        models = client.models.list()
        data = list(getattr(models, "data", []) or [])
        expected_model_ids = {model}
        if "/" not in model:
            expected_model_ids.add(f"{'google' if provider == 'gemini' else provider}/{model}")
        if any(getattr(item, "id", None) in expected_model_ids for item in data):
            return "ok", None
        return "error", f"model {model!r} not found"
    except Exception as exc:  # pragma: no cover - network/SDK dependent
        return _model_validation_exception(exc)
    return "warning", "unknown AI Gateway validation path"


def _model_validation_exception(exc: Exception) -> Tuple[str, str]:
    text = str(exc)
    lowered = text.lower()
    if "not found" in lowered or "404" in lowered or "unknown model" in lowered or "does not exist" in lowered:
        return "error", text
    return "warning", f"unable to validate model online: {text}"

def _looks_like_gateway_key(api_key: str) -> bool:
    return bool((api_key or "").strip())
