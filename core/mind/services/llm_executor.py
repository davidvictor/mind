"""Lower-level routed LLM execution under the product-facing service."""
from __future__ import annotations

import contextlib
from dataclasses import dataclass
import signal
import threading
import time
from typing import Any, Iterable

from .llm_cache import LLMCacheIdentity
from .llm_rate_limiter import GLOBAL_RATE_LIMITER, normalize_concurrency
from .llm_telemetry import log_event
from .llm_validation import warn_strict_schema_route
from .llm_routing import ResolvedRoute, SUPPORTED_PROVIDERS, TaskClass, resolve_backup, resolve_route
from .providers.base import (
    LLMConfigurationError,
    LLMInputPart,
    LLMProviderClient,
    LLMProviderResponse,
    LLMRequest,
    LLMRateLimitError,
    LLMServiceError,
    LLMStrictSchemaError,
    LLMStructuredOutputError,
    LLMTransportError,
    LLMTimeoutError,
    LLMUnavailableError,
    parse_first_json_object,
)
from .providers.gateway import GatewayProviderClient

_STAGE_WALL_CLOCK_TIMEOUTS: dict[str, int] = {
    "summary": 8 * 60,
    "personalization": 5 * 60,
    "stance": 12 * 60,
    "dream_signal": 5 * 60,
    "dream_decision": 8 * 60,
    "dream_writer": 8 * 60,
    "dream_reflection": 15 * 60,
}
_PROMPT_WALL_CLOCK_TIMEOUTS: tuple[tuple[str, int], ...] = (
    ("dream.pass-d", 8 * 60),
)


@dataclass(frozen=True)
class LLMExecutionResult:
    text: str | None
    data: dict[str, Any] | None
    cache_identity: LLMCacheIdentity
    response_metadata: dict[str, Any]


class LLMExecutor:
    def __init__(self, *, sleeper: callable | None = None):
        self._sleeper = sleeper or time.sleep

    def cache_identity(self, *, task_class: TaskClass, prompt_version: str) -> LLMCacheIdentity:
        route = resolve_route(task_class)
        return self._identity_from_route(
            route=route,
            task_class=task_class,
            prompt_version=prompt_version,
            request_fingerprint={"kind": "text-prompt"},
        )

    def cache_identities(self, *, task_class: TaskClass, prompt_version: str) -> list[LLMCacheIdentity]:
        route = resolve_route(task_class)
        identities = [
            self._identity_from_route(
                route=route,
                task_class=task_class,
                prompt_version=prompt_version,
                request_fingerprint={"kind": "text-prompt"},
            )
        ]
        backup = resolve_backup(task_class=task_class)
        if backup is not None:
            identities.append(
                self._identity_from_route(
                    route=backup,
                    task_class=task_class,
                    prompt_version=prompt_version,
                    request_fingerprint={"kind": "text-prompt"},
                )
            )
        return identities

    def cache_identities_for_parts(
        self,
        *,
        task_class: TaskClass,
        instructions: str,
        input_parts: Iterable[LLMInputPart],
        prompt_version: str,
        input_mode: str | None = None,
        request_metadata: dict[str, Any] | None = None,
    ) -> list[LLMCacheIdentity]:
        route = resolve_route(task_class)
        parts = tuple(input_parts)
        identities = [
            self._identity_from_request(
                self._build_parts_request(
                    route=route,
                    task_class=task_class,
                    instructions=instructions,
                    input_parts=parts,
                    output_mode="json",
                    input_mode=input_mode,
                    request_metadata=request_metadata,
                ),
                prompt_version=prompt_version,
            )
        ]
        backup = resolve_backup(task_class=task_class)
        if backup is not None:
            identities.append(
                self._identity_from_request(
                    self._build_parts_request(
                        route=backup,
                        task_class=task_class,
                        instructions=instructions,
                        input_parts=parts,
                        output_mode="json",
                        input_mode=input_mode,
                        request_metadata=request_metadata,
                    ),
                    prompt_version=prompt_version,
                )
            )
        return identities

    def _identity_from_route(
        self,
        *,
        route: ResolvedRoute,
        task_class: TaskClass,
        prompt_version: str,
        request_fingerprint: dict[str, Any] | None,
    ) -> LLMCacheIdentity:
        return LLMCacheIdentity(
            task_class=task_class,
            provider=route.provider,
            model=route.model,
            transport=route.transport,
            api_family=route.api_family,
            input_mode=route.input_mode,
            prompt_version=prompt_version,
            request_fingerprint={
                **(request_fingerprint or {}),
                **(
                    {
                        "route_top_p": route.top_p,
                        "route_truncation": route.truncation,
                        "route_gateway_options": route.gateway_options,
                        "route_provider_options": route.provider_options,
                    }
                    if any(
                        value is not None
                        for value in (
                            route.top_p,
                            route.truncation,
                            route.gateway_options,
                            route.provider_options,
                        )
                    )
                    else {}
                ),
            } or None,
            temperature=route.temperature,
            max_tokens=route.max_tokens,
            timeout_seconds=route.timeout_seconds,
            reasoning_effort=route.reasoning_effort,
        )

    def execute_text(self, *, task_class: TaskClass, prompt: str, prompt_version: str) -> LLMExecutionResult:
        route = resolve_route(task_class)
        request = self._build_prompt_request(
            route=route,
            task_class=task_class,
            prompt=prompt,
            output_mode="text",
        )
        return self._execute_with_backup(
            primary_request=request,
            task_class=task_class,
            prompt_version=prompt_version,
            build_backup=lambda backup_route: self._build_prompt_request(
                route=backup_route,
                task_class=task_class,
                prompt=prompt,
                output_mode="text",
            ),
        )

    def execute_json(
        self,
        *,
        task_class: TaskClass,
        prompt: str,
        prompt_version: str,
        response_schema: dict[str, Any] | None = None,
        request_metadata: dict[str, Any] | None = None,
    ) -> LLMExecutionResult:
        route = resolve_route(task_class)
        request = self._build_prompt_request(
            route=route,
            task_class=task_class,
            prompt=prompt,
            output_mode="json",
            response_schema=response_schema,
            request_metadata=request_metadata,
        )
        return self._execute_with_backup(
            primary_request=request,
            task_class=task_class,
            prompt_version=prompt_version,
            build_backup=lambda backup_route: self._build_prompt_request(
                route=backup_route,
                task_class=task_class,
                prompt=prompt,
                output_mode="json",
                response_schema=response_schema,
                request_metadata=request_metadata,
            ),
        )

    def execute_parts_json(
        self,
        *,
        task_class: TaskClass,
        instructions: str,
        input_parts: Iterable[LLMInputPart],
        prompt_version: str,
        input_mode: str | None = None,
        request_metadata: dict[str, Any] | None = None,
        response_schema: dict[str, Any] | None = None,
    ) -> LLMExecutionResult:
        route = resolve_route(task_class)
        parts = tuple(input_parts)
        request = self._build_parts_request(
            route=route,
            task_class=task_class,
            instructions=instructions,
            input_parts=parts,
            output_mode="json",
            input_mode=input_mode,
            request_metadata=request_metadata,
            response_schema=response_schema,
        )
        return self._execute_with_backup(
            primary_request=request,
            task_class=task_class,
            prompt_version=prompt_version,
            build_backup=lambda backup_route: self._build_parts_request(
                route=backup_route,
                task_class=task_class,
                instructions=instructions,
                input_parts=parts,
                output_mode="json",
                input_mode=input_mode,
                request_metadata=request_metadata,
                response_schema=response_schema,
            ),
        )

    def _execute_with_backup(
        self,
        *,
        primary_request: LLMRequest,
        task_class: TaskClass,
        prompt_version: str,
        build_backup: callable,
    ) -> LLMExecutionResult:
        backup_route = resolve_backup(task_class=task_class)
        last_error: Exception | None = None
        strict_schema_fallback_attempted = False
        attempt_index = 0
        for index, delay in enumerate((1, 3), start=1):
            try:
                attempt_index += 1
                attempt_role = "primary" if index == 1 else "primary-retry"
                return self._run_request(
                    primary_request,
                    prompt_version=prompt_version,
                    attempt_role=attempt_role,
                    attempt_index=attempt_index,
                )
            except LLMConfigurationError:
                raise
            except LLMTimeoutError as exc:
                last_error = exc
                break
            except (LLMRateLimitError, LLMUnavailableError) as exc:
                last_error = exc
                self._sleeper(exc.retry_after_seconds or delay)
                if index == 2:
                    break
            except LLMStrictSchemaError as exc:
                last_error = exc
                if strict_schema_fallback_attempted or primary_request.response_schema is None:
                    break
                strict_schema_fallback_attempted = True
                primary_request = self._without_response_schema(
                    primary_request,
                    attempt_role="strict-schema-fallback",
                )
                attempt_index += 1
                try:
                    return self._run_request(
                        primary_request,
                        prompt_version=prompt_version,
                        attempt_role="strict-schema-fallback",
                        attempt_index=attempt_index,
                    )
                except (LLMTransportError, LLMStructuredOutputError) as fallback_exc:
                    last_error = fallback_exc
                    self._sleeper(delay)
                continue
            except (LLMTransportError, LLMStructuredOutputError) as exc:
                last_error = exc
                self._sleeper(delay)
                if index == 2:
                    break
        if isinstance(last_error, LLMTimeoutError):
            raise last_error
        if backup_route is None:
            if last_error is not None:
                raise last_error
            raise LLMServiceError("shared backup route is not configured")
        backup_request = build_backup(backup_route)
        attempt_index += 1
        return self._run_request(
            backup_request,
            prompt_version=prompt_version,
            attempt_role="backup",
            attempt_index=attempt_index,
        )

    def _build_prompt_request(
        self,
        *,
        route: ResolvedRoute,
        task_class: TaskClass,
        prompt: str,
        output_mode: str,
        response_schema: dict[str, Any] | None = None,
        request_metadata: dict[str, Any] | None = None,
    ) -> LLMRequest:
        response_schema = self._effective_response_schema(
            route=route,
            task_class=task_class,
            response_schema=response_schema,
        )
        return LLMRequest.from_prompt(
            prompt=prompt,
            output_mode=output_mode,  # type: ignore[arg-type]
            task_class=task_class,
            model=route.model,
            transport=route.transport,
            api_family=route.api_family,
            input_mode="text",
            temperature=route.temperature,
            top_p=route.top_p,
            max_tokens=route.max_tokens,
            timeout_seconds=route.timeout_seconds,
            truncation=route.truncation,
            reasoning_effort=route.reasoning_effort,
            response_schema=response_schema,
            gateway_options=route.gateway_options,
            provider_options=route.provider_options,
            request_metadata=request_metadata,
        )

    def _build_parts_request(
        self,
        *,
        route: ResolvedRoute,
        task_class: TaskClass,
        instructions: str,
        input_parts: tuple[LLMInputPart, ...],
        output_mode: str,
        input_mode: str | None,
        request_metadata: dict[str, Any] | None,
        response_schema: dict[str, Any] | None = None,
    ) -> LLMRequest:
        response_schema = self._effective_response_schema(
            route=route,
            task_class=task_class,
            response_schema=response_schema,
        )
        return LLMRequest(
            instructions=instructions,
            input_parts=input_parts,
            output_mode=output_mode,  # type: ignore[arg-type]
            task_class=task_class,
            model=route.model,
            transport=route.transport,
            api_family=route.api_family,
            input_mode=(input_mode or route.input_mode),  # type: ignore[arg-type]
            temperature=route.temperature,
            top_p=route.top_p,
            max_tokens=route.max_tokens,
            timeout_seconds=route.timeout_seconds,
            truncation=route.truncation,
            reasoning_effort=route.reasoning_effort,
            request_metadata=dict(request_metadata or {}),
            response_schema=response_schema,
            gateway_options=route.gateway_options,
            provider_options=route.provider_options,
        )

    def _run_request(
        self,
        request: LLMRequest,
        *,
        prompt_version: str,
        attempt_role: str,
        attempt_index: int,
    ) -> LLMExecutionResult:
        client = self._provider_client(request)
        start = time.monotonic()
        response_metadata: dict[str, Any] = {}
        try:
            with GLOBAL_RATE_LIMITER.acquire(
                provider=request.provider,
                model=request.model,
                configured_cap=self._concurrency_cap(request.provider),
            ):
                raw_response = self._execute_provider_call(
                    client,
                    request,
                    prompt_version=prompt_version,
                )
            provider_response = self._normalize_provider_response(raw_response)
            response_metadata = dict(provider_response.metadata)
            text = provider_response.text
            identity = self._identity_from_request(request, prompt_version=prompt_version)
            if request.output_mode == "json":
                data = parse_first_json_object(text)
                result = LLMExecutionResult(
                    text=None,
                    data=data,
                    cache_identity=identity,
                    response_metadata=response_metadata,
                )
            else:
                result = LLMExecutionResult(
                    text=text,
                    data=None,
                    cache_identity=identity,
                    response_metadata=response_metadata,
                )
            self._log_attempt(
                request=request,
                prompt_version=prompt_version,
                attempt_role=attempt_role,
                attempt_index=attempt_index,
                latency_ms=int((time.monotonic() - start) * 1000),
                response_metadata=response_metadata,
                error_class=None,
                status="success",
            )
            return result
        except Exception as exc:
            self._log_attempt(
                request=request,
                prompt_version=prompt_version,
                attempt_role=attempt_role,
                attempt_index=attempt_index,
                latency_ms=int((time.monotonic() - start) * 1000),
                response_metadata=response_metadata,
                error_class=type(exc).__name__,
                status="error",
            )
            raise

    def _execute_provider_call(
        self,
        client: LLMProviderClient,
        request: LLMRequest,
        *,
        prompt_version: str,
    ) -> str | LLMProviderResponse:
        timeout_seconds = self._wall_clock_timeout_seconds_for_request(
            request,
            prompt_version=prompt_version,
        )
        label = f"{request.task_class}:{prompt_version}"
        with _wall_clock_timeout(timeout_seconds, label=label):
            return client.execute(request)

    def _wall_clock_timeout_seconds_for_request(
        self,
        request: LLMRequest,
        *,
        prompt_version: str,
    ) -> int | None:
        stage_timeout: int | None = None
        for prefix, seconds in _PROMPT_WALL_CLOCK_TIMEOUTS:
            if prompt_version.startswith(prefix):
                stage_timeout = seconds
                break
        if stage_timeout is None:
            stage_timeout = _STAGE_WALL_CLOCK_TIMEOUTS.get(request.task_class)

        configured_timeout = request.timeout_seconds
        if configured_timeout and stage_timeout:
            return min(configured_timeout, stage_timeout)
        return configured_timeout or stage_timeout

    def _identity_from_request(self, request: LLMRequest, *, prompt_version: str) -> LLMCacheIdentity:
        return LLMCacheIdentity(
            task_class=request.task_class,
            provider=request.provider,
            model=request.model,
            transport=request.transport,
            api_family=request.api_family,
            input_mode=request.input_mode,
            prompt_version=prompt_version,
            request_fingerprint=request.request_fingerprint(),
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            timeout_seconds=request.timeout_seconds,
            reasoning_effort=request.reasoning_effort,
        )

    def _provider_client(self, request: LLMRequest) -> LLMProviderClient:
        from scripts.common import env

        provider = request.provider
        if provider not in SUPPORTED_PROVIDERS:
            raise LLMConfigurationError(f"unsupported llm provider {provider!r}")
        cfg = env.load()
        return GatewayProviderClient(
            api_key=getattr(cfg, "ai_gateway_api_key", ""),
            model=request.model,
        )

    def execute_request(self, *, request: LLMRequest, prompt_version: str) -> LLMExecutionResult:
        attempt_role = str(request.request_metadata.get("attempt_role") or "primary")
        attempt_index = int(request.request_metadata.get("attempt_index") or 1)
        return self._run_request(
            request,
            prompt_version=prompt_version,
            attempt_role=attempt_role,
            attempt_index=attempt_index,
        )

    def build_prompt_request(
        self,
        *,
        task_class: TaskClass,
        prompt: str,
        output_mode: str,
        response_schema: dict[str, Any] | None = None,
        request_metadata: dict[str, Any] | None = None,
    ) -> LLMRequest:
        route = resolve_route(task_class)
        return self._build_prompt_request(
            route=route,
            task_class=task_class,
            prompt=prompt,
            output_mode=output_mode,
            response_schema=response_schema,
            request_metadata=request_metadata,
        )

    def build_parts_request(
        self,
        *,
        task_class: TaskClass,
        instructions: str,
        input_parts: Iterable[LLMInputPart],
        output_mode: str,
        input_mode: str | None = None,
        request_metadata: dict[str, Any] | None = None,
        response_schema: dict[str, Any] | None = None,
    ) -> LLMRequest:
        route = resolve_route(task_class)
        return self._build_parts_request(
            route=route,
            task_class=task_class,
            instructions=instructions,
            input_parts=tuple(input_parts),
            output_mode=output_mode,
            input_mode=input_mode,
            request_metadata=request_metadata,
            response_schema=response_schema,
        )

    def _effective_response_schema(
        self,
        *,
        route: ResolvedRoute,
        task_class: TaskClass,
        response_schema: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if response_schema is None:
            return None
        if route.supports_strict_schema:
            return response_schema
        warn_strict_schema_route(task_class, route.to_public_dict())
        return None

    def _without_response_schema(self, request: LLMRequest, *, attempt_role: str) -> LLMRequest:
        metadata = dict(request.request_metadata)
        metadata["attempt_role"] = attempt_role
        return LLMRequest(
            instructions=request.instructions,
            input_parts=request.input_parts,
            output_mode=request.output_mode,
            task_class=request.task_class,
            model=request.model,
            transport=request.transport,
            api_family=request.api_family,
            input_mode=request.input_mode,
            tools=request.tools,
            response_schema=None,
            temperature=request.temperature,
            top_p=request.top_p,
            max_tokens=request.max_tokens,
            timeout_seconds=request.timeout_seconds,
            truncation=request.truncation,
            reasoning_effort=request.reasoning_effort,
            gateway_options=request.gateway_options,
            provider_options=request.provider_options,
            request_metadata=metadata,
        )

    def _normalize_provider_response(self, response: str | LLMProviderResponse) -> LLMProviderResponse:
        if isinstance(response, LLMProviderResponse):
            return response
        return LLMProviderResponse(text=str(response), metadata={})

    def _log_attempt(
        self,
        *,
        request: LLMRequest,
        prompt_version: str,
        attempt_role: str,
        attempt_index: int,
        latency_ms: int,
        response_metadata: dict[str, Any],
        error_class: str | None,
        status: str,
    ) -> None:
        try:
            from scripts.common import env

            cfg = env.load()
            usage = response_metadata.get("usage") if isinstance(response_metadata, dict) else {}
            usage = usage if isinstance(usage, dict) else {}
            log_event(
                cfg.repo_root,
                task_class=request.task_class,
                prompt_version=prompt_version,
                provider=request.provider,
                model=request.model,
                bundle_id=_bundle_id_from_metadata(request.request_metadata),
                attempt_role=attempt_role,
                attempt_index=attempt_index,
                status=status,
                latency_ms=latency_ms,
                response_id=_text_or_none(response_metadata.get("response_id")),
                generation_id=_text_or_none(response_metadata.get("generation_id")),
                tokens_in=_int_or_none(usage.get("prompt_tokens")),
                tokens_out=_int_or_none(usage.get("completion_tokens")),
                tokens_total=_int_or_none(usage.get("total_tokens")),
                error_class=error_class,
                request_metadata=request.request_metadata,
            )
        except Exception:
            return

    def _concurrency_cap(self, provider: str) -> int:
        from scripts.common import env

        cfg = env.load()
        configured = getattr(cfg, "llm_concurrency", {}).get(provider)
        return normalize_concurrency(configured)


def _bundle_id_from_metadata(metadata: dict[str, Any]) -> str | None:
    value = metadata.get("bundle_id")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@contextlib.contextmanager
def _wall_clock_timeout(timeout_seconds: int | None, *, label: str):
    if not timeout_seconds or timeout_seconds <= 0:
        yield
        return
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)
    start = time.monotonic()

    def _handle_timeout(_signum, _frame):
        raise LLMTimeoutError(
            f"LLM wall-clock timeout after {timeout_seconds}s during {label}"
        )

    try:
        signal.signal(signal.SIGALRM, _handle_timeout)
        signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
        yield
    finally:
        elapsed = time.monotonic() - start
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_delay > 0:
            remaining = max(previous_delay - elapsed, 0.0)
            signal.setitimer(signal.ITIMER_REAL, remaining, previous_interval)
