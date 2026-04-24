from __future__ import annotations

from dataclasses import dataclass

from .llm_routing import ResolvedRoute, resolve_route
from .providers.base import LLMConfigurationError, LLMServiceError, LLMTransportError
from .providers.gateway import GatewayProviderClient
from scripts.common import env


@dataclass(frozen=True)
class EmbeddingIdentity:
    provider: str
    model: str
    transport: str
    api_family: str
    input_mode: str
    dimensions: int | None = None


@dataclass(frozen=True)
class EmbeddingExecutionResult:
    vectors: list[list[float]]
    identity: EmbeddingIdentity
    response_metadata: dict[str, object]


class EmbeddingExecutor:
    def execute(
        self,
        *,
        inputs: list[str],
        dimensions: int | None = None,
    ) -> EmbeddingExecutionResult:
        route = resolve_route("embedding")
        if route.transport != "ai_gateway":
            raise LLMConfigurationError(f"unsupported embedding transport {route.transport!r}")
        client = self._provider_client(route)
        try:
            vectors, metadata = client.embed(
                inputs=inputs,
                dimensions=dimensions,
                timeout_seconds=route.timeout_seconds,
            )
        except LLMTransportError:
            raise
        except Exception as exc:
            raise LLMServiceError(str(exc)) from exc
        return EmbeddingExecutionResult(
            vectors=vectors,
            identity=EmbeddingIdentity(
                provider=route.provider,
                model=route.model,
                transport=route.transport,
                api_family=route.api_family,
                input_mode=route.input_mode,
                dimensions=dimensions,
            ),
            response_metadata=metadata,
        )

    def _provider_client(self, route: ResolvedRoute) -> GatewayProviderClient:
        cfg = env.load()
        return GatewayProviderClient(
            api_key=getattr(cfg, "ai_gateway_api_key", ""),
            model=route.model,
        )
