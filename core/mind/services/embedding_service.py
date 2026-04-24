from __future__ import annotations

from dataclasses import dataclass

from .embedding_executor import EmbeddingExecutionResult, EmbeddingExecutor
from .providers.base import LLMStructuredOutputError


@dataclass(frozen=True)
class EmbeddingRequest:
    target_id: str
    target_type: str
    content: str
    content_sha256: str


@dataclass(frozen=True)
class EmbeddingRecord:
    target_id: str
    target_type: str
    content_sha256: str
    vector: list[float]
    model: str
    vector_dim: int


@dataclass(frozen=True)
class EmbeddingBatchResult:
    records: list[EmbeddingRecord]
    model: str
    provider: str
    transport: str
    api_family: str
    input_mode: str
    response_metadata: dict[str, object]


class EmbeddingService:
    def __init__(self, *, executor: EmbeddingExecutor | None = None):
        self.executor = executor or EmbeddingExecutor()

    def embed_requests(self, requests: list[EmbeddingRequest]) -> EmbeddingBatchResult:
        if not requests:
            return EmbeddingBatchResult(
                records=[],
                model="",
                provider="",
                transport="",
                api_family="",
                input_mode="",
                response_metadata={},
            )
        execution = self.executor.execute(inputs=[request.content for request in requests])
        if len(execution.vectors) != len(requests):
            raise LLMStructuredOutputError(
                "embedding response cardinality mismatch: "
                f"expected {len(requests)} vectors, got {len(execution.vectors)}"
            )
        records = [
            EmbeddingRecord(
                target_id=request.target_id,
                target_type=request.target_type,
                content_sha256=request.content_sha256,
                vector=vector,
                model=execution.identity.model,
                vector_dim=len(vector),
            )
            for request, vector in zip(requests, execution.vectors, strict=False)
        ]
        return EmbeddingBatchResult(
            records=records,
            model=execution.identity.model,
            provider=execution.identity.provider,
            transport=execution.identity.transport,
            api_family=execution.identity.api_family,
            input_mode=execution.identity.input_mode,
            response_metadata=execution.response_metadata,
        )

    def embed_query(self, text: str) -> EmbeddingExecutionResult:
        return self.executor.execute(inputs=[text])


_EMBEDDING_SERVICE: EmbeddingService | None = None


def get_embedding_service() -> EmbeddingService:
    global _EMBEDDING_SERVICE
    if _EMBEDDING_SERVICE is None:
        _EMBEDDING_SERVICE = EmbeddingService()
    return _EMBEDDING_SERVICE
