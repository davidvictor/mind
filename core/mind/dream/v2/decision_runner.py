from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ValidationError

from mind.services.llm_executor import LLMExecutor
from mind.services.llm_repair import compact_validation_errors, repair_once
from mind.services.llm_schema import prepare_strict_schema

from .contracts import PromptReceipt

TModel = TypeVar("TModel", bound=BaseModel)


@dataclass(frozen=True)
class DecisionRunResult(Generic[TModel]):
    payload: TModel
    receipt: PromptReceipt


class DecisionRunner:
    def __init__(self, *, executor: LLMExecutor | None = None):
        self.executor = executor or LLMExecutor()

    def run_prompt(
        self,
        *,
        prompt_family: str,
        prompt: str,
        response_model: type[TModel],
        task_class: str,
        prompt_version: str,
        request_metadata: dict[str, Any] | None = None,
    ) -> DecisionRunResult[TModel]:
        response_schema = prepare_strict_schema(response_model)
        result = self.executor.execute_json(
            task_class=task_class,  # type: ignore[arg-type]
            prompt=prompt,
            prompt_version=prompt_version,
            response_schema=response_schema,
            request_metadata=request_metadata,
        )
        payload_data = result.data or {}
        repaired = False
        try:
            payload = response_model.model_validate(payload_data)
        except ValidationError as exc:
            request = self.executor.build_prompt_request(
                task_class=task_class,  # type: ignore[arg-type]
                prompt=prompt,
                output_mode="json",
                response_schema=response_schema,
                request_metadata=request_metadata,
            )
            repaired_payload = repair_once(
                self.executor,
                original_request=request,
                prompt_version=prompt_version,
                response_schema=response_schema,
                validation_errors=compact_validation_errors(exc.errors()),
                invalid_payload=payload_data,
            )
            payload = response_model.model_validate(repaired_payload)
            payload_data = repaired_payload
            repaired = True
        receipt = PromptReceipt(
            prompt_family=prompt_family,
            prompt_version=prompt_version,
            task_class=task_class,
            provider=result.cache_identity.provider,
            model=result.cache_identity.model,
            input_mode=result.cache_identity.input_mode,
            request_fingerprint=result.cache_identity.request_fingerprint or {},
            request_metadata=dict(request_metadata or {}),
            response_metadata=result.response_metadata,
            repaired=repaired,
        )
        return DecisionRunResult(payload=payload, receipt=receipt)
