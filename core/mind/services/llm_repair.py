"""Single-turn structured-output repair helpers."""
from __future__ import annotations

import json
from typing import Any

from .llm_executor import LLMExecutor
from .providers.base import LLMInputPart, LLMRequest


def compact_validation_errors(errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for error in errors:
        compact.append(
            {
                "loc": list(error.get("loc") or ()),
                "msg": str(error.get("msg") or ""),
                "type": str(error.get("type") or ""),
            }
        )
    return compact


def repair_once(
    executor: LLMExecutor,
    *,
    original_request: LLMRequest,
    prompt_version: str,
    response_schema: dict[str, Any] | None,
    validation_errors: list[dict[str, Any]],
    invalid_payload: dict[str, Any],
) -> dict[str, Any]:
    repair_request = _build_repair_request(
        original_request=original_request,
        response_schema=response_schema,
        validation_errors=validation_errors,
        invalid_payload=invalid_payload,
    )
    result = executor.execute_request(request=repair_request, prompt_version=prompt_version)
    return result.data or {}


def _build_repair_request(
    *,
    original_request: LLMRequest,
    response_schema: dict[str, Any] | None,
    validation_errors: list[dict[str, Any]],
    invalid_payload: dict[str, Any],
) -> LLMRequest:
    repair_context = (
        "Your previous JSON output failed validation.\n"
        "Return corrected JSON only. Do not include commentary, markdown fences, or explanations.\n\n"
        f"Validation errors:\n{json.dumps(validation_errors, indent=2, ensure_ascii=False)}\n\n"
        f"Invalid JSON:\n{json.dumps(invalid_payload, indent=2, ensure_ascii=False)}"
    )
    metadata = dict(original_request.request_metadata)
    metadata["attempt_role"] = "repair"
    return LLMRequest(
        instructions=_merge_instructions(original_request.instructions),
        input_parts=original_request.input_parts + (LLMInputPart.text_part(repair_context),),
        output_mode=original_request.output_mode,
        task_class=original_request.task_class,
        model=original_request.model,
        transport=original_request.transport,
        api_family=original_request.api_family,
        input_mode=original_request.input_mode,
        tools=original_request.tools,
        response_schema=response_schema,
        temperature=original_request.temperature,
        max_tokens=original_request.max_tokens,
        timeout_seconds=original_request.timeout_seconds,
        reasoning_effort=original_request.reasoning_effort,
        request_metadata=metadata,
    )


def _merge_instructions(original_instructions: str) -> str:
    repair_instructions = (
        "Repair the prior JSON so it satisfies the attached schema exactly. "
        "Keep the intended meaning where possible and do not invent unrelated content."
    )
    if not original_instructions.strip():
        return repair_instructions
    return f"{original_instructions.rstrip()}\n\n{repair_instructions}"
