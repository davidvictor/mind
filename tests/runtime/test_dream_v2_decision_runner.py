from __future__ import annotations

from types import SimpleNamespace

from mind.dream.v2.decision_runner import DecisionRunner
from mind.services.llm_cache import LLMCacheIdentity
from mind.services.providers.base import LLMRequest
from pydantic import BaseModel, ConfigDict, Field


class ReflectionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observations: list[str] = Field(default_factory=list)
    carry_forward_atom_ids: list[str] = Field(default_factory=list)


class _FakeExecutor:
    def __init__(self, payload: dict[str, object]):
        self.payload = payload
        self.built_requests: list[LLMRequest] = []
        self.request_metadata_seen: list[dict[str, object] | None] = []

    def execute_json(self, *, task_class, prompt, prompt_version, response_schema=None, request_metadata=None):
        self.request_metadata_seen.append(request_metadata)
        return SimpleNamespace(
            data=self.payload,
            cache_identity=LLMCacheIdentity(
                task_class=task_class,
                provider="anthropic",
                model="anthropic/claude-sonnet-4.6",
                transport="ai_gateway",
                api_family="responses",
                input_mode="text",
                prompt_version=prompt_version,
                request_fingerprint={"kind": "text-prompt"},
            ),
            response_metadata={"generation_id": "gen-test"},
        )

    def build_prompt_request(self, *, task_class, prompt, output_mode, response_schema=None, request_metadata=None):
        request = LLMRequest.from_prompt(
            prompt=prompt,
            output_mode=output_mode,
            task_class=task_class,
            model="anthropic/claude-sonnet-4.6",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
            response_schema=response_schema,
            request_metadata=request_metadata,
        )
        self.built_requests.append(request)
        return request


def test_decision_runner_returns_validated_payload_and_receipt() -> None:
    executor = _FakeExecutor(
            {
                "observations": ["singleton window"],
                "carry_forward_atom_ids": ["alpha"],
            }
        )
    runner = DecisionRunner(executor=executor)

    result = runner.run_prompt(
        prompt_family="rem.reflection",
        prompt="prompt",
        response_model=ReflectionDecision,
        task_class="dream_decision",
        prompt_version="dream.rem.reflection.test",
        request_metadata={"run_id": "run-1", "stage": "rem"},
    )

    assert executor.request_metadata_seen == [{"run_id": "run-1", "stage": "rem"}]
    assert result.payload.observations == ["singleton window"]
    assert result.receipt.model == "anthropic/claude-sonnet-4.6"
    assert result.receipt.request_metadata == {"run_id": "run-1", "stage": "rem"}
    assert result.receipt.repaired is False


def test_decision_runner_repairs_invalid_payload_once(monkeypatch) -> None:
    executor = _FakeExecutor(
        {
            "observations": "invalid",
            "carry_forward_atom_ids": [],
        }
    )
    runner = DecisionRunner(executor=executor)

    monkeypatch.setattr(
        "mind.dream.v2.decision_runner.repair_once",
        lambda *_args, **_kwargs: {
            "observations": ["repaired"],
            "carry_forward_atom_ids": ["alpha"],
        },
    )

    result = runner.run_prompt(
        prompt_family="rem.reflection",
        prompt="prompt",
        response_model=ReflectionDecision,
        task_class="dream_decision",
        prompt_version="dream.rem.reflection.test",
        request_metadata={"run_id": "run-1", "stage": "rem"},
    )

    assert executor.built_requests
    assert executor.built_requests[0].request_metadata == {"run_id": "run-1", "stage": "rem"}
    assert result.payload.observations == ["repaired"]
    assert result.receipt.repaired is True
