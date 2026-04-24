from __future__ import annotations

from types import SimpleNamespace

from mind.dream.v2.contracts import WeaveLocalProposalResponse
from mind.dream.v2.decision_runner import DecisionRunner
from mind.services.llm_cache import LLMCacheIdentity
from mind.services.providers.base import LLMRequest


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
                "clusters": [],
                "leftover_atom_ids": ["alpha"],
                "bridge_candidates": [],
                "window_observations": ["singleton window"],
            }
        )
    runner = DecisionRunner(executor=executor)

    result = runner.run_prompt(
        prompt_family="weave.local_cluster",
        prompt="prompt",
        response_model=WeaveLocalProposalResponse,
        task_class="dream_decision",
        prompt_version="dream.weave.local-cluster.v2",
        request_metadata={"run_id": "run-1", "window_id": "window-001-alpha"},
    )

    assert executor.request_metadata_seen == [{"run_id": "run-1", "window_id": "window-001-alpha"}]
    assert result.payload.window_observations == ["singleton window"]
    assert result.receipt.model == "anthropic/claude-sonnet-4.6"
    assert result.receipt.request_metadata == {"run_id": "run-1", "window_id": "window-001-alpha"}
    assert result.receipt.repaired is False


def test_decision_runner_repairs_invalid_payload_once(monkeypatch) -> None:
    executor = _FakeExecutor(
        {
            "clusters": "invalid",
            "leftover_atom_ids": [],
            "bridge_candidates": [],
            "window_observations": [],
        }
    )
    runner = DecisionRunner(executor=executor)

    monkeypatch.setattr(
        "mind.dream.v2.decision_runner.repair_once",
        lambda *_args, **_kwargs: {
            "clusters": [],
            "leftover_atom_ids": ["alpha"],
            "bridge_candidates": [],
            "window_observations": ["repaired"],
        },
    )

    result = runner.run_prompt(
        prompt_family="weave.local_cluster",
        prompt="prompt",
        response_model=WeaveLocalProposalResponse,
        task_class="dream_decision",
        prompt_version="dream.weave.local-cluster.v2",
        request_metadata={"run_id": "run-1", "window_id": "window-001-alpha"},
    )

    assert executor.built_requests
    assert executor.built_requests[0].request_metadata == {"run_id": "run-1", "window_id": "window-001-alpha"}
    assert result.payload.window_observations == ["repaired"]
    assert result.receipt.repaired is True
