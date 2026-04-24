from __future__ import annotations

import pytest
from pydantic import ValidationError

from mind.dream.v2.contracts import WeaveLocalProposalArtifact, WeaveLocalProposalResponse
from mind.services.llm_schema import prepare_strict_schema


def test_weave_local_proposal_contract_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        WeaveLocalProposalArtifact.model_validate(
            {
                "window_id": "window-001-alpha",
                "seed_atom_id": "alpha",
                "clusters": [],
                "leftover_atom_ids": [],
                "bridge_candidates": [],
                "window_observations": [],
                "unexpected": True,
            }
        )


def test_weave_local_proposal_contract_produces_strict_schema() -> None:
    schema = prepare_strict_schema(WeaveLocalProposalArtifact)
    assert "$defs" not in schema
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert "window_id" in schema["required"]


def test_weave_local_prompt_response_schema_does_not_require_runtime_owned_ids() -> None:
    schema = prepare_strict_schema(WeaveLocalProposalResponse)
    cluster_schema = schema["properties"]["clusters"]["items"]
    assert "cluster_id" not in cluster_schema["required"]
