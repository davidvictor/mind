from __future__ import annotations

from typing import Any

from mind.services.llm_schema import prepare_strict_schema
from mind.services.onboarding_synthesis import GraphArtifact, MergeArtifact, SemanticArtifact, VerifyArtifact


def test_prepare_strict_schema_normalizes_onboarding_models() -> None:
    for model_cls in (SemanticArtifact, GraphArtifact, MergeArtifact, VerifyArtifact):
        schema = prepare_strict_schema(model_cls)
        assert "$defs" not in schema
        _assert_strict_schema_tree(schema)


def test_prepare_strict_schema_preserves_nullable_unions() -> None:
    schema = prepare_strict_schema(MergeArtifact)
    target_page_id = schema["properties"]["decisions"]["items"]["properties"]["target_page_id"]
    assert "anyOf" in target_page_id
    assert any(option.get("type") == "null" for option in target_page_id["anyOf"])


def _assert_strict_schema_tree(node: Any) -> None:
    if isinstance(node, dict):
        assert "$ref" not in node
        if any(key in node for key in ("type", "properties", "items", "anyOf", "oneOf", "allOf", "$ref")):
            assert "title" not in node
        if node.get("type") == "object" and isinstance(node.get("properties"), dict):
            properties = node["properties"]
            assert node.get("additionalProperties") is False
            assert sorted(node.get("required") or []) == sorted(properties.keys())
        for value in node.values():
            _assert_strict_schema_tree(value)
    elif isinstance(node, list):
        for value in node:
            _assert_strict_schema_tree(value)
