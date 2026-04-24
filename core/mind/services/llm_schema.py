"""Pydantic → OpenAI Responses API strict-mode JSON schema helper.

The Vercel AI Gateway forwards `text.format = {type: "json_schema", strict: true, schema: ...}`
to Anthropic and OpenAI providers. Anthropic and OpenAI both require:

  * `additionalProperties: false` on every object
  * No external `$ref` (defs must be inlined)
  * No `title` keys (purely informational, occasionally rejected)
  * Every property listed under `required`

Pydantic emits schemas that violate all four rules by default. This
module converts a Pydantic model class into a schema the gateway will
accept under strict mode, without modifying the original model.

Gemini's OpenAI-compatible shim has documented bugs handling required
vs optional fields, so callers that target Gemini should NOT pass the
schema (route accordingly via `ResolvedRoute.supports_strict_schema`).
See plans/lexical-plotting-fairy.md Phase B + F.
"""
from __future__ import annotations

import copy
from typing import Any

from pydantic import BaseModel


def prepare_strict_schema(model_cls: type[BaseModel]) -> dict[str, Any]:
    """Convert a Pydantic model class to a strict-mode JSON Schema.

    Returns a deep-copied, mutated dict ready to pass as the `schema`
    field inside `text.format = {type: "json_schema", strict: true, ...}`.
    """
    raw = model_cls.model_json_schema()
    defs = dict(raw.get("$defs") or {})
    schema = copy.deepcopy(raw)
    # Remove top-level $defs after we use it for inlining
    schema.pop("$defs", None)
    schema = _inline_refs(schema, defs)
    _strict_normalize(schema)
    return schema


def _inline_refs(node: Any, defs: dict[str, dict[str, Any]]) -> Any:
    """Recursively replace $ref nodes with the inlined definition."""
    if isinstance(node, dict):
        if "$ref" in node and isinstance(node["$ref"], str):
            ref = node["$ref"]
            if ref.startswith("#/$defs/"):
                key = ref.split("/", 2)[-1]
                target = defs.get(key)
                if target is not None:
                    inlined = _inline_refs(copy.deepcopy(target), defs)
                    # Merge any sibling keys (e.g. description) onto the inlined node.
                    for sibling_key, sibling_val in node.items():
                        if sibling_key == "$ref":
                            continue
                        inlined.setdefault(sibling_key, sibling_val)
                    return inlined
            return node
        return {key: _inline_refs(value, defs) for key, value in node.items()}
    if isinstance(node, list):
        return [_inline_refs(item, defs) for item in node]
    return node


def _strict_normalize(node: Any) -> None:
    """In-place: drop `title`, set `additionalProperties: false`, and force
    every property into `required` for object nodes.

    For union/anyOf nodes containing `null`, leaves them alone — Pydantic
    emits `Optional[T]` as `anyOf: [T, {type: null}]`. Strict mode allows
    nullable fields when the union explicitly lists null; the field still
    needs to appear in the parent's `required` list.
    """
    if isinstance(node, dict):
        node.pop("title", None)
        # Remove `default` — strict mode treats it as a model hint, not a
        # contract; Pydantic includes it on every Optional field.
        node.pop("default", None)
        if node.get("type") == "object":
            properties = node.get("properties")
            if isinstance(properties, dict):
                node["additionalProperties"] = False
                # Strict mode requires *all* properties listed in required.
                node["required"] = sorted(properties.keys())
                for prop_value in properties.values():
                    _strict_normalize(prop_value)
        # Recurse into typical nested schema containers.
        for key in ("items", "prefixItems"):
            if key in node:
                _strict_normalize(node[key])
        for key in ("anyOf", "oneOf", "allOf"):
            if key in node and isinstance(node[key], list):
                for sub in node[key]:
                    _strict_normalize(sub)
        # Recurse into property values too (covers root-level case where the
        # node is *not* declared as type=object but still has properties).
        if "properties" in node and isinstance(node["properties"], dict):
            for prop_value in node["properties"].values():
                _strict_normalize(prop_value)
    elif isinstance(node, list):
        for item in node:
            _strict_normalize(item)
