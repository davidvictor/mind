"""Field-name normalization shims for cached LLM output.

When prompt schemas evolve (e.g. Phase 5 renames), previously-cached research
JSON still uses the old field names. These shims map old → new so that
downstream renderers and enrichment passes see a consistent schema without
re-calling the LLM.
"""
from __future__ import annotations

from typing import Any


def normalize_book_research(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize legacy book research field names to the universal schema.

    Mappings:
      key_ideas        → key_claims  (shape: {idea, explanation} → {claim, evidence_context})
      memorable_stories → memorable_examples
      famous_quotes     → notable_quotes

    Idempotent: if the new field name already exists, it takes precedence.
    Returns a new dict (does not mutate the input).
    """
    out = dict(data)

    # key_ideas → key_claims
    if "key_claims" not in out and "key_ideas" in out:
        old_items = out.pop("key_ideas", []) or []
        normalized = []
        for item in old_items:
            if isinstance(item, dict):
                normalized.append({
                    "claim": item.get("idea", item.get("claim", "")),
                    "evidence_context": item.get("explanation", item.get("evidence_context", "")),
                })
            elif isinstance(item, str):
                normalized.append({"claim": item, "evidence_context": ""})
        out["key_claims"] = normalized

    # memorable_stories → memorable_examples
    if "memorable_examples" not in out and "memorable_stories" in out:
        out["memorable_examples"] = out.pop("memorable_stories")

    # famous_quotes → notable_quotes
    if "notable_quotes" not in out and "famous_quotes" in out:
        out["notable_quotes"] = out.pop("famous_quotes")

    return out
