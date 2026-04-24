"""Pass D prompt template construction.

Builds the fully-substituted Gemini prompt for run_pass_d. Handles all the
%% substitutions including %%ANTI_SALES%% (imports from
scripts.common.anti_sales).

Historical design notes are kept outside the public release tree.
"""
from __future__ import annotations

import json

from scripts.common.anti_sales import ANTI_SALES_RULE_PROMPT
from scripts.atoms.types import Atom

PASS_D_PROMPT_VERSION = "dream.pass-d.v3"


def _serialize_working_set(working_set: list[Atom]) -> str:
    if not working_set:
        return "(empty working set)"
    lines = []
    for atom in working_set:
        domains = ",".join(atom.domains)
        lines.append(
            f"- {atom.id} | {atom.type} | {atom.lifecycle_state} | domains:{domains} | {atom.tldr}"
        )
    return "\n".join(lines)


def build_pass_d_prompt(
    *,
    source_id: str,
    source_link: str,
    source_kind: str,
    body_or_transcript: str,
    summary: dict,
    applied: dict | None,
    pass_c_delta: str | None,
    stance_context: str,
    prior_source_context: str,
    working_set: list[Atom],
    open_inquiries_context: str = "",
) -> str:
    """Return the fully-substituted Pass D prompt string.

    The prompt stays generic at the shared seam and constrains outputs to the
    atom substrate contract only.
    """
    summary_json = json.dumps(summary, ensure_ascii=False, sort_keys=True)
    applied_json = json.dumps(applied or {"status": "skipped"}, ensure_ascii=False, sort_keys=True)
    stance_delta = pass_c_delta or "(skipped)"
    prior_context = prior_source_context or "(none)"
    creator_stance = stance_context or "(none)"
    working_set_text = _serialize_working_set(working_set)

    # Open inquiries block (optional — only when wiki/me/open-inquiries.md exists)
    inquiries_block = ""
    if open_inquiries_context and open_inquiries_context.strip():
        inquiries_block = (
            "## Owner's open inquiries\n"
            f"{open_inquiries_context.strip()}\n\n"
            "When evaluating q2_candidates, give extra weight to atoms that speak "
            "to an active inquiry. An atom addressing an open inquiry is more valuable "
            "than one that's merely interesting.\n\n"
        )

    return (
        "You are running Pass D for the Brain knowledge base.\n\n"
        f"Source kind: {source_kind}\n"
        f"Source id: {source_id}\n"
        f"Source link: {source_link}\n\n"
        "## Prior source context\n"
        f"{prior_context}\n\n"
        "## Current creator stance context\n"
        f"{creator_stance}\n\n"
        "## Pass A summary\n"
        f"{summary_json}\n\n"
        "## Pass B applied-to-you\n"
        f"{applied_json}\n\n"
        "## Pass C stance delta\n"
        f"{stance_delta}\n\n"
        "## Full source body\n"
        f"{body_or_transcript}\n\n"
        f"## Working set ({len(working_set)} entries)\n"
        f"{working_set_text}\n\n"
        f"{inquiries_block}"
        f"{ANTI_SALES_RULE_PROMPT}\n\n"
        "Return ONLY a JSON object with this exact shape:\n"
        "{\n"
        '  "q1_matches": [\n'
        '    {"atom_id": "string", "atom_type": "concept|playbook|stance|inquiry", '
        '"snippet": "string", "polarity": "for|against|neutral", '
        '"confidence": "low|medium|high", '
        '"evidence_strength": "anecdotal|empirical|theoretical|experiential", '
        '"relation_kind": "supports|contradicts|example_of|applies_to|depends_on|extends|adjacent_to"}\n'
        "  ],\n"
        '  "q2_candidates": [\n'
        '    {"type": "concept|playbook|stance|inquiry", "proposed_id": "string", '
        '"title": "string", "description": "string", "tldr": "string", '
        '"snippet": "string", "polarity": "for|against|neutral", '
        '"rationale": "string", "domains": ["meta|work|craft|learning|identity"], '
        '"in_conversation_with": ["other-atom-id"], '
        '"steps": ["string"], "position": "string", "question": "string"}\n'
        "  ]\n"
        "}\n"
    )
