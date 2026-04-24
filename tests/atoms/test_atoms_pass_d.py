from __future__ import annotations

import json
from pathlib import Path

import pytest

from mind.services.llm_cache import LLMCacheIdentity
from scripts.atoms.pass_d import (
    PASS_D_TASK_CLASS,
    _parse_pass_d_result,
    pass_d_cache_identities,
    pass_d_cache_path,
    run_pass_d,
    stage_outcomes_from_payload,
)
from scripts.atoms.prompts import PASS_D_PROMPT_VERSION, build_pass_d_prompt
from scripts.atoms.types import Atom


def _atom(atom_id: str = "concept-a") -> Atom:
    return Atom(
        id=atom_id,
        type="concept",
        path=Path(f"wiki/concepts/{atom_id}.md"),
        lifecycle_state="active",
        domains=["learning"],
        topics=["systems"],
        last_evidence_date="2026-04-08",
        evidence_count=2,
        tldr="a concept",
    )


def test_build_pass_d_prompt_contains_required_sections() -> None:
    prompt = build_pass_d_prompt(
        source_id="source-1",
        source_link="[[summary-source-1]]",
        source_kind="article",
        body_or_transcript="Body text",
        summary={"tldr": "Summary", "topics": ["systems"]},
        applied={"applied_paragraph": "Applies"},
        pass_c_delta="Changed stance",
        stance_context="stance ctx",
        prior_source_context="prior ctx",
        working_set=[_atom()],
    )
    assert "source-1" in prompt
    assert "[[summary-source-1]]" in prompt
    assert "stance ctx" in prompt
    assert "prior ctx" in prompt
    assert "ANTI-SALES RULE" in prompt
    assert "concept-a | concept | active" in prompt


def test_run_pass_d_parses_and_caches(monkeypatch, tmp_path: Path) -> None:
    response = {
        "q1_matches": [
            {
                "atom_id": "concept-a",
                "atom_type": "concept",
                "snippet": "supports the concept",
                "polarity": "for",
                "confidence": "high",
                "evidence_strength": "empirical",
                "relation_kind": "example_of",
            }
        ],
        "q2_candidates": [
            {
                "type": "inquiry",
                "proposed_id": "open-question",
                "title": "Open Question",
                "description": "something to track",
                "tldr": "A new question to track.",
                "snippet": "new inquiry",
                "polarity": "neutral",
                "rationale": "worth tracking",
                "domains": ["work"],
                "in_conversation_with": ["existing-question"],
                "question": "What should we track next?",
            }
        ],
    }

    class FakeService:
        def generate_json_prompt(self, prompt: str) -> dict:
            assert "summary-source-1" in prompt
            return response

    monkeypatch.setattr("scripts.atoms.pass_d.get_llm_service", lambda: FakeService())

    result = run_pass_d(
        source_id="source-1",
        source_link="[[summary-source-1]]",
        source_kind="article",
        body_or_transcript="Body text",
        summary={"tldr": "Summary"},
        applied=None,
        pass_c_delta=None,
        stance_context="",
        prior_source_context="",
        working_set=[_atom()],
        repo_root=tmp_path,
        today_str="2026-04-09",
    )

    assert result.q1_matches[0].atom_type == "concept"
    assert result.q1_matches[0].evidence_strength == "empirical"
    assert result.q1_matches[0].relation_kind == "example_of"
    assert result.q2_candidates[0].type == "inquiry"
    cache_path = tmp_path / "raw" / "transcripts" / "article" / "source-1.pass_d.json"
    assert cache_path.exists()
    cached = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cached["data"]["q1_matches"][0]["atom_id"] == "concept-a"


def test_run_pass_d_writes_cache_with_pass_d_identity(monkeypatch, tmp_path: Path) -> None:
    response = {"q1_matches": [], "q2_candidates": []}

    class FakeService:
        def generate_json_prompt(self, prompt: str, **kwargs):
            assert kwargs["task_class"] == PASS_D_TASK_CLASS
            assert kwargs["prompt_version"] == "dream.pass-d.v3"
            return (
                response,
                LLMCacheIdentity(
                    task_class=PASS_D_TASK_CLASS,
                    provider="anthropic",
                    model="anthropic/claude-sonnet-4.6",
                    transport="ai_gateway",
                    api_family="responses",
                    input_mode="text",
                    prompt_version="dream.pass-d.v1",
                    request_fingerprint={"kind": "text-prompt"},
                ),
            )

    monkeypatch.setattr("scripts.atoms.pass_d.get_llm_service", lambda: FakeService())

    run_pass_d(
        source_id="source-identity",
        source_link="[[summary-source-identity]]",
        source_kind="article",
        body_or_transcript="Body text",
        summary={"tldr": "Summary"},
        applied=None,
        pass_c_delta=None,
        stance_context="",
        prior_source_context="",
        working_set=[_atom()],
        repo_root=tmp_path,
        today_str="2026-04-09",
    )

    cache_path = tmp_path / "raw" / "transcripts" / "article" / "source-identity.pass_d.json"
    cached = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cached["_llm"]["prompt_version"] == "dream.pass-d.v1"


def test_run_pass_d_reuses_legacy_dream_cache_identity(monkeypatch, tmp_path: Path) -> None:
    legacy_identity = LLMCacheIdentity(
        task_class="dream",
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
        request_fingerprint={"kind": "text-prompt"},
    )
    cache_path = pass_d_cache_path(
        repo_root=tmp_path,
        source_kind="article",
        source_id="source-legacy",
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "_llm": legacy_identity.to_dict(),
                "data": {"q1_matches": [], "q2_candidates": []},
            }
        ),
        encoding="utf-8",
    )

    class FakeService:
        def cache_identities(self, *, task_class: str, prompt_version: str):
            if task_class == PASS_D_TASK_CLASS:
                return [
                    LLMCacheIdentity(
                        task_class=PASS_D_TASK_CLASS,
                        provider="gemini",
                        model="google/gemini-3.1-flash-lite-preview",
                        transport="ai_gateway",
                        api_family="responses",
                        input_mode="text",
                        prompt_version=prompt_version,
                        request_fingerprint={"kind": "text-prompt"},
                    )
                ]
            if task_class == "dream":
                return [legacy_identity]
            return []

        def generate_json_prompt(self, *args, **kwargs):
            raise AssertionError("legacy Pass D cache should have been reused")

    monkeypatch.setattr("scripts.atoms.pass_d.get_llm_service", lambda: FakeService())

    result = run_pass_d(
        source_id="source-legacy",
        source_link="[[summary-source-legacy]]",
        source_kind="article",
        body_or_transcript="Body text",
        summary={"tldr": "Summary"},
        applied=None,
        pass_c_delta=None,
        stance_context="",
        prior_source_context="",
        working_set=[_atom()],
        repo_root=tmp_path,
        today_str="2026-04-09",
    )

    assert result.q1_matches == []
    assert result.q2_candidates == []


def test_pass_d_cache_identities_mirror_canonical_routes_to_legacy_task_class() -> None:
    primary = LLMCacheIdentity(
        task_class=PASS_D_TASK_CLASS,
        provider="gemini",
        model="google/gemini-3.1-flash-lite-preview",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
        request_fingerprint={"kind": "text-prompt"},
        timeout_seconds=300,
    )
    backup = LLMCacheIdentity(
        task_class=PASS_D_TASK_CLASS,
        provider="anthropic",
        model="anthropic/claude-sonnet-4.6",
        transport="ai_gateway",
        api_family="responses",
        input_mode="text",
        prompt_version=PASS_D_PROMPT_VERSION,
        request_fingerprint={"kind": "text-prompt"},
        timeout_seconds=300,
    )

    class FakeService:
        def cache_identities(self, *, task_class: str, prompt_version: str):
            if task_class == PASS_D_TASK_CLASS:
                return [primary, backup]
            if task_class == "dream":
                return [
                    LLMCacheIdentity(
                        task_class="dream",
                        provider="gemini",
                        model="google/gemini-3.1-flash-lite-preview",
                        transport="ai_gateway",
                        api_family="responses",
                        input_mode="text",
                        prompt_version=prompt_version,
                        request_fingerprint={"kind": "text-prompt"},
                        timeout_seconds=480,
                    )
                ]
            return []

    identities = [identity.to_dict() for identity in pass_d_cache_identities(FakeService())]

    assert backup.to_dict() in identities
    assert {**backup.to_dict(), "task_class": "dream"} in identities


def test_run_pass_d_rejects_invalid_atom_type(monkeypatch, tmp_path: Path) -> None:
    class FakeService:
        def generate_json_prompt(self, prompt: str) -> dict:
            return {
                "q1_matches": [],
                "q2_candidates": [
                    {
                        "type": "note",
                        "proposed_id": "bad",
                        "title": "Bad",
                        "description": "bad",
                        "snippet": "bad",
                        "polarity": "neutral",
                        "rationale": "bad",
                    }
                ],
            }

    monkeypatch.setattr("scripts.atoms.pass_d.get_llm_service", lambda: FakeService())

    with pytest.raises(ValueError):
        run_pass_d(
            source_id="source-1",
            source_link="[[summary-source-1]]",
            source_kind="article",
            body_or_transcript="Body text",
            summary={"tldr": "Summary"},
            applied=None,
            pass_c_delta=None,
            stance_context="",
            prior_source_context="",
            working_set=[_atom()],
            repo_root=tmp_path,
            today_str="2026-04-09",
        )


def test_parse_pass_d_recovers_known_drift_and_records_warnings() -> None:
    result = _parse_pass_d_result(
        {
            "q1_matches": [
                {
                    "atom_id": "concept-a",
                    "type": "concept",
                    "snippet": "supports the concept",
                    "polarity": "",
                    "confidence": "",
                }
            ],
            "q2_candidates": [
                {
                    "atom_type": "stance",
                    "proposed_id": "question-authority",
                    "title": "Question Authority",
                    "description": "A stance candidate",
                    "tldr": "A stance candidate.",
                    "snippet": "question it",
                    "polarity": "",
                    "rationale": "repeated theme",
                    "domains": ["work"],
                    "in_conversation_with": ["authority-structure"],
                }
            ],
        }
    )

    assert result.q1_matches[0].atom_type == "concept"
    assert result.q1_matches[0].polarity == "neutral"
    assert result.q1_matches[0].confidence == "low"
    assert result.q1_matches[0].evidence_strength == "anecdotal"
    assert result.q1_matches[0].relation_kind == "adjacent_to"
    assert result.q2_candidates[0].type == "stance"
    assert result.q2_candidates[0].polarity == "neutral"
    assert result.q2_candidates[0].domains == ["work"]
    assert result.q2_candidates[0].in_conversation_with == ["authority-structure"]
    assert result.dropped_q1_matches == 0
    assert result.dropped_q2_candidates == 0
    assert any("recovered atom_type from type" in warning for warning in result.warnings)
    assert any("recovered type from atom_type" in warning for warning in result.warnings)


def test_parse_pass_d_q2_candidate_derives_missing_rich_fields() -> None:
    result = _parse_pass_d_result(
        {
            "q1_matches": [],
            "q2_candidates": [
                {
                    "type": "playbook",
                    "proposed_id": "incident-runbook",
                    "title": "Incident Runbook",
                    "description": "Write the runbook before the incident happens. It should be explicit.",
                    "snippet": "Runbook first.",
                    "polarity": "for",
                    "rationale": "repeated operational pattern",
                }
            ],
        }
    )

    candidate = result.q2_candidates[0]
    assert candidate.tldr == "Write the runbook before the incident happens."
    assert candidate.steps == []
    assert candidate.domains == []
    assert candidate.in_conversation_with == []


def test_parse_pass_d_drops_q2_candidates_missing_required_fields() -> None:
    result = _parse_pass_d_result(
        {
            "q1_matches": [
                {
                    "atom_id": "concept-a",
                    "atom_type": "concept",
                    "snippet": "supports the concept",
                    "polarity": "for",
                    "confidence": "high",
                }
            ],
            "q2_candidates": [
                {
                    "type": "concept",
                    "proposed_id": "",
                    "title": "Bad Candidate",
                    "description": "Missing id",
                    "tldr": "Missing id.",
                    "snippet": "bad",
                    "polarity": "neutral",
                    "rationale": "bad",
                },
                {
                    "type": "concept",
                    "proposed_id": "also-bad",
                    "title": "Also Bad",
                    "description": "",
                    "snippet": "bad",
                    "polarity": "neutral",
                    "rationale": "bad",
                },
            ],
        }
    )

    assert result.q2_candidates == []
    assert result.dropped_q2_candidates == 2
    assert any("missing proposed_id" in warning for warning in result.warnings)
    assert any("missing description" in warning for warning in result.warnings)


def test_parse_pass_d_drops_invalid_items_when_other_items_remain_valid() -> None:
    result = _parse_pass_d_result(
        {
            "q1_matches": [
                {
                    "atom_id": "concept-a",
                    "atom_type": "concept",
                    "snippet": "supports the concept",
                    "polarity": "for",
                    "confidence": "high",
                },
                {
                    "atom_id": "note-a",
                    "atom_type": "note",
                    "snippet": "invalid type",
                },
            ],
            "q2_candidates": [],
        }
    )

    assert len(result.q1_matches) == 1
    assert result.dropped_q1_matches == 1
    assert result.dropped_q2_candidates == 0
    assert any("unsupported atom_type 'note'" in warning for warning in result.warnings)


def test_parse_pass_d_drops_non_object_items_when_other_items_remain_valid() -> None:
    result = _parse_pass_d_result(
        {
            "q1_matches": [
                {
                    "atom_id": "concept-a",
                    "atom_type": "concept",
                    "snippet": "supports the concept",
                    "polarity": "for",
                    "confidence": "high",
                },
                "unexpected-string-item",
            ],
            "q2_candidates": [],
        }
    )

    assert len(result.q1_matches) == 1
    assert result.dropped_q1_matches == 1
    assert any("expected object, got str" in warning for warning in result.warnings)


def test_parse_pass_d_still_raises_when_all_items_are_unusable() -> None:
    with pytest.raises(ValueError, match="Pass D payload was unusable"):
        _parse_pass_d_result(
            {
                "q1_matches": [
                    {
                        "atom_id": "note-a",
                        "atom_type": "note",
                        "snippet": "invalid type",
                    }
                ],
                "q2_candidates": [],
            }
        )


def test_stage_outcomes_from_payload_reports_warning_and_dispatch_entries() -> None:
    outcomes = stage_outcomes_from_payload(
        {
            "warnings": [
                "q2_candidates[0]: recovered type from atom_type",
                "q2_candidates[1]: unsupported type 'note'",
            ],
            "dropped_q1_matches": 0,
            "dropped_q2_candidates": 1,
            "error": "RuntimeError: boom",
            "error_stage": "pass_d.dispatch",
        }
    )

    assert outcomes == [
        {
            "status": "warning",
            "stage": "pass_d.parse",
            "summary": (
                "2 warning(s); dropped 0 q1 match(es) and 1 q2 candidate(s); "
                "first=q2_candidates[0]: recovered type from atom_type"
            ),
            "warnings": [
                "q2_candidates[0]: recovered type from atom_type",
                "q2_candidates[1]: unsupported type 'note'",
            ],
            "dropped_q1_matches": 0,
            "dropped_q2_candidates": 1,
        },
        {
            "status": "error",
            "stage": "pass_d.dispatch",
            "summary": "RuntimeError: boom",
        },
    ]


def test_run_pass_d_does_not_write_durable_source_pages(monkeypatch, tmp_path: Path) -> None:
    class FakeService:
        def generate_json_prompt(self, prompt: str) -> dict:
            return {"q1_matches": [], "q2_candidates": []}

    def fail_write_page(*_args, **_kwargs):
        raise AssertionError("Pass D must not write durable pages")

    monkeypatch.setattr("scripts.atoms.pass_d.get_llm_service", lambda: FakeService())
    monkeypatch.setattr("scripts.common.wiki_writer.write_page", fail_write_page)

    result = run_pass_d(
        source_id="source-1",
        source_link="[[summary-source-1]]",
        source_kind="article",
        body_or_transcript="Body text",
        summary={"tldr": "Summary"},
        applied=None,
        pass_c_delta=None,
        stance_context="",
        prior_source_context="",
        working_set=[_atom()],
        repo_root=tmp_path,
        today_str="2026-04-09",
    )

    assert result.q1_matches == []
    assert result.q2_candidates == []
