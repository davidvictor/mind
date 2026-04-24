from __future__ import annotations

from pathlib import Path

from mind.services.onboarding import _normalize_payload, build_decisions, validate_evidence_bundle


def test_normalize_payload_handles_string_and_dict_positioning_and_preserves_uploads(tmp_path: Path) -> None:
    raw_input = tmp_path / "raw-input.json"
    uploads = [
        {
            "id": "upload-1",
            "file_name": "notes.md",
            "path": (tmp_path / "uploads" / "notes.md").as_posix(),
            "media_type": "text/markdown",
            "size_bytes": 42,
            "evidence_refs": ["upload:notes.md"],
        }
    ]

    string_positioning = _normalize_payload(
        {
            "name": "Example Owner",
            "summary": "Builds local-first systems.",
            "values": ["clarity", "taste"],
            "positioning": "Design engineer and founder.",
            "open_threads": ["How should Brain evolve?"],
        },
        bundle_id="bundle-string",
        raw_input_path=raw_input,
        uploads=uploads,
    )
    assert string_positioning["positioning"]["summary"] == "Design engineer and founder."
    assert string_positioning["open_inquiries"] == [
        {
            "slug": "how-should-brain-evolve",
            "question": "How should Brain evolve?",
            "evidence_refs": ["input:open-inquiries:0"],
        }
    ]
    assert string_positioning["uploads"] == uploads
    assert string_positioning["values"][0]["evidence_refs"] == ["input:values:0"]

    dict_positioning = _normalize_payload(
        {
            "identity": {"name": "Example Owner", "summary": "Builds local-first systems."},
            "values": ["clarity"],
            "positioning": {
                "summary": "Builder of durable workflows.",
                "work_priorities": ["craft quality"],
                "constraints": ["keep it local-first"],
            },
            "open_inquiries": ["What deserves automation next?"],
        },
        bundle_id="bundle-dict",
        raw_input_path=raw_input,
        uploads=[],
    )
    assert dict_positioning["positioning"]["summary"] == "Builder of durable workflows."
    assert dict_positioning["positioning"]["work_priorities"] == ["craft quality"]
    assert dict_positioning["positioning"]["constraints"] == ["keep it local-first"]
    assert dict_positioning["positioning"]["evidence_refs"] == ["input:positioning"]


def test_build_decisions_distinguishes_rich_candidates_from_sparse_candidates() -> None:
    decisions = build_decisions(
        {
            "bundle_id": "bundle-decisions",
            "projects": [
                {
                    "slug": "brain",
                    "title": "Brain",
                    "summary": "Private knowledge base.",
                    "priorities": ["craft quality"],
                    "constraints": [],
                    "evidence_refs": ["input:project:0"],
                },
                {
                    "slug": "scratch",
                    "title": "Scratch",
                    "summary": "",
                    "evidence_refs": ["input:project:1"],
                },
            ],
            "people": [],
            "concepts": [],
            "playbooks": [],
            "stances": [],
            "inquiries": [],
        }
    )

    entries = {(entry["family"], entry["target"]): entry for entry in decisions["entries"]}
    rich = entries[("projects", "brain")]
    assert rich["action"] == "create"
    assert rich["confidence"] == "high"
    assert rich["evidence_refs"] == ["input:project:0"]
    assert "enough detail" in rich["rationale"]

    sparse = entries[("projects", "scratch")]
    assert sparse["action"] == "not-create"
    assert sparse["confidence"] == "low"
    assert sparse["evidence_refs"] == ["input:project:1"]
    assert "lacks enough detail" in sparse["rationale"]

    assert entries[("people", "people")]["action"] == "not-create"


def test_validate_evidence_bundle_reports_required_errors_and_optional_warnings() -> None:
    validation = validate_evidence_bundle(
        {
            "identity": {"name": "", "summary": ""},
            "values": [],
            "positioning": {"summary": "", "work_priorities": [], "constraints": []},
            "open_inquiries": [],
            "uploads": [],
        }
    )

    assert validation["ready_for_materialization"] is False
    assert validation["errors"] == [
        "missing identity name",
        "missing profile summary",
        "missing values",
        "missing positioning narrative",
        "missing open inquiries",
    ]
    assert validation["warnings"] == [
        "work priorities were not provided",
        "constraints were not provided",
        "no onboarding uploads were collected",
    ]
