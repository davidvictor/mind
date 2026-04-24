from __future__ import annotations

import json
from pathlib import Path

from mind.services.onboarding import (
    import_onboarding_bundle,
    synthesize_onboarding_bundle,
    validate_onboarding_bundle_state,
    validate_onboarding_session_ready,
)
from scripts.common.vault import Vault
from tests.support import write_repo_config


def _write_page(path: Path, frontmatter: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(frontmatter + "\n\n" + body, encoding="utf-8")


def test_onboarding_readiness_accepts_semantic_heading_flexibility(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True, create_me=True)
    vault = Vault.load(tmp_path)
    _write_page(
        vault.owner_profile,
        "---\nid: profile\ntype: profile\ntitle: Example Owner\nstatus: active\ncreated: 2026-04-10\nlast_updated: 2026-04-10\naliases: []\ntags:\n  - domain/identity\n  - function/identity\n  - signal/canon\ndomains:\n  - identity\nrelates_to: []\nsources: []\n---",
        "# Example Owner\n\nExample Owner builds local-first systems.\n",
    )
    _write_page(
        vault.values_path,
        "---\nid: values\ntype: note\ntitle: Values\nstatus: active\ncreated: 2026-04-10\nlast_updated: 2026-04-10\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - identity\nrelates_to: []\nsources: []\n---",
        "# Principles\n\n- Clarity\n- Taste\n",
    )
    _write_page(
        vault.positioning_path,
        "---\nid: positioning\ntype: note\ntitle: Positioning\nstatus: active\ncreated: 2026-04-10\nlast_updated: 2026-04-10\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - work\nrelates_to: []\nsources: []\n---",
        "# Current Focus\n\nExample Owner builds tools that help people think more clearly at work.\n",
    )
    _write_page(
        vault.open_inquiries_path,
        "---\nid: open-inquiries\ntype: note\ntitle: Open Inquiries\nstatus: active\ncreated: 2026-04-10\nlast_updated: 2026-04-10\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - meta\nrelates_to: []\nsources: []\n---",
        "# Live Questions\n\n- [[how-should-the-system-evolve]]\n",
    )
    summaries = []
    for name in ("overview", "profile", "values", "positioning", "open-inquiries"):
        summary = vault.wiki / "summaries" / f"summary-onboarding-session-ready-{name}.md"
        _write_page(
            summary,
            f"---\nid: summary-onboarding-session-ready-{name}\ntype: summary\ntitle: Summary {name}\nstatus: active\ncreated: 2026-04-10\nlast_updated: 2026-04-10\naliases: []\ntags:\n  - domain/learning\n  - function/summary\n  - signal/canon\ndomains:\n  - meta\nrelates_to: []\nsources: []\nsource_type: onboarding\nsource_date: 2026-04-10\ningested: 2026-04-10\nexternal_id: session-ready\nsource_path: raw/onboarding/bundles/session-ready/normalized-evidence.json\n---",
            f"# Summary {name}\n\nReady.\n",
        )
        summaries.append(summary.as_posix())
    decision_page = vault.wiki / "decisions" / "onboarding-session-ready.md"
    _write_page(
        decision_page,
        "---\nid: onboarding-session-ready\ntype: decision\ntitle: Onboarding decisions session-ready\nstatus: active\ncreated: 2026-04-10\nlast_updated: 2026-04-10\naliases: []\ntags:\n  - domain/meta\n  - function/note\n  - signal/working\ndomains:\n  - meta\nrelates_to: []\nsources: []\n---",
        "# Decisions\n\n- create Brain project\n",
    )

    bundle_dir = vault.raw / "onboarding" / "bundles" / "session-ready"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle = bundle_dir / "normalized-evidence.json"
    bundle.write_text(json.dumps({"bundle_id": "session-ready"}), encoding="utf-8")
    bundle_sha = __import__("hashlib").sha256(bundle.read_bytes()).hexdigest()
    manifest = bundle_dir / "materialization.json"
    manifest.write_text(
        json.dumps(
            {
                "bundle_id": "session-ready",
                "bundle_sha256": bundle_sha,
                "materialized_pages": [
                    vault.owner_profile.as_posix(),
                    vault.values_path.as_posix(),
                    vault.positioning_path.as_posix(),
                    vault.open_inquiries_path.as_posix(),
                ],
                "summary_pages": summaries,
                "decision_page": decision_page.as_posix(),
            }
        ),
        encoding="utf-8",
    )
    state = bundle_dir / "state.json"
    state.write_text(
        json.dumps(
            {
                "bundle_id": "session-ready",
                "status": "materialized",
                "bundle_sha256": bundle_sha,
                "validation": {"ready_for_materialization": True},
                "materialized_pages": [
                    vault.owner_profile.as_posix(),
                    vault.values_path.as_posix(),
                    vault.positioning_path.as_posix(),
                    vault.open_inquiries_path.as_posix(),
                ],
                "summary_pages": summaries,
                "materialization_manifest": manifest.as_posix(),
            }
        ),
        encoding="utf-8",
    )
    current = vault.raw / "onboarding" / "current.json"
    current.parent.mkdir(parents=True, exist_ok=True)
    current.write_text(
        json.dumps(
            {
                "bundle_id": "session-ready",
                "state_path": state.as_posix(),
                "bundle_path": bundle.as_posix(),
                "bundle_sha256": bundle_sha,
                "status": "materialized",
            }
        ),
        encoding="utf-8",
    )

    readiness = validate_onboarding_session_ready(vault)
    assert readiness["ready"] is True


def test_onboarding_readiness_fails_when_decision_page_missing(tmp_path: Path) -> None:
    test_onboarding_readiness_accepts_semantic_heading_flexibility(tmp_path)
    vault = Vault.load(tmp_path)
    decision_page = vault.wiki / "decisions" / "onboarding-session-ready.md"
    decision_page.unlink()

    readiness = validate_onboarding_session_ready(vault)

    assert readiness["ready"] is False
    assert any("decision page" in error or "missing projected file" in error for error in readiness["errors"])


def test_onboarding_readiness_fails_when_live_bundle_hash_changes(tmp_path: Path) -> None:
    test_onboarding_readiness_accepts_semantic_heading_flexibility(tmp_path)
    vault = Vault.load(tmp_path)
    bundle = vault.raw / "onboarding" / "bundles" / "session-ready" / "normalized-evidence.json"
    bundle.write_text(json.dumps({"bundle_id": "session-ready", "tampered": True}), encoding="utf-8")

    readiness = validate_onboarding_session_ready(vault)

    assert readiness["ready"] is False
    assert "onboarding state hash does not match live bundle" in readiness["errors"]


def test_onboarding_validate_does_not_switch_current_pointer(tmp_path: Path) -> None:
    write_repo_config(tmp_path, create_indexes=True, create_me=True)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "summary": "Builds local-first systems.",
                "values": ["clarity"],
                "positioning": {
                    "summary": "Founder and builder.",
                    "work_priorities": ["craft quality"],
                    "constraints": ["local-first"],
                },
                "open_threads": ["What should Brain do next?"],
            }
        ),
        encoding="utf-8",
    )
    import_onboarding_bundle(tmp_path, from_json=str(payload), bundle_id="session-z")
    import_onboarding_bundle(tmp_path, from_json=str(payload), bundle_id="session-a")
    before = json.loads((tmp_path / "raw" / "onboarding" / "current.json").read_text(encoding="utf-8"))

    status = validate_onboarding_bundle_state(tmp_path, bundle_id="session-z")
    after = json.loads((tmp_path / "raw" / "onboarding" / "current.json").read_text(encoding="utf-8"))

    assert status.bundle_id == "session-z"
    assert before["bundle_id"] == "session-a"
    assert after["bundle_id"] == "session-a"


def test_synthesize_marks_bundle_in_progress_before_backend_call(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True, create_me=True)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "summary": "Builds local-first systems.",
                "values": ["clarity"],
                "positioning": {
                    "summary": "Founder and builder.",
                    "work_priorities": ["craft quality"],
                    "constraints": ["local-first"],
                },
                "open_threads": ["What should Brain do next?"],
            }
        ),
        encoding="utf-8",
    )
    import_onboarding_bundle(tmp_path, from_json=str(payload), bundle_id="session-synth")

    observed: dict[str, str] = {}

    def _fake_synthesize(repo_root: Path, *, bundle_dir: Path, bundle: dict, transcript_path: Path):
        state = json.loads((bundle_dir / "state.json").read_text(encoding="utf-8"))
        observed["status"] = state["status"]
        observed["synthesis_status"] = state["synthesis_status"]
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr("mind.services.onboarding.synthesize_bundle", _fake_synthesize)

    status = synthesize_onboarding_bundle(tmp_path, bundle_id="session-synth")

    assert observed == {"status": "synthesizing", "synthesis_status": "in-progress"}
    assert status.status == "blocked"
    assert status.synthesis_status == "blocked"
    assert status.blocking_reasons == ["synthetic failure"]
