from __future__ import annotations

import json
from pathlib import Path

import mind.services.onboarding_synthesis as onboarding_synthesis_module
from mind.cli import main
from mind.services.llm_cache import LLMCacheIdentity
from mind.services.llm_executor import LLMExecutionResult
from mind.services.llm_service import LLMService
from mind.services.onboarding_state import ChunkState, write_chunk_state
from mind.services.onboarding_synthesis import synthesize_bundle
from mind.services.prompt_builders import (
    ONBOARDING_GRAPH_CHUNK_PROMPT_VERSION,
    ONBOARDING_GRAPH_PROMPT_VERSION,
    ONBOARDING_MERGE_CHUNK_PROMPT_VERSION,
    ONBOARDING_MERGE_PROMPT_VERSION,
    ONBOARDING_MERGE_RELATIONSHIPS_PROMPT_VERSION,
    ONBOARDING_SYNTHESIS_PROMPT_VERSION,
)
from mind.services.providers.base import LLMRequest
from tests.support import FakeOnboardingLLMService, patch_onboarding_llm, write_repo_config


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)


def _write_payload(root: Path) -> Path:
    payload = root / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds local-first tools.",
                "values": ["clarity", "taste"],
                "positioning": {
                    "summary": "Design engineer and founder.",
                    "work_priorities": ["craft quality"],
                    "constraints": ["keep it local-first"],
                },
                "open_threads": ["How should Brain evolve?"],
                "people": [{"name": "Jordan Lee", "summary": "Collaborator"}],
                "projects": [{"title": "Brain", "summary": "Personal wiki"}],
            }
        ),
        encoding="utf-8",
    )
    return payload


class RepairingOnboardingExecutor:
    def __init__(self, *, repair_succeeds: bool):
        self.repair_succeeds = repair_succeeds
        self.helper = FakeOnboardingLLMService()
        self.repair_request_metadata: list[dict[str, object]] = []

    def execute_parts_json(self, *, task_class, instructions, input_parts, prompt_version, input_mode, request_metadata=None, response_schema=None):
        assert task_class == "onboarding_synthesis"
        assert prompt_version == ONBOARDING_SYNTHESIS_PROMPT_VERSION
        payload = self.helper.synthesize_onboarding_semantics(
            bundle_id=request_metadata["bundle_id"],
            input_parts=input_parts,
        )
        payload["owner"]["positioning"] = None
        return LLMExecutionResult(
            text=None,
            data=payload,
            cache_identity=self._identity(task_class, prompt_version),
            response_metadata={},
        )

    def execute_json(self, *, task_class, prompt, prompt_version, response_schema=None):
        if prompt_version in {ONBOARDING_GRAPH_PROMPT_VERSION, ONBOARDING_GRAPH_CHUNK_PROMPT_VERSION}:
            data = {"bundle_id": "repair-pass", "node_proposals": [], "edge_proposals": [], "notes": ["graph ok"]}
        elif prompt_version == ONBOARDING_MERGE_PROMPT_VERSION:
            data = {"bundle_id": "repair-pass", "decisions": [], "relationship_decisions": [], "notes": ["merge ok"]}
        elif prompt_version == ONBOARDING_MERGE_CHUNK_PROMPT_VERSION:
            data = {"bundle_id": "repair-pass", "decisions": [], "notes": ["merge chunk ok"]}
        elif prompt_version == ONBOARDING_MERGE_RELATIONSHIPS_PROMPT_VERSION:
            data = {"bundle_id": "repair-pass", "relationship_decisions": [], "notes": ["merge relationships ok"]}
        else:
            raise AssertionError(f"unexpected prompt_version: {prompt_version}")
        return LLMExecutionResult(
            text=None,
            data=data,
            cache_identity=self._identity(task_class, prompt_version),
            response_metadata={},
        )

    def build_parts_request(self, *, task_class, instructions, input_parts, output_mode, input_mode=None, request_metadata=None, response_schema=None):
        return LLMRequest(
            instructions=instructions,
            input_parts=tuple(input_parts),
            output_mode=output_mode,
            task_class=task_class,
            model="anthropic/claude-sonnet-4.6",
            transport="ai_gateway",
            api_family="responses",
            input_mode=input_mode or "file",
            request_metadata=dict(request_metadata or {}),
            response_schema=response_schema,
        )

    def build_prompt_request(self, *, task_class, prompt, output_mode, response_schema=None, request_metadata=None):
        return LLMRequest.from_prompt(
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

    def execute_request(self, *, request, prompt_version):
        self.repair_request_metadata.append(dict(request.request_metadata))
        if self.repair_succeeds:
            data = self.helper.synthesize_onboarding_semantics(
                bundle_id=request.request_metadata["bundle_id"],
                input_parts=request.input_parts,
            )
        else:
            data = {"bundle_id": request.request_metadata["bundle_id"], "owner": None}
        return LLMExecutionResult(
            text=None,
            data=data,
            cache_identity=self._identity(request.task_class, prompt_version),
            response_metadata={},
        )

    def _identity(self, task_class: str, prompt_version: str) -> LLMCacheIdentity:
        return LLMCacheIdentity(
            task_class=task_class,
            provider="anthropic",
            model="anthropic/claude-sonnet-4.6",
            transport="ai_gateway",
            api_family="responses",
            input_mode="text",
            prompt_version=prompt_version,
            request_fingerprint={"kind": "test-double"},
        )


def test_onboarding_synthesize_and_verify_persist_artifacts(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "llm-pass"]) == 0
    assert main(["onboard", "synthesize", "--bundle", "llm-pass"]) == 0
    assert main(["onboard", "verify", "--bundle", "llm-pass"]) == 0

    bundle_dir = tmp_path / "raw" / "onboarding" / "bundles" / "llm-pass"
    for name in (
        "synthesis-semantic.json",
        "synthesis-graph.json",
        "merge-candidate-context.json",
        "merge-decisions.json",
        "verify-report.json",
        "materialization-plan.json",
    ):
        assert (bundle_dir / name).exists(), name

    state = json.loads((bundle_dir / "state.json").read_text(encoding="utf-8"))
    assert state["synthesis_status"] == "verified"
    assert state["verifier_verdict"] == "approved"
    assert state["materialization_plan_path"].endswith("materialization-plan.json")
    semantic_artifact = json.loads((bundle_dir / "synthesis-semantic.json").read_text(encoding="utf-8"))
    assert semantic_artifact["_llm"]["repair_count"] == 0
    assert main(["onboard", "status", "--bundle", "llm-pass"]) == 0


def test_synthesize_bundle_reuses_existing_semantic_artifact(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    bundle_dir = tmp_path / "raw" / "onboarding" / "bundles" / "resume-pass"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    transcript_path = bundle_dir / "interview.jsonl"
    transcript_path.write_text('{"question_id":"open-inquiries","answer":"What should Brain evolve into next?"}\n', encoding="utf-8")

    semantic = {
        "bundle_id": "resume-pass",
        "owner": {
            "name": "Example Owner",
            "role": "Founder",
            "location": "Remote",
            "summary": "Example Owner builds local-first tools.",
            "values": [{"text": "clarity", "evidence_refs": ["input:values:0"]}],
            "positioning": {
                "summary": "Design engineer and founder.",
                "work_priorities": ["craft quality"],
                "life_priorities": [],
                "constraints": ["keep it local-first"],
                "evidence_refs": ["input:positioning"],
            },
            "open_inquiries": [
                {
                    "slug": "what-should-brain-evolve-into-next",
                    "question": "What should Brain evolve into next?",
                    "evidence_refs": ["input:open-inquiries:0"],
                }
            ],
            "identity_links": {},
            "education": [],
            "skills": {},
            "notes": [],
        },
        "entities": [],
        "relationships": [],
        "notes": [],
    }
    semantic_path = bundle_dir / "synthesis-semantic.json"
    semantic_path.write_text(
        json.dumps(
            {
                "_llm": {
                    "task_class": "onboarding_synthesis",
                    "provider": "anthropic",
                    "model": "anthropic/claude-sonnet-4.6",
                    "transport": "ai_gateway",
                    "api_family": "responses",
                    "input_mode": "file",
                    "prompt_version": ONBOARDING_SYNTHESIS_PROMPT_VERSION,
                    "request_fingerprint": {"kind": "resume-test"},
                },
                "data": semantic,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    class _FailIfSemanticCalled:
        def synthesize_onboarding_semantics(self, **kwargs):
            raise AssertionError("semantic synthesis should not rerun when artifact exists")

    monkeypatch.setattr("mind.services.onboarding_synthesis.get_llm_service", lambda: _FailIfSemanticCalled())
    monkeypatch.setattr(
        "mind.services.onboarding_synthesis._run_chunked_graph_stage",
        lambda **kwargs: (
            {"bundle_id": "resume-pass", "node_proposals": [], "edge_proposals": [], "notes": []},
            [],
        ),
    )
    monkeypatch.setattr(
        "mind.services.onboarding_synthesis.build_merge_candidate_context",
        lambda repo_root, graph_artifact: {"candidates": []},
    )
    monkeypatch.setattr(
        "mind.services.onboarding_synthesis._run_chunked_merge_stage",
        lambda **kwargs: (
            {"bundle_id": "resume-pass", "decisions": [], "relationship_decisions": [], "notes": []},
            [],
        ),
    )

    artifacts = synthesize_bundle(
        tmp_path,
        bundle_dir=bundle_dir,
        bundle={
            "bundle_id": "resume-pass",
            "identity": {"name": "Example Owner"},
            "uploads": [],
        },
        transcript_path=transcript_path,
    )

    assert artifacts.semantic["bundle_id"] == "resume-pass"
    assert (bundle_dir / "synthesis-graph.json").exists()
    assert (bundle_dir / "merge-decisions.json").exists()


def test_run_chunk_phase_ignores_stale_states_for_old_chunk_ids(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundle"
    write_chunk_state(
        bundle_dir,
        state=ChunkState(
            bundle_id="bundle-1",
            phase="graph_nodes",
            chunk_id="old-stale-chunk",
            status="in_flight",
            attempts=1,
            last_attempt_started_at="2000-01-01T00:00:00Z",
        ),
    )

    payloads = onboarding_synthesis_module._run_chunk_phase(
        bundle_dir=bundle_dir,
        bundle_id="bundle-1",
        phase="graph_nodes",
        chunks=[{"chunk_id": "current-chunk"}],
        max_workers=1,
        runner=lambda chunk: ({"chunk_id": chunk["chunk_id"]}, None),
    )

    assert payloads == [{"chunk_id": "current-chunk"}]


def test_onboarding_merge_artifact_records_model_merge_decision(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    existing = tmp_path / "memory" / "people" / "jordan-lee.md"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text(
        "---\n"
        "id: jordan-lee\n"
        "type: person\n"
        "title: Jordan Lee\n"
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-04-08\n"
        "aliases: []\n"
        "tags:\n  - domain/relationships\n  - function/note\n  - signal/canon\n"
        "domains:\n  - relationships\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Jordan Lee\n\nExisting collaborator.\n",
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "merge-pass"]) == 0
    assert main(["onboard", "synthesize", "--bundle", "merge-pass"]) == 0
    assert main(["onboard", "verify", "--bundle", "merge-pass"]) == 0

    merge_artifact = json.loads(
        (tmp_path / "raw" / "onboarding" / "bundles" / "merge-pass" / "merge-decisions.json").read_text(encoding="utf-8")
    )["data"]
    jordan_decision = next(item for item in merge_artifact["decisions"] if item["proposal_id"] == "people:jordan-lee")
    assert jordan_decision["action"] == "merge"
    assert jordan_decision["target_path"] == "memory/people/jordan-lee.md"

    plan = json.loads(
        (tmp_path / "raw" / "onboarding" / "bundles" / "merge-pass" / "materialization-plan.json").read_text(encoding="utf-8")
    )["data"]
    jordan_page = next(item for item in plan["pages"] if item["target_path"] == "memory/people/jordan-lee.md")
    assert jordan_page["write_mode"] == "update"
    assert jordan_page["body_markdown"] is None
    assert jordan_page["intro_mode"] == "preserve"
    assert jordan_page["intro_markdown"] is None
    assert jordan_page["section_operations"] == []


def test_onboarding_update_materialization_preserves_metadata_and_writes_backup(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    existing = tmp_path / "memory" / "people" / "jordan-lee.md"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text(
        "---\n"
        "id: jordan-lee\n"
        "type: person\n"
        "title: Jordan Lee\n"
        "status: active\n"
        "created: 2026-01-01\n"
        "last_updated: 2026-03-01\n"
        "aliases:\n  - Ally\n"
        "tags:\n  - domain/relationships\n  - function/note\n  - signal/canon\n  - collaborator\n"
        "domains:\n  - relationships\n"
        "relates_to:\n  - \"[[existing-link]]\"\n"
        "sources:\n  - \"[[summary-older]]\"\n"
        "---\n\n"
        "# Jordan Lee\n\nExisting collaborator body.\n",
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "update-pass"]) == 0
    assert main(["onboard", "materialize", "--bundle", "update-pass"]) == 0

    updated_text = existing.read_text(encoding="utf-8")
    assert "created: 2026-01-01" in updated_text
    assert "- Ally" in updated_text
    assert "- collaborator" in updated_text
    assert '"[[existing-link]]"' in updated_text
    assert '"[[summary-older]]"' in updated_text

    manifest = json.loads((tmp_path / "raw" / "onboarding" / "bundles" / "update-pass" / "materialization.json").read_text(encoding="utf-8"))
    backup_paths = list(manifest["backup_paths"])
    assert backup_paths
    backup_text = Path(backup_paths[0]).read_text(encoding="utf-8")
    assert "Existing collaborator body." in backup_text


def test_onboarding_materialize_overwrites_seeded_fixed_targets(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    assert main(["seed", "--preset", "framework"]) == 0
    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "seeded-pass"]) == 0
    assert main(["onboard", "verify", "--bundle", "seeded-pass"]) == 0
    assert main(["onboard", "materialize", "--bundle", "seeded-pass"]) == 0

    profile = (tmp_path / "memory" / "me" / "profile.md").read_text(encoding="utf-8")
    values = (tmp_path / "memory" / "me" / "values.md").read_text(encoding="utf-8")
    assert "Example Owner builds local-first tools." in profile
    assert "id: profile" in profile
    assert "id: values" in values

    manifest = json.loads(
        (tmp_path / "raw" / "onboarding" / "bundles" / "seeded-pass" / "materialization.json").read_text(encoding="utf-8")
    )
    assert any(path.endswith("memory/me/profile.md") for path in manifest["backup_paths"])


def test_onboarding_blocked_patch_emits_review_artifact_and_keeps_page_untouched(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    service = patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    existing = tmp_path / "memory" / "people" / "jordan-lee.md"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_text(
        "---\n"
        "id: jordan-lee\n"
        "type: person\n"
        "title: Jordan Lee\n"
        "status: active\n"
        "created: 2026-01-01\n"
        "last_updated: 2026-03-01\n"
        "aliases: []\n"
        "tags:\n  - domain/relationships\n  - function/note\n  - signal/canon\n"
        "domains:\n  - relationships\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Jordan Lee\n\nExisting collaborator body.\n",
        encoding="utf-8",
    )

    original_plan = onboarding_synthesis_module.build_materialization_plan

    def _unsupported_plan(**kwargs):
        data = original_plan(**kwargs)
        page = next(item for item in data["pages"] if item.get("target_path") == "memory/people/jordan-lee.md")
        page["intro_mode"] = "preserve"
        page["section_operations"] = [
            {
                "heading": "## Unsupported Section",
                "mode": "replace",
                "content": "- no\n",
                "insert_after": None,
            }
        ]
        return data

    monkeypatch.setattr(onboarding_synthesis_module, "build_materialization_plan", _unsupported_plan)

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "blocked-pass"]) == 0
    assert main(["onboard", "materialize", "--bundle", "blocked-pass"]) == 1

    assert "Existing collaborator body." in existing.read_text(encoding="utf-8")
    review_dir = tmp_path / "raw" / "onboarding" / "bundles" / "blocked-pass" / "patch-reviews"
    assert list(review_dir.glob("*.json"))
    state = json.loads((tmp_path / "raw" / "onboarding" / "bundles" / "blocked-pass" / "state.json").read_text(encoding="utf-8"))
    assert state["status"] == "blocked"
    assert any("semantic patch review required" in item for item in state["blocking_reasons"])


def test_onboarding_materialize_uses_verified_plan_not_mutated_bundle(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "plan-pass"]) == 0
    assert main(["onboard", "verify", "--bundle", "plan-pass"]) == 0

    bundle_path = tmp_path / "raw" / "onboarding" / "bundles" / "plan-pass" / "normalized-evidence.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle["identity"]["summary"] = "MUTATED AFTER PLAN"
    bundle_path.write_text(json.dumps(bundle, indent=2) + "\n", encoding="utf-8")

    assert main(["onboard", "materialize", "--bundle", "plan-pass"]) == 0
    profile = (tmp_path / "memory" / "me" / "profile.md").read_text(encoding="utf-8")
    assert "Example Owner builds local-first tools." in profile
    assert "MUTATED AFTER PLAN" not in profile


def test_onboarding_verifier_rejection_blocks_materialization(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    service = patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    def _reject(*, bundle, semantic_artifact, graph_artifact, merge_artifact, with_meta=False, response_schema=None):
        data = {
            "bundle_id": bundle["bundle_id"],
            "approved": False,
            "blocking_issues": ["Verifier rejected the onboarding synthesis."],
            "warnings": [],
            "notes": ["rejected in test"],
        }
        identity = service._identity("onboarding_verify", "onboarding.verify.v1")
        return (data, identity) if with_meta else data

    monkeypatch.setattr(service, "verify_onboarding_graph", _reject)

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "reject-pass"]) == 0
    assert main(["onboard", "verify", "--bundle", "reject-pass"]) == 1
    assert main(["onboard", "materialize", "--bundle", "reject-pass"]) == 1
    state = json.loads((tmp_path / "raw" / "onboarding" / "bundles" / "reject-pass" / "state.json").read_text(encoding="utf-8"))
    assert state["verifier_verdict"] == "rejected"
    assert state["blocking_reasons"] == ["Verifier rejected the onboarding synthesis."]


def test_onboarding_synthesize_repairs_invalid_semantic_artifact(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    payload = _write_payload(tmp_path)
    executor = RepairingOnboardingExecutor(repair_succeeds=True)
    monkeypatch.setattr("mind.services.onboarding_synthesis.get_llm_service", lambda: LLMService(executor=executor))

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "repair-pass"]) == 0
    assert main(["onboard", "synthesize", "--bundle", "repair-pass"]) == 0

    bundle_dir = tmp_path / "raw" / "onboarding" / "bundles" / "repair-pass"
    semantic_artifact = json.loads((bundle_dir / "synthesis-semantic.json").read_text(encoding="utf-8"))
    assert semantic_artifact["_llm"]["repair_count"] == 1
    assert executor.repair_request_metadata == [{"attempt_role": "repair", "bundle_id": "repair-pass"}]


def test_onboarding_synthesize_fails_when_repair_does_not_converge(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    payload = _write_payload(tmp_path)
    executor = RepairingOnboardingExecutor(repair_succeeds=False)
    monkeypatch.setattr("mind.services.onboarding_synthesis.get_llm_service", lambda: LLMService(executor=executor))

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "repair-pass"]) == 0
    assert main(["onboard", "synthesize", "--bundle", "repair-pass"]) == 1
    assert executor.repair_request_metadata == [{"attempt_role": "repair", "bundle_id": "repair-pass"}]


def test_onboarding_migrate_merge_denormalizes_legacy_artifact_idempotently(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "migrate-pass"]) == 0
    assert main(["onboard", "verify", "--bundle", "migrate-pass"]) == 0

    merge_path = tmp_path / "raw" / "onboarding" / "bundles" / "migrate-pass" / "merge-decisions.json"
    merge_payload = json.loads(merge_path.read_text(encoding="utf-8"))
    for decision in merge_payload["data"]["decisions"]:
        for key in ("source_proposal_id", "title", "slug", "summary", "page_type", "domains", "relates_to"):
            decision.pop(key, None)
    merge_path.write_text(json.dumps(merge_payload, indent=2) + "\n", encoding="utf-8")

    assert main(["onboard", "migrate-merge", "--bundle", "migrate-pass"]) == 0
    migrated = json.loads(merge_path.read_text(encoding="utf-8"))["data"]
    assert all("source_proposal_id" in decision for decision in migrated["decisions"])
    assert main(["onboard", "migrate-merge", "--bundle", "migrate-pass"]) == 0
    migrated_again = json.loads(merge_path.read_text(encoding="utf-8"))["data"]
    assert migrated == migrated_again


def test_onboarding_plan_builder_matches_legacy_and_denormalized_merge_shapes(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "plan-compare"]) == 0
    assert main(["onboard", "verify", "--bundle", "plan-compare"]) == 0
    assert main(["onboard", "plan", "--bundle", "plan-compare", "--print-json"]) == 0

    from mind.services.onboarding import render_onboarding_materialization_plan

    denormalized = render_onboarding_materialization_plan(tmp_path, bundle_id="plan-compare")

    merge_path = tmp_path / "raw" / "onboarding" / "bundles" / "plan-compare" / "merge-decisions.json"
    merge_payload = json.loads(merge_path.read_text(encoding="utf-8"))
    for decision in merge_payload["data"]["decisions"]:
        for key in ("source_proposal_id", "title", "slug", "summary", "page_type", "domains", "relates_to"):
            decision.pop(key, None)
    merge_path.write_text(json.dumps(merge_payload, indent=2) + "\n", encoding="utf-8")

    legacy = render_onboarding_materialization_plan(tmp_path, bundle_id="plan-compare")

    assert denormalized == legacy


def test_onboarding_synthesize_aborts_on_low_gateway_balance(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)
    monkeypatch.setattr(
        "mind.services.providers.gateway.GatewayProviderClient.get_credits",
        lambda self, timeout_seconds=15: {"balance": 0.1},
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "low-balance"]) == 0
    assert main(["onboard", "synthesize", "--bundle", "low-balance"]) == 1
    state = json.loads((tmp_path / "raw" / "onboarding" / "bundles" / "low-balance" / "state.json").read_text(encoding="utf-8"))
    assert any("below configured minimum" in item for item in state["blocking_reasons"])


def test_onboarding_synthesize_continues_when_credit_check_warns(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = _write_payload(tmp_path)
    monkeypatch.setattr(
        "mind.services.providers.gateway.GatewayProviderClient.get_credits",
        lambda self, timeout_seconds=15: (_ for _ in ()).throw(RuntimeError("credits unavailable")),
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "credits-warning"]) == 0
    assert main(["onboard", "synthesize", "--bundle", "credits-warning"]) == 0
