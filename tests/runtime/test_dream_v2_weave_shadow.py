from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

from mind.dream.v2.contracts import (
    AtomSnapshot,
    CandidateSet,
    HotnessFeatures,
    NeighborhoodWindow,
    PromptReceipt,
    ReconciledCluster,
    SourceEvidenceRef,
    WeaveClusterReportsArtifact,
    WeaveCritiqueArtifact,
    WeaveLocalProposalResponse,
    WeaveReconcileArtifact,
    WeaveStructuralActionsArtifact,
)
from mind.dream.v2.decision_runner import DecisionRunResult
from mind.dream.v2.weave_stage import WEAVE_V2_ADAPTER, run_weave_v2, run_weave_v2_shadow
from mind.dream.common import DreamPreconditionError
from mind.runtime_state import RuntimeState
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    text = cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1)
    text = text.replace(
        "llm:\n  provider: gemini\n  model: gemini-2.5-pro\n",
        "llm:\n"
        "  provider: gemini\n"
        "  model: google/gemini-2.5-pro\n"
        "  routes:\n"
        "    dream_decision:\n"
        "      model: anthropic/claude-sonnet-4.6\n",
        1,
    )
    cfg.write_text(text, encoding="utf-8")
    return target


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def _candidate_set() -> CandidateSet:
    alpha = AtomSnapshot(
        atom_id="alpha",
        atom_type="concept",
        path="memory/concepts/alpha.md",
        title="Alpha",
        frontmatter={},
        tldr="Alpha",
        evidence_refs=[SourceEvidenceRef(source_id="summary-a", observed_at="2026-04-20", snippet="alpha evidence")],
        generic_relation_ids=["beta"],
        typed_relation_ids=[],
        hotness_features=HotnessFeatures(hot_score=20, relation_degree=1),
    )
    beta = AtomSnapshot(
        atom_id="beta",
        atom_type="concept",
        path="memory/concepts/beta.md",
        title="Beta",
        frontmatter={},
        tldr="Beta",
        evidence_refs=[SourceEvidenceRef(source_id="summary-b", observed_at="2026-04-20", snippet="beta evidence")],
        generic_relation_ids=["alpha"],
        typed_relation_ids=[],
        hotness_features=HotnessFeatures(hot_score=10, relation_degree=1),
    )
    return CandidateSet(
        run_id="run-test",
        stage="weave",
        generated_at="2026-04-21T00:00:00Z",
        mode="shadow",
        atom_snapshots=[alpha, beta],
        windows=[
            NeighborhoodWindow(
                window_id="window-001-alpha",
                seed_atom_id="alpha",
                atom_ids=["alpha", "beta"],
                ranked_atom_ids=["alpha", "beta"],
                rationale=["direct relation ties: beta"],
            )
        ],
        notes=[],
    )


def _write_active_concept(root: Path, *, atom_id: str, title: str, relates_to: list[str] | None = None) -> None:
    target = root / "memory" / "concepts" / f"{atom_id}.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    relates_yaml = "\n".join(f'  - "[[{item}]]"' for item in (relates_to or []))
    target.write_text(
        "---\n"
        f"id: {atom_id}\n"
        "type: concept\n"
        f'title: "{title}"\n'
        "status: active\n"
        "created: 2026-04-01\n"
        "last_updated: 2026-04-20\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        f"relates_to:\n{relates_yaml if relates_yaml else '  []'}\n"
        "sources: []\n"
        "lifecycle_state: active\n"
        "last_evidence_date: 2026-04-20\n"
        "evidence_count: 1\n"
        "---\n\n"
        f"# {title}\n\n"
        "## TL;DR\n\n"
        f"{title}\n\n"
        "## Evidence log\n\n"
        f"- 2026-04-20 — [[summary-example-seed]] — evidence for {atom_id}\n",
        encoding="utf-8",
    )


class _FakeDecisionRunner:
    def run_prompt(self, *, prompt_family, prompt, response_model, task_class, prompt_version, request_metadata=None):
        del prompt
        del response_model
        if prompt_family == "weave.local_cluster":
            payload = WeaveLocalProposalResponse.model_validate(
                {
                    "clusters": [
                        {
                            "cluster_title": "Alpha cluster",
                            "cluster_thesis": "Alpha and beta belong together.",
                            "member_atom_ids": ["alpha", "beta"],
                            "member_roles": [
                                {"atom_id": "alpha", "role": "hub", "why_included": "seed atom", "primary_signals": ["seed"]},
                                {"atom_id": "beta", "role": "core", "why_included": "direct relation", "primary_signals": ["relates_to"]},
                            ],
                            "borderline_atom_ids": [],
                            "excluded_atom_ids": [],
                            "bridge_candidate_ids": [],
                            "confidence": 0.82,
                            "rationale": "shared evidence and direct relation",
                            "why_now": "recent shared evidence",
                        }
                    ],
                    "leftover_atom_ids": [],
                    "bridge_candidates": [
                        {
                            "source_atom_id": "alpha",
                            "target_atom_id": "beta",
                            "bridge_type": "shared-evidence",
                            "why_it_matters": "reinforces the pair",
                            "confidence": 0.76,
                        }
                    ],
                    "window_observations": ["clear structural pair"],
                }
            )
        elif prompt_family == "weave.reconcile":
            payload = WeaveReconcileArtifact.model_validate(
                {
                    "merged_clusters": [
                        {
                            "cluster_id": "window-001-alpha-cluster-01-alpha-beta",
                            "source_cluster_ids": ["window-001-alpha-cluster-01-alpha-beta"],
                            "cluster_title": "Alpha cluster",
                            "cluster_thesis": "Alpha and beta belong together.",
                            "member_atom_ids": ["alpha", "beta"],
                            "member_roles": [
                                {"cluster_id": "window-001-alpha-cluster-01-alpha-beta", "atom_id": "alpha", "role": "hub", "why_included": "seed atom", "primary_signals": ["seed"]},
                                {"cluster_id": "window-001-alpha-cluster-01-alpha-beta", "atom_id": "beta", "role": "core", "why_included": "direct relation", "primary_signals": ["relates_to"]},
                            ],
                            "borderline_atom_ids": [],
                            "excluded_atom_ids": [],
                            "bridge_candidate_ids": ["window-001-alpha-bridge-01-alpha-beta"],
                            "confidence": 0.82,
                            "rationale": "shared evidence and direct relation",
                            "why_now": "recent shared evidence",
                        }
                    ],
                    "discarded_cluster_ids": [],
                    "split_instructions": [],
                    "hierarchy_edges": [],
                    "global_observations": ["single coherent cluster"],
                }
            )
        elif prompt_family == "weave.critique":
            payload = WeaveCritiqueArtifact.model_validate(
                {
                    "approved_cluster_ids": ["window-001-alpha-cluster-01-alpha-beta"],
                    "clusters_requiring_split": [],
                    "clusters_requiring_boundary_trim": [],
                    "parent_concept_candidates": [],
                    "review_flags": [],
                }
            )
        elif prompt_family == "weave.report_writer":
            payload = WeaveClusterReportsArtifact.model_validate(
                {
                    "reports": [
                        {
                            "cluster_id": "window-001-alpha-cluster-01-alpha-beta",
                            "title": "Alpha cluster",
                            "thesis": "Alpha and beta belong together.",
                            "why_now": "recent shared evidence",
                            "member_sections": [
                                {"atom_id": "alpha", "role": "hub", "summary": "Hub atom"},
                                {"atom_id": "beta", "role": "core", "summary": "Core atom"},
                            ],
                            "bridge_sections": [
                                {"bridge_id": "window-001-alpha-bridge-01-alpha-beta", "summary": "Shared evidence bridge"}
                            ],
                            "tension_sections": [],
                            "parent_concept_candidates": [],
                            "evidence_anchors": ["summary-a", "summary-b"],
                        }
                    ]
                }
            )
        else:
            payload = WeaveStructuralActionsArtifact.model_validate(
                {
                    "safe_cluster_ref_updates": [
                        {
                            "cluster_id": "window-001-alpha-cluster-01-alpha-beta",
                            "atom_ids": ["alpha", "beta"],
                            "cluster_ref": "[[window-001-alpha-cluster-01-alpha-beta]]",
                        }
                    ],
                    "report_only_merges": [],
                    "report_only_splits": [],
                    "review_nudges": [
                        {
                            "nudge_id": "review-alpha-cluster",
                            "title": "Review Alpha cluster",
                            "body": "Inspect Alpha cluster boundary",
                            "target_path": "memory/inbox/nudges/review-alpha-cluster.md",
                        }
                    ],
                }
            )
        receipt = {
            "prompt_family": prompt_family,
            "prompt_version": prompt_version,
            "task_class": task_class,
            "provider": "anthropic",
            "model": "anthropic/claude-sonnet-4.6",
            "input_mode": "text",
            "request_fingerprint": {"kind": "text-prompt"},
            "request_metadata": dict(request_metadata or {}),
            "response_metadata": {"generation_id": "gen-shadow"},
            "repaired": False,
        }
        return DecisionRunResult(
            payload=payload,
            receipt=PromptReceipt.model_validate(receipt),
        )


def test_weave_v2_shadow_writes_artifacts_without_touching_canonical_weave(monkeypatch, tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    monkeypatch.setattr("mind.dream.v2.weave_stage.gather_weave_candidate_set", lambda **_kwargs: _candidate_set())

    result = run_weave_v2_shadow(dry_run=False, decision_runner=_FakeDecisionRunner())

    assert result.stage == "weave-v2-shadow"
    adapter_state = RuntimeState.for_repo_root(root).get_adapter_state(WEAVE_V2_ADAPTER)
    assert adapter_state is not None
    run_root = root / adapter_state["artifact_root"]
    assert run_root.exists()
    assert (run_root / "stage-weave" / "candidate-set.json").exists()
    assert (run_root / "stage-weave" / "local-proposals" / "window-001-alpha.json").exists()
    assert (run_root / "stage-weave" / "reconciled-clusters.json").exists()
    assert (run_root / "stage-weave" / "critique.json").exists()
    assert (run_root / "stage-weave" / "cluster-reports.json").exists()
    assert (run_root / "stage-weave" / "structural-actions.json").exists()
    assert (run_root / "stage-weave" / "apply-plan.json").exists()
    assert (run_root / "stage-weave" / "apply-manifest.json").exists()
    assert (run_root / "stage-weave" / "compare.json").exists()
    assert not (root / "memory" / "dreams" / "weave").exists()
    local_proposal = json.loads((run_root / "stage-weave" / "local-proposals" / "window-001-alpha.json").read_text(encoding="utf-8"))
    assert local_proposal["prompt_receipt"]["prompt_family"] == "weave.local_cluster"
    assert local_proposal["prompt_receipt"]["request_metadata"]["window_id"] == "window-001-alpha"
    assert local_proposal["payload"]["clusters"][0]["cluster_id"] == "window-001-alpha-cluster-01-alpha-beta"
    manifest = json.loads((run_root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["config_snapshot"]["llm"]["routes"]["dream_decision"]["model"] == "anthropic/claude-sonnet-4.6"
    compare = json.loads((run_root / "stage-weave" / "compare.json").read_text(encoding="utf-8"))
    assert compare["v2_reconciled_cluster_count"] == 1
    assert compare["bridge_candidate_count"] == 1
    assert adapter_state["compare_path"] == "raw/reports/dream/v2/runs/run-1/stage-weave/compare.json"


def test_weave_v2_write_mode_materializes_canonical_outputs(monkeypatch, tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    monkeypatch.setattr("mind.dream.v2.weave_stage.gather_weave_candidate_set", lambda **_kwargs: _candidate_set())
    _write_active_concept(root, atom_id="alpha", title="Alpha", relates_to=["beta"])
    _write_active_concept(root, atom_id="beta", title="Beta", relates_to=["alpha"])

    result = run_weave_v2(dry_run=False, decision_runner=_FakeDecisionRunner())

    assert result.stage == "weave"
    adapter_state = RuntimeState.for_repo_root(root).get_adapter_state(WEAVE_V2_ADAPTER)
    assert adapter_state is not None
    assert adapter_state["mode"] == "write"
    cluster_page = root / "memory" / "dreams" / "weave" / "window-001-alpha-cluster-01-alpha-beta.md"
    assert cluster_page.exists()
    cluster_text = cluster_page.read_text(encoding="utf-8")
    assert "origin: dream.weave.v2" in cluster_text
    assert "Alpha cluster" in cluster_text
    alpha_text = (root / "memory" / "concepts" / "alpha.md").read_text(encoding="utf-8")
    beta_text = (root / "memory" / "concepts" / "beta.md").read_text(encoding="utf-8")
    assert 'weave_cluster_refs:\n  - "[[window-001-alpha-cluster-01-alpha-beta]]"' in alpha_text
    assert 'weave_cluster_refs:\n  - "[[window-001-alpha-cluster-01-alpha-beta]]"' in beta_text
    run_root = root / str(adapter_state["artifact_root"])
    apply_manifest = json.loads((run_root / "stage-weave" / "apply-manifest.json").read_text(encoding="utf-8"))
    assert any(entry["status"] == "written" for entry in apply_manifest["entries"])


def test_weave_v2_shadow_rejects_invented_atom_ids(monkeypatch, tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    monkeypatch.setattr("mind.dream.v2.weave_stage.gather_weave_candidate_set", lambda **_kwargs: _candidate_set())

    class _BadDecisionRunner:
        def run_prompt(self, *, prompt_family, prompt, response_model, task_class, prompt_version, request_metadata=None):
            del prompt
            del response_model
            del task_class
            del prompt_version
            del request_metadata
            if prompt_family != "weave.local_cluster":
                raise AssertionError("local cluster should fail before later prompts")
            return DecisionRunResult(
                payload=WeaveLocalProposalResponse.model_validate(
                    {
                        "clusters": [
                            {
                                "cluster_title": "Bad cluster",
                                "cluster_thesis": "Invented atom",
                                "member_atom_ids": ["alpha", "omega"],
                                "member_roles": [
                                    {"atom_id": "alpha", "role": "hub", "why_included": "real", "primary_signals": []},
                                    {"atom_id": "omega", "role": "core", "why_included": "invented", "primary_signals": []},
                                ],
                                "borderline_atom_ids": [],
                                "excluded_atom_ids": [],
                                "bridge_candidate_ids": [],
                                "confidence": 0.5,
                                "rationale": "bad",
                                "why_now": "bad",
                            }
                        ],
                        "leftover_atom_ids": [],
                        "bridge_candidates": [],
                        "window_observations": [],
                    }
                ),
                receipt=PromptReceipt.model_validate(
                    {
                        "prompt_family": prompt_family,
                        "prompt_version": "dream.weave.local-cluster.v2",
                        "task_class": "dream_decision",
                        "provider": "anthropic",
                        "model": "anthropic/claude-sonnet-4.6",
                        "input_mode": "text",
                        "request_fingerprint": {"kind": "text-prompt"},
                        "request_metadata": {},
                        "response_metadata": {},
                        "repaired": False,
                    }
                ),
            )

    try:
        run_weave_v2_shadow(dry_run=False, decision_runner=_BadDecisionRunner())
        raise AssertionError("expected DreamPreconditionError")
    except DreamPreconditionError as exc:
        assert "outside the window" in str(exc)


def test_weave_v2_shadow_repairs_semantic_window_miss_when_executor_is_available(monkeypatch, tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    monkeypatch.setattr("mind.dream.v2.weave_stage.gather_weave_candidate_set", lambda **_kwargs: _candidate_set())

    class _RepairingExecutor:
        def build_prompt_request(self, *, task_class, prompt, output_mode, response_schema=None, request_metadata=None):
            return SimpleNamespace(
                task_class=task_class,
                prompt=prompt,
                output_mode=output_mode,
                response_schema=response_schema,
                request_metadata=dict(request_metadata or {}),
                instructions="",
                input_parts=(),
                model="anthropic/claude-sonnet-4.6",
                transport="responses",
                api_family="responses",
                input_mode="text",
                tools=(),
                temperature=None,
                max_tokens=None,
                timeout_seconds=None,
                reasoning_effort=None,
            )

        def execute_request(self, *, request, prompt_version):
            del request
            del prompt_version
            return SimpleNamespace(
                data={
                    "clusters": [
                        {
                            "cluster_title": "Recovered cluster",
                            "cluster_thesis": "Keep only in-window atoms.",
                            "member_atom_ids": ["alpha", "beta"],
                            "member_roles": [
                                {"atom_id": "alpha", "role": "hub", "why_included": "real", "primary_signals": []},
                                {"atom_id": "beta", "role": "core", "why_included": "real", "primary_signals": []},
                            ],
                            "borderline_atom_ids": [],
                            "excluded_atom_ids": [],
                            "bridge_candidate_ids": [],
                            "confidence": 0.8,
                            "rationale": "repair removed invented atom",
                            "why_now": "repair",
                        }
                    ],
                    "leftover_atom_ids": [],
                    "bridge_candidates": [],
                    "window_observations": ["repair succeeded"],
                }
            )

    class _RepairingDecisionRunner:
        def __init__(self) -> None:
            self.executor = _RepairingExecutor()

        def run_prompt(self, *, prompt_family, prompt, response_model, task_class, prompt_version, request_metadata=None):
            del prompt
            del task_class
            del prompt_version
            del request_metadata
            if prompt_family != "weave.local_cluster":
                return _FakeDecisionRunner().run_prompt(
                    prompt_family=prompt_family,
                    prompt="",
                    response_model=response_model,
                    task_class="dream_decision",
                    prompt_version="dream.weave.local-cluster.v2",
                    request_metadata={},
                )
            return DecisionRunResult(
                payload=WeaveLocalProposalResponse.model_validate(
                    {
                        "clusters": [
                            {
                                "cluster_title": "Bad cluster",
                                "cluster_thesis": "Invented atom",
                                "member_atom_ids": ["alpha", "omega"],
                                "member_roles": [
                                    {"atom_id": "alpha", "role": "hub", "why_included": "real", "primary_signals": []},
                                    {"atom_id": "omega", "role": "core", "why_included": "invented", "primary_signals": []},
                                ],
                                "borderline_atom_ids": [],
                                "excluded_atom_ids": [],
                                "bridge_candidate_ids": [],
                                "confidence": 0.5,
                                "rationale": "bad",
                                "why_now": "bad",
                            }
                        ],
                        "leftover_atom_ids": [],
                        "bridge_candidates": [],
                        "window_observations": [],
                    }
                ),
                receipt=PromptReceipt.model_validate(
                    {
                        "prompt_family": prompt_family,
                        "prompt_version": "dream.weave.local-cluster.v2",
                        "task_class": "dream_decision",
                        "provider": "anthropic",
                        "model": "anthropic/claude-sonnet-4.6",
                        "input_mode": "text",
                        "request_fingerprint": {"kind": "text-prompt"},
                        "request_metadata": {},
                        "response_metadata": {},
                        "repaired": False,
                    }
                ),
            )

    result = run_weave_v2_shadow(dry_run=False, decision_runner=_RepairingDecisionRunner())

    assert result.stage == "weave-v2-shadow"
    adapter_state = RuntimeState.for_repo_root(root).get_adapter_state(WEAVE_V2_ADAPTER)
    assert adapter_state is not None
    run_root = root / adapter_state["artifact_root"]
    local_proposal = json.loads(
        (run_root / "stage-weave" / "local-proposals" / "window-001-alpha.json").read_text(encoding="utf-8")
    )
    assert local_proposal["prompt_receipt"]["repaired"] is True
    assert local_proposal["payload"]["clusters"][0]["member_atom_ids"] == ["alpha", "beta"]


def test_weave_v2_shadow_repairs_report_writer_non_approved_clusters(monkeypatch, tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    monkeypatch.setattr("mind.dream.v2.weave_stage.gather_weave_candidate_set", lambda **_kwargs: _candidate_set())

    class _ReportRepairExecutor:
        def build_prompt_request(self, *, task_class, prompt, output_mode, response_schema=None, request_metadata=None):
            return SimpleNamespace(
                task_class=task_class,
                prompt=prompt,
                output_mode=output_mode,
                response_schema=response_schema,
                request_metadata=dict(request_metadata or {}),
                instructions="",
                input_parts=(),
                model="anthropic/claude-sonnet-4.6",
                transport="responses",
                api_family="responses",
                input_mode="text",
                tools=(),
                temperature=None,
                max_tokens=None,
                timeout_seconds=None,
                reasoning_effort=None,
            )

        def execute_request(self, *, request, prompt_version):
            del request
            del prompt_version
            return SimpleNamespace(
                data={
                    "reports": [
                        {
                            "cluster_id": "window-001-alpha-cluster-01-alpha-beta",
                            "title": "Alpha cluster",
                            "thesis": "Alpha and beta belong together.",
                            "why_now": "recent shared evidence",
                            "member_sections": [
                                {"atom_id": "alpha", "role": "hub", "summary": "Hub atom"},
                                {"atom_id": "beta", "role": "core", "summary": "Core atom"},
                            ],
                            "bridge_sections": [
                                {"bridge_id": "window-001-alpha-bridge-01-alpha-beta", "summary": "Shared evidence bridge"}
                            ],
                            "tension_sections": [],
                            "parent_concept_candidates": [],
                            "evidence_anchors": ["summary-a", "summary-b"],
                        }
                    ]
                }
            )

    class _ReportRepairDecisionRunner:
        def __init__(self) -> None:
            self.executor = _ReportRepairExecutor()
            self.base = _FakeDecisionRunner()

        def run_prompt(self, *, prompt_family, prompt, response_model, task_class, prompt_version, request_metadata=None):
            if prompt_family != "weave.report_writer":
                return self.base.run_prompt(
                    prompt_family=prompt_family,
                    prompt=prompt,
                    response_model=response_model,
                    task_class=task_class,
                    prompt_version=prompt_version,
                    request_metadata=request_metadata,
                )
            return DecisionRunResult(
                payload=WeaveClusterReportsArtifact.model_validate(
                    {
                        "reports": [
                            {
                                "cluster_id": "window-001-coherence-fragility-and-evaluation",
                                "title": "Bad report",
                                "thesis": "Unapproved cluster",
                                "why_now": "bad",
                                "member_sections": [],
                                "bridge_sections": [],
                                "tension_sections": [],
                                "parent_concept_candidates": [],
                                "evidence_anchors": [],
                            }
                        ]
                    }
                ),
                receipt=PromptReceipt.model_validate(
                    {
                        "prompt_family": prompt_family,
                        "prompt_version": "dream.weave.report-writer.v2",
                        "task_class": "dream_writer",
                        "provider": "anthropic",
                        "model": "anthropic/claude-sonnet-4.6",
                        "input_mode": "text",
                        "request_fingerprint": {"kind": "text-prompt"},
                        "request_metadata": {},
                        "response_metadata": {},
                        "repaired": False,
                    }
                ),
            )

    result = run_weave_v2_shadow(dry_run=False, decision_runner=_ReportRepairDecisionRunner())

    assert result.stage == "weave-v2-shadow"
    adapter_state = RuntimeState.for_repo_root(root).get_adapter_state(WEAVE_V2_ADAPTER)
    assert adapter_state is not None
    run_root = root / adapter_state["artifact_root"]
    cluster_reports = json.loads((run_root / "stage-weave" / "cluster-reports.json").read_text(encoding="utf-8"))
    assert cluster_reports["prompt_receipt"]["repaired"] is True
    assert cluster_reports["payload"]["reports"][0]["cluster_id"] == "window-001-alpha-cluster-01-alpha-beta"


def test_weave_v2_shadow_repairs_structural_actions_unknown_clusters(monkeypatch, tmp_path: Path) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    monkeypatch.setattr("mind.dream.v2.weave_stage.gather_weave_candidate_set", lambda **_kwargs: _candidate_set())

    class _ActionsRepairExecutor:
        def build_prompt_request(self, *, task_class, prompt, output_mode, response_schema=None, request_metadata=None):
            return SimpleNamespace(
                task_class=task_class,
                prompt=prompt,
                output_mode=output_mode,
                response_schema=response_schema,
                request_metadata=dict(request_metadata or {}),
                instructions="",
                input_parts=(),
                model="anthropic/claude-sonnet-4.6",
                transport="responses",
                api_family="responses",
                input_mode="text",
                tools=(),
                temperature=None,
                max_tokens=None,
                timeout_seconds=None,
                reasoning_effort=None,
            )

        def execute_request(self, *, request, prompt_version):
            del request
            del prompt_version
            return SimpleNamespace(
                data={
                    "safe_cluster_ref_updates": [
                        {
                            "cluster_id": "window-001-alpha-cluster-01-alpha-beta",
                            "atom_ids": ["alpha", "beta"],
                            "cluster_ref": "[[window-001-alpha-cluster-01-alpha-beta]]",
                        }
                    ],
                    "report_only_merges": [],
                    "report_only_splits": [],
                    "review_nudges": [],
                }
            )

    class _ActionsRepairDecisionRunner:
        def __init__(self) -> None:
            self.executor = _ActionsRepairExecutor()
            self.base = _FakeDecisionRunner()

        def run_prompt(self, *, prompt_family, prompt, response_model, task_class, prompt_version, request_metadata=None):
            if prompt_family != "weave.structural_actions":
                return self.base.run_prompt(
                    prompt_family=prompt_family,
                    prompt=prompt,
                    response_model=response_model,
                    task_class=task_class,
                    prompt_version=prompt_version,
                    request_metadata=request_metadata,
                )
            return DecisionRunResult(
                payload=WeaveStructuralActionsArtifact.model_validate(
                    {
                        "safe_cluster_ref_updates": [
                            {
                                "cluster_id": "window-001-coherence-fragility-and-evaluation",
                                "atom_ids": ["alpha", "beta"],
                                "cluster_ref": "[[window-001-coherence-fragility-and-evaluation]]",
                            }
                        ],
                        "report_only_merges": [],
                        "report_only_splits": [],
                        "review_nudges": [],
                    }
                ),
                receipt=PromptReceipt.model_validate(
                    {
                        "prompt_family": prompt_family,
                        "prompt_version": "dream.weave.structural-actions.v2",
                        "task_class": "dream_decision",
                        "provider": "anthropic",
                        "model": "anthropic/claude-sonnet-4.6",
                        "input_mode": "text",
                        "request_fingerprint": {"kind": "text-prompt"},
                        "request_metadata": {},
                        "response_metadata": {},
                        "repaired": False,
                    }
                ),
            )

    result = run_weave_v2_shadow(dry_run=False, decision_runner=_ActionsRepairDecisionRunner())

    assert result.stage == "weave-v2-shadow"
    adapter_state = RuntimeState.for_repo_root(root).get_adapter_state(WEAVE_V2_ADAPTER)
    assert adapter_state is not None
    run_root = root / adapter_state["artifact_root"]
    actions = json.loads((run_root / "stage-weave" / "structural-actions.json").read_text(encoding="utf-8"))
    assert actions["prompt_receipt"]["repaired"] is True
    assert actions["payload"]["safe_cluster_ref_updates"][0]["cluster_id"] == "window-001-alpha-cluster-01-alpha-beta"
