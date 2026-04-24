from __future__ import annotations

from datetime import datetime, timezone

from mind.dream.common import (
    DreamPreconditionError,
    DreamResult,
    dream_today,
    dream_run,
    ensure_dream_enabled,
    ensure_onboarded,
    maybe_locked,
    runtime_state,
    vault,
)

from .apply import (
    apply_weave_write_plan,
    build_apply_manifest_from_plan,
    build_weave_apply_plan,
    normalize_weave_local_proposal,
    render_cluster_report_markdown,
    validate_reconciled_clusters,
    validate_weave_local_proposal,
)
from .artifacts import build_layout, write_run_manifest, write_stage_json
from .compare import build_weave_compare_artifact
from .contracts import (
    DecisionArtifactEnvelope,
    DreamRunManifest,
    ReconciledCluster,
    StageRunSummary,
    WeaveClusterReportsArtifact,
    WeaveCritiqueArtifact,
    WeaveLocalProposalArtifact,
    WeaveLocalProposalResponse,
    WeaveReconcileArtifact,
    WeaveStructuralActionsArtifact,
)
from .decision_runner import DecisionRunResult, DecisionRunner
from .gather import gather_weave_candidate_set
from .prompt_registry import get_prompt_spec
from mind.services.llm_repair import repair_once
from mind.services.llm_schema import prepare_strict_schema

WEAVE_V2_ADAPTER = "dream.v2.weave"


def run_weave_v2(
    *,
    dry_run: bool,
    acquire_lock: bool = True,
    context=None,
    decision_runner: DecisionRunner | None = None,
) -> DreamResult:
    return _run_weave_v2(
        dry_run=dry_run,
        acquire_lock=acquire_lock,
        context=context,
        decision_runner=decision_runner,
        mode="write",
    )


def run_weave_v2_shadow(
    *,
    dry_run: bool,
    acquire_lock: bool = True,
    context=None,
    decision_runner: DecisionRunner | None = None,
) -> DreamResult:
    return _run_weave_v2(
        dry_run=dry_run,
        acquire_lock=acquire_lock,
        context=context,
        decision_runner=decision_runner,
        mode="shadow",
    )


def _run_weave_v2(
    *,
    dry_run: bool,
    acquire_lock: bool,
    context,
    decision_runner: DecisionRunner | None,
    mode: str,
) -> DreamResult:
    ensure_dream_enabled()
    ensure_onboarded()
    v = vault()
    runtime = runtime_state()
    v2_cfg = v.config.dream.v2
    weave_cfg = v.config.dream.weave
    runner = decision_runner or DecisionRunner()
    started_at = _utc_now_string()
    stage_name = "weave-v2-shadow" if mode == "shadow" else "weave"
    with dream_run(stage_name, dry_run=dry_run, context=context) as (state, runtime_run_id):
        with maybe_locked(stage_name, dry_run=dry_run, acquire_lock=acquire_lock):
            run_id = f"run-{runtime_run_id}"
            layout = build_layout(
                repo_root=v.root,
                artifact_root=v2_cfg.artifact_root,
                run_id=run_id,
                stage="weave",
            )
            candidate_set = gather_weave_candidate_set(
                vault=v,
                runtime=runtime,
                run_id=run_id,
                mode="shadow" if mode == "shadow" else "write",
                candidate_cap=int(weave_cfg.candidate_cap),
                window_size=int(v2_cfg.weave_window_size),
            )
            mutations: list[str] = []
            warnings: list[str] = []
            prefix = "would write" if dry_run else "wrote"
            candidate_path = write_stage_json(layout, "candidate-set.json", candidate_set, dry_run=dry_run)
            mutations.append(f"{prefix} Dream v2 Weave candidate set {candidate_path}")

            local_proposals = _run_local_proposals(
                state=state,
                runtime_run_id=runtime_run_id,
                runner=runner,
                candidate_set=candidate_set,
                layout=layout,
                dry_run=dry_run,
                max_clusters=int(v2_cfg.weave_max_local_clusters),
                mutations=mutations,
                prefix=prefix,
            )

            reconciled = _run_reconcile(
                runner=runner,
                candidate_set=candidate_set,
                local_proposals=local_proposals,
            )
            _validate_reconcile(candidate_set=candidate_set, local_proposals=local_proposals, reconciled=reconciled.payload)
            mutations.append(
                f"{prefix} Dream v2 Weave reconciliation artifact "
                f"{write_stage_json(layout, 'reconciled-clusters.json', DecisionArtifactEnvelope(stage='weave', artifact_name='reconciled-clusters', prompt_receipt=reconciled.receipt, payload=reconciled.payload.model_dump(mode='json')), dry_run=dry_run)}"
            )

            critique = _run_critique(
                runner=runner,
                candidate_set=candidate_set,
                reconciled=reconciled.payload,
            )
            mutations.append(
                f"{prefix} Dream v2 Weave critique artifact "
                f"{write_stage_json(layout, 'critique.json', DecisionArtifactEnvelope(stage='weave', artifact_name='critique', prompt_receipt=critique.receipt, payload=critique.payload.model_dump(mode='json')), dry_run=dry_run)}"
            )

            reports = _run_report_writer(
                runner=runner,
                candidate_set=candidate_set,
                reconciled_clusters=reconciled.payload.merged_clusters,
                critique=critique.payload,
            )
            if reports.receipt.repaired:
                state.add_run_event(
                    runtime_run_id,
                    stage="weave",
                    event_type="shadow-report-repaired",
                    message="repaired invalid report writer payload",
                )
            mutations.append(
                f"{prefix} Dream v2 Weave report artifact "
                f"{write_stage_json(layout, 'cluster-reports.json', DecisionArtifactEnvelope(stage='weave', artifact_name='cluster-reports', prompt_receipt=reports.receipt, payload=reports.payload.model_dump(mode='json')), dry_run=dry_run)}"
            )

            structural_actions = _run_structural_actions(
                runner=runner,
                run_id=run_id,
                reports=reports.payload,
                critique=critique.payload,
            )
            if structural_actions.receipt.repaired:
                state.add_run_event(
                    runtime_run_id,
                    stage="weave",
                    event_type="shadow-structural-actions-repaired",
                    message="repaired invalid structural actions payload",
                )
            mutations.append(
                f"{prefix} Dream v2 Weave structural actions artifact "
                f"{write_stage_json(layout, 'structural-actions.json', DecisionArtifactEnvelope(stage='weave', artifact_name='structural-actions', prompt_receipt=structural_actions.receipt, payload=structural_actions.payload.model_dump(mode='json')), dry_run=dry_run)}"
            )

            apply_plan = build_weave_apply_plan(
                run_id=run_id,
                mode=mode,
                reports=reports.payload,
                actions=structural_actions.payload,
                critique=critique.payload,
            )
            apply_manifest = build_apply_manifest_from_plan(
                run_id=run_id,
                mode=mode,
                plan=apply_plan,
            )
            mutations.append(
                f"{prefix} Dream v2 Weave apply plan "
                f"{write_stage_json(layout, 'apply-plan.json', apply_plan, dry_run=dry_run)}"
            )
            mutations.append(
                f"{prefix} Dream v2 Weave apply manifest "
                f"{write_stage_json(layout, 'apply-manifest.json', apply_manifest, dry_run=dry_run)}"
            )

            compare = build_weave_compare_artifact(
                vault=v,
                run_id=run_id,
                candidate_set=candidate_set,
                local_proposals=local_proposals,
                reconciled_clusters=reconciled.payload.merged_clusters,
                critique=critique.payload,
                reports=reports.payload,
            )
            compare_path = write_stage_json(layout, "compare.json", compare, dry_run=dry_run)
            mutations.append(f"{prefix} Dream v2 Weave compare artifact {compare_path}")

            if mode == "write":
                if dry_run:
                    apply_manifest = build_apply_manifest_from_plan(
                        run_id=run_id,
                        mode="write",
                        plan=apply_plan,
                    )
                else:
                    apply_manifest = apply_weave_write_plan(
                        repo_root=v.root,
                        reports=reports.payload,
                        actions=structural_actions.payload,
                        critique=critique.payload,
                        today=dream_today(context),
                        context=context,
                    ).model_copy(update={"run_id": run_id})
                    runtime.update_dream_state(last_weave=dream_today(context), last_skip_reason=None)
                write_stage_json(layout, "apply-manifest.json", apply_manifest, dry_run=dry_run)
                mutations.append(
                    f"{prefix} Dream v2 Weave canonical write manifest "
                    f"{layout.relative_path(layout.stage_path('apply-manifest.json'))}"
                )

            completed_at = _utc_now_string()
            manifest = DreamRunManifest(
                run_id=run_id,
                started_at=started_at,
                completed_at=completed_at,
                mode="shadow" if mode == "shadow" else "write",
                shadow_mode=mode == "shadow",
                config_snapshot=v.config.model_dump(mode="json"),
                artifact_root=layout.relative_path(layout.run_root),
                stages=[
                    StageRunSummary(
                        stage="weave",
                        status="completed",
                        candidate_count=len(candidate_set.atom_snapshots),
                        decision_artifact_count=len(local_proposals) + 4,
                        write_count=apply_manifest.write_count,
                        warning_count=len(warnings),
                    )
                ],
            )
            manifest_path = write_run_manifest(layout, manifest, dry_run=dry_run)
            mutations.append(f"{prefix} Dream v2 run manifest {manifest_path}")
            if not dry_run:
                runtime.upsert_adapter_state(
                    adapter=WEAVE_V2_ADAPTER,
                    state={
                        "run_id": run_id,
                        "status": "completed",
                        "mode": mode,
                        "artifact_root": layout.relative_path(layout.run_root),
                        "candidate_count": len(candidate_set.atom_snapshots),
                        "window_count": len(candidate_set.windows),
                        "proposal_count": len(local_proposals),
                        "reconciled_cluster_count": len(reconciled.payload.merged_clusters),
                        "report_count": len(reports.payload.reports),
                        "compare_path": compare_path,
                        "compare": compare.model_dump(mode="json"),
                        "started_at": started_at,
                        "completed_at": completed_at,
                    },
                )
            if mode == "shadow":
                summary = (
                    f"Weave v2 shadow prepared {len(candidate_set.atom_snapshots)} candidates across "
                    f"{len(candidate_set.windows)} windows, reconciled {len(reconciled.payload.merged_clusters)} clusters, "
                    f"and emitted compare metadata against {compare.v1_cluster_count} v1 weave pages."
                )
            else:
                summary = (
                    f"Weave v2 organized {len(candidate_set.atom_snapshots)} candidates across "
                    f"{len(candidate_set.windows)} windows, wrote {len(reports.payload.reports)} canonical cluster pages, "
                    f"and emitted compare metadata against {compare.v1_cluster_count} baseline weave pages."
                )
            return DreamResult(
                stage="weave-v2-shadow" if mode == "shadow" else "weave",
                dry_run=dry_run,
                summary=summary,
                mutations=mutations,
                warnings=warnings,
            )


def _run_local_proposals(
    *,
    state,
    runtime_run_id: int,
    runner: DecisionRunner,
    candidate_set,
    layout,
    dry_run: bool,
    max_clusters: int,
    mutations: list[str],
    prefix: str,
) -> list[WeaveLocalProposalArtifact]:
    local_prompt = get_prompt_spec("weave.local_cluster")
    proposals: list[WeaveLocalProposalArtifact] = []
    for window in candidate_set.windows:
        state.add_run_event(
            runtime_run_id,
            stage="weave",
            event_type="shadow-window",
            message=f"{window.window_id}: {len(window.atom_ids)} atoms",
        )
        prompt = local_prompt.render(
            window=window,
            candidate_set=candidate_set,
            max_clusters=max_clusters,
        )
        decision = runner.run_prompt(
            prompt_family=local_prompt.family,
            prompt=prompt,
            response_model=local_prompt.response_model,
            task_class=local_prompt.task_class,
            prompt_version=local_prompt.prompt_version,
            request_metadata={
                "run_id": candidate_set.run_id,
                "stage": "weave",
                "window_id": window.window_id,
            },
        )
        proposal, decision = _normalize_validate_or_repair_local_proposal(
            runner=runner,
            prompt_spec=local_prompt,
            prompt=prompt,
            request_metadata={
                "run_id": candidate_set.run_id,
                "stage": "weave",
                "window_id": window.window_id,
            },
            window=window,
            decision=decision,
            max_clusters=max_clusters,
        )
        if decision.receipt.repaired:
            state.add_run_event(
                runtime_run_id,
                stage="weave",
                event_type="shadow-window-repaired",
                message=f"{window.window_id}: repaired invalid local proposal",
            )
        proposals.append(proposal)
        envelope = DecisionArtifactEnvelope(
            stage="weave",
            artifact_name=window.window_id,
            prompt_receipt=decision.receipt,
            payload=proposal.model_dump(mode="json"),
        )
        proposal_path = write_stage_json(
            layout,
            f"local-proposals/{window.window_id}.json",
            envelope,
            dry_run=dry_run,
        )
        mutations.append(f"{prefix} Dream v2 Weave local proposal {proposal_path}")
    return proposals


def _normalize_validate_or_repair_local_proposal(
    *,
    runner: DecisionRunner,
    prompt_spec,
    prompt: str,
    request_metadata: dict[str, object],
    window,
    decision: DecisionRunResult[WeaveLocalProposalResponse],
    max_clusters: int,
) -> tuple[WeaveLocalProposalArtifact, DecisionRunResult[WeaveLocalProposalResponse]]:
    proposal = normalize_weave_local_proposal(window=window, proposal=decision.payload)
    try:
        validate_weave_local_proposal(window=window, proposal=proposal, max_clusters=max_clusters)
        return proposal, decision
    except ValueError as exc:
        repaired = _repair_local_proposal_semantics(
            runner=runner,
            prompt_spec=prompt_spec,
            prompt=prompt,
            request_metadata=request_metadata,
            validation_error=exc,
            invalid_payload=decision.payload.model_dump(mode="json"),
        )
        if repaired is None:
            raise DreamPreconditionError(str(exc)) from exc
        repaired_decision = DecisionRunResult(
            payload=repaired,
            receipt=decision.receipt.model_copy(update={"repaired": True}),
        )
        repaired_proposal = normalize_weave_local_proposal(window=window, proposal=repaired_decision.payload)
        try:
            validate_weave_local_proposal(window=window, proposal=repaired_proposal, max_clusters=max_clusters)
        except ValueError as repaired_exc:
            raise DreamPreconditionError(
                f"{exc}\n\nsemantic repair attempted and failed: {repaired_exc}"
            ) from repaired_exc
        return repaired_proposal, repaired_decision


def _repair_local_proposal_semantics(
    *,
    runner: DecisionRunner,
    prompt_spec,
    prompt: str,
    request_metadata: dict[str, object],
    validation_error: ValueError,
    invalid_payload: dict[str, object],
) -> WeaveLocalProposalResponse | None:
    repaired = _repair_decision_payload_semantics(
        runner=runner,
        prompt_spec=prompt_spec,
        prompt=prompt,
        request_metadata=request_metadata,
        validation_error=validation_error,
        invalid_payload=invalid_payload,
    )
    if repaired is None:
        return None
    return prompt_spec.response_model.model_validate(repaired)


def _repair_decision_payload_semantics(
    *,
    runner: DecisionRunner,
    prompt_spec,
    prompt: str,
    request_metadata: dict[str, object],
    validation_error: ValueError,
    invalid_payload: dict[str, object],
) -> dict[str, object] | None:
    executor = getattr(runner, "executor", None)
    if executor is None:
        return None
    schema = prepare_strict_schema(prompt_spec.response_model)
    request = executor.build_prompt_request(
        task_class=prompt_spec.task_class,
        prompt=prompt,
        output_mode="json",
        response_schema=schema,
        request_metadata=request_metadata,
    )
    return repair_once(
        executor,
        original_request=request,
        prompt_version=prompt_spec.prompt_version,
        response_schema=schema,
        validation_errors=[
            {
                "loc": ["semantic_validation"],
                "msg": str(validation_error),
                "type": "semantic_validation",
            }
        ],
        invalid_payload=invalid_payload,
    )


def _run_reconcile(
    *,
    runner: DecisionRunner,
    candidate_set,
    local_proposals: list[WeaveLocalProposalArtifact],
) -> DecisionRunResult[WeaveReconcileArtifact]:
    prompt_spec = get_prompt_spec("weave.reconcile")
    prompt = prompt_spec.render(candidate_set=candidate_set, local_proposals=local_proposals)
    return runner.run_prompt(
        prompt_family=prompt_spec.family,
        prompt=prompt,
        response_model=prompt_spec.response_model,
        task_class=prompt_spec.task_class,
        prompt_version=prompt_spec.prompt_version,
        request_metadata={
            "run_id": candidate_set.run_id,
            "stage": "weave",
            "artifact": "reconciled-clusters",
        },
    )


def _validate_reconcile(
    *,
    candidate_set,
    local_proposals: list[WeaveLocalProposalArtifact],
    reconciled: WeaveReconcileArtifact,
) -> None:
    valid_atom_ids = {snapshot.atom_id for snapshot in candidate_set.atom_snapshots}
    valid_cluster_ids = {
        cluster.cluster_id
        for proposal in local_proposals
        for cluster in proposal.clusters
    }
    validate_reconciled_clusters(
        clusters=reconciled.merged_clusters,
        valid_atom_ids=valid_atom_ids,
        valid_cluster_ids=valid_cluster_ids,
    )


def _run_critique(
    *,
    runner: DecisionRunner,
    candidate_set,
    reconciled: WeaveReconcileArtifact,
) -> DecisionRunResult[WeaveCritiqueArtifact]:
    prompt_spec = get_prompt_spec("weave.critique")
    prompt = prompt_spec.render(candidate_set=candidate_set, reconciled=reconciled)
    critique = runner.run_prompt(
        prompt_family=prompt_spec.family,
        prompt=prompt,
        response_model=prompt_spec.response_model,
        task_class=prompt_spec.task_class,
        prompt_version=prompt_spec.prompt_version,
        request_metadata={
            "run_id": candidate_set.run_id,
            "stage": "weave",
            "artifact": "critique",
        },
    )
    reconciled_ids = {cluster.cluster_id for cluster in reconciled.merged_clusters}
    sanitized = _sanitize_critique_payload(critique=critique.payload, reconciled_ids=reconciled_ids)
    if sanitized != critique.payload:
        return DecisionRunResult(
            payload=sanitized,
            receipt=critique.receipt.model_copy(update={"repaired": True}),
        )
    return critique


def _sanitize_critique_payload(
    *,
    critique: WeaveCritiqueArtifact,
    reconciled_ids: set[str],
) -> WeaveCritiqueArtifact:
    approved_cluster_ids = [
        cluster_id
        for cluster_id in critique.approved_cluster_ids
        if cluster_id in reconciled_ids
    ]
    clusters_requiring_split = [
        cluster_id
        for cluster_id in critique.clusters_requiring_split
        if cluster_id in reconciled_ids
    ]
    clusters_requiring_boundary_trim = [
        trim
        for trim in critique.clusters_requiring_boundary_trim
        if trim.cluster_id in reconciled_ids
    ]
    review_flags = [
        flag
        for flag in critique.review_flags
        if flag.cluster_id in reconciled_ids
    ]
    return critique.model_copy(
        update={
            "approved_cluster_ids": approved_cluster_ids,
            "clusters_requiring_split": clusters_requiring_split,
            "clusters_requiring_boundary_trim": clusters_requiring_boundary_trim,
            "review_flags": review_flags,
        }
    )


def _run_report_writer(
    *,
    runner: DecisionRunner,
    candidate_set,
    reconciled_clusters: list[ReconciledCluster],
    critique: WeaveCritiqueArtifact,
) -> DecisionRunResult[WeaveClusterReportsArtifact]:
    prompt_spec = get_prompt_spec("weave.report_writer")
    prompt = prompt_spec.render(
        candidate_set=candidate_set,
        reconciled_clusters=reconciled_clusters,
        critique=critique,
    )
    request_metadata = {
        "run_id": candidate_set.run_id,
        "stage": "weave",
        "artifact": "cluster-reports",
    }
    reports = runner.run_prompt(
        prompt_family=prompt_spec.family,
        prompt=prompt,
        response_model=prompt_spec.response_model,
        task_class=prompt_spec.task_class,
        prompt_version=prompt_spec.prompt_version,
        request_metadata=request_metadata,
    )
    try:
        _validate_report_writer_payload(reports=reports.payload, critique=critique)
        return reports
    except ValueError as exc:
        repaired = _repair_decision_payload_semantics(
            runner=runner,
            prompt_spec=prompt_spec,
            prompt=prompt,
            request_metadata=request_metadata,
            validation_error=exc,
            invalid_payload=reports.payload.model_dump(mode="json"),
        )
        if repaired is None:
            raise DreamPreconditionError(str(exc)) from exc
        repaired_reports = DecisionRunResult(
            payload=prompt_spec.response_model.model_validate(repaired),
            receipt=reports.receipt.model_copy(update={"repaired": True}),
        )
        try:
            _validate_report_writer_payload(reports=repaired_reports.payload, critique=critique)
        except ValueError as repaired_exc:
            raise DreamPreconditionError(
                f"{exc}\n\nsemantic repair attempted and failed: {repaired_exc}"
            ) from repaired_exc
        return repaired_reports


def _validate_report_writer_payload(
    *,
    reports: WeaveClusterReportsArtifact,
    critique: WeaveCritiqueArtifact,
) -> None:
    approved = set(critique.approved_cluster_ids)
    unknown = sorted({report.cluster_id for report in reports.reports} - approved)
    if unknown:
        raise ValueError(
            "weave report writer emitted reports for non-approved clusters: "
            + ", ".join(unknown)
        )


def _run_structural_actions(
    *,
    runner: DecisionRunner,
    run_id: str,
    reports: WeaveClusterReportsArtifact,
    critique: WeaveCritiqueArtifact,
) -> DecisionRunResult[WeaveStructuralActionsArtifact]:
    prompt_spec = get_prompt_spec("weave.structural_actions")
    prompt = prompt_spec.render(reports=reports, critique=critique)
    request_metadata = {
        "run_id": run_id,
        "stage": "weave",
        "artifact": "structural-actions",
    }
    actions = runner.run_prompt(
        prompt_family=prompt_spec.family,
        prompt=prompt,
        response_model=prompt_spec.response_model,
        task_class=prompt_spec.task_class,
        prompt_version=prompt_spec.prompt_version,
        request_metadata=request_metadata,
    )
    try:
        _validate_structural_actions_payload(actions=actions.payload, reports=reports)
        return actions
    except ValueError as exc:
        repaired = _repair_decision_payload_semantics(
            runner=runner,
            prompt_spec=prompt_spec,
            prompt=prompt,
            request_metadata=request_metadata,
            validation_error=exc,
            invalid_payload=actions.payload.model_dump(mode="json"),
        )
        if repaired is None:
            raise DreamPreconditionError(str(exc)) from exc
        repaired_actions = DecisionRunResult(
            payload=prompt_spec.response_model.model_validate(repaired),
            receipt=actions.receipt.model_copy(update={"repaired": True}),
        )
        try:
            _validate_structural_actions_payload(actions=repaired_actions.payload, reports=reports)
        except ValueError as repaired_exc:
            raise DreamPreconditionError(
                f"{exc}\n\nsemantic repair attempted and failed: {repaired_exc}"
            ) from repaired_exc
        return repaired_actions


def _validate_structural_actions_payload(
    *,
    actions: WeaveStructuralActionsArtifact,
    reports: WeaveClusterReportsArtifact,
) -> None:
    valid_cluster_ids = {report.cluster_id for report in reports.reports}
    unknown_updates = sorted(
        update.cluster_id
        for update in actions.safe_cluster_ref_updates
        if update.cluster_id not in valid_cluster_ids
    )
    if unknown_updates:
        raise ValueError(
            "weave structural actions referenced unknown clusters: " + ", ".join(unknown_updates)
        )


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
