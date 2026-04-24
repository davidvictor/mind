from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import re
from typing import Any

from scripts.common.contract import atom_collection_dirs

from mind.dream.common import (
    DreamExecutionContext,
    DreamPreconditionError,
    DreamResult,
    dream_run,
    dream_today,
    ensure_dream_enabled,
    ensure_onboarded,
    extract_wikilinks,
    maybe_locked,
    read_page,
    source_pages,
    vault,
)
from mind.dream.v2.artifacts import build_layout, write_run_manifest, write_stage_json
from mind.dream.v2.contracts import (
    ApplyManifest,
    ApplyManifestEntry,
    ApplyPlan,
    ApplyPlanAction,
    DreamRunManifest,
    KENE_ID_RE,
    KeneArrangementPlan,
    KeneArtifactReference,
    KeneAtomSnapshot,
    KeneCritique,
    KeneGroup,
    KeneInputBundle,
    KeneMove,
    KenePriorOutputMap,
    KeneRelationChange,
    KeneRelationDiff,
    KeneRenderPackage,
    StageRunSummary,
)

PRIOR_STAGES = ("light", "deep", "rem")


def run_kene(
    *,
    dry_run: bool,
    acquire_lock: bool = True,
    context: DreamExecutionContext | None = None,
    progress_callback=None,
) -> DreamResult:
    del progress_callback
    ensure_dream_enabled()
    ensure_onboarded()
    if not dry_run:
        raise DreamPreconditionError("Kene write mode is not enabled; run mind dream kene --dry-run")

    v = vault()
    cfg = v.config.dream.v2.kene
    if not cfg.enabled:
        raise DreamPreconditionError("Kene is disabled in dream.v2.kene.enabled")
    today = dream_today(context)
    generated_at = _utc_now_string()
    warnings = [
        "Kene is shadow-only; canonical markdown and relation writes are blocked.",
        "Kene decisions are deterministic placeholders until prompt-backed decisions are enabled.",
    ]

    with dream_run("kene", dry_run=True, context=context) as (runtime, run_id):
        artifact_run_id = f"run-{run_id}"
        layout = build_layout(
            repo_root=v.root,
            artifact_root=v.config.dream.v2.artifact_root,
            run_id=artifact_run_id,
            stage="kene",
        )
        runtime.add_run_event(run_id, stage="kene", event_type="selected", message="shadow artifact pass")
        with maybe_locked("kene", dry_run=True, acquire_lock=acquire_lock):
            atom_snapshots = _atom_snapshots(v, max_atoms=int(cfg.max_atoms))
            source_ids = _source_ids(v)
            prior_map = _prior_output_map(v, max_artifacts=int(cfg.max_prior_artifacts), run_id=artifact_run_id)
            input_bundle = KeneInputBundle(
                run_id=artifact_run_id,
                generated_at=generated_at,
                atoms=atom_snapshots,
                source_ids=source_ids,
                prior_artifacts=[
                    artifact
                    for stage in PRIOR_STAGES
                    for artifact in prior_map.outputs_by_stage.get(stage, [])
                ],
            )
            arrangement_plan = _arrangement_plan(
                run_id=artifact_run_id,
                atoms=atom_snapshots,
                prior_artifacts=input_bundle.prior_artifacts,
            )
            relation_diff = _relation_diff(
                run_id=artifact_run_id,
                groups=arrangement_plan.groups,
                atoms=atom_snapshots,
                warnings=warnings,
            )
            critique = KeneCritique(
                run_id=artifact_run_id,
                findings=[
                    "Review groups for false joins before enabling prompt-backed reframing.",
                    "Review relation diffs as proposals only; no relation mutation is enabled.",
                ],
                blocked_reasons=[
                    "Kene apply mode is not enabled.",
                    "Prompt-backed arrange/reframe/regraph decisions are not enabled.",
                ],
            )
            render_package = _render_package(
                run_id=artifact_run_id,
                today=today,
                groups=arrangement_plan.groups,
                relation_diff=relation_diff,
            )
            apply_plan = ApplyPlan(
                run_id=artifact_run_id,
                stage="kene",
                mode="shadow",
                actions=[
                    ApplyPlanAction(
                        action_id=f"write-kene-map-{today}",
                        action_type="write_markdown",
                        target_path=render_package.markdown_target_path,
                        safe_to_apply=False,
                        rationale="Kene map/report write is blocked until apply mode is approved.",
                        atom_ids=[],
                    )
                ],
                notes=[
                    "shadow-only dry-run artifact emission",
                    "relation mutations remain review-only",
                ],
            )
            apply_manifest = ApplyManifest(
                run_id=artifact_run_id,
                stage="kene",
                mode="shadow",
                entries=[
                    ApplyManifestEntry(
                        action_id=action.action_id,
                        status="blocked",
                        target_path=action.target_path,
                        notes=["shadow-only"],
                    )
                    for action in apply_plan.actions
                ],
                write_count=0,
                warning_count=len(warnings),
                notes=list(apply_plan.notes),
            )
            compare = {
                "run_id": artifact_run_id,
                "stage": "kene",
                "candidate_atoms": len(atom_snapshots),
                "prior_artifacts": len(input_bundle.prior_artifacts),
                "groups": len(arrangement_plan.groups),
                "planned_moves": len(arrangement_plan.moves),
                "relation_diffs": len(relation_diff.changes),
                "blocked_writes": len(apply_manifest.entries),
            }

            mutations = [
                f"wrote Kene input bundle {write_stage_json(layout, 'input-bundle.json', input_bundle, dry_run=False)}",
                f"wrote Kene prior output map {write_stage_json(layout, 'prior-output-map.json', prior_map, dry_run=False)}",
                f"wrote Kene arrangement plan {write_stage_json(layout, 'arrangement-plan.json', arrangement_plan, dry_run=False)}",
                f"wrote Kene relation diff {write_stage_json(layout, 'relation-diff.json', relation_diff, dry_run=False)}",
                f"wrote Kene critique {write_stage_json(layout, 'critique.json', critique, dry_run=False)}",
                f"wrote Kene render package {write_stage_json(layout, 'render-package.json', render_package, dry_run=False)}",
                f"wrote Kene apply plan {write_stage_json(layout, 'apply-plan.json', apply_plan, dry_run=False)}",
                f"wrote Kene apply manifest {write_stage_json(layout, 'apply-manifest.json', apply_manifest, dry_run=False)}",
                f"wrote Kene compare report {write_stage_json(layout, 'compare.json', compare, dry_run=False)}",
            ]
            write_run_manifest(
                layout,
                DreamRunManifest(
                    run_id=artifact_run_id,
                    started_at=generated_at,
                    completed_at=_utc_now_string(),
                    mode="shadow",
                    shadow_mode=True,
                    config_snapshot=v.config.model_dump(mode="json"),
                    artifact_root=layout.relative_path(layout.run_root),
                    stages=[
                        StageRunSummary(
                            stage="kene",
                            status="completed",
                            candidate_count=len(atom_snapshots),
                            decision_artifact_count=5,
                            write_count=0,
                            warning_count=len(warnings),
                        )
                    ],
                ),
                dry_run=False,
            )

    summary = (
        f"Kene Dream gathered {compare['candidate_atoms']} atoms, "
        f"{compare['prior_artifacts']} prior artifacts, planned {compare['groups']} groups, "
        f"{compare['planned_moves']} moves, {compare['relation_diffs']} relation diffs, "
        f"and blocked {compare['blocked_writes']} canonical writes."
    )
    return DreamResult(stage="kene", dry_run=True, summary=summary, mutations=mutations, warnings=warnings)


def _atom_snapshots(v, *, max_atoms: int) -> list[KeneAtomSnapshot]:
    snapshots: list[KeneAtomSnapshot] = []
    for atom_type, dirname in atom_collection_dirs().items():
        root = v.wiki / dirname
        if not root.exists():
            continue
        for path in sorted(root.glob("*.md")):
            frontmatter, _body = read_page(path)
            lifecycle_state = str(frontmatter.get("lifecycle_state") or frontmatter.get("status") or "active")
            if lifecycle_state == "probationary":
                continue
            atom_id = str(frontmatter.get("id") or path.stem).strip()
            if not _is_safe_id(atom_id):
                continue
            relation_ids = [
                item
                for item in _relation_ids(frontmatter)
                if _is_safe_id(item)
            ]
            snapshots.append(
                KeneAtomSnapshot(
                    atom_id=atom_id,
                    atom_type=atom_type,
                    title=str(frontmatter.get("title") or atom_id),
                    path=v.logical_path(path),
                    lifecycle_state=lifecycle_state,
                    domains=[str(item) for item in (frontmatter.get("domains") or [])],
                    relation_ids=relation_ids,
                    evidence_count=int(frontmatter.get("evidence_count") or 0),
                    last_evidence_date=str(frontmatter.get("last_evidence_date") or ""),
                )
            )
    snapshots.sort(
        key=lambda item: (
            item.evidence_count,
            item.last_evidence_date,
            item.title.lower(),
            item.atom_id,
        ),
        reverse=True,
    )
    return snapshots[: max(0, max_atoms)]


def _source_ids(v) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for path in source_pages(v):
        frontmatter, _body = read_page(path)
        source_id = str(frontmatter.get("id") or path.stem).strip()
        if not _is_safe_id(source_id) or source_id in seen:
            continue
        ids.append(source_id)
        seen.add(source_id)
    return ids


def _prior_output_map(v, *, max_artifacts: int, run_id: str) -> KenePriorOutputMap:
    outputs: dict[str, list[KeneArtifactReference]] = {stage: [] for stage in PRIOR_STAGES}
    runs_root = v.root / v.config.dream.v2.artifact_root / "runs"
    if runs_root.exists():
        for stage in PRIOR_STAGES:
            stage_refs: list[KeneArtifactReference] = []
            for stage_root in sorted(runs_root.glob(f"*/stage-{stage}")):
                for path in sorted(stage_root.glob("*.json")):
                    stage_refs.append(
                        KeneArtifactReference(
                            stage=stage,
                            artifact_name=path.name,
                            artifact_path=v.logical_path(path),
                        )
                    )
            outputs[stage] = stage_refs[-max(0, max_artifacts):]

    rem_root = v.wiki / "dreams" / "rem"
    if rem_root.exists():
        rem_refs = [
            KeneArtifactReference(
                stage="rem",
                artifact_name=path.name,
                artifact_path=v.logical_path(path),
            )
            for path in sorted(rem_root.glob("*.md"))[-max(0, max_artifacts):]
        ]
        outputs["rem"] = [*outputs["rem"], *rem_refs]
    return KenePriorOutputMap(run_id=run_id, outputs_by_stage=outputs)


def _arrangement_plan(
    *,
    run_id: str,
    atoms: list[KeneAtomSnapshot],
    prior_artifacts: list[KeneArtifactReference],
) -> KeneArrangementPlan:
    grouped: dict[str, list[KeneAtomSnapshot]] = defaultdict(list)
    for atom in atoms:
        domain = atom.domains[0] if atom.domains else "uncategorized"
        grouped[f"{domain}-{atom.atom_type}"].append(atom)

    groups: list[KeneGroup] = []
    moves: list[KeneMove] = []
    provenance = [artifact.artifact_path for artifact in prior_artifacts[:5]]
    for key, members in sorted(grouped.items()):
        group_id = _slug(f"group-{key}")
        groups.append(
            KeneGroup(
                group_id=group_id,
                title=key.replace("-", " ").title(),
                member_atom_ids=[member.atom_id for member in members],
                rationale="Deterministic shadow grouping by primary domain and atom type.",
                provenance=provenance,
            )
        )
        if len(members) > 1:
            moves.append(
                KeneMove(
                    move_id=f"review-{group_id}",
                    action="review_only",
                    atom_ids=[member.atom_id for member in members[:8]],
                    to_group_id=group_id,
                    rationale="Review whether this deterministic group wants a stronger parent frame.",
                    provenance=provenance,
                )
            )
    return KeneArrangementPlan(
        run_id=run_id,
        groups=groups,
        moves=moves,
        notes=["Deterministic placeholder output; no model-backed rearrangement has run."],
    )


def _relation_diff(
    *,
    run_id: str,
    groups: list[KeneGroup],
    atoms: list[KeneAtomSnapshot],
    warnings: list[str],
) -> KeneRelationDiff:
    atom_lookup = {atom.atom_id: atom for atom in atoms}
    existing = {
        frozenset({atom.atom_id, relation_id})
        for atom in atoms
        for relation_id in atom.relation_ids
        if relation_id in atom_lookup and relation_id != atom.atom_id
    }
    changes: list[KeneRelationChange] = []
    for group in groups:
        members = [atom_id for atom_id in group.member_atom_ids if atom_id in atom_lookup]
        if len(members) < 2:
            continue
        for left, right in zip(members, members[1:]):
            if frozenset({left, right}) in existing:
                continue
            changes.append(
                KeneRelationChange(
                    change_id=f"review-{len(changes) + 1}",
                    action="review_only",
                    source_atom_id=left,
                    target_atom_id=right,
                    relation_type="adjacent_to",
                    review_only=True,
                    rationale="Atoms share a deterministic Kene group but are not currently linked.",
                    provenance=[group.group_id],
                )
            )
            if len(changes) >= 20:
                break
        if len(changes) >= 20:
            break
    return KeneRelationDiff(
        run_id=run_id,
        changes=changes,
        blocked_write_count=len(changes),
        warnings=warnings,
    )


def _render_package(
    *,
    run_id: str,
    today: str,
    groups: list[KeneGroup],
    relation_diff: KeneRelationDiff,
) -> KeneRenderPackage:
    return KeneRenderPackage(
        run_id=run_id,
        markdown_target_path=f"memory/dreams/kene/{today}-{run_id}.md",
        title=f"Kene Map {today}",
        sections=[
            {
                "heading": "Groups",
                "items": [
                    {
                        "group_id": group.group_id,
                        "title": group.title,
                        "member_atom_ids": group.member_atom_ids,
                    }
                    for group in groups
                ],
            },
            {
                "heading": "Review-only relation diffs",
                "items": [change.model_dump(mode="json") for change in relation_diff.changes],
            },
        ],
    )


def _relation_ids(frontmatter: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for item in frontmatter.get("relates_to") or []:
        ids.extend(extract_wikilinks(str(item)))
    typed_relations = frontmatter.get("typed_relations")
    if isinstance(typed_relations, dict):
        for values in typed_relations.values():
            for item in values or []:
                ids.extend(extract_wikilinks(str(item)))
    deduped: list[str] = []
    seen: set[str] = set()
    for item in ids:
        if item and item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped


def _is_safe_id(value: str) -> bool:
    return bool(KENE_ID_RE.match(value))


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug if slug and slug[0].isalnum() else "group"


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
