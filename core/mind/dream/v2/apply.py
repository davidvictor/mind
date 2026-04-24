from __future__ import annotations

from pathlib import Path

from mind.dream.common import DreamExecutionContext, extract_wikilinks, read_page, write_note_page, write_page_force
from mind.services.graph_registry import GraphRegistry
from scripts.common.slugify import slugify
from scripts.common.vault import Vault

from .contracts import (
    ApplyManifest,
    ApplyManifestEntry,
    ApplyPlan,
    ApplyPlanAction,
    NeighborhoodWindow,
    ReconciledCluster,
    ReviewNudge,
    WeaveLocalProposalResponse,
    WeaveBridgeCandidate,
    WeaveClusterCandidate,
    WeaveClusterMember,
    WeaveClusterReport,
    WeaveClusterReportsArtifact,
    WeaveCritiqueArtifact,
    WeaveLocalProposalArtifact,
    WeaveStructuralActionsArtifact,
)


def normalize_weave_local_proposal(
    *,
    window: NeighborhoodWindow,
    proposal: WeaveLocalProposalResponse,
) -> WeaveLocalProposalArtifact:
    normalized_clusters: list[WeaveClusterCandidate] = []
    normalized_bridge_ids: list[str] = []
    normalized_bridges: list[WeaveBridgeCandidate] = []
    for bridge_index, bridge in enumerate(proposal.bridge_candidates, start=1):
        bridge_id = f"{window.window_id}-bridge-{bridge_index:02d}-{slugify(bridge.source_atom_id)}-{slugify(bridge.target_atom_id)}"
        normalized_bridges.append(
            WeaveBridgeCandidate(
                bridge_id=bridge_id,
                source_atom_id=bridge.source_atom_id,
                target_atom_id=bridge.target_atom_id,
                bridge_type=bridge.bridge_type,
                why_it_matters=bridge.why_it_matters,
                confidence=bridge.confidence,
            )
        )
        normalized_bridge_ids.append(bridge_id)

    for cluster_index, cluster in enumerate(proposal.clusters, start=1):
        member_seed = "-".join(sorted(cluster.member_atom_ids)[:3]) or f"cluster-{cluster_index:02d}"
        cluster_id = f"{window.window_id}-cluster-{cluster_index:02d}-{slugify(member_seed)}"
        normalized_members = [
            WeaveClusterMember(
                cluster_id=cluster_id,
                atom_id=member.atom_id,
                role=member.role,
                why_included=member.why_included,
                primary_signals=list(member.primary_signals),
            )
            for member in cluster.member_roles
        ]
        cluster_atom_ids = set(cluster.member_atom_ids) | set(cluster.borderline_atom_ids)
        normalized_clusters.append(
            WeaveClusterCandidate(
                cluster_id=cluster_id,
                source_window_id=window.window_id,
                cluster_title=cluster.cluster_title,
                cluster_thesis=cluster.cluster_thesis,
                member_atom_ids=list(cluster.member_atom_ids),
                member_roles=normalized_members,
                borderline_atom_ids=list(cluster.borderline_atom_ids),
                excluded_atom_ids=list(cluster.excluded_atom_ids),
                bridge_candidate_ids=(
                    [bridge_id for bridge_id in normalized_bridge_ids if bridge_id in set(cluster.bridge_candidate_ids)]
                    if cluster.bridge_candidate_ids
                    else [
                        bridge.bridge_id
                        for bridge in normalized_bridges
                        if bridge.source_atom_id in cluster_atom_ids or bridge.target_atom_id in cluster_atom_ids
                    ]
                ),
                confidence=cluster.confidence,
                rationale=cluster.rationale or cluster.why_now,
                why_now=cluster.why_now,
            )
        )
    return WeaveLocalProposalArtifact(
        window_id=window.window_id,
        seed_atom_id=window.seed_atom_id,
        clusters=normalized_clusters,
        leftover_atom_ids=list(proposal.leftover_atom_ids),
        bridge_candidates=normalized_bridges,
        window_observations=list(proposal.window_observations),
    )


def validate_weave_local_proposal(
    *,
    window: NeighborhoodWindow,
    proposal: WeaveLocalProposalArtifact,
    max_clusters: int,
) -> None:
    if proposal.window_id != window.window_id:
        raise ValueError(
            f"weave local proposal window mismatch: expected {window.window_id}, got {proposal.window_id}"
        )
    if proposal.seed_atom_id != window.seed_atom_id:
        raise ValueError(
            f"weave local proposal seed mismatch: expected {window.seed_atom_id}, got {proposal.seed_atom_id}"
        )
    if len(proposal.clusters) > max_clusters:
        raise ValueError(
            f"weave local proposal emitted {len(proposal.clusters)} clusters; max is {max_clusters}"
        )
    valid_ids = set(window.atom_ids)
    cluster_ids: set[str] = set()
    bridge_ids = {bridge.bridge_id for bridge in proposal.bridge_candidates}
    referenced_ids: set[str] = set(proposal.leftover_atom_ids)
    for cluster in proposal.clusters:
        if cluster.source_window_id != window.window_id:
            raise ValueError(
                f"weave local proposal cluster {cluster.cluster_id} has wrong source_window_id={cluster.source_window_id!r}"
            )
        if not cluster.cluster_id:
            raise ValueError("weave local proposal cluster is missing a deterministic cluster_id")
        if cluster.cluster_id in cluster_ids:
            raise ValueError(f"weave local proposal duplicated cluster_id {cluster.cluster_id}")
        cluster_ids.add(cluster.cluster_id)
        if any(member.cluster_id != cluster.cluster_id for member in cluster.member_roles):
            raise ValueError(f"weave local proposal member role cluster_id mismatch in {cluster.cluster_id}")
        if any(bridge_id not in bridge_ids for bridge_id in cluster.bridge_candidate_ids):
            raise ValueError(f"weave local proposal references unknown bridge ids in {cluster.cluster_id}")
        referenced_ids.update(cluster.member_atom_ids)
        referenced_ids.update(cluster.borderline_atom_ids)
        referenced_ids.update(cluster.excluded_atom_ids)
        for member in cluster.member_roles:
            referenced_ids.add(member.atom_id)
    for bridge in proposal.bridge_candidates:
        if not bridge.bridge_id:
            raise ValueError("weave local proposal bridge candidate is missing a deterministic bridge_id")
        referenced_ids.add(bridge.source_atom_id)
        referenced_ids.add(bridge.target_atom_id)
    missing = sorted(referenced_ids - valid_ids)
    if missing:
        raise ValueError(
            "weave local proposal referenced atoms outside the window: "
            + ", ".join(missing)
        )


def validate_reconciled_clusters(
    *,
    clusters: list[ReconciledCluster],
    valid_atom_ids: set[str],
    valid_cluster_ids: set[str],
) -> None:
    seen_cluster_ids: set[str] = set()
    referenced_ids: set[str] = set()
    for cluster in clusters:
        if cluster.cluster_id in seen_cluster_ids:
            raise ValueError(f"duplicate reconciled cluster_id {cluster.cluster_id}")
        seen_cluster_ids.add(cluster.cluster_id)
        if any(source_id not in valid_cluster_ids for source_id in cluster.source_cluster_ids):
            raise ValueError(f"reconciled cluster {cluster.cluster_id} references unknown source cluster ids")
        referenced_ids.update(cluster.member_atom_ids)
        referenced_ids.update(cluster.borderline_atom_ids)
        referenced_ids.update(cluster.excluded_atom_ids)
        for member in cluster.member_roles:
            if member.cluster_id != cluster.cluster_id:
                raise ValueError(f"reconciled cluster member cluster_id mismatch in {cluster.cluster_id}")
            referenced_ids.add(member.atom_id)
    missing = sorted(referenced_ids - valid_atom_ids)
    if missing:
        raise ValueError(
            "reconciled clusters referenced unknown atoms: "
            + ", ".join(missing)
        )


def build_weave_apply_plan(
    *,
    run_id: str,
    mode: str,
    reports: WeaveClusterReportsArtifact,
    actions: WeaveStructuralActionsArtifact,
    critique: WeaveCritiqueArtifact,
) -> ApplyPlan:
    plan_actions: list[ApplyPlanAction] = []
    for report in reports.reports:
        plan_actions.append(
            ApplyPlanAction(
                action_id=f"write-{report.cluster_id}",
                action_type="write_markdown",
                target_path=f"memory/dreams/weave/{report.cluster_id}.md",
                safe_to_apply=mode != "shadow",
                rationale="shadow mode report preview" if mode == "shadow" else "validated cluster report",
            )
        )
    for update in actions.safe_cluster_ref_updates:
        plan_actions.append(
            ApplyPlanAction(
                action_id=f"cluster-ref-{update.cluster_id}",
                action_type="update_frontmatter",
                target_path=f"memory/dreams/weave/{update.cluster_id}.md",
                safe_to_apply=mode != "shadow",
                rationale="safe cluster ref updates from structural action review",
                atom_ids=update.atom_ids,
            )
        )
    for nudge in actions.review_nudges:
        plan_actions.append(
            ApplyPlanAction(
                action_id=nudge.nudge_id,
                action_type="emit_nudge",
                target_path=nudge.target_path,
                safe_to_apply=mode != "shadow",
                rationale=nudge.title,
            )
        )
    notes = [
        f"approved clusters: {len(critique.approved_cluster_ids)}",
        f"review nudges: {len(actions.review_nudges)}",
    ]
    return ApplyPlan(
        run_id=run_id,
        stage="weave",
        mode="shadow" if mode == "shadow" else "write",
        actions=plan_actions,
        notes=notes,
    )


def build_apply_manifest_from_plan(
    *,
    run_id: str,
    mode: str,
    plan: ApplyPlan,
) -> ApplyManifest:
    entries: list[ApplyManifestEntry] = []
    for action in plan.actions:
        status = "skipped" if mode == "shadow" or not action.safe_to_apply else "written"
        entries.append(
            ApplyManifestEntry(
                action_id=action.action_id,
                status=status,
                target_path=action.target_path,
                notes=[action.rationale] if action.rationale else [],
            )
        )
    return ApplyManifest(
        run_id=run_id,
        stage="weave",
        mode="shadow" if mode == "shadow" else "write",
        entries=entries,
        write_count=sum(1 for entry in entries if entry.status == "written"),
        warning_count=sum(len(entry.notes) for entry in entries if entry.status != "written"),
        notes=list(plan.notes),
    )


def render_cluster_report_markdown(
    report: WeaveClusterReport,
    *,
    review_nudges: list[ReviewNudge] | None = None,
) -> str:
    lines = [f"# {report.title}", "", "## Thesis", "", report.thesis, "", "## Why now", "", report.why_now]
    lines.extend(["", "## Members", ""])
    for section in report.member_sections:
        lines.append(f"- [[{section.atom_id}]] ({section.role}) — {section.summary}")
    lines.extend(["", "## Bridges", ""])
    for section in report.bridge_sections:
        lines.append(f"- `{section.bridge_id}` — {section.summary}")
    lines.extend(["", "## Tensions", ""])
    for section in report.tension_sections:
        atom_ref = ", ".join(f"[[{atom_id}]]" for atom_id in section.atom_ids)
        lines.append(f"- {section.summary}" + (f" ({atom_ref})" if atom_ref else ""))
    lines.extend(["", "## Evidence anchors", ""])
    for anchor in report.evidence_anchors:
        lines.append(f"- {anchor}")
    if report.parent_concept_candidates:
        lines.extend(["", "## Parent concept candidates", ""])
        for candidate in report.parent_concept_candidates:
            lines.append(f"- {candidate}")
    if review_nudges:
        lines.extend(["", "## Review nudges", ""])
        for nudge in review_nudges:
            lines.append(f"- {nudge.title}")
    return "\n".join(lines).rstrip() + "\n"


def apply_weave_write_plan(
    *,
    repo_root: Path,
    reports: WeaveClusterReportsArtifact,
    actions: WeaveStructuralActionsArtifact,
    critique: WeaveCritiqueArtifact,
    today: str,
    context: DreamExecutionContext | None = None,
) -> ApplyManifest:
    vault = Vault.load(repo_root)
    wiki_rel = _relative_to_repo(repo_root, vault.wiki)
    report_by_cluster = {report.cluster_id: report for report in reports.reports}
    review_nudges_by_cluster: dict[str, list[ReviewNudge]] = {}
    for nudge in actions.review_nudges:
        for cluster_id in report_by_cluster:
            if cluster_id in nudge.nudge_id or cluster_id in nudge.title or cluster_id in nudge.body:
                review_nudges_by_cluster.setdefault(cluster_id, []).append(nudge)

    entries: list[ApplyManifestEntry] = []
    for report in reports.reports:
        target = vault.wiki / "dreams" / "weave" / f"{report.cluster_id}.md"
        body = render_cluster_report_markdown(
            report,
            review_nudges=review_nudges_by_cluster.get(report.cluster_id),
        )
        write_note_page(
            target,
            page_type="note",
            title=report.title,
            body=body,
            domains=["meta", "dream"],
            extra_frontmatter={
                "origin": "dream.weave.v2",
                "kind": "structural-cluster",
                "cluster_id": report.cluster_id,
                "member_atom_ids": [section.atom_id for section in report.member_sections],
                "last_weaved_at": today,
                "relates_to": [f"[[{section.atom_id}]]" for section in report.member_sections],
                "approved": report.cluster_id in set(critique.approved_cluster_ids),
            },
            force=True,
            context=context,
        )
        entries.append(
            ApplyManifestEntry(
                action_id=f"write-{report.cluster_id}",
                status="written",
                target_path=_relative_to_repo(repo_root, target),
                notes=["validated cluster report"],
            )
        )

    for update in actions.safe_cluster_ref_updates:
        for atom_id in update.atom_ids:
            atom_path = _find_atom_page(repo_root=repo_root, atom_id=atom_id)
            if atom_path is None:
                entries.append(
                    ApplyManifestEntry(
                        action_id=f"cluster-ref-{update.cluster_id}",
                        status="blocked",
                        target_path=f"{wiki_rel}/**/{atom_id}.md",
                        notes=[f"missing atom page for {atom_id}"],
                    )
                )
                continue
            _update_atom_cluster_ref(
                atom_path=atom_path,
                cluster_ref=update.cluster_ref,
                today=today,
            )
        entries.append(
            ApplyManifestEntry(
                action_id=f"cluster-ref-{update.cluster_id}",
                status="written",
                target_path=f"{wiki_rel}/dreams/weave/{update.cluster_id}.md",
                notes=[f"updated {len(update.atom_ids)} atom cluster refs"],
            )
        )

    for nudge in actions.review_nudges:
        target, relative_target = _safe_nudge_target(repo_root=repo_root, nudge=nudge)
        write_note_page(
            target,
            page_type="note",
            title=nudge.title,
            body=nudge.body,
            domains=["meta", "dream"],
            extra_frontmatter={
                "origin": "dream.weave.v2",
                "kind": "review-nudge",
                "last_dream_pass": today,
            },
            force=True,
            context=context,
        )
        entries.append(
            ApplyManifestEntry(
                action_id=nudge.nudge_id,
                status="written",
                target_path=relative_target,
                notes=[nudge.title],
            )
        )

    GraphRegistry.for_repo_root(repo_root).rebuild()
    return ApplyManifest(
        run_id="",
        stage="weave",
        mode="write",
        entries=entries,
        write_count=sum(1 for entry in entries if entry.status == "written"),
        warning_count=sum(1 for entry in entries if entry.status != "written"),
        notes=[
            f"approved clusters: {len(critique.approved_cluster_ids)}",
            f"review nudges: {len(actions.review_nudges)}",
        ],
    )


def _find_atom_page(*, repo_root: Path, atom_id: str) -> Path | None:
    memory_root = Vault.load(repo_root).wiki
    for path in sorted(memory_root.rglob(f"{atom_id}.md")):
        if "/dreams/" in path.as_posix() or "/inbox/" in path.as_posix():
            continue
        return path
    return None


def _strip_weave_cluster_refs(relates_to: list[str]) -> list[str]:
    cleaned: list[str] = []
    for item in relates_to:
        targets = extract_wikilinks(str(item))
        if any(target.startswith("weave-") or target.startswith("window-") for target in targets):
            continue
        cleaned.append(str(item))
    return cleaned


def _coerce_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _update_atom_cluster_ref(
    *,
    atom_path: Path,
    cluster_ref: str,
    today: str,
) -> None:
    frontmatter, body = read_page(atom_path)
    existing_relates = _strip_weave_cluster_refs(_coerce_list(frontmatter.get("relates_to")))
    deduped_relates: list[str] = []
    seen_relates: set[str] = set()
    for item in [*existing_relates, cluster_ref]:
        if item in seen_relates:
            continue
        seen_relates.add(item)
        deduped_relates.append(item)
    frontmatter["relates_to"] = deduped_relates
    frontmatter["weave_cluster_refs"] = [cluster_ref]
    frontmatter["last_weaved_at"] = today
    frontmatter["last_updated"] = today
    frontmatter["last_dream_pass"] = today
    write_page_force(atom_path, frontmatter, body)


def _safe_nudge_target(*, repo_root: Path, nudge: ReviewNudge) -> tuple[Path, str]:
    vault = Vault.load(repo_root)
    raw_target = str(nudge.target_path or "").strip()
    default = vault.wiki / "inbox" / "nudges" / f"{nudge.nudge_id}.md"
    candidate = vault.resolve_logical_path(raw_target) if raw_target else default
    try:
        candidate.resolve().relative_to((vault.wiki / "inbox" / "nudges").resolve())
    except ValueError:
        candidate = default
    if candidate.suffix.lower() != ".md":
        candidate = candidate.with_suffix(".md")
    return candidate, vault.logical_path(candidate)


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    return Vault.load(repo_root).logical_path(path)
