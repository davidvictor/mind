from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
import re

from scripts.atoms.canonical import canonicalize_atom_page, page_payload
from scripts.common.contract import atom_collection_dir, atom_collection_dirs
from scripts.common.section_rewriter import replace_or_insert_section
from mind.services.digest_service import write_digest_snapshot

from mind.dream.active_synthesis import run_active_synthesis_pass
from mind.dream.common import (
    DreamExecutionContext,
    DreamResult,
    campaign_setting,
    dream_today,
    ensure_dream_enabled,
    ensure_onboarded,
    maybe_locked,
    read_page,
    regenerate_index,
    regenerate_open_inquiries,
    runtime_state,
    vault,
    write_page_force,
    dream_run,
)
from mind.dream.external_grounding import run_external_grounding_pass
from mind.dream.quality import evaluate_and_persist_quality, lane_state_for_summary_id, supports_full_dream_mutation
from mind.dream.substrate_queries import active_atoms, atom_path, probationary_atoms

EVIDENCE_SOURCE_RE = re.compile(r"\[\[([^\]]+)\]\]")


def _threshold_for(frontmatter: dict, config) -> tuple[int, int]:
    page_type = str(frontmatter.get("type") or "")
    if page_type == "concept":
        entry = config.atom_promotion.concept
    elif page_type == "playbook":
        entry = config.atom_promotion.playbook
    elif page_type == "stance":
        entry = config.atom_promotion.stance
    else:
        entry = config.atom_promotion.inquiry
    return entry.min_distinct_sources, entry.min_days_observed


def _days_between(created: str, today: str) -> int:
    try:
        start = date.fromisoformat(created[:10])
        end = date.fromisoformat(today[:10])
    except Exception:
        return 0
    return (end - start).days


def _contradiction_action(frontmatter: dict, entry: str) -> str:
    action = str(frontmatter.get("dream_action") or frontmatter.get("resolution") or "").strip().lower()
    if action in {"apply", "dismiss", "escalate"}:
        return action
    lowered = entry.lower()
    if "[dismiss]" in lowered or " dismiss " in f" {lowered} ":
        return "dismiss"
    if "[escalate]" in lowered or " escalate " in f" {lowered} ":
        return "escalate"
    return "apply"


def _archive_target_dir(nudge_dir: Path, *, action: str) -> Path:
    if action == "apply":
        return nudge_dir / ".processed"
    if action == "dismiss":
        return nudge_dir / ".dismissed"
    return nudge_dir / ".escalated"


def _archive_nudge_status(nudge_dir: Path, *, action: str, nudge: Path) -> tuple[str, Path]:
    target_dir = _archive_target_dir(nudge_dir, action=action)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / nudge.name
    if not target.exists():
        return "archive", target
    source_hash = hashlib.sha256(nudge.read_bytes()).hexdigest()
    target_hash = hashlib.sha256(target.read_bytes()).hexdigest()
    if source_hash == target_hash:
        return "reuse", target
    return "conflict", target


def _archive_nudge(nudge_dir: Path, *, action: str, nudge: Path) -> tuple[str, Path]:
    status, target = _archive_nudge_status(nudge_dir, action=action, nudge=nudge)
    if status == "archive":
        nudge.replace(target)
    elif status == "reuse":
        nudge.unlink()
    return status, target


def _find_active_path(v, atom_id: str) -> Path | None:
    for dirname in atom_collection_dirs().values():
        candidate = v.wiki / dirname / f"{atom_id}.md"
        if candidate.exists():
            return candidate
    return None


def _find_probationary_path(v, atom_id: str) -> Path | None:
    for dirname in atom_collection_dirs().values():
        candidate_dir = v.wiki / "inbox" / "probationary" / dirname
        if not candidate_dir.exists():
            continue
        matches = sorted(candidate_dir.glob(f"*-{atom_id}.md"))
        if matches:
            return matches[0]
    return None


def _evidence_sources(body: str) -> set[str]:
    if "## Evidence log" not in body:
        return set()
    evidence = body.split("## Evidence log", 1)[1]
    return {match for match in EVIDENCE_SOURCE_RE.findall(evidence)}


def run_deep(
    *,
    dry_run: bool,
    acquire_lock: bool = True,
    write_digest: bool | None = None,
    update_runtime_state: bool = True,
    context: DreamExecutionContext | None = None,
) -> DreamResult:
    ensure_dream_enabled()
    ensure_onboarded()
    v = vault()
    state = runtime_state()
    dream_state = state.get_dream_state()
    today = dream_today(context)
    effective_write_digest = context.write_digest if context is not None else True
    if write_digest is not None:
        effective_write_digest = write_digest
    promotions = 0
    holds = 0
    merges = 0
    link_applications = 0
    contradiction_applies = 0
    contradiction_dismisses = 0
    contradiction_escalations = 0
    polarity_reviews = 0
    synthesized = 0
    relation_updates = 0
    grounded = 0
    mutations: list[str] = []
    warnings: list[str] = []
    probationaries = probationary_atoms(v)
    probationary_cap = max(0, int(campaign_setting(context, "deep_probationary_cap", 0)))
    if probationary_cap > 0:
        probationaries = probationaries[:probationary_cap]
    progress_every = max(1, int(campaign_setting(context, "deep_progress_every_probationaries", 100)))
    emit_verbose_mutations = bool(campaign_setting(context, "emit_verbose_mutations", True))
    quality = evaluate_and_persist_quality(persist=not dry_run, report_key="deep")

    def record_mutation(message: str) -> None:
        if emit_verbose_mutations:
            mutations.append(message)

    with dream_run("deep", dry_run=dry_run, context=context) as (runtime, run_id):
        runtime.add_run_event(run_id, stage="deep", event_type="selected", message=f"{len(probationaries)} probationary atoms")
        with maybe_locked("deep", dry_run=dry_run, acquire_lock=acquire_lock):
            total_probationaries = len(probationaries)
            for index, atom in enumerate(probationaries, start=1):
                path = atom_path(v, atom)
                frontmatter, body = read_page(path)
                min_sources, min_days = _threshold_for(frontmatter, v.config)
                evidence_count = int(frontmatter.get("evidence_count") or 0)
                observed_days = _days_between(str(frontmatter.get("created") or today), today)
                evidence_sources = _evidence_sources(body)
                trusted_sources = 0
                degraded_sources = 0
                blocked_sources = 0
                for source_id in evidence_sources:
                    lane_state = lane_state_for_summary_id(source_id, quality)
                    if supports_full_dream_mutation(lane_state):
                        trusted_sources += 1
                    elif lane_state == "blocked":
                        blocked_sources += 1
                    else:
                        degraded_sources += 1
                target_dir = atom_collection_dir(str(frontmatter.get("type") or "inquiry"))
                target = v.wiki / target_dir / f"{str(frontmatter.get('id') or path.stem)}.md"
                if evidence_count < min_sources or observed_days < min_days:
                    holds += 1
                    record_mutation(
                        f"{path.name}: hold (sources={evidence_count}/{min_sources}, days={observed_days}/{min_days})"
                    )
                elif evidence_sources and trusted_sources == 0 and (degraded_sources + blocked_sources) > 0:
                    holds += 1
                    record_mutation(
                        f"{path.name}: hold (trusted_sources=0 degraded_sources={degraded_sources} blocked_sources={blocked_sources})"
                    )
                elif dry_run:
                    if target.exists():
                        merges += 1
                        record_mutation(f"would merge {path.name} -> {target.relative_to(v.wiki)}")
                    else:
                        promotions += 1
                        record_mutation(f"would promote {path.name} -> {target.relative_to(v.wiki)}")
                elif target.exists():
                    target_frontmatter, target_body = read_page(target)
                    evidence = body.split("## Evidence log", 1)[-1].strip()
                    if evidence:
                        prior = target_body.split("## Evidence log", 1)[-1].strip() if "## Evidence log" in target_body else ""
                        merged = "\n".join(part for part in [prior, evidence] if part.strip())
                        replace_or_insert_section(
                            file_path=target,
                            section_heading="## Evidence log",
                            new_content=merged,
                        )
                        target_frontmatter, target_body = read_page(target)
                    rendered = canonicalize_atom_page(
                        frontmatter=target_frontmatter,
                        body=target_body,
                        candidate=page_payload(frontmatter, body),
                    )
                    write_page_force(target, rendered.frontmatter, rendered.body)
                    archive_dir = v.wiki / ".archive" / target_dir
                    archive_dir.mkdir(parents=True, exist_ok=True)
                    path.replace(archive_dir / path.name)
                    merges += 1
                    record_mutation(f"merged {path.name} into {target.name}")
                else:
                    frontmatter["lifecycle_state"] = "active"
                    frontmatter["status"] = "active"
                    frontmatter["last_updated"] = today
                    frontmatter["last_dream_pass"] = today
                    target.parent.mkdir(parents=True, exist_ok=True)
                    rendered = canonicalize_atom_page(
                        frontmatter=frontmatter,
                        body=body,
                        force_lifecycle_state="active",
                    )
                    write_page_force(target, rendered.frontmatter, rendered.body)
                    path.unlink()
                    promotions += 1
                    record_mutation(f"promoted {path.name} -> {target.name}")

                if not dry_run and (index % progress_every == 0 or index == total_probationaries):
                    runtime.add_run_event(
                        run_id,
                        stage="deep",
                        event_type="progress",
                        message=f"processed {index}/{total_probationaries} probationary atoms",
                    )

            nudge_dir = v.wiki / "inbox" / "nudges"
            if nudge_dir.exists():
                for nudge in sorted(nudge_dir.glob("*-merge-*.md")):
                    nudge_frontmatter, _nudge_body = read_page(nudge)
                    left_id = str(nudge_frontmatter.get("left_atom") or "")
                    right_id = str(nudge_frontmatter.get("right_atom") or "")
                    left_path = _find_probationary_path(v, left_id)
                    right_path = _find_probationary_path(v, right_id)
                    intended_action = "dismiss" if left_path is None or right_path is None else "apply"
                    archive_status, archive_target = _archive_nudge_status(
                        nudge_dir,
                        action=intended_action,
                        nudge=nudge,
                    )
                    if archive_status == "reuse":
                        if not dry_run:
                            _archive_nudge(nudge_dir, action=intended_action, nudge=nudge)
                        record_mutation(
                            f"reused archived {intended_action} merge nudge {nudge.name} ({archive_target.relative_to(v.wiki)})"
                        )
                        continue
                    if archive_status == "conflict":
                        warnings.append(
                            f"archive collision for {nudge.name}; preserved {archive_target.relative_to(v.wiki)} and left live nudge untouched"
                        )
                        record_mutation(f"skipped merge nudge {nudge.name} due to archive collision")
                        continue
                    if left_path is None or right_path is None:
                        if not dry_run:
                            _archive_nudge(nudge_dir, action="dismiss", nudge=nudge)
                        record_mutation(f"{nudge.name}: merge nudge dismissed")
                        continue
                    left_frontmatter, left_body = read_page(left_path)
                    right_frontmatter, right_body = read_page(right_path)
                    left_score = int(left_frontmatter.get("evidence_count") or 0)
                    right_score = int(right_frontmatter.get("evidence_count") or 0)
                    winner_path, winner_body, loser_path, loser_body = (
                        (left_path, left_body, right_path, right_body)
                        if (left_score, left_id) >= (right_score, right_id)
                        else (right_path, right_body, left_path, left_body)
                    )
                    if dry_run:
                        merges += 1
                        record_mutation(f"would merge probationary nudge {nudge.name}")
                        continue
                    evidence = loser_body.split("## Evidence log", 1)[-1].strip()
                    if evidence:
                        winner_frontmatter, winner_body_latest = read_page(winner_path)
                        prior = winner_body_latest.split("## Evidence log", 1)[-1].strip() if "## Evidence log" in winner_body_latest else ""
                        merged = "\n".join(part for part in [prior, evidence] if part.strip())
                        replace_or_insert_section(
                            file_path=winner_path,
                            section_heading="## Evidence log",
                            new_content=merged,
                        )
                        winner_frontmatter, winner_body_latest = read_page(winner_path)
                        winner_frontmatter["last_updated"] = today
                        rendered = canonicalize_atom_page(
                            frontmatter=winner_frontmatter,
                            body=winner_body_latest,
                            candidate=page_payload(left_frontmatter, left_body)
                            if winner_path == right_path
                            else page_payload(right_frontmatter, right_body),
                        )
                        write_page_force(winner_path, rendered.frontmatter, rendered.body)
                    archive_dir = v.wiki / ".archive" / loser_path.parent.relative_to(v.wiki)
                    archive_dir.mkdir(parents=True, exist_ok=True)
                    loser_path.replace(archive_dir / loser_path.name)
                    merges += 1
                    _archive_nudge(nudge_dir, action="apply", nudge=nudge)
                    record_mutation(f"merged probationary nudge {nudge.name}")

                for nudge in sorted(nudge_dir.glob("*-cooccurrence-*.md")):
                    nudge_frontmatter, _nudge_body = read_page(nudge)
                    left_id = str(nudge_frontmatter.get("left_atom") or "")
                    right_id = str(nudge_frontmatter.get("right_atom") or "")
                    left_path = _find_active_path(v, left_id)
                    right_path = _find_active_path(v, right_id)
                    if left_path is None or right_path is None:
                        if not dry_run:
                            _archive_nudge(nudge_dir, action="dismiss", nudge=nudge)
                        record_mutation(f"{nudge.name}: co-occurrence nudge dismissed")
                        continue
                    left_frontmatter, left_body = read_page(left_path)
                    right_frontmatter, right_body = read_page(right_path)
                    left_relation = f"[[{right_id}]]"
                    right_relation = f"[[{left_id}]]"
                    already_linked = left_relation in list(left_frontmatter.get("relates_to") or []) and right_relation in list(right_frontmatter.get("relates_to") or [])
                    intended_action = "dismiss" if already_linked else "apply"
                    archive_status, archive_target = _archive_nudge_status(
                        nudge_dir,
                        action=intended_action,
                        nudge=nudge,
                    )
                    if archive_status == "reuse":
                        if not dry_run:
                            _archive_nudge(nudge_dir, action=intended_action, nudge=nudge)
                        record_mutation(
                            f"reused archived {intended_action} co-occurrence nudge {nudge.name} ({archive_target.relative_to(v.wiki)})"
                        )
                        continue
                    if archive_status == "conflict":
                        warnings.append(
                            f"archive collision for {nudge.name}; preserved {archive_target.relative_to(v.wiki)} and left live nudge untouched"
                        )
                        record_mutation(f"skipped co-occurrence nudge {nudge.name} due to archive collision")
                        continue
                    if dry_run:
                        record_mutation(f"would {'dismiss' if already_linked else 'apply'} co-occurrence nudge {nudge.name}")
                        if not already_linked:
                            link_applications += 1
                        continue
                    if already_linked:
                        _archive_nudge(nudge_dir, action="dismiss", nudge=nudge)
                        record_mutation(f"dismissed co-occurrence {nudge.name}")
                        continue
                    left_relates = list(left_frontmatter.get("relates_to") or [])
                    right_relates = list(right_frontmatter.get("relates_to") or [])
                    if left_relation not in left_relates:
                        left_relates.append(left_relation)
                        left_frontmatter["relates_to"] = left_relates
                        left_frontmatter["last_updated"] = today
                        write_page_force(left_path, left_frontmatter, left_body)
                    if right_relation not in right_relates:
                        right_relates.append(right_relation)
                        right_frontmatter["relates_to"] = right_relates
                        right_frontmatter["last_updated"] = today
                        write_page_force(right_path, right_frontmatter, right_body)
                    link_applications += 1
                    _archive_nudge(nudge_dir, action="apply", nudge=nudge)
                    record_mutation(f"applied co-occurrence {nudge.name}")

                for nudge in sorted(nudge_dir.glob("*-contradiction-*.md")):
                    nudge_frontmatter, nudge_body = read_page(nudge)
                    atom_id = nudge.stem.split("-contradiction-", 1)[-1]
                    target = None
                    for dirname in atom_collection_dirs().values():
                        candidate = v.wiki / dirname / f"{atom_id}.md"
                        if candidate.exists():
                            target = candidate
                            break
                    if target is None:
                        continue
                    entry = nudge_body.strip().splitlines()[-1]
                    action = _contradiction_action(nudge_frontmatter, entry)
                    archive_status, archive_target = _archive_nudge_status(
                        nudge_dir,
                        action=action,
                        nudge=nudge,
                    )
                    if archive_status == "reuse":
                        if not dry_run:
                            _archive_nudge(nudge_dir, action=action, nudge=nudge)
                        record_mutation(
                            f"reused archived {action} contradiction nudge {nudge.name} ({archive_target.relative_to(v.wiki)})"
                        )
                        continue
                    if archive_status == "conflict":
                        warnings.append(
                            f"archive collision for {nudge.name}; preserved {archive_target.relative_to(v.wiki)} and left live nudge untouched"
                        )
                        record_mutation(f"skipped contradiction nudge {nudge.name} due to archive collision")
                        continue
                    if dry_run:
                        if action == "apply":
                            contradiction_applies += 1
                        elif action == "dismiss":
                            contradiction_dismisses += 1
                        else:
                            contradiction_escalations += 1
                        record_mutation(f"would {action} contradiction nudge {nudge.name}")
                        continue
                    if action == "apply":
                        target_frontmatter, target_body = read_page(target)
                        section = target_body.split("## Contradictions", 1)[-1].strip() if "## Contradictions" in target_body else ""
                        new_section = (section + "\n" + entry).strip() if section else entry
                        replace_or_insert_section(
                            file_path=target,
                            section_heading="## Contradictions",
                            new_content=new_section,
                        )
                        target_frontmatter, target_body = read_page(target)
                        rendered = canonicalize_atom_page(frontmatter=target_frontmatter, body=target_body)
                        write_page_force(target, rendered.frontmatter, rendered.body)
                        contradiction_applies += 1
                    elif action == "dismiss":
                        contradiction_dismisses += 1
                    else:
                        contradiction_escalations += 1
                    _archive_nudge(nudge_dir, action=action, nudge=nudge)
                    record_mutation(f"{action}d contradiction {nudge.name}")

                for nudge in sorted(nudge_dir.glob("*-polarity-audit-*.md")):
                    archive_status, archive_target = _archive_nudge_status(
                        nudge_dir,
                        action="escalate",
                        nudge=nudge,
                    )
                    if archive_status == "reuse":
                        if not dry_run:
                            _archive_nudge(nudge_dir, action="escalate", nudge=nudge)
                        record_mutation(
                            f"reused archived escalate polarity audit {nudge.name} ({archive_target.relative_to(v.wiki)})"
                        )
                        continue
                    if archive_status == "conflict":
                        warnings.append(
                            f"archive collision for {nudge.name}; preserved {archive_target.relative_to(v.wiki)} and left live nudge untouched"
                        )
                        record_mutation(f"skipped polarity audit {nudge.name} due to archive collision")
                        continue
                    if dry_run:
                        polarity_reviews += 1
                        record_mutation(f"would escalate polarity audit {nudge.name}")
                        continue
                    polarity_reviews += 1
                    _archive_nudge(nudge_dir, action="escalate", nudge=nudge)
                    record_mutation(f"escalated polarity audit {nudge.name}")

            synthesis = run_active_synthesis_pass(v=v, today=today, dry_run=dry_run, context=context)
            synthesized += synthesis.synthesized_count
            relation_updates += synthesis.relation_updates
            mutations.extend(synthesis.mutations)
            warnings.extend(synthesis.warnings)

            grounding = run_external_grounding_pass(v=v, today=today, dry_run=dry_run, context=context)
            grounded += grounding.grounded_count
            mutations.extend(grounding.mutations)
            warnings.extend(grounding.warnings)

            if dry_run:
                record_mutation("would regenerate INDEX.md")
                record_mutation("would regenerate open-inquiries.md")
                record_mutation("would write weekly digest" if effective_write_digest else "would write bootstrap checkpoint report")
            else:
                index_path = regenerate_index(v)
                open_inquiries_path = regenerate_open_inquiries(v, context=context)
                if update_runtime_state:
                    runtime.update_dream_state(
                        last_deep=today,
                        light_passes_since_deep=0,
                        deep_passes_since_rem=dream_state.deep_passes_since_rem + 1,
                        last_skip_reason=None,
                    )
                mutations.extend([f"updated {index_path.name}", f"updated {open_inquiries_path.name}"])
                if effective_write_digest:
                    digest_path = write_digest_snapshot(
                        v.root,
                        promotions=promotions,
                        merges=merges,
                        relation_updates=relation_updates + link_applications,
                        contradictions=contradiction_applies,
                        polarity_reviews=polarity_reviews,
                        today=today,
                        context=context,
                    )
                    record_mutation(f"wrote digest {digest_path.name}")
    summary = (
        f"Deep Dream processed {len(probationaries)} probationary atoms, "
        f"{promotions} promotions, {holds} holds, {merges} merges, "
        f"{synthesized} synthesized, {grounded} grounded, "
        f"{relation_updates + link_applications} relationship updates, "
        f"{contradiction_applies} contradiction applies, {contradiction_dismisses} dismissals, "
        f"{polarity_reviews} polarity reviews, "
        f"{contradiction_escalations} escalations."
    )
    return DreamResult(stage="deep", dry_run=dry_run, summary=summary, mutations=mutations, warnings=warnings)
