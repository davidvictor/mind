from __future__ import annotations

from dataclasses import replace
from datetime import date
from itertools import combinations
from pathlib import Path
import re
from typing import Any, Callable

from mind.services.content_policy import working_set_domains
from scripts.atoms import cache as atom_cache
from scripts.atoms.evidence_writer import append_evidence

from .common import (
    DreamExecutionContext,
    DreamPreconditionError,
    DreamResult,
    campaign_setting,
    dream_today,
    ensure_dream_enabled,
    ensure_onboarded,
    extract_wikilinks,
    maybe_locked,
    read_page,
    runtime_state,
    source_pages,
    summary_snippet,
    vault,
    write_note_page,
    write_page_force,
    dream_run,
)
from .quality import (
    blocked_lane_summaries,
    degraded_lane_summaries,
    evaluate_and_persist_quality,
    lane_state_for_frontmatter,
    supports_full_dream_mutation,
)
from .substrate_queries import active_atoms, atom_path, inverse_tail_candidates_from_atoms, probationary_atoms


CONTRADICTION_RE = re.compile(r"\b(however|contradict|tension|but)\b", re.IGNORECASE)
EVIDENCE_SOURCE_RE = re.compile(r"\[\[([^\]]+)\]\]")
LIGHT_PROGRESS_EVENT_EVERY = 50


def _source_in_scope(path: Path, *, last_light: str | None) -> bool:
    if last_light is None:
        return True
    frontmatter, _body = read_page(path)
    updated = str(frontmatter.get("last_updated") or "")
    return not updated or updated >= last_light[:10]


def _source_topics(frontmatter: dict, body: str) -> list[str]:
    topics = set(extract_wikilinks(body))
    for key in ("concepts", "entities", "relates_to"):
        for link in frontmatter.get(key) or []:
            topics.update(extract_wikilinks(str(link)))
    return sorted(topics)


def _source_domains(frontmatter: dict) -> list[str]:
    return working_set_domains(frontmatter)


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def _days_between(left: str, right: str) -> int:
    try:
        start = date.fromisoformat(left[:10])
        end = date.fromisoformat(right[:10])
    except Exception:
        return 999
    return (end - start).days


def _source_mentions_atom(*, body: str, atom_id: str, tldr: str) -> bool:
    normalized = _normalize(body)
    return (
        f"[[{atom_id}]]" in body
        or atom_id.replace("-", " ") in normalized
        or (_normalize(tldr) and _normalize(tldr) in normalized)
    )


def _write_light_nudge(
    *,
    kind: str,
    source_id: str,
    today: str,
    title: str,
    body: str,
    relates_to: list[str],
    sources: list[str] | None = None,
    v,
    extra_frontmatter: dict | None = None,
    context: DreamExecutionContext | None = None,
) -> tuple[str, bool, str]:
    note_id = f"{today}-{kind}-{source_id}"
    nudge_dir = v.wiki / "inbox" / "nudges"
    target = nudge_dir / f"{note_id}.md"
    for candidate in (
        target,
        nudge_dir / ".processed" / target.name,
        nudge_dir / ".dismissed" / target.name,
        nudge_dir / ".escalated" / target.name,
    ):
        if candidate.exists():
            return target.name, False, candidate.relative_to(v.wiki).as_posix()
    frontmatter = {
        "relates_to": relates_to,
        "origin": "dream.light",
        "kind": kind,
        "last_dream_pass": today,
    }
    if extra_frontmatter:
        frontmatter.update(extra_frontmatter)
    write_note_page(
        target,
        page_type="note",
        title=title,
        body=body,
        domains=["meta"],
        sources=sources if sources is not None else [f"[[{source_id}]]"],
        extra_frontmatter=frontmatter,
        force=True,
        context=context,
    )
    return target.name, True, target.relative_to(v.wiki).as_posix()


def _record_cap_miss(*, v, atom, today: str) -> tuple[bool, int]:
    path = atom_path(v, atom)
    frontmatter, body = read_page(path)
    prior_date = str(frontmatter.get("light_last_cap_miss") or "")
    if prior_date and _days_between(prior_date, today) <= 30:
        miss_count = int(frontmatter.get("light_cap_miss_count") or 0) + 1
    else:
        miss_count = 1
    frontmatter["light_last_cap_miss"] = today
    frontmatter["light_cap_miss_count"] = miss_count
    if miss_count >= 3:
        frontmatter["lifecycle_state"] = "dormant"
    frontmatter["last_updated"] = today
    write_page_force(path, frontmatter, body)
    return miss_count >= 3, miss_count


def _reactivate_if_needed(*, v, atom, today: str) -> bool:
    if atom.lifecycle_state != "dormant":
        return False
    path = atom_path(v, atom)
    frontmatter, body = read_page(path)
    frontmatter["lifecycle_state"] = "active"
    frontmatter["light_cap_miss_count"] = 0
    frontmatter["light_last_cap_miss"] = today
    frontmatter["last_updated"] = today
    write_page_force(path, frontmatter, body)
    return True


def _recent_probationary_pairs(*, v, today: str) -> list[tuple[str, str]]:
    recent: list[tuple[str, str, str]] = []
    for atom in probationary_atoms(v):
        path = atom_path(v, atom)
        frontmatter, _body = read_page(path)
        created = str(frontmatter.get("created") or today)
        if _days_between(created, today) > v.config.dream.merge_window_days:
            continue
        fingerprint = _normalize(str(frontmatter.get("title") or atom.id))
        recent.append((atom.type, atom.id, fingerprint))

    pairs: list[tuple[str, str]] = []
    for left, right in combinations(sorted(recent), 2):
        left_type, left_id, left_fp = left
        right_type, right_id, right_fp = right
        if left_type != right_type:
            continue
        if left_fp == right_fp or left_fp in right_fp or right_fp in left_fp:
            pairs.append((left_id, right_id))
    return pairs


def _evidence_sources(path: Path) -> set[str]:
    _frontmatter, body = read_page(path)
    if "## Evidence log" not in body:
        return set()
    evidence = body.split("## Evidence log", 1)[1]
    return {match for match in EVIDENCE_SOURCE_RE.findall(evidence)}


def _replace_atom_snapshot(atoms: list, atom_id: str, **changes) -> list:
    return [replace(atom, **changes) if atom.id == atom_id else atom for atom in atoms]


def run_light(
    *,
    dry_run: bool,
    acquire_lock: bool = True,
    context: DreamExecutionContext | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> DreamResult:
    ensure_dream_enabled()
    ensure_onboarded()
    v = vault()
    today = dream_today(context)
    dream_state = runtime_state().get_dream_state()
    all_candidates = (
        source_pages(v)
        if context and context.mode == "campaign"
        else [path for path in source_pages(v) if _source_in_scope(path, last_light=dream_state.last_light)]
    )
    resume_from_index = max(0, int(context.campaign_resume_from_source_index if context else 0))
    candidates = all_candidates[resume_from_index:]
    working_set_cap = int(
        campaign_setting(context, "light_working_set_cap", v.config.dream.working_set_cap)
    )
    emit_verbose_mutations = bool(campaign_setting(context, "emit_verbose_mutations", True))
    write_operator_nudges = bool(campaign_setting(context, "write_audit_nudges", True))
    apply_cap_miss_lifecycle_changes = bool(campaign_setting(context, "apply_cap_miss_lifecycle_changes", True))
    checkpoint_every_sources = max(1, int(campaign_setting(context, "checkpoint_every_sources", LIGHT_PROGRESS_EVENT_EVERY)))
    mutations: list[str] = []
    warnings: list[str] = []
    evidence_updates = 0
    tail_matches = 0
    merge_nudges = 0
    cooccurrence_nudges = 0
    polarity_audits = 0
    cap_audits = 0
    lifecycle_updates = 0
    trusted_sources_processed = 0
    relation_only_sources_processed = 0
    light_window_sources: set[str] = set()
    quality = evaluate_and_persist_quality(persist=not dry_run, report_key="light")
    blocked_lanes = blocked_lane_summaries(quality)
    degraded_lanes = degraded_lane_summaries(quality)
    if blocked_lanes:
        warnings.extend(f"lane quality blocked: {item}" for item in blocked_lanes)
    if degraded_lanes:
        warnings.extend(f"lane quality degraded: {item}" for item in degraded_lanes)
    skipped_blocked = 0
    skipped_bootstrap = 0
    relaxed_bootstrap_source_atoms: dict[str, set[str]] = {}

    def record_mutation(message: str) -> None:
        if emit_verbose_mutations:
            mutations.append(message)

    with dream_run("light", dry_run=dry_run, context=context) as (runtime, run_id):
        total_candidates = len(all_candidates)
        runtime.add_run_event(run_id, stage="light", event_type="selected", message=f"{total_candidates} source pages")
        if progress_callback is not None and context and context.mode == "campaign":
            progress_callback(
                {
                    "processed_sources": resume_from_index,
                    "total_sources": total_candidates,
                    "last_source_id": None,
                }
            )
        with maybe_locked("light", dry_run=dry_run, acquire_lock=acquire_lock):
            atom_cache.rebuild(v.root)
            active_atom_snapshot = active_atoms(v)
            active_count = max(1, len(active_atom_snapshot))
            for index, path in enumerate(candidates, start=resume_from_index + 1):
                emit_progress = bool(
                    not dry_run
                    and context
                    and context.mode == "campaign"
                    and (index % checkpoint_every_sources == 0 or index == total_candidates)
                )
                current_source_id: str | None = None
                try:
                    frontmatter, body = read_page(path)
                    source_id = str(frontmatter.get("id") or path.stem)
                    current_source_id = source_id
                    lane_state = lane_state_for_frontmatter(frontmatter, quality)
                    if lane_state == "blocked":
                        skipped_blocked += 1
                        record_mutation(f"{source_id}: skipped blocked lane quality")
                        continue
                    allow_relation_only_bootstrap = bool(
                        context
                        and context.mode == "campaign"
                        and context.lane_relaxation_mode == "relation_only"
                        and lane_state == "bootstrap-only"
                    )
                    if lane_state == "bootstrap-only" and not allow_relation_only_bootstrap:
                        skipped_bootstrap += 1
                        record_mutation(f"{source_id}: skipped bootstrap-only lane quality")
                        continue
                    allow_full_mutation = supports_full_dream_mutation(lane_state)
                    allow_relation_nudges = allow_full_mutation or allow_relation_only_bootstrap
                    if allow_full_mutation:
                        trusted_sources_processed += 1
                        light_window_sources.add(source_id)
                    elif allow_relation_only_bootstrap:
                        relation_only_sources_processed += 1
                    snippet = summary_snippet(body, max_chars=v.config.dream.snippet_max_chars)
                    source_topics = _source_topics(frontmatter, body)
                    source_domains = _source_domains(frontmatter)
                    inverse_atoms = (
                        inverse_tail_candidates_from_atoms(
                            atoms=active_atom_snapshot,
                            source_topics=source_topics,
                            source_domains=source_domains,
                            cap=active_count,
                        )
                        if v.config.dream.tail_rescan_enabled
                        else []
                    )
                    tail_candidates = inverse_atoms[:working_set_cap]
                    overflow_atoms = inverse_atoms[working_set_cap:]
                    if overflow_atoms and allow_relation_nudges:
                        cap_audits += 1
                        if dry_run:
                            if write_operator_nudges:
                                record_mutation(f"{source_id}: would write working-set-cap audit")
                        elif write_operator_nudges:
                            name, created, existing_path = _write_light_nudge(
                                kind="working-set-cap-audit",
                                source_id=source_id,
                                today=today,
                                title=f"Working-set cap audit for {source_id}",
                                body=(
                                    f"# Working-set cap audit for {source_id}\n\n"
                                    "## Findings\n\n"
                                    f"- Inverse tail selection exceeded the cap of {working_set_cap}.\n"
                                ),
                                relates_to=[f"[[{source_id}]]"],
                                v=v,
                                context=context,
                            )
                            if created:
                                record_mutation(f"{source_id}: wrote {name}")
                            else:
                                cap_audits -= 1
                                record_mutation(f"{source_id}: skipped existing working-set-cap audit {name} ({existing_path})")
                        if allow_full_mutation and apply_cap_miss_lifecycle_changes:
                            for atom in overflow_atoms:
                                if atom.lifecycle_state == "dormant":
                                    continue
                                demoted, miss_count = _record_cap_miss(v=v, atom=atom, today=today)
                                if demoted:
                                    lifecycle_updates += 1
                                    active_atom_snapshot = _replace_atom_snapshot(
                                        active_atom_snapshot,
                                        atom.id,
                                        lifecycle_state="dormant",
                                    )
                                    record_mutation(f"{atom.id}: set dormant after {miss_count} cap misses")

                    matched_ids: list[str] = []
                    contradiction_signal = bool(CONTRADICTION_RE.search(body))
                    for atom in tail_candidates:
                        if not _source_mentions_atom(body=body, atom_id=atom.id, tldr=atom.tldr):
                            continue
                        polarity = "against" if contradiction_signal and atom.type == "stance" else "for"
                        tail_matches += 1
                        matched_ids.append(atom.id)
                        if not allow_full_mutation:
                            continue
                        if dry_run:
                            record_mutation(f"{source_id}: would tail-rescan append evidence to {atom.type}:{atom.id}")
                            continue
                        appended = append_evidence(
                            atom_id=atom.id,
                            atom_type=atom.type,
                            date=today,
                            dedupe_by_source=bool(context and context.mode == "campaign"),
                            source_link=f"[[{source_id}]]",
                            snippet=snippet or source_id,
                            polarity=polarity,
                            repo_root=v.root,
                        )
                        if appended:
                            evidence_updates += 1
                            active_atom_snapshot = _replace_atom_snapshot(
                                active_atom_snapshot,
                                atom.id,
                                last_evidence_date=today,
                                evidence_count=atom.evidence_count + 1,
                            )
                            record_mutation(f"{source_id}: tail-rescan appended evidence to {atom.type}:{atom.id}")
                        if _reactivate_if_needed(v=v, atom=atom, today=today):
                            lifecycle_updates += 1
                            active_atom_snapshot = _replace_atom_snapshot(
                                active_atom_snapshot,
                                atom.id,
                                lifecycle_state="active",
                            )
                            record_mutation(f"{atom.id}: reactivated after inverse-tail match")

                    if allow_relation_only_bootstrap and matched_ids:
                        relaxed_bootstrap_source_atoms[source_id] = set(matched_ids)

                    if contradiction_signal and matched_ids:
                        if dry_run:
                            polarity_audits += 1
                            if write_operator_nudges:
                                name = f"{today}-polarity-audit-{source_id}.md"
                                nudge_dir = v.wiki / "inbox" / "nudges"
                                existing = None
                                for candidate in (
                                    nudge_dir / name,
                                    nudge_dir / ".processed" / name,
                                    nudge_dir / ".dismissed" / name,
                                    nudge_dir / ".escalated" / name,
                                ):
                                    if candidate.exists():
                                        existing = candidate.relative_to(v.wiki).as_posix()
                                        break
                                if existing is None:
                                    record_mutation(f"{source_id}: would write polarity-audit nudge")
                                else:
                                    polarity_audits -= 1
                                    record_mutation(f"{source_id}: would skip existing polarity-audit nudge {name} ({existing})")
                        elif write_operator_nudges:
                            name, created, existing_path = _write_light_nudge(
                                kind="polarity-audit",
                                source_id=source_id,
                                today=today,
                                title=f"Polarity-audit nudge for {source_id}",
                                body=(
                                    f"# Polarity-audit nudge for {source_id}\n\n"
                                    "## Contradiction pressure\n\n"
                                    + "\n".join(f"- [[{atom_id}]]" for atom_id in matched_ids[:5])
                                ),
                                relates_to=[f"[[{source_id}]]", *[f"[[{atom_id}]]" for atom_id in matched_ids[:5]]],
                                v=v,
                                extra_frontmatter={"source_id": source_id},
                                context=context,
                            )
                            if created:
                                polarity_audits += 1
                                record_mutation(f"{source_id}: wrote {name}")
                            else:
                                record_mutation(f"{source_id}: skipped existing polarity-audit nudge {name} ({existing_path})")
                        else:
                            polarity_audits += 1
                finally:
                    if emit_progress:
                        payload = {
                            "processed_sources": index,
                            "total_sources": total_candidates,
                            "last_source_id": current_source_id,
                        }
                        if progress_callback is not None:
                            progress_callback(payload)
                        runtime.add_run_event(
                            run_id,
                            stage="light",
                            event_type="progress",
                            message=f"processed {index}/{total_candidates} source pages",
                            payload={
                                "processed": index,
                                "total": total_candidates,
                                "last_source_id": current_source_id,
                                "skipped_blocked": skipped_blocked,
                                "skipped_bootstrap": skipped_bootstrap,
                                "tail_matches": tail_matches,
                            },
                        )

            for left_id, right_id in _recent_probationary_pairs(v=v, today=today):
                if dry_run:
                    if write_operator_nudges:
                        name = f"{today}-merge-{left_id}-{right_id}.md"
                        nudge_dir = v.wiki / "inbox" / "nudges"
                        existing = None
                        for candidate in (
                            nudge_dir / name,
                            nudge_dir / ".processed" / name,
                            nudge_dir / ".dismissed" / name,
                            nudge_dir / ".escalated" / name,
                        ):
                            if candidate.exists():
                                existing = candidate.relative_to(v.wiki).as_posix()
                                break
                        if existing is None:
                            merge_nudges += 1
                            record_mutation(f"would write merge-detection nudge for {left_id} and {right_id}")
                        else:
                            record_mutation(f"would skip existing merge-detection nudge {name} ({existing})")
                    else:
                        merge_nudges += 1
                    continue
                if not write_operator_nudges:
                    merge_nudges += 1
                    continue
                name, created, existing_path = _write_light_nudge(
                    kind="merge",
                    source_id=f"{left_id}-{right_id}",
                    today=today,
                    title=f"Merge-detection nudge for {left_id} and {right_id}",
                    body=(
                        f"# Merge-detection nudge for {left_id} and {right_id}\n\n"
                        "## Candidate pairs\n\n"
                        f"- [[{left_id}]] -> [[{right_id}]]\n"
                    ),
                    relates_to=[f"[[{left_id}]]", f"[[{right_id}]]"],
                    sources=[],
                    v=v,
                    extra_frontmatter={"left_atom": left_id, "right_atom": right_id},
                    context=context,
                )
                if created:
                    merge_nudges += 1
                    record_mutation(f"wrote {name}")
                else:
                    record_mutation(f"skipped existing merge-detection nudge {name} ({existing_path})")

            source_to_atoms: dict[str, set[str]] = {}
            for atom in active_atom_snapshot:
                for source_id in _evidence_sources(atom_path(v, atom)) & light_window_sources:
                    source_to_atoms.setdefault(source_id, set()).add(atom.id)
            for source_id, atom_ids in relaxed_bootstrap_source_atoms.items():
                source_to_atoms.setdefault(source_id, set()).update(atom_ids)
            pair_counts: dict[tuple[str, str], int] = {}
            for atom_ids in source_to_atoms.values():
                for left_id, right_id in combinations(sorted(atom_ids), 2):
                    pair_counts[(left_id, right_id)] = pair_counts.get((left_id, right_id), 0) + 1
            for (left_id, right_id), count in sorted(pair_counts.items()):
                if count < v.config.dream.cooccurrence_threshold:
                    continue
                if dry_run:
                    if write_operator_nudges:
                        name = f"{today}-cooccurrence-{left_id}-{right_id}.md"
                        nudge_dir = v.wiki / "inbox" / "nudges"
                        existing = None
                        for candidate in (
                            nudge_dir / name,
                            nudge_dir / ".processed" / name,
                            nudge_dir / ".dismissed" / name,
                            nudge_dir / ".escalated" / name,
                        ):
                            if candidate.exists():
                                existing = candidate.relative_to(v.wiki).as_posix()
                                break
                        if existing is None:
                            cooccurrence_nudges += 1
                            record_mutation(f"would write co-occurrence nudge for {left_id} and {right_id} (count={count})")
                        else:
                            record_mutation(f"would skip existing co-occurrence nudge {name} ({existing})")
                    else:
                        cooccurrence_nudges += 1
                    continue
                if not write_operator_nudges:
                    cooccurrence_nudges += 1
                    continue
                name, created, existing_path = _write_light_nudge(
                    kind="cooccurrence",
                    source_id=f"{left_id}-{right_id}",
                    today=today,
                    title=f"Co-occurrence nudge for {left_id} and {right_id}",
                    body=(
                        f"# Co-occurrence nudge for {left_id} and {right_id}\n\n"
                        "## Aggregated evidence\n\n"
                        f"- [[{left_id}]] and [[{right_id}]] co-appeared in {count} Light-window sources.\n"
                    ),
                    relates_to=[f"[[{left_id}]]", f"[[{right_id}]]"],
                    sources=[],
                    v=v,
                    extra_frontmatter={"left_atom": left_id, "right_atom": right_id, "count": count},
                    context=context,
                )
                if created:
                    cooccurrence_nudges += 1
                    record_mutation(f"wrote {name}")
                else:
                    record_mutation(f"skipped existing co-occurrence nudge {name} ({existing_path})")

            if not dry_run:
                runtime.update_dream_state(
                    last_light=today,
                    light_passes_since_deep=dream_state.light_passes_since_deep + 1,
                    last_skip_reason=None,
                )
            if candidates and (skipped_blocked + skipped_bootstrap) == len(candidates):
                blocked_detail = ", ".join(blocked_lanes or degraded_lanes or ["all canonical lanes below mutation threshold"])
                raise DreamPreconditionError(f"light dream blocked by lane quality: {blocked_detail}")

    summary = (
        f"Light Dream processed {len(candidates) - skipped_blocked - skipped_bootstrap} source pages "
        f"(skipped blocked={skipped_blocked}, bootstrap-only={skipped_bootstrap}), "
        f"trusted-source passes={trusted_sources_processed}, relation-only bootstrap passes={relation_only_sources_processed}, "
        f"{tail_matches} inverse-tail matches, {evidence_updates} evidence appends, "
        f"{merge_nudges} merge {'nudges' if write_operator_nudges else 'signals'}, "
        f"{cooccurrence_nudges} co-occurrence {'nudges' if write_operator_nudges else 'signals'}, "
        f"{polarity_audits} polarity {'audits' if write_operator_nudges else 'signals'}, "
        f"{cap_audits} cap {'audits' if write_operator_nudges else 'signals'}, "
        f"{lifecycle_updates} lifecycle updates."
    )
    return DreamResult(stage="light", dry_run=dry_run, summary=summary, mutations=mutations, warnings=warnings)
