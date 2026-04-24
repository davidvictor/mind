from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mind.services.web_research import build_atom_queries, ingest_web_articles

from .common import DreamExecutionContext, campaign_setting, dream_today, read_page, write_page_force
from .substrate_queries import active_atoms, atom_path


@dataclass(frozen=True)
class ExternalGroundingSummary:
    eligible_count: int = 0
    grounded_count: int = 0
    mutations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def run_external_grounding_pass(
    *,
    v,
    today: str | None = None,
    dry_run: bool,
    context: DreamExecutionContext | None = None,
) -> ExternalGroundingSummary:
    cfg = v.config.dream.external_grounding
    if not cfg.enabled:
        return ExternalGroundingSummary()
    effective_today = today or dream_today(context)
    campaign_cfg = v.config.dream.campaign
    campaign_mode = context is not None and context.mode == "campaign"
    max_atoms_per_run = int(
        campaign_setting(
            context,
            "deep_external_grounding_max_atoms_per_run",
            campaign_cfg.deep_external_grounding_max_atoms_per_run if campaign_mode else cfg.max_atoms_per_run,
        )
    )
    cooldown_days = int(
        campaign_setting(
            context,
            "deep_external_grounding_cooldown_days",
            campaign_cfg.deep_external_grounding_cooldown_days if campaign_mode else cfg.cooldown_days,
        )
    )
    atoms = active_atoms(v)
    selected = _eligible_atoms(v=v, atoms=atoms, today=effective_today, cooldown_days=cooldown_days)[:max_atoms_per_run]
    mutations: list[str] = []
    warnings: list[str] = []
    grounded = 0
    if dry_run:
        for atom in selected:
            mutations.append(f"would ground mature {atom.type} {atom.id}")
        return ExternalGroundingSummary(
            eligible_count=len(selected),
            grounded_count=len(selected),
            mutations=mutations,
            warnings=warnings,
        )

    active_lookup = {atom.id: atom for atom in atoms}
    for atom in selected:
        path = atom_path(v, atom)
        frontmatter, body = read_page(path)
        typed_neighbors = _typed_neighbor_context(frontmatter, active_lookup)
        queries = build_atom_queries(
            title=str(frontmatter.get("title") or atom.id),
            tldr=atom.tldr,
            typed_neighbors=typed_neighbors,
            max_queries=int(cfg.max_queries_per_atom),
        )
        if not queries:
            warnings.append(f"{atom.id}: grounding skipped (no usable queries)")
            continue
        try:
            results = ingest_web_articles(
                repo_root=v.root,
                queries=queries,
                source_label=f"deep-grounding:{atom.id}",
                today=effective_today,
                results_per_query=int(cfg.max_results_per_query),
            )
        except Exception as exc:
            warnings.append(f"{atom.id}: grounding failed ({type(exc).__name__}: {exc})")
            continue
        if not results:
            warnings.append(f"{atom.id}: grounding found no usable sources")
            continue
        refs = list(frontmatter.get("grounding_source_refs") or [])
        seen = set(refs)
        for item in results:
            ref = f"[[{item.article_page_id}]]"
            if ref not in seen:
                refs.append(ref)
                seen.add(ref)
        frontmatter["last_grounded_at"] = effective_today
        frontmatter["grounding_source_refs"] = refs
        frontmatter["last_updated"] = effective_today
        write_page_force(path, frontmatter, body)
        grounded += 1
        mutations.append(f"grounded mature {atom.type} {atom.id} ({len(results)} sources)")

    return ExternalGroundingSummary(
        eligible_count=len(selected),
        grounded_count=grounded,
        mutations=mutations,
        warnings=warnings,
    )


def _eligible_atoms(*, v, atoms: list, today: str, cooldown_days: int) -> list:
    thresholds = v.config.dream.active_synthesis.maturity_thresholds
    cfg = v.config.dream.external_grounding
    selected: list = []
    for atom in atoms:
        path = atom_path(v, atom)
        frontmatter, _body = read_page(path)
        if frontmatter.get("seed_managed"):
            continue
        threshold = max(int(getattr(thresholds, atom.type)), int(cfg.min_evidence_count))
        if int(frontmatter.get("evidence_count") or 0) < threshold:
            continue
        last_evidence = str(frontmatter.get("last_evidence_date") or "").strip()
        if last_evidence and _days_between(last_evidence, today) > int(cfg.freshness_window_days):
            continue
        last_grounded = str(frontmatter.get("last_grounded_at") or "").strip()
        if last_grounded and _days_between(last_grounded, today) < cooldown_days:
            continue
        selected.append(atom)
    selected.sort(key=lambda atom: (-atom.evidence_count, atom.id))
    return selected


def _typed_neighbor_context(frontmatter: dict[str, Any], active_lookup: dict[str, Any]) -> list[dict[str, str]]:
    relations = frontmatter.get("typed_relations")
    if not isinstance(relations, dict):
        return []
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for values in relations.values():
        for item in values or []:
            target = str(item).strip().replace("[[", "").replace("]]", "")
            if not target or target in seen or target not in active_lookup:
                continue
            seen.add(target)
            atom = active_lookup[target]
            rows.append({"atom_id": atom.id, "type": atom.type, "tldr": atom.tldr})
    return rows[:6]


def _days_between(left: str, right: str) -> int:
    from datetime import date

    try:
        start = date.fromisoformat(left[:10])
        end = date.fromisoformat(right[:10])
    except Exception:
        return 9999
    return (end - start).days
