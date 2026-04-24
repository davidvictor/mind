from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.atoms.canonical import RELATION_KINDS, canonicalize_atom_page
from scripts.atoms.synthesis import run_active_synthesis

from .common import DreamExecutionContext, campaign_setting, dream_today, read_page, write_page_force
from .substrate_queries import active_atoms, atom_path


@dataclass(frozen=True)
class ActiveSynthesisSummary:
    eligible_count: int = 0
    synthesized_count: int = 0
    relation_updates: int = 0
    mutations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def run_active_synthesis_pass(
    *,
    v,
    today: str | None = None,
    dry_run: bool,
    context: DreamExecutionContext | None = None,
) -> ActiveSynthesisSummary:
    cfg = v.config.dream.active_synthesis
    if not cfg.enabled:
        return ActiveSynthesisSummary()
    effective_today = today or dream_today(context)
    campaign_cfg = v.config.dream.campaign
    campaign_mode = context is not None and context.mode == "campaign"
    max_atoms_per_run = int(
        campaign_setting(
            context,
            "deep_active_synthesis_max_atoms_per_run",
            campaign_cfg.deep_active_synthesis_max_atoms_per_run if campaign_mode else cfg.max_atoms_per_run,
        )
    )
    cooldown_days = int(
        campaign_setting(
            context,
            "deep_active_synthesis_cooldown_days",
            campaign_cfg.deep_active_synthesis_cooldown_days if campaign_mode else cfg.cooldown_days,
        )
    )
    atoms = active_atoms(v)
    active_lookup = {atom.id: atom for atom in atoms}
    eligible = _eligible_atoms(v=v, atoms=atoms, today=effective_today, cooldown_days=cooldown_days)
    selected = eligible[:max_atoms_per_run]
    mutations: list[str] = []
    warnings: list[str] = []
    synthesized = 0
    relation_updates = 0
    if dry_run:
        for atom in selected:
            mutations.append(f"would synthesize mature {atom.type} {atom.id}")
        return ActiveSynthesisSummary(
            eligible_count=len(selected),
            synthesized_count=len(selected),
            relation_updates=len(selected),
            mutations=mutations,
            warnings=warnings,
        )

    for atom in selected:
        path = atom_path(v, atom)
        frontmatter, body = read_page(path)
        typed_neighbors = _neighbor_context(
            relation_targets=_typed_relation_targets(frontmatter),
            active_lookup=active_lookup,
        )
        generic_neighbors = _neighbor_context(
            relation_targets=_generic_relation_targets(frontmatter),
            active_lookup=active_lookup,
        )
        contradiction_signals = _nudge_signals(v.wiki / "inbox" / "nudges", atom_id=atom.id, kind="contradiction")
        cooccurrence_signals = _nudge_signals(v.wiki / "inbox" / "nudges", atom_id=atom.id, kind="cooccurrence")
        evidence_log = _evidence_lines(body)
        previous_typed = _filter_typed_relations(
            _typed_relation_targets_map(frontmatter),
            atom_id=atom.id,
            valid_targets=set(active_lookup),
        )
        previous_related = set(_generic_relation_targets(frontmatter))
        try:
            result = run_active_synthesis(
                atom_type=atom.type,
                atom_id=atom.id,
                title=str(frontmatter.get("title") or atom.id),
                frontmatter=frontmatter,
                body=body,
                evidence_log=evidence_log,
                typed_neighbors=typed_neighbors,
                generic_neighbors=generic_neighbors,
                contradiction_signals=contradiction_signals,
                cooccurrence_signals=cooccurrence_signals,
            )
        except Exception as exc:
            warnings.append(f"{atom.id}: synthesis skipped ({type(exc).__name__}: {exc})")
            continue

        payload = result.to_payload()
        payload["typed_relations"] = _filter_typed_relations(
            result.typed_relations,
            atom_id=atom.id,
            valid_targets=set(active_lookup),
        )
        if not payload["in_conversation_with"]:
            payload["in_conversation_with"] = _relation_targets(payload["typed_relations"])
        frontmatter["last_synthesized_at"] = effective_today
        frontmatter["synthesis_version"] = int(cfg.synthesis_version)
        frontmatter["last_updated"] = effective_today
        frontmatter["typed_relations"] = payload["typed_relations"]
        frontmatter["relates_to"] = [f"[[{item}]]" for item in payload["in_conversation_with"]]
        rendered = canonicalize_atom_page(
            frontmatter=frontmatter,
            body=body,
            candidate=payload,
            render_mode="mature",
            replace_relations=True,
        )
        write_page_force(path, rendered.frontmatter, rendered.body)
        synthesized += 1
        next_typed = rendered.frontmatter.get("typed_relations") or {}
        next_related = {
            str(item).strip().replace("[[", "").replace("]]", "")
            for item in rendered.frontmatter.get("relates_to") or []
            if str(item).strip()
        }
        if next_typed != previous_typed or next_related != previous_related:
            relation_updates += 1
        mutations.append(f"synthesized mature {atom.type} {atom.id}")

    return ActiveSynthesisSummary(
        eligible_count=len(selected),
        synthesized_count=synthesized,
        relation_updates=relation_updates,
        mutations=mutations,
        warnings=warnings,
    )


def _eligible_atoms(*, v, atoms: list, today: str, cooldown_days: int) -> list:
    thresholds = v.config.dream.active_synthesis.maturity_thresholds
    selected: list = []
    for atom in atoms:
        path = atom_path(v, atom)
        frontmatter, _body = read_page(path)
        if frontmatter.get("seed_managed"):
            continue
        if str(frontmatter.get("lifecycle_state") or "active") in {"dormant", "archived"}:
            continue
        threshold = int(getattr(thresholds, atom.type))
        if int(frontmatter.get("evidence_count") or 0) < threshold:
            continue
        last_synthesized = str(frontmatter.get("last_synthesized_at") or "").strip()
        if last_synthesized and _days_between(last_synthesized, today) < cooldown_days:
            continue
        selected.append(atom)
    selected.sort(key=lambda atom: (-atom.evidence_count, atom.id))
    return selected


def _days_between(left: str, right: str) -> int:
    from datetime import date

    try:
        start = date.fromisoformat(left[:10])
        end = date.fromisoformat(right[:10])
    except Exception:
        return 9999
    return (end - start).days


def _evidence_lines(body: str) -> list[str]:
    if "## Evidence log" not in body:
        return []
    evidence = body.split("## Evidence log", 1)[1]
    return [line.strip() for line in evidence.splitlines() if line.strip().startswith("- ")]


def _typed_relation_targets(frontmatter: dict[str, Any]) -> list[str]:
    return [
        target
        for values in _typed_relation_targets_map(frontmatter).values()
        for target in values
    ]


def _typed_relation_targets_map(frontmatter: dict[str, Any]) -> dict[str, list[str]]:
    relations = frontmatter.get("typed_relations")
    if not isinstance(relations, dict):
        return {}
    targets: dict[str, list[str]] = {}
    for kind in RELATION_KINDS:
        items: list[str] = []
        for item in relations.get(kind) or []:
            cleaned = str(item).strip().replace("[[", "").replace("]]", "")
            if cleaned:
                items.append(cleaned)
        if items:
            targets[kind] = items
    return targets


def _generic_relation_targets(frontmatter: dict[str, Any]) -> list[str]:
    typed_targets = set(_typed_relation_targets(frontmatter))
    targets: list[str] = []
    for item in frontmatter.get("relates_to") or []:
        cleaned = str(item).strip().replace("[[", "").replace("]]", "")
        if cleaned and cleaned not in typed_targets:
            targets.append(cleaned)
    return targets


def _neighbor_context(*, relation_targets: list[str], active_lookup: dict[str, Any]) -> list[dict[str, str]]:
    context: list[dict[str, str]] = []
    for target in relation_targets:
        atom = active_lookup.get(target)
        if atom is None:
            continue
        context.append(
            {
                "atom_id": atom.id,
                "type": atom.type,
                "tldr": atom.tldr,
            }
        )
    return context[:8]


def _nudge_signals(root: Path, *, atom_id: str, kind: str) -> list[str]:
    signals: list[str] = []
    if not root.exists():
        return signals
    for path in sorted(root.rglob(f"*-{kind}-*.md")):
        text = path.read_text(encoding="utf-8")
        if atom_id not in text:
            continue
        lines = [line.strip() for line in text.splitlines() if line.strip().startswith("- ")]
        signals.extend(lines[:3])
    return signals[:8]


def _filter_typed_relations(
    relations: dict[str, list[str]],
    *,
    atom_id: str,
    valid_targets: set[str],
) -> dict[str, list[str]]:
    filtered: dict[str, list[str]] = {}
    for kind in RELATION_KINDS:
        if kind not in relations:
            continue
        seen: set[str] = set()
        items: list[str] = []
        for item in relations.get(kind) or []:
            cleaned = str(item).strip().replace("[[", "").replace("]]", "")
            if not cleaned or cleaned == atom_id or cleaned not in valid_targets or cleaned in seen:
                continue
            items.append(f"[[{cleaned}]]")
            seen.add(cleaned)
        if items:
            filtered[kind] = items
    return filtered


def _relation_targets(relations: dict[str, list[str]]) -> list[str]:
    seen: set[str] = set()
    targets: list[str] = []
    for kind in RELATION_KINDS:
        for item in relations.get(kind) or []:
            cleaned = str(item).strip().replace("[[", "").replace("]]", "")
            if not cleaned or cleaned in seen:
                continue
            targets.append(cleaned)
            seen.add(cleaned)
    return targets
