from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import json
from pathlib import Path
from typing import Callable, Literal

from mind.services.graph_registry import GraphRegistry, GraphRebuildResult
from scripts.atoms import cache as atoms_cache
from scripts.common.default_tags import default_tags
from scripts.common.frontmatter import split_frontmatter
from scripts.common.wiki_writer import _serialize_frontmatter, write_page
from scripts.common.vault import Vault

SeedPreset = Literal["core", "skeleton", "framework"]
PRESET_CHOICES: tuple[SeedPreset, ...] = ("core", "skeleton", "framework")
DEFAULT_PRESET: SeedPreset = "skeleton"
_INDEX_PLACEHOLDER = "# INDEX"
_SEED_VERSION = 1


@dataclass(frozen=True)
class SeedPageSpec:
    relative_path: str
    page_type: str
    title: str
    domains: tuple[str, ...]
    body_builder: Callable[[set[str]], str]
    extra_frontmatter: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class SeedResult:
    preset: SeedPreset
    created_paths: list[Path]
    updated_paths: list[Path]
    skipped_paths: list[Path]
    graph_result: GraphRebuildResult | None
    atom_count: int

    def render(self) -> str:
        lines = [
            "seed:",
            f"- preset={self.preset}",
            f"- created={len(self.created_paths)}",
            f"- updated={len(self.updated_paths)}",
            f"- skipped={len(self.skipped_paths)}",
            f"- atom_count={self.atom_count}",
        ]
        if self.graph_result is not None:
            lines.append(f"- graph_nodes={self.graph_result.node_count}")
            lines.append(f"- graph_documents={self.graph_result.document_count}")
        if self.created_paths:
            lines.append("- created_paths:")
            lines.extend(
                f"  - {path.as_posix()}"
                for path in sorted(self.created_paths, key=lambda item: item.as_posix())
            )
        if self.updated_paths:
            lines.append("- updated_paths:")
            lines.extend(
                f"  - {path.as_posix()}"
                for path in sorted(self.updated_paths, key=lambda item: item.as_posix())
            )
        if self.skipped_paths:
            lines.append("- skipped_paths:")
            lines.extend(
                f"  - {path.as_posix()}"
                for path in sorted(self.skipped_paths, key=lambda item: item.as_posix())
            )
        return "\n".join(lines)


def seed_brain(repo_root: Path, *, preset: SeedPreset = DEFAULT_PRESET) -> SeedResult:
    vault = Vault.load(repo_root)
    selected_specs = _selected_specs(preset)
    created_paths: list[Path] = []
    updated_paths: list[Path] = []
    skipped_paths: list[Path] = []
    included_keys = set(selected_specs)

    index_path = vault.index
    if _should_initialize_index(index_path):
        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(_index_body(included_keys), encoding="utf-8")
        created_paths.append(Path(vault.logical_path(index_path)))
    elif _should_upgrade_index(index_path, preset):
        index_path.write_text(_index_body(included_keys), encoding="utf-8")
        updated_paths.append(Path(vault.logical_path(index_path)))
    else:
        skipped_paths.append(Path(vault.logical_path(index_path)))

    for key in _ordered_keys(preset):
        spec = selected_specs[key]
        target = vault.wiki / spec.relative_path
        if target.exists():
            if _should_upgrade_seed_page(target, key=key, spec=spec, preset=preset):
                _write_seed_page(target, key=key, spec=spec, preset=preset, force=True)
                updated_paths.append(Path(vault.logical_path(target)))
            else:
                skipped_paths.append(Path(vault.logical_path(target)))
            continue
        _write_seed_page(target, key=key, spec=spec, preset=preset, force=False)
        created_paths.append(Path(vault.logical_path(target)))

    graph_result: GraphRebuildResult | None = None
    mutated = bool(created_paths or updated_paths)
    atom_state = atoms_cache.rebuild(repo_root) if mutated else _read_atom_state(vault)
    atom_count = int((atom_state.get("atoms") or {}).get("count") or 0)
    if mutated:
        graph_result = GraphRegistry.for_repo_root(repo_root).rebuild()

    return SeedResult(
        preset=preset,
        created_paths=created_paths,
        updated_paths=updated_paths,
        skipped_paths=skipped_paths,
        graph_result=graph_result,
        atom_count=atom_count,
    )


def _today() -> str:
    return date.today().isoformat()


def _should_initialize_index(path: Path) -> bool:
    if not path.exists():
        return True
    text = path.read_text(encoding="utf-8").strip()
    return text in {"", _INDEX_PLACEHOLDER}


def _frontmatter_for(target: Path, spec: SeedPageSpec) -> dict[str, object]:
    return _frontmatter_for_preset(
        target,
        spec,
        preset=DEFAULT_PRESET,
        key=target.stem,
        created=_today(),
        last_updated=_today(),
    )


def _frontmatter_for_preset(
    target: Path,
    spec: SeedPageSpec,
    *,
    preset: SeedPreset,
    key: str,
    created: str,
    last_updated: str,
) -> dict[str, object]:
    frontmatter: dict[str, object] = {
        "id": target.stem,
        "type": spec.page_type,
        "title": spec.title,
        "status": "active",
        "created": created,
        "last_updated": last_updated,
        "aliases": [],
        "tags": default_tags(spec.page_type),
        "domains": list(spec.domains),
        "relates_to": [],
        "sources": [],
        "seed_managed": True,
        "seed_key": key,
        "seed_preset": preset,
        "seed_version": _SEED_VERSION,
    }
    frontmatter.update(spec.extra_frontmatter)
    return frontmatter


def _write_seed_page(
    target: Path,
    *,
    key: str,
    spec: SeedPageSpec,
    preset: SeedPreset,
    force: bool,
) -> None:
    if target.exists():
        frontmatter, _body = split_frontmatter(target.read_text(encoding="utf-8"))
        created = str(frontmatter.get("created") or _today())
    else:
        created = _today()
    frontmatter = _frontmatter_for_preset(
        target,
        spec,
        preset=preset,
        key=key,
        created=created,
        last_updated=_today(),
    )
    body = spec.body_builder(set(_selected_specs(preset)))
    write_page(target, frontmatter=frontmatter, body=body, force=force)


def _read_atom_state(vault: Vault) -> dict[str, object]:
    if not vault.brain_state.exists():
        return {}
    try:
        loaded = json.loads(vault.brain_state.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _render_page_text(
    target: Path,
    *,
    key: str,
    spec: SeedPageSpec,
    preset: SeedPreset,
    created: str,
    last_updated: str,
    legacy: bool,
) -> str:
    frontmatter = _frontmatter_for_preset(
        target,
        spec,
        preset=preset,
        key=key,
        created=created,
        last_updated=last_updated,
    )
    if legacy:
        frontmatter.pop("seed_managed", None)
        frontmatter.pop("seed_key", None)
        frontmatter.pop("seed_preset", None)
        frontmatter.pop("seed_version", None)
    body = spec.body_builder(set(_selected_specs(preset))).rstrip() + "\n"
    return f"{_serialize_frontmatter(frontmatter)}\n\n{body}"


def _should_upgrade_seed_page(target: Path, *, key: str, spec: SeedPageSpec, preset: SeedPreset) -> bool:
    current = target.read_text(encoding="utf-8")
    frontmatter, _body = split_frontmatter(current)
    created = str(frontmatter.get("created") or _today())
    last_updated = str(frontmatter.get("last_updated") or created)
    desired = _render_page_text(
        target,
        key=key,
        spec=spec,
        preset=preset,
        created=created,
        last_updated=_today(),
        legacy=False,
    )
    if current == desired:
        return False
    for candidate_preset in PRESET_CHOICES:
        if not _preset_includes_key(candidate_preset, key):
            continue
        if _preset_rank(candidate_preset) > _preset_rank(preset):
            continue
        for legacy in (False, True):
            candidate = _render_page_text(
                target,
                key=key,
                spec=spec,
                preset=candidate_preset,
                created=created,
                last_updated=last_updated,
                legacy=legacy,
            )
            if current == candidate:
                return True
    return False


def _should_upgrade_index(path: Path, preset: SeedPreset) -> bool:
    current = path.read_text(encoding="utf-8")
    desired = _index_body(set(_selected_specs(preset)))
    if current == desired:
        return False
    for candidate_preset in PRESET_CHOICES:
        if _preset_rank(candidate_preset) > _preset_rank(preset):
            continue
        if current == _index_body(set(_selected_specs(candidate_preset))):
            return True
    return False


def _preset_rank(preset: SeedPreset) -> int:
    return PRESET_CHOICES.index(preset)


def _preset_includes_key(preset: SeedPreset, key: str) -> bool:
    return key in _selected_specs(preset)


def _ordered_keys(preset: SeedPreset) -> list[str]:
    keys = list(_CORE_KEYS)
    if preset in {"skeleton", "framework"}:
        keys.extend(_SKELETON_KEYS)
    if preset == "framework":
        keys.extend(_FRAMEWORK_KEYS)
    return keys


def _selected_specs(preset: SeedPreset) -> dict[str, SeedPageSpec]:
    return {key: _PAGE_SPECS[key] for key in _ordered_keys(preset)}


def _link(page_id: str) -> str:
    return f"[[{page_id}]]"


def _index_body(included: set[str]) -> str:
    lines = [
        "# INDEX",
        "",
        "## Starter Graph",
        "",
        f"- {_link('brain-structure')}",
        f"- {_link('profile')}",
        f"- {_link('values')}",
        f"- {_link('positioning')}",
        f"- {_link('open-inquiries')}",
        "",
        "## Hubs",
        "",
        f"- {_link('concepts')}",
        f"- {_link('playbooks')}",
        f"- {_link('stances')}",
        f"- {_link('inquiries')}",
        f"- {_link('projects')}",
        f"- {_link('people')}",
        f"- {_link('sources')}",
    ]
    optional = [
        "brain",
        "graph-conventions",
        "inbox-intake-flow",
        "source-to-atom-promotion",
        "starter-graph",
        "local-first-knowledge-should-stay-file-first",
        "how-should-the-system-evolve",
        "channels",
        "companies",
        "decision-log",
        "review-conventions",
        "current-focus",
        "relationship-map",
        "books-lane",
        "articles-lane",
        "videos-lane",
        "podcasts-lane",
        "web-discovery-lane",
        "contradiction-review",
        "page-family-semantics",
    ]
    visible = [item for item in optional if item in included]
    if visible:
        lines.extend(["", "## Seeded Pages", ""])
        lines.extend(f"- {_link(item)}" for item in visible)
    return "\n".join(lines).rstrip() + "\n"


def _profile_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Owner Profile",
            "",
            "This page is the lightweight identity anchor for the active Brain vault.",
            "",
            "## Snapshot",
            "",
            f"- Use {_link('values')}, {_link('positioning')}, and {_link('open-inquiries')} to keep owner context connected.",
            f"- Treat {_link('brain-structure')} as the map for where new material should attach.",
        ]
    )


def _values_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Values",
            "",
            "## Operating Principles",
            "",
            "- Keep the system legible and file-first.",
            "- Let evidence reshape structure over time.",
            f"- Use {_link('brain-structure')} and {_link('positioning')} as coordination anchors.",
        ]
    )


def _positioning_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Positioning",
            "",
            "## Positioning Narrative",
            "",
            "This vault helps its owner build a durable, local-first knowledge system around real work.",
            "",
            "## Work Priorities",
            "",
            f"- Keep the graph understandable as {_link('projects')}, {_link('sources')}, and {_link('concepts')} expand.",
            "",
            "## Life Priorities",
            "",
            "- Keep the system usable enough to maintain in ordinary life.",
            "",
            "## Constraints",
            "",
            "- Prefer structure that stays inspectable without hiding the raw record.",
        ]
    )


def _open_inquiries_body(included: set[str]) -> str:
    active = (
        f"- {_link('how-should-the-system-evolve')}"
        if "how-should-the-system-evolve" in included
        else f"- What should this Brain learn to connect next through {_link('inquiries')}?"
    )
    return "\n".join(
        [
            "# Open Inquiries",
            "",
            "## Active Inquiries",
            "",
            active,
            f"- Keep unresolved questions legible from {_link('profile')} and {_link('positioning')}.",
        ]
    )


def _brain_structure_body(included: set[str]) -> str:
    lines = [
        "# Brain Structure",
        "",
        "This decision keeps the starter graph intentionally small and connected.",
        f"The core spine links {_link('profile')}, {_link('values')}, {_link('positioning')}, {_link('open-inquiries')}, {_link('concepts')}, {_link('playbooks')}, {_link('stances')}, {_link('inquiries')}, {_link('projects')}, {_link('people')}, and {_link('sources')}.",
    ]
    if "channels" in included and "companies" in included:
        lines.append(
            f"The framework preset also opens reference lanes through {_link('channels')} and {_link('companies')}."
        )
    lines.append("Grow the graph by attaching new material to these hubs before inventing more structure.")
    return "\n".join(lines)


def _concepts_hub_body(included: set[str]) -> str:
    lines = [
        "# Concepts",
        "",
        "Concept pages hold ideas that should compound across sources and projects.",
        f"Route new concepts through {_link('brain-structure')} so they stay attached to the active system map.",
    ]
    if "starter-graph" in included:
        lines.append(f"Current starter concept: {_link('starter-graph')}.")
    return "\n".join(lines)


def _playbooks_hub_body(included: set[str]) -> str:
    lines = [
        "# Playbooks",
        "",
        "Playbooks capture repeatable operating loops for maintaining this vault.",
        f"Use this hub to connect maintenance routines back to {_link('projects')} and {_link('brain-structure')}.",
    ]
    starter = [
        item
        for item in ("inbox-intake-flow", "source-to-atom-promotion", "contradiction-review")
        if item in included
    ]
    if starter:
        lines.append("Starter playbooks: " + ", ".join(_link(item) for item in starter) + ".")
    return "\n".join(lines)


def _stances_hub_body(included: set[str]) -> str:
    lines = [
        "# Stances",
        "",
        "Stances keep operating beliefs explicit enough to revisit as evidence changes.",
        f"Anchor new stances to {_link('concepts')} and {_link('projects')} instead of leaving them implicit.",
    ]
    if "local-first-knowledge-should-stay-file-first" in included:
        lines.append(
            "Current starter stance: "
            + _link("local-first-knowledge-should-stay-file-first")
            + "."
        )
    return "\n".join(lines)


def _inquiries_hub_body(included: set[str]) -> str:
    lines = [
        "# Inquiries",
        "",
        "Inquiry pages keep open questions visible while the rest of the graph evolves.",
        f"Link active questions from {_link('open-inquiries')} so they stay near owner context.",
    ]
    if "how-should-the-system-evolve" in included:
        lines.append(f"Current starter inquiry: {_link('how-should-the-system-evolve')}.")
    return "\n".join(lines)


def _projects_hub_body(included: set[str]) -> str:
    lines = [
        "# Projects",
        "",
        "Project pages gather active work, decisions, and source material into one durable lane.",
        f"Use this hub to connect work back to {_link('people')}, {_link('sources')}, and {_link('playbooks')}.",
    ]
    if "brain" in included:
        lines.append(f"Current starter project: {_link('brain')}.")
    if "current-focus" in included:
        lines.append(f"Active work hub: {_link('current-focus')}.")
    return "\n".join(lines)


def _people_hub_body(included: set[str]) -> str:
    lines = [
        "# People",
        "",
        f"People pages track the owner in {_link('profile')} and other durable relationships that shape the graph.",
        f"Use this hub to connect collaborators, creators, and references back to {_link('projects')}.",
    ]
    if "relationship-map" in included:
        lines.append(f"Relationship routing lives in {_link('relationship-map')}.")
    return "\n".join(lines)


def _sources_hub_body(included: set[str]) -> str:
    lines = [
        "# Sources",
        "",
        "Source pages preserve the raw evidence that later feeds concepts, playbooks, stances, and project updates.",
        f"Use this hub to route new material toward the active hubs in {_link('projects')}.",
    ]
    lanes = [
        item
        for item in ("books-lane", "articles-lane", "videos-lane", "podcasts-lane", "web-discovery-lane")
        if item in included
    ]
    if lanes:
        lines.append("Lane hubs: " + ", ".join(_link(item) for item in lanes) + ".")
    return "\n".join(lines)


def _summaries_hub_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Summaries",
            "",
            "Summary pages are the synthetic/operator layer that points back to source evidence.",
            f"Use this hub to keep summary outputs legible before they propagate into {_link('concepts')} or {_link('playbooks')}.",
        ]
    )


def _brain_project_body(included: set[str]) -> str:
    lines = [
        "# Brain",
        "",
        "Brain is the active project that maintains this vault as a durable markdown knowledge graph.",
        f"It depends on {_link('brain-structure')}, {_link('graph-conventions')}, {_link('inbox-intake-flow')}, and {_link('source-to-atom-promotion')} to keep new material attached cleanly.",
        "",
        "## Status",
        "",
        "- This starter graph is the current working baseline.",
        "",
        "## Open Questions",
        "",
    ]
    if "how-should-the-system-evolve" in included:
        lines.append(f"- See {_link('how-should-the-system-evolve')}.")
    else:
        lines.append("- Expand structure only when the next routing decision becomes unclear.")
    return "\n".join(lines)


def _graph_conventions_body(included: set[str]) -> str:
    lines = [
        "# Graph Conventions",
        "",
        f"Use hub pages like {_link('concepts')}, {_link('playbooks')}, {_link('projects')}, and {_link('sources')} before creating finer structure.",
        f"Prefer explicit wiki-links so new material stays discoverable from {_link('brain-structure')} and {_link('profile')}.",
    ]
    if "page-family-semantics" in included:
        lines.append(f"See {_link('page-family-semantics')} for the page-family intent map.")
    return "\n".join(lines)


def _playbook_body(title: str, summary: str, steps: list[str], related: list[str]) -> str:
    lines = [
        f"# {title}",
        "",
        "## TL;DR",
        "",
        summary,
        "",
        "## Steps",
        "",
    ]
    lines.extend(f"- {step}" for step in steps)
    lines.extend(["", "## Evidence log", "", "", "## Related Pages", ""])
    lines.extend(f"- {_link(item)}" for item in related)
    return "\n".join(lines)


def _inbox_intake_flow_body(_included: set[str]) -> str:
    return _playbook_body(
        "Inbox Intake Flow",
        "Use the dropbox as the supervised entry lane for new material before it becomes durable knowledge.",
        [
            f"Sweep new files through {_link('sources')} and the inbox rules in {_link('brain-structure')}.",
            f"Move review-heavy items toward {_link('people')} or {_link('projects')} only after they resolve cleanly.",
            f"Let confirmed material propagate into canonical project, concept, playbook, stance, or inquiry pages.",
        ],
        ["playbooks", "brain", "sources"],
    )


def _source_to_atom_promotion_body(_included: set[str]) -> str:
    return _playbook_body(
        "Source to Atom Promotion",
        "Promote durable ideas only after the source layer and canonical pages are legible.",
        [
            f"Keep raw evidence attached through {_link('sources')}.",
            f"Promote repeated ideas into {_link('concepts')}, {_link('playbooks')}, {_link('stances')}, or {_link('inquiries')}.",
            f"Use {_link('graph-conventions')} when a routing choice is ambiguous.",
        ],
        ["playbooks", "sources", "concepts", "stances", "inquiries"],
    )


def _starter_graph_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Starter Graph",
            "",
            "A starter graph is the minimum connected structure that gives new knowledge a clear place to land.",
            "",
            "## TL;DR",
            "",
            "A starter graph is the minimum connected structure that gives new knowledge a clear place to land.",
            "",
            "## Evidence log",
            "",
            "",
            "## Why It Matters Here",
            "",
            f"It keeps {_link('brain')} usable before the vault fills with real sources and decisions.",
            "",
            "## Related Concepts",
            "",
            f"- {_link('brain-structure')}",
            f"- {_link('graph-conventions')}",
            f"- {_link('concepts')}",
        ]
    )


def _local_first_stance_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Local-First Knowledge Should Stay File-First",
            "",
            "The durable record should remain visible in files even as automation and agents help maintain it.",
            "",
            "## TL;DR",
            "",
            "The durable record should remain visible in files even as automation and agents help maintain it.",
            "",
            "## Evidence log",
            "",
            "",
            "## Contradictions",
            "",
            "- None observed yet.",
            "",
            "## Why This Matters",
            "",
            f"It keeps {_link('brain')} inspectable and reduces the chance that structure hides the source of truth.",
            "",
            "## Related Pages",
            "",
            f"- {_link('starter-graph')}",
            f"- {_link('projects')}",
            f"- {_link('sources')}",
        ]
    )


def _how_system_evolve_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# How Should the System Evolve",
            "",
            "Grow the system only when the next piece of incoming material no longer fits cleanly on the current backbone.",
            "",
            "## TL;DR",
            "",
            "Grow the system only when the next piece of incoming material no longer fits cleanly on the current backbone.",
            "",
            "## Evidence log",
            "",
            "",
            "## What Would Resolve It",
            "",
            f"- Clear evidence from {_link('brain')} that one of the current hubs is overloaded.",
        ]
    )


def _channels_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Channels",
            "",
            "Use this hub when repeated creators or feeds deserve their own durable routing surface.",
            f"Channels should connect source lanes back to {_link('sources')} and relationship work in {_link('people')}.",
        ]
    )


def _companies_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Companies",
            "",
            "Use this hub for organizations that matter across projects, sources, or long-running decisions.",
            f"Connect company pages back to {_link('projects')} and {_link('people')} before expanding the taxonomy.",
        ]
    )


def _decision_log_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Decision Log",
            "",
            f"Use this page to keep high-level decisions discoverable alongside {_link('brain-structure')} and {_link('graph-conventions')}.",
            "New decisions should stay short, link-rich, and easy to revisit.",
        ]
    )


def _review_conventions_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Review Conventions",
            "",
            f"Use this page to describe how inbox review should escalate into {_link('people')}, {_link('projects')}, or {_link('sources')}.",
            "Keep review conventions simple enough to apply during normal intake work.",
        ]
    )


def _current_focus_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Current Focus",
            "",
            f"Use this hub to keep the active work lane visible inside {_link('brain')} without crowding the long-term project page.",
            f"Link short-horizon priorities back to {_link('positioning')} and the supporting playbooks.",
        ]
    )


def _relationship_map_body(included: set[str]) -> str:
    lines = [
        "# Relationship Map",
        "",
        f"Use this page to connect durable people, channels, and companies back to {_link('projects')} and {_link('sources')}.",
    ]
    if "channels" in included and "companies" in included:
        lines.append(f"Reference hubs: {_link('channels')} and {_link('companies')}.")
    return "\n".join(lines)


def _lane_body(title: str, lane_name: str) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            f"Use this page when the {lane_name} lane needs routing notes, conventions, or repeated source references.",
            f"Keep it connected to {_link('sources')} so evidence stays traceable.",
        ]
    )


def _contradiction_review_body(_included: set[str]) -> str:
    return _playbook_body(
        "Contradiction Review",
        "Resolve conflicting evidence by preserving the tension first and deciding later.",
        [
            f"Record the conflict where it touches {_link('concepts')}, {_link('stances')}, or {_link('inquiries')}.",
            f"Link the disagreement back to {_link('sources')} before simplifying it.",
            f"Escalate durable tension into {_link('decision-log')} when it starts shaping project direction.",
        ],
        ["playbooks", "sources", "decision-log"],
    )


def _page_family_semantics_body(_included: set[str]) -> str:
    return "\n".join(
        [
            "# Page Family Semantics",
            "",
            f"Use {_link('sources')} for evidence and the atom hubs for durable abstractions.",
            f"Treat {_link('projects')} and {_link('people')} as coordination surfaces, not dumping grounds for every thought.",
        ]
    )


_CORE_KEYS: tuple[str, ...] = (
    "brain-structure",
    "profile",
    "values",
    "positioning",
    "open-inquiries",
    "concepts",
    "playbooks",
    "stances",
    "inquiries",
    "projects",
    "people",
    "sources",
)

_SKELETON_KEYS: tuple[str, ...] = (
    "inbox-intake-flow",
    "source-to-atom-promotion",
    "graph-conventions",
    "brain",
    "how-should-the-system-evolve",
    "starter-graph",
    "local-first-knowledge-should-stay-file-first",
)

_FRAMEWORK_KEYS: tuple[str, ...] = (
    "channels",
    "companies",
    "decision-log",
    "review-conventions",
    "current-focus",
    "relationship-map",
    "books-lane",
    "articles-lane",
    "videos-lane",
    "podcasts-lane",
    "web-discovery-lane",
    "contradiction-review",
    "page-family-semantics",
)

_PAGE_SPECS: dict[str, SeedPageSpec] = {
    "brain-structure": SeedPageSpec(
        relative_path="decisions/brain-structure.md",
        page_type="decision",
        title="Brain structure",
        domains=("work",),
        body_builder=_brain_structure_body,
    ),
    "profile": SeedPageSpec(
        relative_path="me/profile.md",
        page_type="profile",
        title="Owner Profile",
        domains=("identity", "work"),
        body_builder=_profile_body,
        extra_frontmatter={"role": "", "location": ""},
    ),
    "values": SeedPageSpec(
        relative_path="me/values.md",
        page_type="note",
        title="Values",
        domains=("identity",),
        body_builder=_values_body,
    ),
    "positioning": SeedPageSpec(
        relative_path="me/positioning.md",
        page_type="note",
        title="Positioning",
        domains=("work",),
        body_builder=_positioning_body,
    ),
    "open-inquiries": SeedPageSpec(
        relative_path="me/open-inquiries.md",
        page_type="note",
        title="Open Inquiries",
        domains=("meta",),
        body_builder=_open_inquiries_body,
    ),
    "concepts": SeedPageSpec(
        relative_path="concepts/concepts.md",
        page_type="note",
        title="Concepts",
        domains=("meta",),
        body_builder=_concepts_hub_body,
    ),
    "playbooks": SeedPageSpec(
        relative_path="playbooks/playbooks.md",
        page_type="note",
        title="Playbooks",
        domains=("meta",),
        body_builder=_playbooks_hub_body,
    ),
    "stances": SeedPageSpec(
        relative_path="stances/stances.md",
        page_type="note",
        title="Stances",
        domains=("meta",),
        body_builder=_stances_hub_body,
    ),
    "inquiries": SeedPageSpec(
        relative_path="inquiries/inquiries.md",
        page_type="note",
        title="Inquiries",
        domains=("meta",),
        body_builder=_inquiries_hub_body,
    ),
    "projects": SeedPageSpec(
        relative_path="projects/projects.md",
        page_type="note",
        title="Projects",
        domains=("work",),
        body_builder=_projects_hub_body,
    ),
    "people": SeedPageSpec(
        relative_path="people/people.md",
        page_type="note",
        title="People",
        domains=("relationships",),
        body_builder=_people_hub_body,
    ),
    "sources": SeedPageSpec(
        relative_path="sources/sources.md",
        page_type="note",
        title="Sources",
        domains=("learning",),
        body_builder=_sources_hub_body,
    ),
    "inbox-intake-flow": SeedPageSpec(
        relative_path="playbooks/inbox-intake-flow.md",
        page_type="playbook",
        title="Inbox Intake Flow",
        domains=("meta", "work"),
        body_builder=_inbox_intake_flow_body,
    ),
    "source-to-atom-promotion": SeedPageSpec(
        relative_path="playbooks/source-to-atom-promotion.md",
        page_type="playbook",
        title="Source to Atom Promotion",
        domains=("meta", "learning"),
        body_builder=_source_to_atom_promotion_body,
    ),
    "graph-conventions": SeedPageSpec(
        relative_path="decisions/graph-conventions.md",
        page_type="decision",
        title="Graph conventions",
        domains=("work",),
        body_builder=_graph_conventions_body,
    ),
    "brain": SeedPageSpec(
        relative_path="projects/brain.md",
        page_type="project",
        title="Brain",
        domains=("work", "meta"),
        body_builder=_brain_project_body,
        extra_frontmatter={
            "started": "",
            "ended": "",
            "priority": None,
            "people": ["[[profile]]"],
            "concepts": ["[[starter-graph]]"],
        },
    ),
    "how-should-the-system-evolve": SeedPageSpec(
        relative_path="inquiries/how-should-the-system-evolve.md",
        page_type="inquiry",
        title="How should the system evolve",
        domains=("meta", "work"),
        body_builder=_how_system_evolve_body,
        extra_frontmatter={
            "question": "What additional structure would make the graph more useful without making it harder to operate?",
            "origin": "seed",
            "resolution": "",
            "sources_pro": [],
            "sources_con": [],
        },
    ),
    "starter-graph": SeedPageSpec(
        relative_path="concepts/starter-graph.md",
        page_type="concept",
        title="Starter Graph",
        domains=("meta", "work"),
        body_builder=_starter_graph_body,
        extra_frontmatter={"category": "starter", "first_encountered": ""},
    ),
    "local-first-knowledge-should-stay-file-first": SeedPageSpec(
        relative_path="stances/local-first-knowledge-should-stay-file-first.md",
        page_type="stance",
        title="Local-first knowledge should stay file-first",
        domains=("meta", "work"),
        body_builder=_local_first_stance_body,
        extra_frontmatter={"position": "Keep the durable record visible in files.", "confidence": "starter"},
    ),
    "channels": SeedPageSpec(
        relative_path="channels/channels.md",
        page_type="note",
        title="Channels",
        domains=("learning",),
        body_builder=_channels_body,
    ),
    "companies": SeedPageSpec(
        relative_path="companies/companies.md",
        page_type="note",
        title="Companies",
        domains=("work",),
        body_builder=_companies_body,
    ),
    "decision-log": SeedPageSpec(
        relative_path="decisions/decision-log.md",
        page_type="note",
        title="Decision Log",
        domains=("work",),
        body_builder=_decision_log_body,
    ),
    "review-conventions": SeedPageSpec(
        relative_path="inbox/review-conventions.md",
        page_type="note",
        title="Review Conventions",
        domains=("meta", "work"),
        body_builder=_review_conventions_body,
    ),
    "current-focus": SeedPageSpec(
        relative_path="projects/current-focus.md",
        page_type="note",
        title="Current Focus",
        domains=("work",),
        body_builder=_current_focus_body,
    ),
    "relationship-map": SeedPageSpec(
        relative_path="people/relationship-map.md",
        page_type="note",
        title="Relationship Map",
        domains=("relationships",),
        body_builder=_relationship_map_body,
    ),
    "books-lane": SeedPageSpec(
        relative_path="sources/books-lane.md",
        page_type="note",
        title="Books Lane",
        domains=("learning",),
        body_builder=lambda _included: _lane_body("Books Lane", "books"),
    ),
    "articles-lane": SeedPageSpec(
        relative_path="sources/articles-lane.md",
        page_type="note",
        title="Articles Lane",
        domains=("learning",),
        body_builder=lambda _included: _lane_body("Articles Lane", "articles"),
    ),
    "videos-lane": SeedPageSpec(
        relative_path="sources/videos-lane.md",
        page_type="note",
        title="Videos Lane",
        domains=("learning",),
        body_builder=lambda _included: _lane_body("Videos Lane", "video"),
    ),
    "podcasts-lane": SeedPageSpec(
        relative_path="sources/podcasts-lane.md",
        page_type="note",
        title="Podcasts Lane",
        domains=("learning",),
        body_builder=lambda _included: _lane_body("Podcasts Lane", "podcast"),
    ),
    "web-discovery-lane": SeedPageSpec(
        relative_path="sources/web-discovery-lane.md",
        page_type="note",
        title="Web Discovery Lane",
        domains=("learning",),
        body_builder=lambda _included: _lane_body("Web Discovery Lane", "web discovery"),
    ),
    "contradiction-review": SeedPageSpec(
        relative_path="playbooks/contradiction-review.md",
        page_type="playbook",
        title="Contradiction Review",
        domains=("meta", "work"),
        body_builder=_contradiction_review_body,
    ),
    "page-family-semantics": SeedPageSpec(
        relative_path="decisions/page-family-semantics.md",
        page_type="decision",
        title="Page Family Semantics",
        domains=("work",),
        body_builder=_page_family_semantics_body,
    ),
}
