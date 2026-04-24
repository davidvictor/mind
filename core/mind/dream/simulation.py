from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Any, Iterator

import yaml

from mind.dream.campaign import CAMPAIGN_ADAPTER, run_campaign
from mind.dream.common import DreamPreconditionError, DreamResult
from mind.runtime_state import RuntimeState
from scripts.common.config import (
    BRAIN_CONFIG_PATH_ENV,
    BRAIN_DROPBOX_ROOT_ENV,
    BRAIN_LOCAL_DATA_ROOT_ENV,
    BRAIN_MEMORY_ROOT_ENV,
    BRAIN_RAW_ROOT_ENV,
    BRAIN_STATE_ROOT_ENV,
)
from scripts.common.frontmatter import split_frontmatter, today_str
from scripts.common.vault import Vault


SIMULATION_ROOT = ("local_data", "simulations")
SIMULATION_REPORT_NAMES = ("graph-deltas.json", "graph-deltas.md")
ENV_KEYS = (
    BRAIN_CONFIG_PATH_ENV,
    BRAIN_LOCAL_DATA_ROOT_ENV,
    BRAIN_MEMORY_ROOT_ENV,
    BRAIN_RAW_ROOT_ENV,
    BRAIN_DROPBOX_ROOT_ENV,
    BRAIN_STATE_ROOT_ENV,
)


@dataclass(frozen=True)
class MarkdownSnapshotEntry:
    sha256: str
    frontmatter: dict[str, Any]


@dataclass(frozen=True)
class DreamSimulationResult:
    run_id: str
    days: int
    start_date: str
    simulation_root: Path
    config_path: Path
    campaign: DreamResult
    stage_counts: dict[str, int]
    deltas: dict[str, Any]
    report_json_path: Path
    report_markdown_path: Path

    def render(self) -> str:
        lines = [
            "Dream simulation: simulate-year",
            f"- run_id={self.run_id}",
            f"- days={self.days}",
            f"- start_date={self.start_date}",
            f"- simulation_root={self.simulation_root}",
            f"- config={self.config_path}",
            f"- report_json={self.report_json_path}",
            f"- report_md={self.report_markdown_path}",
            (
                "- stage_counts="
                + ", ".join(f"{stage}={count}" for stage, count in sorted(self.stage_counts.items()))
            ),
            (
                "- graph_deltas="
                f"added={len(self.deltas['added'])} "
                f"modified={len(self.deltas['modified'])} "
                f"deleted={len(self.deltas['deleted'])}"
            ),
            "",
            self.campaign.render(),
        ]
        return "\n".join(lines)


def run_simulate_year(
    *,
    repo_root: Path,
    start_date: str | None = None,
    run_id: str | None = None,
    days: int = 365,
    dry_run: bool = False,
) -> DreamSimulationResult:
    if days <= 0:
        raise DreamPreconditionError("mind dream simulate-year: --days must be greater than 0")

    start = start_date or today_str()
    actual_run_id = run_id or _simulation_run_id(start_date=start)
    simulation_root = repo_root.joinpath(*SIMULATION_ROOT, actual_run_id)
    if simulation_root.exists() and any(simulation_root.iterdir()):
        raise DreamPreconditionError(
            f"mind dream simulate-year: simulation root already exists: {simulation_root}"
        )

    live_vault = Vault.load(repo_root)
    roots = _SimulationRoots(
        root=simulation_root,
        memory=simulation_root / "memory",
        raw=simulation_root / "raw",
        dropbox=simulation_root / "dropbox",
        state=simulation_root / "state",
        reports=simulation_root / "reports",
    )
    _assert_isolated_roots(live_vault, roots)
    _seed_simulation_roots(live_vault, roots)
    config_path = _write_simulation_config(live_vault, roots)
    before = _snapshot_markdown(roots.memory)

    with _simulation_env(config_path=config_path, roots=roots):
        campaign = run_campaign(
            days=days,
            start_date=start,
            dry_run=dry_run,
            resume=False,
            profile="yearly",
        )
        adapter = RuntimeState.for_repo_root(repo_root).get_adapter_state(CAMPAIGN_ADAPTER)

    after = _snapshot_markdown(roots.memory)
    deltas = _graph_deltas(before, after)
    stage_counts = _stage_counts(adapter)
    report_json_path, report_markdown_path = _write_reports(
        roots=roots,
        run_id=actual_run_id,
        days=days,
        start_date=start,
        config_path=config_path,
        campaign=campaign,
        adapter=adapter,
        deltas=deltas,
        live_vault=live_vault,
    )
    return DreamSimulationResult(
        run_id=actual_run_id,
        days=days,
        start_date=start,
        simulation_root=simulation_root,
        config_path=config_path,
        campaign=campaign,
        stage_counts=stage_counts,
        deltas=deltas,
        report_json_path=report_json_path,
        report_markdown_path=report_markdown_path,
    )


@dataclass(frozen=True)
class _SimulationRoots:
    root: Path
    memory: Path
    raw: Path
    dropbox: Path
    state: Path
    reports: Path


def _simulation_run_id(*, start_date: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{start_date}-simulate-year-{stamp}"


def _assert_isolated_roots(live_vault: Vault, roots: _SimulationRoots) -> None:
    live_paths = {
        "memory": live_vault.wiki.resolve(),
        "raw": live_vault.raw.resolve(),
        "dropbox": live_vault.dropbox.resolve(),
        "state": live_vault.state_root.resolve(),
        "runtime_db": live_vault.runtime_db.resolve(),
        "vector_db": live_vault.vector_db.resolve(),
    }
    sim_paths = {
        "memory": roots.memory.resolve(),
        "raw": roots.raw.resolve(),
        "dropbox": roots.dropbox.resolve(),
        "state": roots.state.resolve(),
        "runtime_db": (roots.state / "brain-runtime.sqlite3").resolve(),
        "vector_db": (roots.state / "graph-vectors.sqlite3").resolve(),
    }
    overlaps = sorted(name for name, path in sim_paths.items() if path == live_paths[name])
    if overlaps:
        raise DreamPreconditionError(
            "mind dream simulate-year: simulation roots overlap live roots "
            f"({', '.join(overlaps)})"
        )


def _seed_simulation_roots(live_vault: Vault, roots: _SimulationRoots) -> None:
    roots.root.mkdir(parents=True, exist_ok=True)
    if live_vault.wiki.exists():
        shutil.copytree(live_vault.wiki, roots.memory)
    else:
        roots.memory.mkdir(parents=True, exist_ok=True)
    roots.raw.mkdir(parents=True, exist_ok=True)
    _copy_dream_raw_inputs(live_vault, roots)
    roots.dropbox.mkdir(parents=True, exist_ok=True)
    roots.state.mkdir(parents=True, exist_ok=True)
    roots.reports.mkdir(parents=True, exist_ok=True)


def _copy_dream_raw_inputs(live_vault: Vault, roots: _SimulationRoots) -> None:
    transcripts = live_vault.raw / "transcripts"
    if not transcripts.exists():
        return
    shutil.copytree(transcripts, roots.raw / "transcripts")


def _write_simulation_config(live_vault: Vault, roots: _SimulationRoots) -> Path:
    config_path = roots.root / "config.yaml"
    payload = {
        "paths": {
            "local_data_root": roots.root.as_posix(),
            "memory_root": roots.memory.as_posix(),
            "raw_root": roots.raw.as_posix(),
            "dropbox_root": roots.dropbox.as_posix(),
            "state_root": roots.state.as_posix(),
        },
        "vault": {
            "wiki_dir": roots.memory.as_posix(),
            "raw_dir": roots.raw.as_posix(),
            "dropbox_dir": roots.dropbox.as_posix(),
            "state_dir": roots.state.as_posix(),
            "owner_profile": live_vault.config.vault.owner_profile,
        },
        "state": {
            "runtime_db": (roots.state / "brain-runtime.sqlite3").as_posix(),
            "graph_db": (roots.state / "brain-graph.sqlite3").as_posix(),
            "sources_db": (roots.state / "brain-sources.sqlite3").as_posix(),
        },
        "retrieval": {
            "vector_db": (roots.state / "graph-vectors.sqlite3").as_posix(),
        },
        "dream": {
            "external_grounding": {
                "enabled": False,
            },
            "quality": {
                "persist_receipts": False,
            },
            "campaign": {
                "yearly": {
                    "fast_forward_skip_unchanged_light": True,
                },
            },
        },
    }
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


@contextmanager
def _simulation_env(*, config_path: Path, roots: _SimulationRoots) -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in ENV_KEYS}
    os.environ[BRAIN_CONFIG_PATH_ENV] = str(config_path)
    os.environ[BRAIN_LOCAL_DATA_ROOT_ENV] = str(roots.root)
    os.environ[BRAIN_MEMORY_ROOT_ENV] = str(roots.memory)
    os.environ[BRAIN_RAW_ROOT_ENV] = str(roots.raw)
    os.environ[BRAIN_DROPBOX_ROOT_ENV] = str(roots.dropbox)
    os.environ[BRAIN_STATE_ROOT_ENV] = str(roots.state)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _snapshot_markdown(root: Path) -> dict[str, MarkdownSnapshotEntry]:
    snapshot: dict[str, MarkdownSnapshotEntry] = {}
    if not root.exists():
        return snapshot
    for path in sorted(root.rglob("*.md")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        text = path.read_text(encoding="utf-8")
        frontmatter, _body = split_frontmatter(text)
        snapshot[rel] = MarkdownSnapshotEntry(
            sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
            frontmatter=frontmatter,
        )
    return snapshot


def _graph_deltas(
    before: dict[str, MarkdownSnapshotEntry],
    after: dict[str, MarkdownSnapshotEntry],
) -> dict[str, Any]:
    before_paths = set(before)
    after_paths = set(after)
    added = sorted(after_paths - before_paths)
    deleted = sorted(before_paths - after_paths)
    modified = sorted(path for path in before_paths & after_paths if before[path].sha256 != after[path].sha256)
    return {
        "added": added,
        "modified": modified,
        "deleted": deleted,
        "atom_changes": _atom_changes(before, after, modified),
        "relation_changes": _relation_changes(before, after, modified),
        "dream_outputs": sorted(path for path in added + modified if path.startswith("dreams/")),
        "archive_outputs": sorted(path for path in added + modified if path.startswith(".archive/")),
    }


def _atom_changes(
    before: dict[str, MarkdownSnapshotEntry],
    after: dict[str, MarkdownSnapshotEntry],
    modified: list[str],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for path in modified:
        if not path.startswith(("concepts/", "playbooks/", "stances/", "inquiries/")):
            continue
        prior = before[path].frontmatter
        current = after[path].frontmatter
        entry: dict[str, Any] = {"path": path}
        for key in ("lifecycle_state", "evidence_count", "last_evidence_date", "status"):
            if prior.get(key) != current.get(key):
                entry[key] = {"before": prior.get(key), "after": current.get(key)}
        if len(entry) > 1:
            changes.append(entry)
    return changes


def _relation_changes(
    before: dict[str, MarkdownSnapshotEntry],
    after: dict[str, MarkdownSnapshotEntry],
    modified: list[str],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    keys = ("relates_to", "typed_relations", "sources")
    for path in modified:
        prior = before[path].frontmatter
        current = after[path].frontmatter
        entry: dict[str, Any] = {"path": path}
        for key in keys:
            if prior.get(key) != current.get(key):
                entry[key] = {"before": prior.get(key), "after": current.get(key)}
        if len(entry) > 1:
            changes.append(entry)
    return changes


def _stage_counts(adapter: dict[str, Any] | None) -> dict[str, int]:
    counts = (adapter or {}).get("completed_counts") or {}
    return {
        "light": int(counts.get("light") or 0),
        "deep": int(counts.get("deep") or 0),
        "rem": int(counts.get("rem") or 0),
    }


def _write_reports(
    *,
    roots: _SimulationRoots,
    run_id: str,
    days: int,
    start_date: str,
    config_path: Path,
    campaign: DreamResult,
    adapter: dict[str, Any] | None,
    deltas: dict[str, Any],
    live_vault: Vault,
) -> tuple[Path, Path]:
    report_json_path = roots.reports / SIMULATION_REPORT_NAMES[0]
    report_markdown_path = roots.reports / SIMULATION_REPORT_NAMES[1]
    payload = {
        "run_id": run_id,
        "days": days,
        "start_date": start_date,
        "simulation_root": roots.root.as_posix(),
        "config_path": config_path.as_posix(),
        "live_roots": {
            "memory": live_vault.logical_path(live_vault.wiki),
            "raw": live_vault.logical_path(live_vault.raw),
            "state": live_vault.logical_path(live_vault.state_root),
        },
        "simulation_roots": {
            "memory": roots.memory.as_posix(),
            "raw": roots.raw.as_posix(),
            "dropbox": roots.dropbox.as_posix(),
            "state": roots.state.as_posix(),
        },
        "stage_counts": _stage_counts(adapter),
        "campaign_adapter": adapter or {},
        "campaign_summary": campaign.summary,
        "graph_deltas": deltas,
    }
    report_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    report_markdown_path.write_text(_render_markdown_report(payload), encoding="utf-8")
    return report_json_path, report_markdown_path


def _render_markdown_report(payload: dict[str, Any]) -> str:
    stage_counts = payload["stage_counts"]
    deltas = payload["graph_deltas"]
    lines = [
        "# Dream Simulation Report",
        "",
        f"- Run id: `{payload['run_id']}`",
        f"- Start date: {payload['start_date']}",
        f"- Simulated days: {payload['days']}",
        f"- Simulation root: `{payload['simulation_root']}`",
        f"- Light runs: {stage_counts.get('light', 0)}",
        f"- Deep runs: {stage_counts.get('deep', 0)}",
        f"- REM runs: {stage_counts.get('rem', 0)}",
        "",
        "## Graph Deltas",
        "",
        f"- Added pages: {len(deltas['added'])}",
        f"- Modified pages: {len(deltas['modified'])}",
        f"- Deleted pages: {len(deltas['deleted'])}",
        f"- Atom lifecycle/evidence changes: {len(deltas['atom_changes'])}",
        f"- Relation changes: {len(deltas['relation_changes'])}",
        f"- Dream outputs: {len(deltas['dream_outputs'])}",
        "",
        "## Candidate Pages",
        "",
    ]
    for label in ("added", "modified", "deleted"):
        lines.append(f"### {label.title()}")
        lines.append("")
        values = deltas[label]
        if values:
            lines.extend(f"- `{path}`" for path in values[:25])
            if len(values) > 25:
                lines.append(f"- ... {len(values) - 25} more")
        else:
            lines.append("- None")
        lines.append("")
    lines.extend(["## Campaign Summary", "", str(payload["campaign_summary"]).strip(), ""])
    return "\n".join(lines).rstrip() + "\n"
