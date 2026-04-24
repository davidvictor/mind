from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Any

from pydantic import BaseModel

from .contracts import DreamRunManifest


@dataclass(frozen=True)
class DreamArtifactLayout:
    repo_root: Path
    artifact_root: Path
    run_id: str
    stage: str

    @property
    def run_root(self) -> Path:
        return self.artifact_root / "runs" / self.run_id

    @property
    def stage_root(self) -> Path:
        return self.run_root / f"stage-{self.stage}"

    def relative_path(self, path: Path) -> str:
        return path.relative_to(self.repo_root).as_posix()

    def stage_path(self, relative_path: str | Path) -> Path:
        return self.stage_root / Path(relative_path)

    @property
    def manifest_path(self) -> Path:
        return self.run_root / "manifest.json"


def build_layout(*, repo_root: Path, artifact_root: str, run_id: str, stage: str) -> DreamArtifactLayout:
    return DreamArtifactLayout(
        repo_root=repo_root,
        artifact_root=repo_root / artifact_root,
        run_id=run_id,
        stage=stage,
    )


def write_run_manifest(layout: DreamArtifactLayout, manifest: DreamRunManifest, *, dry_run: bool) -> str:
    return _write_json(layout, layout.manifest_path, manifest, dry_run=dry_run)


def write_stage_json(
    layout: DreamArtifactLayout,
    relative_path: str | Path,
    payload: BaseModel | dict[str, Any] | list[Any],
    *,
    dry_run: bool,
) -> str:
    return _write_json(layout, layout.stage_path(relative_path), payload, dry_run=dry_run)


def _write_json(
    layout: DreamArtifactLayout,
    path: Path,
    payload: BaseModel | dict[str, Any] | list[Any],
    *,
    dry_run: bool,
) -> str:
    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_render_json(payload), encoding="utf-8")
    return layout.relative_path(path)


def _render_json(payload: BaseModel | dict[str, Any] | list[Any]) -> str:
    if isinstance(payload, BaseModel):
        serializable: Any = payload.model_dump(mode="json", exclude_none=False)
    else:
        serializable = payload
    return json.dumps(serializable, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
