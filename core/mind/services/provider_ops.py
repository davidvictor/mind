from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scripts.common import env
from scripts.common.vault import Vault


@dataclass(frozen=True)
class PullResult:
    label: str
    exit_code: int
    detail: str
    export_path: Path | None = None


def _vault(repo_root: Path) -> Vault:
    return Vault.load(repo_root)


def latest_export(repo_root: Path, *, pattern: str) -> Path | None:
    exports_dir = _vault(repo_root).raw / "exports"
    matches = sorted(exports_dir.glob(pattern))
    if not matches:
        return None
    return matches[-1]


def run_youtube_pull(repo_root: Path, *, dry_run: bool = False, limit: int | None = None) -> PullResult:
    from scripts.youtube import pull as youtube_pull

    cfg = env.load()
    result = youtube_pull.run(
        browser=cfg.browser_for_cookies,
        raw_root=_vault(repo_root).raw,
        limit=limit or 200,
        dry_run=dry_run,
    )
    export_path = None if dry_run or result.exit_code != 0 else result.export_path or latest_export(repo_root, pattern="youtube-recent-*.json")
    detail = result.detail
    if export_path is not None:
        detail = str(export_path)
    return PullResult(label="youtube", exit_code=result.exit_code, detail=detail, export_path=export_path)


def run_audible_pull(
    repo_root: Path,
    *,
    dry_run: bool = False,
    library_only: bool = False,
    sleep: float | None = None,
) -> PullResult:
    from scripts.audible import pull as audible_pull

    argv: list[str] = []
    if dry_run:
        argv.append("--dry-run")
    if library_only:
        argv.append("--library-only")
    if sleep is not None:
        argv.extend(["--sleep", str(sleep)])
    rc = audible_pull.main(argv)
    export_path = None if dry_run or rc != 0 else latest_export(repo_root, pattern="audible-library-*.json")
    detail = f"exit_code={rc}"
    if export_path is not None:
        detail = str(export_path)
    return PullResult(label="audible", exit_code=rc, detail=detail, export_path=export_path)


def run_substack_pull(repo_root: Path, *, today: str | None = None) -> PullResult:
    from scripts.substack import auth as substack_auth
    from scripts.substack import pull as substack_pull

    export_path = substack_pull.pull_saved(
        client=substack_auth.build_client(),
        out_dir=_vault(repo_root).raw / "exports",
        today=today,
    )
    return PullResult(label="substack", exit_code=0, detail=str(export_path), export_path=export_path)
