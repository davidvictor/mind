from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
import sys
from typing import Any

from scripts.common.vault import Vault, project_root

REPO_ROOT = project_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.common.config import BRAIN_CONFIG_PATH_ENV


@dataclass(frozen=True)
class CanaryReport:
    label: str
    config_path: str | None
    run_id: int | None
    status: str
    notes: str | None
    duration_seconds: float | None
    event_counts: dict[str, int]
    prompt_models: list[str]
    repaired_prompt_count: int
    gateway_cost_usd: str
    artifact_root: str | None


def _repo_root() -> Path:
    return REPO_ROOT


def _resolve_config_path(repo_root: Path, value: str | None) -> Path | None:
    if not value:
        return None
    path = Path(value)
    if not path.is_absolute():
        path = repo_root / path
    return path


def _latest_shadow_run_id(db_path: Path) -> int | None:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT max(id) FROM runs WHERE kind = 'dream.weave-v2-shadow'"
        ).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def _duration_seconds(started_at: str | None, finished_at: str | None) -> float | None:
    if not started_at or not finished_at:
        return None
    start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    finish = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    return round((finish - start).total_seconds(), 3)


def _artifact_root_for_run(repo_root: Path, run_id: int) -> Path:
    return Vault.load(repo_root).raw / "reports" / "dream" / "v2" / "runs" / f"run-{run_id}"


def _collect_prompt_receipts(run_root: Path) -> tuple[list[str], int, Decimal]:
    models: set[str] = set()
    repaired = 0
    total_cost = Decimal("0")
    if not run_root.exists():
        return [], 0, total_cost
    for path in run_root.rglob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        receipt = payload.get("prompt_receipt")
        if not isinstance(receipt, dict):
            continue
        model = receipt.get("model")
        if isinstance(model, str) and model:
            models.add(model)
        if bool(receipt.get("repaired")):
            repaired += 1
        response_metadata = receipt.get("response_metadata")
        if not isinstance(response_metadata, dict):
            continue
        gateway = response_metadata.get("gateway")
        if not isinstance(gateway, dict):
            continue
        cost = gateway.get("cost")
        if cost is None:
            continue
        try:
            total_cost += Decimal(str(cost))
        except Exception:
            continue
    return sorted(models), repaired, total_cost


def _build_report(repo_root: Path, run_id: int | None, *, label: str, config_path: Path | None) -> CanaryReport:
    if run_id is None:
        return CanaryReport(
            label=label,
            config_path=str(config_path) if config_path else None,
            run_id=None,
            status="missing",
            notes="no dream.weave-v2-shadow run record found",
            duration_seconds=None,
            event_counts={},
            prompt_models=[],
            repaired_prompt_count=0,
            gateway_cost_usd="0",
            artifact_root=None,
        )
    db_path = Vault.load(repo_root).runtime_db
    with sqlite3.connect(db_path) as conn:
        run_row = conn.execute(
            "SELECT status, notes, started_at, finished_at FROM runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        event_rows = conn.execute(
            """
            SELECT event_type, count(*)
            FROM run_events
            WHERE run_id = ?
            GROUP BY event_type
            ORDER BY event_type
            """,
            (run_id,),
        ).fetchall()
    status = str(run_row[0]) if run_row else "missing"
    notes = str(run_row[1]) if run_row and run_row[1] is not None else None
    started_at = str(run_row[2]) if run_row and run_row[2] is not None else None
    finished_at = str(run_row[3]) if run_row and run_row[3] is not None else None
    event_counts = {str(name): int(count) for name, count in event_rows}
    run_root = _artifact_root_for_run(repo_root, run_id)
    models, repaired_prompt_count, gateway_cost = _collect_prompt_receipts(run_root)
    return CanaryReport(
        label=label,
        config_path=str(config_path) if config_path else None,
        run_id=run_id,
        status=status,
        notes=notes,
        duration_seconds=_duration_seconds(started_at, finished_at),
        event_counts=event_counts,
        prompt_models=models,
        repaired_prompt_count=repaired_prompt_count,
        gateway_cost_usd=format(gateway_cost, "f"),
        artifact_root=run_root.relative_to(repo_root).as_posix(),
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Dream V2 Weave shadow canary under an optional config overlay.")
    parser.add_argument("--config", default=None, help="Optional config overlay path relative to the repo root.")
    parser.add_argument("--label", default="canary", help="Label to include in the final report.")
    parser.add_argument("--print-json", action="store_true", help="Emit the final report as JSON.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    repo_root = _repo_root()
    config_path = _resolve_config_path(repo_root, args.config)
    if config_path is not None:
        os.environ[BRAIN_CONFIG_PATH_ENV] = str(config_path)

    from mind.dream.v2.weave_stage import run_weave_v2_shadow
    from mind.runtime_state import RuntimeState

    state = RuntimeState.for_repo_root(repo_root)
    before_run_id = _latest_shadow_run_id(state.db_path)
    exit_code = 0
    try:
        result = run_weave_v2_shadow(dry_run=False)
        if not args.print_json:
            print(result.render())
    except BaseException as exc:
        exit_code = 1
        if not args.print_json:
            print(f"Dream V2 canary failed: {type(exc).__name__}: {exc}")
    after_run_id = _latest_shadow_run_id(state.db_path)
    if before_run_id is not None and after_run_id == before_run_id:
        run_id = before_run_id
    else:
        run_id = after_run_id
    report = _build_report(repo_root, run_id, label=args.label, config_path=config_path)
    payload = asdict(report)
    if args.print_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print()
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    if report.status not in {"completed", "interrupted"}:
        return 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
