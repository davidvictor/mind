"""Local JSONL telemetry for routed LLM attempts."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any, Iterable
from urllib import error, parse, request


def log_event(
    repo_root: Path,
    *,
    task_class: str,
    prompt_version: str,
    provider: str,
    model: str,
    bundle_id: str | None,
    attempt_role: str,
    attempt_index: int,
    status: str,
    latency_ms: int,
    response_id: str | None,
    generation_id: str | None,
    tokens_in: int | None,
    tokens_out: int | None,
    tokens_total: int | None,
    error_class: str | None,
    request_metadata: dict[str, Any] | None = None,
) -> None:
    event = {
        "timestamp": _utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "task_class": task_class,
        "prompt_version": prompt_version,
        "provider": provider,
        "model": model,
        "bundle_id": bundle_id,
        "attempt_role": attempt_role,
        "attempt_index": attempt_index,
        "status": status,
        "latency_ms": latency_ms,
        "response_id": response_id,
        "generation_id": generation_id,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "tokens_total": tokens_total,
        "error_class": error_class,
        "request_metadata": dict(request_metadata or {}),
    }
    target = log_path(repo_root, day=_utc_now().date())
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


def log_path(repo_root: Path, *, day: date) -> Path:
    return repo_root / ".logs" / "llm" / f"attempts-{day.isoformat()}.jsonl"


def read_events(
    repo_root: Path,
    *,
    day: date | None = None,
    bundle_id: str | None = None,
    task_class: str | None = None,
    model: str | None = None,
) -> list[dict[str, Any]]:
    paths = [log_path(repo_root, day=day)] if day else sorted((repo_root / ".logs" / "llm").glob("attempts-*.jsonl"))
    events: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if bundle_id and event.get("bundle_id") != bundle_id:
                continue
            if task_class and event.get("task_class") != task_class:
                continue
            if model and event.get("model") != model:
                continue
            events.append(event)
    return events


def fetch_generation_details(*, generation_id: str, api_key: str, timeout_seconds: int = 15) -> dict[str, Any]:
    query = parse.urlencode({"id": generation_id})
    req = request.Request(
        f"https://ai-gateway.vercel.sh/v1/generation?{query}",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with request.urlopen(req, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, dict) else {}


def summarize_events(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(events)
    per_task: dict[str, int] = defaultdict(int)
    per_model: dict[str, int] = defaultdict(int)
    success = 0
    failed = 0
    missing_generation = 0
    for row in rows:
        per_task[str(row.get("task_class") or "unknown")] += 1
        per_model[str(row.get("model") or "unknown")] += 1
        if row.get("status") == "success":
            success += 1
        else:
            failed += 1
        if not row.get("generation_id"):
            missing_generation += 1
    slowest = sorted(rows, key=lambda item: int(item.get("latency_ms") or 0), reverse=True)[:5]
    return {
        "total_attempts": len(rows),
        "success_count": success,
        "failure_count": failed,
        "missing_generation_ids": missing_generation,
        "per_task": dict(sorted(per_task.items())),
        "per_model": dict(sorted(per_model.items())),
        "slowest": slowest,
    }


def enrich_events_with_gateway(
    events: list[dict[str, Any]],
    *,
    api_key: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    cache: dict[str, dict[str, Any]] = {}
    enriched: list[dict[str, Any]] = []
    for row in events:
        generation_id = str(row.get("generation_id") or "").strip()
        if not generation_id:
            enriched.append(dict(row))
            continue
        if generation_id not in cache:
            try:
                cache[generation_id] = fetch_generation_details(generation_id=generation_id, api_key=api_key)
            except error.URLError as exc:
                warnings.append(f"{generation_id}: {exc.reason}")
                cache[generation_id] = {}
            except Exception as exc:  # pragma: no cover - defensive
                warnings.append(f"{generation_id}: {exc}")
                cache[generation_id] = {}
        merged = dict(row)
        if cache[generation_id]:
            merged["gateway_generation"] = cache[generation_id]
        enriched.append(merged)
    return enriched, warnings


def summarize_gateway_costs(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    total_cost = 0.0
    per_task: dict[str, float] = defaultdict(float)
    per_model: dict[str, float] = defaultdict(float)
    for row in events:
        generation = row.get("gateway_generation")
        if not isinstance(generation, dict):
            continue
        cost = float(generation.get("total_cost") or 0.0)
        total_cost += cost
        per_task[str(row.get("task_class") or "unknown")] += cost
        per_model[str(row.get("model") or "unknown")] += cost
    return {
        "total_cost": total_cost,
        "per_task_cost": dict(sorted(per_task.items())),
        "per_model_cost": dict(sorted(per_model.items())),
    }


def _utc_now() -> datetime:
    return datetime.utcnow()
