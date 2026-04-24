from __future__ import annotations

import argparse
from datetime import date

from scripts.common import env

from mind.services.cli_progress import progress_for_args
from mind.services.llm_telemetry import enrich_events_with_gateway, read_events, summarize_events, summarize_gateway_costs

from .common import project_root


def cmd_llm_audit(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="auditing llm telemetry", default=bool(args.refresh_gateway)) as progress:
        repo_root = project_root()
        target_day = date.today() if bool(args.today) else (date.fromisoformat(args.date) if args.date else None)
        progress.phase("reading local telemetry")
        events = read_events(
            repo_root,
            day=target_day,
            bundle_id=args.bundle,
            task_class=args.task_class,
            model=args.model,
        )
        if not events:
            print("llm-audit: no telemetry events found")
            return 0

        warnings: list[str] = []
        enriched = events
        if bool(args.refresh_gateway):
            cfg = env.load()
            if not cfg.ai_gateway_api_key:
                warnings.append("AI_GATEWAY_API_KEY is missing; skipping gateway enrichment")
            else:
                progress.phase("refreshing gateway metadata")
                enriched, fetch_warnings = enrich_events_with_gateway(events, api_key=cfg.ai_gateway_api_key)
                warnings.extend(fetch_warnings)

        progress.phase("summarizing costs")
        summary = summarize_events(enriched)
        print(f"llm-audit: attempts={summary['total_attempts']} success={summary['success_count']} failed={summary['failure_count']}")
        print(f"missing generation ids: {summary['missing_generation_ids']}")
        print("\nPer task:")
        for key, count in summary["per_task"].items():
            print(f"- {key}: {count}")
        print("\nPer model:")
        for key, count in summary["per_model"].items():
            print(f"- {key}: {count}")
        print("\nSlowest attempts:")
        for row in summary["slowest"]:
            print(
                "- "
                f"{row.get('latency_ms', 0)}ms "
                f"{row.get('attempt_role', 'unknown')} "
                f"{row.get('task_class', 'unknown')} "
                f"{row.get('model', 'unknown')}"
            )

        if bool(args.refresh_gateway):
            costs = summarize_gateway_costs(enriched)
            print(f"\nGateway total cost: ${costs['total_cost']:.6f}")
            if costs["per_task_cost"]:
                print("Gateway cost by task:")
                for key, cost in costs["per_task_cost"].items():
                    print(f"- {key}: ${cost:.6f}")
            if costs["per_model_cost"]:
                print("Gateway cost by model:")
                for key, cost in costs["per_model_cost"].items():
                    print(f"- {key}: ${cost:.6f}")

        if warnings:
            print("\nWarnings:")
            for warning in warnings:
                print(f"- {warning}")
        return 0
