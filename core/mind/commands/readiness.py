from __future__ import annotations

import argparse
import io
import json
from contextlib import redirect_stdout
from pathlib import Path

from mind.commands.doctor import cmd_doctor
from mind.runtime_state import RuntimeState
from mind.services.ingest_readiness import IngestReadinessResult, run_ingest_readiness
from mind.services.onboarding import read_onboarding_status, validate_onboarding_session_ready
from mind.services.provider_ops import run_audible_pull, run_youtube_pull
from scripts.audible.auth import load_authenticator
from scripts.common import env
from scripts.common.config import BrainConfig
from scripts.common.vault import Vault
from scripts.substack.auth import build_client
from scripts.substack.pull import BASE_URL, PATH as SUBSTACK_SAVED_PATH

from . import common as command_common


def _project_root() -> Path:
    return command_common.project_root()


def _print_gate(name: str, status: str, details: str | None = None) -> None:
    if details:
        print(f"- {name}: {status} ({details})")
    else:
        print(f"- {name}: {status}")


def _enabled_ingestors(root: Path) -> set[str]:
    cfg = BrainConfig.load(root)
    return {name.lower() for name in cfg.ingestors.enabled}


def _check_env() -> tuple[bool, str]:
    try:
        cfg = env.load()
    except Exception as exc:
        return False, str(exc)
    return True, (
        f"model={getattr(cfg, 'llm_model', '-')} "
        f"transport={getattr(cfg, 'llm_transport_mode', 'ai_gateway')}"
    )


def _check_state_health(root: Path) -> tuple[bool, str]:
    try:
        summary = RuntimeState.for_repo_root(root).summary()
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    db_path = Path(summary.db_path)
    return True, (
        f"runs={summary.run_count} queue={summary.queue_entries} "
        f"locks={summary.active_locks} db={db_path.name}"
    )


def _check_doctor() -> tuple[bool, str]:
    stream = io.StringIO()
    with redirect_stdout(stream):
        rc = cmd_doctor(argparse.Namespace())
    if rc == 0:
        return True, "runtime/config diagnostics passed"
    issues: list[str] = []
    capture = False
    for line in stream.getvalue().splitlines():
        stripped = line.strip()
        if stripped == "Issues:":
            capture = True
            continue
        if capture and stripped.startswith("- "):
            issues.append(stripped[2:])
    if issues:
        return False, issues[0]
    return False, "doctor reported issues"


def _check_audible_auth() -> tuple[bool, str]:
    try:
        load_authenticator()
    except Exception as exc:
        return False, str(exc)
    return True, "auth file present and loadable"


def _check_substack_auth() -> tuple[bool, str]:
    try:
        with build_client() as client:
            response = client.get(f"{BASE_URL}{SUBSTACK_SAVED_PATH}", params={"limit": 1})
            response.raise_for_status()
    except Exception as exc:
        return False, str(exc)

    content_type = response.headers.get("content-type", "").lower()
    if "json" not in content_type:
        return False, "Substack returned non-JSON response (likely expired cookie)"
    try:
        response.json()
    except json.JSONDecodeError as exc:
        return False, f"Substack response was not parseable JSON: {exc}"
    return True, f"HTTP {response.status_code}"


def _report_onboarding(root: Path) -> tuple[bool, list[str]]:
    details: list[str] = []
    try:
        status = read_onboarding_status(root)
    except FileNotFoundError as exc:
        details.append(str(exc))
        return False, details

    details.append(f"bundle={status.bundle_id} status={status.status}")
    readiness = validate_onboarding_session_ready(Vault.load(root), bundle_id=status.bundle_id)
    if readiness.get("ready"):
        return True, details
    details.extend(str(error) for error in readiness.get("errors") or [])
    return False, details


def _print_ingest_and_graph(result: IngestReadinessResult, *, include_promotion_gate: bool) -> None:
    graph = result.graph
    graph_ok = graph.graph_built and graph.embedding_count > 0 and graph.embedding_backend_count > 0
    graph_detail = (
        f"nodes={graph.node_count} docs={graph.document_count} "
        f"embeddings={graph.embedding_count}/{graph.embedding_backend_count}"
    )
    _print_gate("graph health", "pass" if graph_ok else "fail", graph_detail)

    if include_promotion_gate:
        gate_ok = graph.promotion_gate_passed is True
        gate_detail = graph.promotion_gate_artifact_markdown or graph.promotion_gate_artifact_json
        if gate_detail is None and graph.promotion_gate_passed is None:
            gate_detail = "promotion gate unavailable"
        _print_gate("promotion gate", "pass" if gate_ok else "fail", gate_detail)

    _print_gate("ingest readiness", "pass" if result.passed else "fail", f"report={result.report_json_path}")
    for lane in result.lanes:
        _print_gate(
            f"lane {lane.lane}",
            "pass" if lane.ready else "fail",
            f"selected={lane.selected_count} blocked={lane.blocked_count}",
        )
    if result.issues:
        print("readiness-issues:")
        for issue in result.issues:
            print(f"- {issue}")


def cmd_readiness(args: argparse.Namespace) -> int:
    root = _project_root()
    enabled = _enabled_ingestors(root)
    exit_code = 0

    print(f"readiness-scope: {args.scope}")

    print("\nPrerequisites:")
    env_ok, env_detail = _check_env()
    _print_gate("env", "pass" if env_ok else "fail", env_detail)
    if not env_ok:
        exit_code = 1

    doctor_ok, doctor_detail = _check_doctor()
    _print_gate("doctor", "pass" if doctor_ok else "fail", doctor_detail)
    if not doctor_ok:
        exit_code = 1

    state_ok, state_detail = _check_state_health(root)
    _print_gate("state health", "pass" if state_ok else "fail", state_detail)
    if not state_ok:
        exit_code = 1

    if args.scope == "new-user":
        print("\nOnboarding:")
        onboarding_ok, onboarding_details = _report_onboarding(root)
        detail = onboarding_details[0] if onboarding_details else None
        _print_gate("onboarding session", "pass" if onboarding_ok else "fail", detail)
        if len(onboarding_details) > 1:
            for item in onboarding_details[1:]:
                print(f"- {item}")
        if not onboarding_ok:
            exit_code = 1

    print("\nGraph And Ingest:")
    ingest_result = run_ingest_readiness(
        repo_root=root,
        dropbox_limit=args.dropbox_limit,
        lane_limit=args.lane_limit,
        include_promotion_gate=bool(args.include_promotion_gate),
    )
    _print_ingest_and_graph(ingest_result, include_promotion_gate=bool(args.include_promotion_gate))
    if not ingest_result.passed:
        exit_code = 1

    if not args.skip_source_checks:
        print("\nSource Lanes:")
        if "youtube" in enabled:
            youtube = run_youtube_pull(root, dry_run=True, limit=5)
            youtube_ok = youtube.exit_code == 0
            _print_gate("youtube dry-run", "pass" if youtube_ok else "fail", youtube.detail)
            if not youtube_ok:
                exit_code = 1
        else:
            _print_gate("youtube dry-run", "skip", "lane disabled in config")

        if "audible" in enabled:
            audible_auth_ok, audible_auth_detail = _check_audible_auth()
            _print_gate("audible auth", "pass" if audible_auth_ok else "fail", audible_auth_detail)
            if not audible_auth_ok:
                exit_code = 1
            audible = run_audible_pull(root, dry_run=True, library_only=True)
            audible_ok = audible.exit_code == 0
            _print_gate("audible dry-run", "pass" if audible_ok else "fail", audible.detail)
            if not audible_ok:
                exit_code = 1
        else:
            _print_gate("audible auth", "skip", "lane disabled in config")
            _print_gate("audible dry-run", "skip", "lane disabled in config")

        if "substack" in enabled:
            substack_ok, substack_detail = _check_substack_auth()
            _print_gate("substack auth", "pass" if substack_ok else "fail", substack_detail)
            if not substack_ok:
                exit_code = 1
        else:
            _print_gate("substack auth", "skip", "lane disabled in config")

        if "books" in enabled:
            _print_gate("books lane", "pass", "file-driven; use `mind ingest books <path>`")
        if "articles" in enabled:
            _print_gate("articles lane", "pass", "queue-driven; see ingest readiness for selected items")

    print(f"\nreadiness: {'pass' if exit_code == 0 else 'fail'}")
    return exit_code
