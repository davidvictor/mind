from __future__ import annotations

import argparse
from collections.abc import Callable
import json
from pathlib import Path

from . import common as command_common
from mind.services.cli_progress import progress_for_args
from mind.services.onboarding import (
    OnboardingStatus,
    import_onboarding_bundle,
    materialize_onboarding_bundle,
    migrate_onboarding_merge_artifact,
    normalize_onboarding_bundle,
    read_onboarding_status,
    render_onboarding_materialization_plan,
    replay_onboarding_bundle,
    synthesize_onboarding_bundle,
    validate_onboarding_bundle_state,
    verify_onboarding_bundle,
)


def _print_lines(label: str, lines: list[str]) -> None:
    if not lines:
        return
    print(f"{label}:")
    for line in lines:
        print(f"- {line}")


def _print_status(prefix: str, status) -> None:
    print(f"{prefix}: bundle={status.bundle_id} status={status.status}")
    print(f"raw_input: {status.raw_input_path}")
    print(f"ready_for_materialization: {'yes' if status.ready_for_materialization else 'no'}")
    print(f"synthesis_status: {status.synthesis_status}")
    print(f"verifier_verdict: {status.verifier_verdict}")
    if status.graph_chunks_summary:
        print(f"graph_chunks: {status.graph_chunks_summary}")
    if status.merge_chunks_summary:
        print(f"merge_chunks: {status.merge_chunks_summary}")
    if status.merge_relationships_summary:
        print(f"merge_relationships: {status.merge_relationships_summary}")
    if status.materialization_plan_path:
        print(f"materialization_plan: {status.materialization_plan_path}")
    if status.replay_provenance:
        print(f"replay_provenance: {status.replay_provenance}")
    if status.uploads:
        print(f"uploads: {len(status.uploads)}")
    if status.next_questions:
        print("next_questions:")
        for item in status.next_questions:
            prompt = item.get("prompt") or ""
            reason = item.get("reason") or ""
            print(f"- {prompt}")
            if reason:
                print(f"  reason: {reason}")
            choices = item.get("choices") or []
            if choices:
                print(f"  choices: {', '.join(str(choice) for choice in choices)}")
    _print_lines("errors", list(status.errors))
    _print_lines("warnings", list(status.warnings))
    _print_lines("blocking_reasons", list(status.blocking_reasons))
    if status.materialized_pages:
        print(f"materialized_pages: {len(status.materialized_pages)}")
    if status.summary_pages:
        print(f"summary_pages: {len(status.summary_pages)}")
    if status.decision_page:
        print(f"decision_page: {status.decision_page}")
    readiness = status.readiness or {}
    if readiness:
        print(f"readiness: {'ready' if readiness.get('ready') else 'not-ready'}")


def _project_root() -> Path:
    return command_common.project_root()


def _run_bundle_command(
    label: str,
    action: Callable[[], OnboardingStatus],
    *,
    is_success: Callable[[OnboardingStatus], bool] | None = None,
    progress=None,
    phase_message: str | None = None,
) -> int:
    if progress is not None and phase_message:
        progress.phase(phase_message)
    try:
        status = action()
    except FileNotFoundError as exc:
        print(str(exc))
        return 1
    _print_status(label, status)
    return 0 if is_success is None or is_success(status) else 1


def _parse_responses(items: list[str] | None) -> list[dict[str, str]]:
    responses: list[dict[str, str]] = []
    for item in items or []:
        question_id, separator, answer = item.partition("=")
        if not separator or not question_id.strip() or not answer.strip():
            raise ValueError(f"invalid response {item!r}; expected <question-id>=<answer>")
        responses.append({"question_id": question_id.strip(), "answer": answer.strip()})
    return responses


def _cmd_onboard_normalize(args: argparse.Namespace) -> int:
    legacy_answers = list(getattr(args, "answers", []) or [])
    try:
        responses = _parse_responses(list(getattr(args, "responses", []) or []))
    except ValueError as exc:
        print(str(exc))
        return 1

    if args.from_json:
        status = import_onboarding_bundle(
            _project_root(),
            from_json=args.from_json,
            upload_paths=list(args.uploads or []),
            bundle_id=args.bundle,
        )
        if responses or legacy_answers:
            status = normalize_onboarding_bundle(
                _project_root(),
                bundle_id=status.bundle_id,
                responses=responses,
                answers=legacy_answers,
            )
    elif args.bundle:
        status = normalize_onboarding_bundle(
            _project_root(),
            bundle_id=args.bundle,
            responses=responses,
            answers=legacy_answers,
            upload_paths=list(args.uploads or []),
        )
    else:
        print("mind onboard normalize requires --from-json <path> or --bundle <bundle-id>")
        return 1
    _print_status("onboard-normalize", status)
    return 0


def _cmd_onboard_import(args: argparse.Namespace) -> int:
    if not args.from_json:
        print("mind onboard import requires --from-json <path>")
        return 1
    status = import_onboarding_bundle(
        _project_root(),
        from_json=args.from_json,
        upload_paths=list(args.uploads or []),
        bundle_id=args.bundle,
    )
    _print_status("onboard-import", status)
    return 0


def _cmd_onboard_materialize(args: argparse.Namespace) -> int:
    return _run_bundle_command(
        "onboard-materialize",
        lambda: materialize_onboarding_bundle(
            _project_root(),
            bundle_id=args.bundle,
            force=bool(args.force),
        ),
        is_success=lambda status: bool(status.readiness.get("ready")),
        progress=getattr(args, "_progress", None),
        phase_message="materializing pages",
    )


def _cmd_onboard_synthesize(args: argparse.Namespace) -> int:
    return _run_bundle_command(
        "onboard-synthesize",
        lambda: synthesize_onboarding_bundle(
            _project_root(),
            bundle_id=args.bundle,
        ),
        is_success=lambda status: status.synthesis_status == "synthesized",
        progress=getattr(args, "_progress", None),
        phase_message="running semantic synthesis",
    )


def _cmd_onboard_verify(args: argparse.Namespace) -> int:
    return _run_bundle_command(
        "onboard-verify",
        lambda: verify_onboarding_bundle(
            _project_root(),
            bundle_id=args.bundle,
        ),
        is_success=lambda status: status.verifier_verdict == "approved",
        progress=getattr(args, "_progress", None),
        phase_message="running graph shaping / merge planning",
    )


def _cmd_onboard_replay(args: argparse.Namespace) -> int:
    return _run_bundle_command(
        "onboard-replay",
        lambda: replay_onboarding_bundle(
            _project_root(),
            bundle_id=args.bundle,
            force=bool(args.force),
        ),
        is_success=lambda status: bool(status.readiness.get("ready")),
        progress=getattr(args, "_progress", None),
        phase_message="replaying materialization",
    )


def _cmd_onboard_status(args: argparse.Namespace) -> int:
    return _run_bundle_command(
        "onboard-status",
        lambda: read_onboarding_status(_project_root(), bundle_id=args.bundle),
        progress=getattr(args, "_progress", None),
        phase_message="reading onboarding status",
    )


def _cmd_onboard_validate(args: argparse.Namespace) -> int:
    return _run_bundle_command(
        "onboard-validate",
        lambda: validate_onboarding_bundle_state(_project_root(), bundle_id=args.bundle),
        is_success=lambda status: bool(status.ready_for_materialization),
        progress=getattr(args, "_progress", None),
        phase_message="validating onboarding state",
    )


def _cmd_onboard_migrate_merge(args: argparse.Namespace) -> int:
    return _run_bundle_command(
        "onboard-migrate-merge",
        lambda: migrate_onboarding_merge_artifact(_project_root(), bundle_id=args.bundle),
    )


def _cmd_onboard_plan(args: argparse.Namespace) -> int:
    try:
        payload = render_onboarding_materialization_plan(_project_root(), bundle_id=args.bundle)
    except FileNotFoundError as exc:
        print(str(exc))
        return 1
    if bool(getattr(args, "print_json", False)):
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    print(f"onboard-plan: bundle={payload.get('bundle_id')} pages={len(payload.get('pages') or [])}")
    return 0


def cmd_onboard(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="running onboarding command", default=bool(getattr(args, "progress_enabled", False))) as progress:
        setattr(args, "_progress", progress)
        command = getattr(args, "onboard_command", None)
        subcommands: dict[str, Callable[[argparse.Namespace], int]] = {
            "import": _cmd_onboard_import,
            "normalize": _cmd_onboard_normalize,
            "synthesize": _cmd_onboard_synthesize,
            "verify": _cmd_onboard_verify,
            "materialize": _cmd_onboard_materialize,
            "replay": _cmd_onboard_replay,
            "status": _cmd_onboard_status,
            "validate": _cmd_onboard_validate,
            "migrate-merge": _cmd_onboard_migrate_merge,
            "plan": _cmd_onboard_plan,
        }
        if command in subcommands:
            if command == "import":
                progress.phase("importing onboarding input")
            elif command == "normalize":
                progress.phase("normalizing responses")
            return subcommands[command](args)

        if not args.from_json:
            print("mind onboard currently requires --from-json <path> or a subcommand")
            return 1

        progress.phase("importing onboarding input")
        imported_status = import_onboarding_bundle(
            _project_root(),
            from_json=args.from_json,
            upload_paths=list(args.uploads or []),
            bundle_id=args.bundle,
        )

        progress.phase("materializing pages")
        materialized = materialize_onboarding_bundle(
            _project_root(),
            bundle_id=imported_status.bundle_id,
            force=bool(args.force),
        )
        created_count = len(materialized.materialized_pages) + len(materialized.summary_pages) + (1 if materialized.decision_page else 0)
        print(f"onboard: created {created_count} pages from bundle {materialized.bundle_id}")
        return 0 if materialized.readiness.get("ready") else 1
