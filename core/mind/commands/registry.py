from __future__ import annotations

import argparse

from .config import cmd_config_path, cmd_config_show
from .digest import cmd_digest
from .doctor import cmd_doctor
from .dropbox import cmd_dropbox_migrate_legacy, cmd_dropbox_status, cmd_dropbox_sweep
from .dream import (
    cmd_dream_bootstrap,
    cmd_dream_campaign,
    cmd_dream_deep,
    cmd_dream_kene,
    cmd_dream_light,
    cmd_dream_rem,
    cmd_dream_simulate_year,
)
from .expand import cmd_expand
from .graph import (
    cmd_graph_health,
    cmd_graph_embed_evaluate,
    cmd_graph_embed_query,
    cmd_graph_embed_rebuild,
    cmd_graph_embed_status,
    cmd_graph_rebuild,
    cmd_graph_resolve,
    cmd_graph_status,
)
from .ingest import (
    cmd_ingest_articles,
    cmd_ingest_audible,
    cmd_ingest_books,
    cmd_ingest_file,
    cmd_ingest_inventory,
    cmd_ingest_plan,
    cmd_ingest_links,
    cmd_ingest_readiness,
    cmd_ingest_reconcile,
    cmd_ingest_rebuild_manifest,
    cmd_ingest_repair_articles,
    cmd_ingest_reingest,
    cmd_ingest_registry_rebuild,
    cmd_ingest_registry_status,
    cmd_ingest_source_show,
    cmd_ingest_substack,
    cmd_ingest_youtube,
)
from .llm import cmd_llm_audit
from .obsidian import cmd_obsidian_theme_apply
from .onboard import cmd_onboard
from .orchestrate import cmd_orchestrate_daily
from .query import cmd_query
from .readiness import cmd_readiness
from .repair import (
    cmd_repair_atom_pages,
    cmd_repair_content_policy,
    cmd_repair_content_policy_migrate,
    cmd_repair_graph,
    cmd_repair_identifiers,
    cmd_repair_personalization_links,
    cmd_repair_vault_housekeeping,
    cmd_repair_weave_cleanup,
)
from .reset import cmd_reset
from .seed import DEFAULT_PRESET, PRESET_CHOICES, cmd_seed
from .skill import cmd_skill_generate
from .worker import cmd_worker_drain_until_empty, cmd_worker_run_once


def _add_onboard_bundle_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bundle", default=None)


def _add_onboard_upload_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--upload", dest="uploads", action="append", default=[])


def _add_onboard_response_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--response",
        dest="responses",
        action="append",
        default=[],
        help="Question response in <question-id>=<answer> form",
    )


def _add_quiet_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--quiet", action="store_true")


def _add_ingest_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--selection", action="append", default=[], help="Selection filter(s): materialized, incomplete, stale, blocked, excluded, unseen, all")
    parser.add_argument("--source-id", dest="source_ids", action="append", default=[])
    parser.add_argument("--external-id", dest="external_ids", action="append", default=[])
    parser.add_argument("--limit", type=int, default=None)


def _add_ingest_plan_policy_arguments(parser: argparse.ArgumentParser) -> None:
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument("--resume", dest="resume", action="store_true", default=True)
    resume_group.add_argument("--no-resume", dest="resume", action="store_false")
    skip_group = parser.add_mutually_exclusive_group()
    skip_group.add_argument("--skip-materialized", dest="skip_materialized", action="store_true", default=True)
    skip_group.add_argument("--no-skip-materialized", dest="skip_materialized", action="store_false")
    parser.add_argument("--refresh-stale", action="store_true")
    parser.add_argument("--recompute-missing", action="store_true")
    parser.add_argument("--from-stage", default=None)
    parser.add_argument("--through", default="propagate")


def register_additional_commands(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    reset_p = sub.add_parser("reset", help="Reset Brain data surfaces to an empty starter layout")
    reset_p.add_argument("--apply", action="store_true", help="Actually wipe memory/, raw/, dropbox/, and local graph/runtime databases")
    reset_p.set_defaults(func=cmd_reset)

    seed_p = sub.add_parser("seed", help="Create an intentionally connected starter graph")
    seed_p.add_argument("--preset", choices=PRESET_CHOICES, default=DEFAULT_PRESET)
    seed_p.set_defaults(func=cmd_seed)

    obsidian_p = sub.add_parser("obsidian", help="Manage repo-owned Obsidian vault artifacts")
    obsidian_sub = obsidian_p.add_subparsers(dest="obsidian_command", required=True)
    obsidian_theme_p = obsidian_sub.add_parser("theme", help="Manage the canonical Obsidian design system")
    obsidian_theme_sub = obsidian_theme_p.add_subparsers(dest="obsidian_theme_command", required=True)
    obsidian_theme_apply_p = obsidian_theme_sub.add_parser("apply", help="Generate and apply the Kanagawa-based Obsidian theme")
    obsidian_theme_apply_p.add_argument("--dark", default="dragon")
    obsidian_theme_apply_p.add_argument("--light", default="lotus")
    obsidian_theme_apply_p.add_argument("--force", action="store_true")
    obsidian_theme_apply_p.set_defaults(func=cmd_obsidian_theme_apply)

    dropbox_p = sub.add_parser("dropbox", help="User inbox sweep and migration commands")
    dropbox_sub = dropbox_p.add_subparsers(dest="dropbox_command", required=True)
    dropbox_sweep_p = dropbox_sub.add_parser("sweep", help="Sweep the configured dropbox inbox into the ingest lanes")
    dropbox_sweep_p.add_argument("--dry-run", action="store_true")
    dropbox_sweep_p.add_argument("--limit", type=int, default=None)
    dropbox_sweep_p.add_argument("--path", default=None, help="Process one file or directory for targeted recovery/testing")
    _add_quiet_argument(dropbox_sweep_p)
    dropbox_sweep_p.set_defaults(func=cmd_dropbox_sweep, progress_enabled=True)
    dropbox_status_p = dropbox_sub.add_parser("status", help="Show pending dropbox files and the last sweep summary")
    dropbox_status_p.set_defaults(func=cmd_dropbox_status)
    dropbox_migrate_p = dropbox_sub.add_parser("migrate-legacy", help="Move legacy user-like files from raw/drops/ into dropbox/")
    dropbox_migrate_p.set_defaults(func=cmd_dropbox_migrate_legacy)

    ingest_p = sub.add_parser("ingest", help="Canonical ingest operator surface")
    ingest_sub = ingest_p.add_subparsers(dest="ingest_command", required=True)

    ingest_file_p = ingest_sub.add_parser("file", help="Ingest one raw source file into a summary page")
    ingest_file_p.add_argument("path")
    ingest_file_p.set_defaults(func=cmd_ingest_file)

    ingest_youtube_p = ingest_sub.add_parser("youtube", help="Ingest one YouTube export file")
    ingest_youtube_p.add_argument("path")
    ingest_youtube_p.add_argument("--default-duration-minutes", type=float, default=30.0)
    _add_quiet_argument(ingest_youtube_p)
    _add_ingest_selection_arguments(ingest_youtube_p)
    _add_ingest_plan_policy_arguments(ingest_youtube_p)
    ingest_youtube_p.set_defaults(func=cmd_ingest_youtube, progress_enabled=True)

    ingest_books_p = ingest_sub.add_parser("books", help="Ingest one books export file")
    ingest_books_p.add_argument("path")
    ingest_books_p.add_argument("--force-deep", action="store_true")
    _add_quiet_argument(ingest_books_p)
    _add_ingest_selection_arguments(ingest_books_p)
    _add_ingest_plan_policy_arguments(ingest_books_p)
    ingest_books_p.set_defaults(func=cmd_ingest_books, progress_enabled=True)

    ingest_substack_p = ingest_sub.add_parser("substack", help="Ingest a Substack export and linked article queue")
    ingest_substack_p.add_argument("path", nargs="?", default=None)
    ingest_substack_p.add_argument("--today", default=None)
    _add_quiet_argument(ingest_substack_p)
    _add_ingest_selection_arguments(ingest_substack_p)
    _add_ingest_plan_policy_arguments(ingest_substack_p)
    ingest_substack_p.set_defaults(func=cmd_ingest_substack, progress_enabled=True)

    ingest_audible_p = ingest_sub.add_parser("audible", help="Pull Audible library data and ingest books")
    ingest_audible_p.add_argument("--library-only", action="store_true")
    ingest_audible_p.add_argument("--sleep", type=float, default=None)
    ingest_audible_p.add_argument("--force-deep", action="store_true")
    _add_quiet_argument(ingest_audible_p)
    _add_ingest_selection_arguments(ingest_audible_p)
    _add_ingest_plan_policy_arguments(ingest_audible_p)
    ingest_audible_p.set_defaults(func=cmd_ingest_audible, progress_enabled=True)

    ingest_articles_p = ingest_sub.add_parser("articles", help="Drain the articles drop queue")
    ingest_articles_p.add_argument("--today", default=None)
    _add_quiet_argument(ingest_articles_p)
    ingest_articles_p.set_defaults(func=cmd_ingest_articles, progress_enabled=True)

    ingest_links_p = ingest_sub.add_parser("links", help="Import links and optionally drain articles")
    ingest_links_p.add_argument("path")
    ingest_links_p.add_argument("--today", default=None)
    ingest_links_p.add_argument("--ingest", action="store_true")
    ingest_links_p.set_defaults(func=cmd_ingest_links)

    ingest_reingest_p = ingest_sub.add_parser("reingest", help="Replay cached ingest work by lane and stage")
    ingest_reingest_p.add_argument("--lane", required=True, choices=["youtube", "books", "articles", "substack"])
    ingest_reingest_p.add_argument("--manifest", default=None)
    ingest_reingest_p.add_argument("--path", default=None)
    ingest_reingest_p.add_argument("--stage", default="acquire")
    ingest_reingest_p.add_argument("--through", default="propagate")
    ingest_reingest_p.add_argument("--today", default=None)
    ingest_reingest_p.add_argument("--limit", type=int, default=None)
    ingest_reingest_p.add_argument("--source-id", dest="source_ids", action="append", default=[])
    ingest_reingest_p.add_argument("--force-deep", action="store_true", help="For books, override a kept light classification to run deep synthesis during this reingest")
    _add_quiet_argument(ingest_reingest_p)
    mode_group = ingest_reingest_p.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--dry-run", action="store_true")
    mode_group.add_argument("--apply", dest="dry_run", action="store_false")
    ingest_reingest_p.set_defaults(func=cmd_ingest_reingest, progress_enabled=True)

    ingest_manifest_p = ingest_sub.add_parser("rebuild-manifest", help="Snapshot the current canonical source corpus into a targeted rebuild manifest")
    ingest_manifest_p.add_argument("--output", default=None)
    ingest_manifest_p.set_defaults(func=cmd_ingest_rebuild_manifest)

    ingest_registry_p = ingest_sub.add_parser("registry", help="Inspect and rebuild the source registry")
    ingest_registry_sub = ingest_registry_p.add_subparsers(dest="ingest_registry_command", required=True)
    ingest_registry_rebuild_p = ingest_registry_sub.add_parser("rebuild", help="Rebuild the source registry from durable artifacts and known upstream inputs")
    _add_quiet_argument(ingest_registry_rebuild_p)
    ingest_registry_rebuild_p.set_defaults(func=cmd_ingest_registry_rebuild, progress_enabled=True)
    ingest_registry_status_p = ingest_registry_sub.add_parser("status", help="Show source registry counts and freshness")
    ingest_registry_status_p.set_defaults(func=cmd_ingest_registry_status)

    ingest_inventory_p = ingest_sub.add_parser("inventory", help="Show upstream-vs-registry source inventory without execution")
    ingest_inventory_p.add_argument("--lane", required=True, choices=["youtube", "books", "articles", "substack"])
    ingest_inventory_p.add_argument("--path", default=None)
    ingest_inventory_p.add_argument("--today", default=None)
    _add_quiet_argument(ingest_inventory_p)
    _add_ingest_selection_arguments(ingest_inventory_p)
    ingest_inventory_p.add_argument("--json", action="store_true")
    ingest_inventory_p.set_defaults(func=cmd_ingest_inventory, progress_enabled=True)

    ingest_plan_p = ingest_sub.add_parser("plan", help="Build an ingest execution plan for a lane and source slice")
    ingest_plan_p.add_argument("--lane", required=True, choices=["youtube", "books", "articles", "substack"])
    ingest_plan_p.add_argument("--path", default=None)
    ingest_plan_p.add_argument("--today", default=None)
    _add_quiet_argument(ingest_plan_p)
    _add_ingest_selection_arguments(ingest_plan_p)
    _add_ingest_plan_policy_arguments(ingest_plan_p)
    ingest_plan_p.add_argument("--json", action="store_true")
    ingest_plan_p.set_defaults(func=cmd_ingest_plan, progress_enabled=True)

    ingest_source_p = ingest_sub.add_parser("source", help="Show one source from the source registry")
    ingest_source_sub = ingest_source_p.add_subparsers(dest="ingest_source_command", required=True)
    ingest_source_show_p = ingest_source_sub.add_parser("show", help="Resolve a source key or alias against the source registry")
    ingest_source_show_p.add_argument("--id", dest="identifier", required=True)
    ingest_source_show_p.set_defaults(func=cmd_ingest_source_show)

    ingest_reconcile_p = ingest_sub.add_parser("reconcile", help="Refresh registry rows for an upstream slice and report drift")
    ingest_reconcile_p.add_argument("--lane", required=True, choices=["youtube", "books", "articles", "substack"])
    ingest_reconcile_p.add_argument("--path", default=None)
    ingest_reconcile_p.add_argument("--today", default=None)
    _add_quiet_argument(ingest_reconcile_p)
    _add_ingest_selection_arguments(ingest_reconcile_p)
    ingest_reconcile_p.add_argument("--json", action="store_true")
    ingest_reconcile_p.set_defaults(func=cmd_ingest_reconcile, progress_enabled=True)

    ingest_readiness_p = ingest_sub.add_parser("readiness", help="Run the unattended-ingest readiness gate")
    ingest_readiness_p.add_argument("--dropbox-limit", type=int, default=None)
    ingest_readiness_p.add_argument("--lane-limit", type=int, default=None)
    ingest_readiness_p.add_argument("--include-promotion-gate", action="store_true")
    ingest_readiness_p.set_defaults(func=cmd_ingest_readiness)

    ingest_repair_articles_p = ingest_sub.add_parser("repair-articles", help="Repair article acquisition/downstream caches for reingest")
    ingest_repair_articles_p.add_argument("--path", default=None)
    ingest_repair_articles_p.add_argument("--today", default=None)
    ingest_repair_articles_p.add_argument("--limit", type=int, default=None)
    ingest_repair_articles_p.add_argument("--source-id", dest="source_ids", action="append", default=[])
    _add_quiet_argument(ingest_repair_articles_p)
    repair_mode = ingest_repair_articles_p.add_mutually_exclusive_group(required=True)
    repair_mode.add_argument("--dry-run", action="store_true")
    repair_mode.add_argument("--apply", dest="apply", action="store_true")
    ingest_repair_articles_p.set_defaults(func=cmd_ingest_repair_articles, apply=False, progress_enabled=True)

    query_p = sub.add_parser("query", help="Answer a question from the wiki")
    query_p.add_argument("question")
    query_p.add_argument("--limit", type=int, default=8)
    query_p.set_defaults(func=cmd_query)

    expand_p = sub.add_parser("expand", help="Search the web, save sources, ingest, and answer")
    expand_p.add_argument("question")
    expand_p.add_argument("--limit", type=int, default=3)
    expand_p.add_argument("--force-web", action="store_true")
    _add_quiet_argument(expand_p)
    expand_p.set_defaults(func=cmd_expand, progress_enabled=True)

    repair_p = sub.add_parser("repair", help="Repair graph/runtime content issues")
    repair_sub = repair_p.add_subparsers(dest="repair_command", required=True)
    repair_graph_p = repair_sub.add_parser("graph", help="Repair tags, links, templates, and stub metadata")
    repair_graph_p.add_argument("--apply", action="store_true")
    repair_graph_p.add_argument("--scope", default="templates,tags,links,stubs")
    repair_graph_p.set_defaults(func=cmd_repair_graph)
    repair_content_policy_p = repair_sub.add_parser("content-policy", help="Audit and clean excluded content residue from memory/")
    repair_content_policy_p.add_argument("--apply", action="store_true")
    repair_content_policy_p.set_defaults(func=cmd_repair_content_policy)
    repair_atom_pages_p = repair_sub.add_parser("atom-pages", help="Canonicalize atom pages and backfill from Pass D cache")
    repair_atom_pages_p.add_argument("--apply", action="store_true")
    repair_atom_pages_p.set_defaults(func=cmd_repair_atom_pages)
    repair_identifiers_p = repair_sub.add_parser("identifiers", help="Normalize page ids into an ASCII-safe space and rewrite affected links")
    repair_identifiers_p.add_argument("--apply", action="store_true")
    repair_identifiers_p.set_defaults(func=cmd_repair_identifiers)
    repair_content_policy_migrate_p = repair_sub.add_parser("content-policy-migrate", help="Audit or apply staged content-policy metadata migration for YouTube or books")
    repair_content_policy_migrate_p.add_argument("--lane", required=True, choices=["youtube", "books"])
    repair_content_policy_migrate_p.add_argument("--apply", action="store_true")
    repair_content_policy_migrate_p.set_defaults(func=cmd_repair_content_policy_migrate)
    repair_personalization_links_p = repair_sub.add_parser("personalization-links", help="Refresh cached Pass B personalization and rematerialize source pages cheaply")
    repair_personalization_links_p.add_argument("--lane", required=True, choices=["youtube", "books"])
    repair_personalization_links_p.add_argument("--path", default=None)
    repair_personalization_links_p.add_argument("--today", default=None)
    repair_personalization_links_p.add_argument("--limit", type=int, default=None)
    repair_personalization_links_p.add_argument("--source-id", dest="source_ids", action="append", default=[])
    repair_personalization_links_p.add_argument("--apply", action="store_true")
    repair_personalization_links_p.set_defaults(func=cmd_repair_personalization_links)
    repair_vault_housekeeping_p = repair_sub.add_parser("vault-housekeeping", help="Audit and clean legacy summary residue, duplicate source titles, and naming drift")
    repair_vault_housekeeping_p.add_argument("--apply", action="store_true")
    repair_vault_housekeeping_p.set_defaults(func=cmd_repair_vault_housekeeping)
    repair_weave_cleanup_p = repair_sub.add_parser("weave-cleanup", help="Strip legacy Weave metadata and archive experimental Weave pages")
    repair_weave_cleanup_p.add_argument("--apply", action="store_true")
    repair_weave_cleanup_p.set_defaults(func=cmd_repair_weave_cleanup)

    llm_p = sub.add_parser("llm", help="Inspect local LLM telemetry and optional gateway enrichment")
    llm_sub = llm_p.add_subparsers(dest="llm_command", required=True)
    llm_audit_p = llm_sub.add_parser("audit", help="Summarize local LLM attempt telemetry")
    llm_audit_p.add_argument("--today", action="store_true")
    llm_audit_p.add_argument("--date", default=None)
    llm_audit_p.add_argument("--bundle", default=None)
    llm_audit_p.add_argument("--task-class", dest="task_class", default=None)
    llm_audit_p.add_argument("--model", default=None)
    llm_audit_p.add_argument("--refresh-gateway", action="store_true")
    _add_quiet_argument(llm_audit_p)
    llm_audit_p.set_defaults(func=cmd_llm_audit, progress_enabled=True)

    onboard_p = sub.add_parser("onboard", help="Run onboarding import, normalize, synthesis, verification, materialization, replay, and status")
    onboard_p.add_argument("--from-json", dest="from_json", default=None)
    _add_onboard_upload_argument(onboard_p)
    _add_onboard_bundle_argument(onboard_p)
    onboard_p.add_argument("--force", action="store_true")
    _add_quiet_argument(onboard_p)
    onboard_sub = onboard_p.add_subparsers(dest="onboard_command")
    onboard_import_p = onboard_sub.add_parser("import", help="Import onboarding input into raw/onboarding without durable wiki writes")
    onboard_import_p.add_argument("--from-json", dest="from_json", required=True)
    _add_onboard_upload_argument(onboard_import_p)
    _add_onboard_bundle_argument(onboard_import_p)
    _add_quiet_argument(onboard_import_p)
    onboard_import_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_normalize_p = onboard_sub.add_parser("normalize", help="Apply onboarding responses/uploads to a persisted bundle and emit next questions without durable writes")
    onboard_normalize_p.add_argument("--from-json", dest="from_json", default=None)
    _add_onboard_upload_argument(onboard_normalize_p)
    _add_onboard_response_argument(onboard_normalize_p)
    onboard_normalize_p.add_argument("--answer", dest="answers", action="append", default=[], help=argparse.SUPPRESS)
    _add_onboard_bundle_argument(onboard_normalize_p)
    _add_quiet_argument(onboard_normalize_p)
    onboard_normalize_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_synthesize_p = onboard_sub.add_parser("synthesize", help="Run AI-native onboarding synthesis and merge planning for a persisted bundle")
    _add_onboard_bundle_argument(onboard_synthesize_p)
    _add_quiet_argument(onboard_synthesize_p)
    onboard_synthesize_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_verify_p = onboard_sub.add_parser("verify", help="Run onboarding verifier and materialization-plan generation for a persisted bundle")
    _add_onboard_bundle_argument(onboard_verify_p)
    _add_quiet_argument(onboard_verify_p)
    onboard_verify_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_materialize_p = onboard_sub.add_parser("materialize", help="Project a persisted onboarding bundle into durable wiki pages")
    _add_onboard_bundle_argument(onboard_materialize_p)
    onboard_materialize_p.add_argument("--force", action="store_true")
    _add_quiet_argument(onboard_materialize_p)
    onboard_materialize_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_replay_p = onboard_sub.add_parser("replay", help="Re-run materialization from a persisted onboarding bundle")
    _add_onboard_bundle_argument(onboard_replay_p)
    onboard_replay_p.add_argument("--force", action="store_true")
    _add_quiet_argument(onboard_replay_p)
    onboard_replay_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_validate_p = onboard_sub.add_parser("validate", help="Validate a persisted onboarding bundle and readiness state")
    _add_onboard_bundle_argument(onboard_validate_p)
    _add_quiet_argument(onboard_validate_p)
    onboard_validate_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_migrate_merge_p = onboard_sub.add_parser("migrate-merge", help="Denormalize a persisted merge artifact against its graph artifact")
    _add_onboard_bundle_argument(onboard_migrate_merge_p)
    onboard_migrate_merge_p.set_defaults(func=cmd_onboard)
    onboard_plan_p = onboard_sub.add_parser("plan", help="Render the deterministic onboarding materialization plan")
    _add_onboard_bundle_argument(onboard_plan_p)
    onboard_plan_p.add_argument("--print-json", action="store_true")
    onboard_plan_p.set_defaults(func=cmd_onboard)
    onboard_status_p = onboard_sub.add_parser("status", help="Show the latest onboarding bundle status and readiness")
    _add_onboard_bundle_argument(onboard_status_p)
    _add_quiet_argument(onboard_status_p)
    onboard_status_p.set_defaults(func=cmd_onboard, progress_enabled=True)
    onboard_p.set_defaults(func=cmd_onboard, progress_enabled=True)

    dream_p = sub.add_parser("dream", help="Dream operator commands")
    dream_sub = dream_p.add_subparsers(dest="dream_command", required=True)
    dream_light_p = dream_sub.add_parser("light", help="Run or validate Light Dream")
    dream_light_p.add_argument("--dry-run", action="store_true")
    dream_light_p.set_defaults(func=cmd_dream_light)
    dream_deep_p = dream_sub.add_parser("deep", help="Run or validate Deep Dream")
    dream_deep_p.add_argument("--dry-run", action="store_true")
    dream_deep_p.set_defaults(func=cmd_dream_deep)
    dream_rem_p = dream_sub.add_parser("rem", help="Run or validate REM Dream")
    dream_rem_p.add_argument("--dry-run", action="store_true")
    dream_rem_p.set_defaults(func=cmd_dream_rem)
    dream_kene_p = dream_sub.add_parser("kene", help="Run Kene Dream in shadow dry-run mode")
    dream_kene_p.add_argument("--dry-run", action="store_true", default=True)
    dream_kene_p.set_defaults(func=cmd_dream_kene)
    dream_bootstrap_p = dream_sub.add_parser("bootstrap", help="Replay historical sources through bootstrap consolidation")
    dream_bootstrap_p.add_argument("--dry-run", action="store_true")
    dream_bootstrap_p.add_argument("--force-pass-d", action="store_true")
    dream_bootstrap_p.add_argument("--checkpoint-every", type=int, default=None)
    dream_bootstrap_p.add_argument("--resume", action="store_true")
    dream_bootstrap_p.add_argument("--limit", type=int, default=None)
    _add_quiet_argument(dream_bootstrap_p)
    dream_bootstrap_p.set_defaults(func=cmd_dream_bootstrap, progress_enabled=True)
    dream_campaign_p = dream_sub.add_parser("campaign", help="Run an operator-only simulated Dream reorg campaign")
    dream_campaign_p.add_argument("--days", type=int, required=True)
    dream_campaign_p.add_argument("--start-date", default=None)
    dream_campaign_p.add_argument("--dry-run", action="store_true")
    dream_campaign_p.add_argument("--resume", action="store_true")
    dream_campaign_p.add_argument("--profile", choices=["aggressive", "yearly"], default="aggressive")
    _add_quiet_argument(dream_campaign_p)
    dream_campaign_p.set_defaults(func=cmd_dream_campaign, progress_enabled=True)
    dream_simulate_year_p = dream_sub.add_parser("simulate-year", help="Run an isolated year-scale Dream simulation")
    dream_simulate_year_p.add_argument("--start-date", default=None)
    dream_simulate_year_p.add_argument("--run-id", default=None)
    dream_simulate_year_p.add_argument("--days", type=int, default=365)
    dream_simulate_year_p.add_argument("--dry-run", action="store_true")
    _add_quiet_argument(dream_simulate_year_p)
    dream_simulate_year_p.set_defaults(func=cmd_dream_simulate_year, progress_enabled=True)

    digest_p = sub.add_parser("digest", help="Generate the current digest snapshot")
    digest_p.add_argument("--today", default=None)
    digest_p.set_defaults(func=cmd_digest)

    skill_p = sub.add_parser("skill", help="Skill operations")
    skill_sub = skill_p.add_subparsers(dest="skill_command", required=True)
    skill_generate_p = skill_sub.add_parser("generate", help="Generate a skill draft")
    skill_generate_p.add_argument("prompt")
    skill_generate_p.add_argument("--name", default=None)
    skill_generate_p.add_argument("--description", default=None)
    skill_generate_p.add_argument("--context", default="")
    skill_generate_p.add_argument("--stdout", action="store_true")
    skill_generate_p.add_argument("--force", action="store_true")
    skill_generate_p.set_defaults(func=cmd_skill_generate)

    doctor_p = sub.add_parser("doctor", help="Run runtime and config diagnostics")
    doctor_p.set_defaults(func=cmd_doctor)

    readiness_p = sub.add_parser("readiness", help="Run the first-run operator readiness check")
    readiness_p.add_argument("--scope", choices=["new-user"], default="new-user")
    readiness_p.add_argument("--dropbox-limit", type=int, default=None)
    readiness_p.add_argument("--lane-limit", type=int, default=None)
    readiness_p.add_argument("--include-promotion-gate", action="store_true")
    readiness_p.add_argument("--skip-source-checks", action="store_true")
    readiness_p.set_defaults(func=cmd_readiness)

    graph_p = sub.add_parser("graph", help="Inspect and rebuild the canonical graph registry")
    graph_sub = graph_p.add_subparsers(dest="graph_command", required=True)
    graph_rebuild_p = graph_sub.add_parser("rebuild", help="Rebuild the SQLite graph registry from memory/")
    _add_quiet_argument(graph_rebuild_p)
    graph_rebuild_p.set_defaults(func=cmd_graph_rebuild, progress_enabled=True)
    graph_status_p = graph_sub.add_parser("status", help="Show graph registry counts and freshness")
    graph_status_p.set_defaults(func=cmd_graph_status)
    graph_health_p = graph_sub.add_parser("health", help="Show graph and advisory shadow-vector health")
    graph_health_p.add_argument("--skip-promotion-gate", action="store_true")
    graph_health_p.set_defaults(func=cmd_graph_health)
    graph_resolve_p = graph_sub.add_parser("resolve", help="Resolve a mention against canonical graph nodes")
    graph_resolve_p.add_argument("text")
    graph_resolve_p.add_argument("--limit", type=int, default=5)
    graph_resolve_p.set_defaults(func=cmd_graph_resolve)
    graph_embed_p = graph_sub.add_parser("embed", help="Manage shadow embeddings and vector candidate evaluation")
    graph_embed_sub = graph_embed_p.add_subparsers(dest="graph_embed_command", required=True)
    graph_embed_rebuild_p = graph_embed_sub.add_parser("rebuild", help="Rebuild node embeddings and vector index")
    _add_quiet_argument(graph_embed_rebuild_p)
    graph_embed_rebuild_p.set_defaults(func=cmd_graph_embed_rebuild, progress_enabled=True)
    graph_embed_status_p = graph_embed_sub.add_parser("status", help="Show embedding index status")
    graph_embed_status_p.set_defaults(func=cmd_graph_embed_status)
    graph_embed_query_p = graph_embed_sub.add_parser("query", help="Query the shadow vector index")
    graph_embed_query_p.add_argument("text")
    graph_embed_query_p.add_argument("--limit", type=int, default=5)
    graph_embed_query_p.set_defaults(func=cmd_graph_embed_query)
    graph_embed_eval_p = graph_embed_sub.add_parser("evaluate", help="Compare phase-1 candidates against shadow vector candidates")
    _add_quiet_argument(graph_embed_eval_p)
    graph_embed_eval_p.set_defaults(func=cmd_graph_embed_evaluate, progress_enabled=True)

    config_p = sub.add_parser("config", help="Inspect resolved config")
    config_sub = config_p.add_subparsers(dest="config_command", required=True)
    config_show_p = config_sub.add_parser("show", help="Print the resolved config")
    config_show_p.set_defaults(func=cmd_config_show)
    config_path_p = config_sub.add_parser("path", help="Print the active config path")
    config_path_p.set_defaults(func=cmd_config_path)

    orchestrate_p = sub.add_parser("orchestrate", help="Run unattended orchestration commands")
    orchestrate_sub = orchestrate_p.add_subparsers(dest="orchestrate_command", required=True)
    orchestrate_daily_p = orchestrate_sub.add_parser("daily", help="Run the daily unattended sweep")
    _add_quiet_argument(orchestrate_daily_p)
    orchestrate_daily_p.set_defaults(func=cmd_orchestrate_daily, progress_enabled=True)

    worker_p = sub.add_parser("worker", help="Consume queued work items")
    worker_sub = worker_p.add_subparsers(dest="worker_command", required=True)
    worker_once_p = worker_sub.add_parser("run-once", help="Claim and execute one queued run")
    _add_quiet_argument(worker_once_p)
    worker_once_p.set_defaults(func=cmd_worker_run_once, progress_enabled=True)
    worker_drain_p = worker_sub.add_parser("drain-until-empty", help="Process queued runs until none remain")
    _add_quiet_argument(worker_drain_p)
    worker_drain_p.set_defaults(func=cmd_worker_drain_until_empty, progress_enabled=True)
