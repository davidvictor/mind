from __future__ import annotations

from contextlib import contextmanager
import argparse
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from mind.cli import build_parser, main
from mind.commands.config import cmd_config_path, cmd_config_show
from mind.commands.doctor import cmd_doctor
from mind.commands.ingest import cmd_ingest_file
from scripts.substack.parse import SubstackRecord
from scripts import lint as lint_module
from scripts.common.vault import Vault
from tests.support import option_strings, patch_onboarding_llm, subcommand_names, write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_indexes=True)


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)


@contextmanager
def _fake_progress(*_args, **_kwargs):
    class _Progress:
        def phase(self, message: str) -> None:
            print(f"[progress] {message}", file=sys.stderr)

        def update(self, message: str) -> None:
            print(f"[progress] {message}", file=sys.stderr)

        def clear(self, *, newline: bool = False) -> None:
            if newline:
                print("", file=sys.stderr)

    yield _Progress()


def test_mind_help_lists_current_top_level_commands():
    parser = build_parser()
    assert subcommand_names(parser) >= {
        "ingest",
        "query",
        "expand",
        "repair",
        "reset",
        "seed",
        "obsidian",
        "onboard",
        "dream",
        "skill",
        "doctor",
        "readiness",
        "graph",
        "config",
        "state",
        "llm",
    }


def test_mind_llm_help_exposes_audit_surface():
    parser = build_parser()
    assert "audit" in subcommand_names(parser, "llm")


def test_mind_reset_help_exposes_apply_flag():
    parser = build_parser()
    assert "--apply" in option_strings(parser, "reset")


def test_mind_onboard_help_exposes_backend_only_surface():
    parser = build_parser()
    commands = subcommand_names(parser, "onboard")
    assert commands >= {"import", "normalize", "synthesize", "verify", "validate", "materialize", "replay", "status", "migrate-merge", "plan"}
    assert "interview" not in commands


def test_mind_ingest_reingest_help_exposes_current_flags():
    parser = build_parser()
    flags = option_strings(parser, "ingest", "reingest")
    assert flags >= {"--lane", "--path", "--stage", "--through", "--dry-run", "--apply", "--source-id"}


def test_mind_ingest_reingest_passes_force_deep(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    seen: dict[str, object] = {}

    def fake_run_reingest(request, repo_root=None, item_callback=None):
        seen["repo_root"] = repo_root
        seen["force_deep"] = request.force_deep
        return SimpleNamespace(exit_code=0)

    monkeypatch.setattr("mind.commands.ingest.run_reingest", fake_run_reingest)
    monkeypatch.setattr("mind.commands.ingest.render_reingest_report", lambda _result: "reingest-ok")

    assert main(["ingest", "reingest", "--lane", "books", "--dry-run", "--force-deep"]) == 0
    assert seen["force_deep"] is True
    assert seen["repo_root"] == tmp_path
    assert "reingest-ok" in capsys.readouterr().out


def test_mind_ingest_readiness_help_exposes_gate_flags():
    parser = build_parser()
    assert option_strings(parser, "ingest", "readiness") >= {"--dropbox-limit", "--lane-limit", "--include-promotion-gate"}


def test_mind_ingest_registry_help_exposes_source_registry_surface():
    parser = build_parser()
    assert subcommand_names(parser, "ingest", "registry") >= {"rebuild", "status"}


def test_mind_ingest_repair_articles_help_exposes_repair_surface():
    parser = build_parser()
    assert option_strings(parser, "ingest", "repair-articles") >= {"--path", "--today", "--limit", "--source-id", "--dry-run", "--apply"}


def test_mind_repair_personalization_links_help_exposes_repair_surface():
    parser = build_parser()
    assert option_strings(parser, "repair", "personalization-links") >= {"--lane", "--path", "--today", "--limit", "--source-id", "--apply"}


def test_mind_readiness_help_exposes_first_run_flags():
    parser = build_parser()
    assert option_strings(parser, "readiness") >= {"--scope", "--dropbox-limit", "--lane-limit", "--include-promotion-gate", "--skip-source-checks"}


def test_mind_config_show_and_path(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "mind.commands.config.BrainConfig.load",
        lambda root: SimpleNamespace(
            model_dump=lambda mode="json": {
                "provider": "gemini",
                "embedding": {"model": "google/text-embedding"},
            }
        ),
    )
    monkeypatch.setattr(
        "mind.commands.config.validate_routed_llm",
        lambda runtime_cfg: SimpleNamespace(
            to_public_dict=lambda: {"ok": True, "errors": [], "warnings": []},
        ),
    )

    assert cmd_config_path(argparse.Namespace()) == 0
    assert str(tmp_path / "config.yaml") in capsys.readouterr().out
    monkeypatch.setattr("scripts.common.env.load", lambda: SimpleNamespace())

    assert cmd_config_show(argparse.Namespace()) == 0
    out = capsys.readouterr().out
    assert '"provider": "gemini"' in out
    assert '"_validation"' in out
    assert '"embedding"' in out


def test_mind_doctor_invalid_routed_config_exits_nonzero(tmp_path: Path, monkeypatch, capsys):
    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
        "  routes:\n"
        "    summary:\n"
        "      model: cohere/command-r\n",
        encoding="utf-8",
    )
    (tmp_path / "memory").mkdir(parents=True, exist_ok=True)
    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "mind.commands.doctor.vault",
        lambda: SimpleNamespace(
            wiki=tmp_path / "memory",
            raw=tmp_path / "raw",
            runtime_db=tmp_path / ".brain-runtime.sqlite3",
        ),
    )
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: SimpleNamespace(
            llm_model="google/gemini-2.5-pro",
            llm_transport_mode="ai_gateway",
            ai_gateway_api_key="gateway",
            substack_session_cookie="",
        ),
    )
    monkeypatch.setattr(
        "mind.commands.doctor.validate_routed_llm",
        lambda cfg: SimpleNamespace(ok=False, errors=["unsupported provider"], warnings=[]),
    )

    assert cmd_doctor(argparse.Namespace()) == 1
    out = capsys.readouterr().out
    assert "unsupported provider" in out


def test_mind_doctor_reports_runtime_shape(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "mind.commands.doctor.vault",
        lambda: SimpleNamespace(
            wiki=tmp_path / "memory",
            raw=tmp_path / "raw",
            runtime_db=tmp_path / ".brain-runtime.sqlite3",
        ),
    )
    monkeypatch.setattr(
        "scripts.common.env.load",
        lambda: SimpleNamespace(
            llm_model="google/gemini-2.5-pro",
            llm_transport_mode="ai_gateway",
            substack_session_cookie="",
            ai_gateway_api_key="gateway",
        ),
    )
    monkeypatch.setattr(
        "mind.commands.doctor.validate_routed_llm",
        lambda cfg: SimpleNamespace(ok=True, errors=[], warnings=[]),
    )

    assert cmd_doctor(argparse.Namespace()) == 0
    out = capsys.readouterr().out
    assert "LLM base/default route model: google/gemini-2.5-pro" in out
    assert "Wiki path:" in out


@pytest.mark.parametrize("mode", ["light", "deep", "rem", "weave"])
def test_mind_dream_commands_have_stable_contract(tmp_path: Path, monkeypatch, capsys, mode: str):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(
        f"mind.commands.dream.run_{mode}",
        lambda dry_run=False, _mode=mode: SimpleNamespace(
            render=lambda: f"Dream stage: {_mode}\nMode: {'dry-run' if dry_run else 'live'}\nsummary",
        ),
    )
    monkeypatch.setattr(
        "mind.commands.dream.run_weave",
        lambda dry_run=False: SimpleNamespace(
            render=lambda: f"Dream stage: weave\nMode: {'dry-run' if dry_run else 'live'}\nsummary",
        ),
    )
    rc = main(["dream", mode])
    assert rc == 0
    out = capsys.readouterr().out
    assert f"Dream stage: {mode}" in out
    assert "Mode: live" in out


def test_mind_dream_bootstrap_help_exposes_flags():
    parser = build_parser()
    assert option_strings(parser, "dream", "bootstrap") >= {"--dry-run", "--force-pass-d", "--checkpoint-every", "--resume", "--limit"}


def test_mind_skill_generate_stdout(monkeypatch, capsys):
    monkeypatch.setattr(
        "mind.commands.skill.get_llm_service",
        lambda: SimpleNamespace(generate_skill=lambda task_description, context_text="": "# Draft Skill\n"),
    )
    assert main(["skill", "generate", "turn this into a skill", "--stdout"]) == 0
    assert "# Draft Skill" in capsys.readouterr().out


def test_mind_query_reads_relevant_pages(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    page = tmp_path / "memory" / "notes.md"
    page.write_text(
        "---\n"
        "id: notes\n"
        "type: note\n"
        "title: Notes\n"
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-04-08\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Notes\n\nExample Owner is thinking about local-first systems.\n",
        encoding="utf-8",
    )

    assert main(["query", "local first systems"]) == 0
    out = capsys.readouterr().out
    assert "[[notes]]" in out


def test_mind_query_surfaces_tension_annotations(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    (tmp_path / "memory" / "concepts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "memory" / "concepts" / "builder-judgment.md").write_text(
        "---\n"
        "id: builder-judgment\n"
        "type: concept\n"
        "title: Builder Judgment\n"
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-04-08\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - work\n"
        "typed_relations:\n"
        "  contradicts:\n"
        "    - \"[[automated-judgment]]\"\n"
        "relates_to:\n  - \"[[automated-judgment]]\"\n"
        "sources: []\n"
        "---\n\n"
        "# Builder Judgment\n\nHuman judgment is the edge.\n\n## TL;DR\n\nHuman judgment is the edge.\n\n## Evidence log\n\n- local evidence\n",
        encoding="utf-8",
    )
    (tmp_path / "memory" / "concepts" / "automated-judgment.md").write_text(
        "---\n"
        "id: automated-judgment\n"
        "type: concept\n"
        "title: Automated Judgment\n"
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-04-08\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - work\n"
        "typed_relations:\n"
        "  contradicts:\n"
        "    - \"[[builder-judgment]]\"\n"
        "relates_to:\n  - \"[[builder-judgment]]\"\n"
        "sources: []\n"
        "---\n\n"
        "# Automated Judgment\n\nAutomation is the edge.\n\n## TL;DR\n\nAutomation is the edge.\n\n## Evidence log\n\n- local evidence\n",
        encoding="utf-8",
    )

    assert main(["query", "judgment edge"]) == 0
    out = capsys.readouterr().out
    assert "tension with [[automated-judgment]]" in out or "tension with [[builder-judgment]]" in out


def test_mind_expand_saves_raw_and_queries(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "mind.commands.expand._search_web",
        lambda question, limit=3: [("Example result", "https://example.com/result")],
    )
    monkeypatch.setattr(
        "mind.commands.expand.ingest_web_articles",
        lambda **kwargs: [
            __import__("mind.services.web_research", fromlist=["GroundedArticleResult"]).GroundedArticleResult(
                query=kwargs["queries"][0],
                url="https://example.com/result",
                article_page_id="example-article",
            )
        ],
    )
    monkeypatch.setattr("mind.commands.expand.cmd_query", lambda args: print("query answer") or 0)

    assert main(["expand", "what is new here"]) == 0
    out = capsys.readouterr().out
    assert "Saved web sources:" in out
    assert "[[example-article]]" in out
    assert "query answer" in out


def test_mind_onboard_from_json_bootstraps_pages(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds tools for thought.",
                "values": ["clarity", "taste"],
                "positioning": "Design engineer and founder.",
                "open_threads": ["How to balance depth and speed"],
                "projects": [{"title": "Brain", "summary": "Personal wiki"}],
                    "people": [{"name": "Jordan Lee", "summary": "Collaborator"}],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "--from-json", str(payload)]) == 0
    v = Vault.load(tmp_path)
    assert (v.raw / "onboarding" / "current.json").exists()
    bundle_dirs = sorted((v.raw / "onboarding" / "bundles").glob("*"))
    assert bundle_dirs
    bundle_dir = bundle_dirs[-1]
    assert (bundle_dir / "normalized-evidence.json").exists()
    assert (bundle_dir / "decisions.json").exists()
    assert (bundle_dir / "state.json").exists()
    assert (bundle_dir / "validation.json").exists()
    assert (bundle_dir / "materialization.json").exists()
    assert (v.wiki / "me" / "profile.md").exists()
    assert (v.wiki / "people" / "example-owner-person.md").exists()
    assert (v.wiki / "projects" / "brain.md").exists()
    assert (v.wiki / "people" / "jordan-lee.md").exists()
    profile_text = (v.wiki / "me" / "profile.md").read_text(encoding="utf-8")
    assert "Example Owner builds tools for thought." in profile_text
    assert (v.wiki / "decisions").exists()
    assert any((v.wiki / "decisions").glob("onboarding-*.md"))
    report = lint_module.run(v)
    assert report.failing_pages == 0


def test_mind_onboard_normalize_collects_uploads_without_materializing(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds tools for thought.",
                "values": ["clarity"],
                "positioning": "Design engineer and founder.",
                "open_threads": ["How should the system evolve?"],
            }
        ),
        encoding="utf-8",
    )
    upload = tmp_path / "notes.md"
    upload.write_text("# Notes\n\nPrivate onboarding evidence.\n", encoding="utf-8")

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "session-upload"]) == 0
    assert main(["onboard", "normalize", "--bundle", "session-upload", "--upload", str(upload)]) == 0
    v = Vault.load(tmp_path)
    bundle_dir = v.raw / "onboarding" / "bundles" / "session-upload"
    assert len(list((bundle_dir / "uploads").glob("*-notes.md"))) == 1
    assert (bundle_dir / "interview.jsonl").exists()
    assert not (v.wiki / "me" / "profile.md").exists()
    assert main(["onboard", "status"]) == 0


def test_mind_onboard_import_and_normalize_continue_existing_bundle(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds tools for thought.",
                "values": ["clarity", "taste"],
                "positioning": {"summary": "Design engineer and founder."},
                "open_threads": ["How to balance depth and speed"],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "session-b"]) == 0
    assert (
        main(
            [
                "onboard",
                "normalize",
                "--bundle",
                "session-b",
                "--response",
                "positioning-work-priorities=craft quality",
                "--response",
                "positioning-constraints=keep it local-first",
            ]
        )
        == 0
    )
    assert main(["onboard", "validate", "--bundle", "session-b"]) == 0


def test_mind_onboard_validate_materialize_and_replay(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds tools for thought.",
                "values": ["clarity", "taste"],
                "positioning": {
                    "summary": "Design engineer and founder.",
                    "work_priorities": ["craft quality"],
                    "constraints": ["keep it local-first"],
                },
                "open_threads": ["How to balance depth and speed"],
                "projects": [{"title": "Brain", "summary": "Personal wiki"}],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "session-a"]) == 0
    assert main(["onboard", "validate", "--bundle", "session-a"]) == 0
    assert main(["onboard", "materialize", "--bundle", "session-a"]) == 0
    assert main(["onboard", "validate", "--bundle", "session-a"]) == 0
    assert main(["onboard", "replay", "--bundle", "session-a", "--force"]) == 0
    assert (tmp_path / "memory" / "me" / "profile.md").exists()
    assert (tmp_path / "memory" / "people" / "example-owner-person.md").exists()


def test_mind_onboard_dedupes_owner_from_people_candidates(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds tools for thought.",
                "values": ["clarity", "taste"],
                "positioning": {
                    "summary": "Design engineer and founder.",
                    "work_priorities": ["craft quality"],
                    "constraints": ["keep it local-first"],
                },
                "open_threads": ["How to balance depth and speed"],
                "people": [
                    {"name": "Example Owner", "summary": "Should resolve to the owner node."},
                    {"name": "Example Owner", "summary": "Collaborator"},
                ],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "--from-json", str(payload)]) == 0
    v = Vault.load(tmp_path)
    assert (v.wiki / "people" / "example-owner-person.md").exists()
    assert not (v.wiki / "people" / "example-owner.md").exists()

    decisions = sorted((v.wiki / "decisions").glob("onboarding-*.md"))
    assert decisions
    decision_text = decisions[-1].read_text(encoding="utf-8")
    assert "people:example-owner" not in decision_text


def test_mind_onboard_import_resume_normalize_and_materialize_preserves_transcript(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "role": "Founder",
                "location": "Remote",
                "summary": "Example Owner builds tools for thought.",
                "positioning": "Design engineer and founder.",
                "open_threads": ["How to balance depth and speed"],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "session-resume"]) == 0
    v = Vault.load(tmp_path)
    assert not (v.wiki / "me" / "profile.md").exists()
    bundle_dir = v.raw / "onboarding" / "bundles" / "session-resume"
    transcript_path = bundle_dir / "interview.jsonl"
    import_transcript = transcript_path.read_text(encoding="utf-8")
    assert '"kind": "import"' in import_transcript
    assert '"question_id": "values"' in import_transcript

    assert main(["onboard", "status", "--bundle", "session-resume"]) == 0

    assert (
        main(
            [
                "onboard",
                "normalize",
                "--bundle",
                "session-resume",
                "--response",
                "values=clarity\ntaste",
            ]
        )
        == 0
    )
    assert main(["onboard", "validate", "--bundle", "session-resume"]) == 0
    assert main(["onboard", "materialize", "--bundle", "session-resume"]) == 0

    transcript = transcript_path.read_text(encoding="utf-8")
    assert import_transcript in transcript
    assert '"question_id": "values"' in transcript
    assert '"answer": "clarity\\ntaste"' in transcript
    assert (v.wiki / "me" / "profile.md").exists()
    assert (v.wiki / "me" / "values.md").exists()
    assert any((v.wiki / "decisions").glob("onboarding-session-resume.md"))


def test_mind_onboard_normalize_keeps_duplicate_upload_basenames_without_overwrite(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "summary": "Example Owner builds tools for thought.",
                "values": ["clarity"],
                "positioning": "Design engineer and founder.",
                "open_threads": ["How should the system evolve?"],
            }
        ),
        encoding="utf-8",
    )
    upload_a_dir = tmp_path / "a"
    upload_b_dir = tmp_path / "b"
    upload_a_dir.mkdir()
    upload_b_dir.mkdir()
    upload_a = upload_a_dir / "notes.md"
    upload_b = upload_b_dir / "notes.md"
    upload_a.write_text("# Notes\n\nFirst file.\n", encoding="utf-8")
    upload_b.write_text("# Notes\n\nSecond file.\n", encoding="utf-8")

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "session-upload-dupes"]) == 0
    assert main(["onboard", "normalize", "--bundle", "session-upload-dupes", "--upload", str(upload_a)]) == 0
    assert main(["onboard", "normalize", "--bundle", "session-upload-dupes", "--upload", str(upload_b)]) == 0

    uploads_dir = Vault.load(tmp_path).raw / "onboarding" / "bundles" / "session-upload-dupes" / "uploads"
    uploads = sorted(uploads_dir.glob("*-notes.md"))
    assert len(uploads) == 2
    contents = {path.read_text(encoding="utf-8") for path in uploads}
    assert contents == {"# Notes\n\nFirst file.\n", "# Notes\n\nSecond file.\n"}


def test_mind_onboard_status_defaults_to_current_pointer_not_latest_bundle(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)
    payload = tmp_path / "onboarding.json"
    payload.write_text(
        json.dumps(
            {
                "name": "Example Owner",
                "summary": "Example Owner builds tools for thought.",
                "values": ["clarity"],
                "positioning": "Design engineer and founder.",
                "open_threads": ["How should the system evolve?"],
            }
        ),
        encoding="utf-8",
    )

    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "session-z"]) == 0
    assert main(["onboard", "import", "--from-json", str(payload), "--bundle", "session-a"]) == 0

    assert main(["onboard", "status"]) == 0
    out = capsys.readouterr().out
    assert "bundle=session-a" in out


def test_mind_ingest_file_writes_summary(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    raw = tmp_path / "raw" / "web" / "2026-04-08-example.md"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text("# Example\n\nA useful note.\n", encoding="utf-8")
    target = tmp_path / "raw" / "files" / "summary-example.md"
    monkeypatch.setattr("mind.commands.ingest.ingest_file", lambda path: target)

    assert cmd_ingest_file(argparse.Namespace(path=str(raw))) == 0
    assert str(target) in capsys.readouterr().out


def test_mind_ingest_substack_runs_full_lane(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-08.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text('{"posts": []}', encoding="utf-8")

    records = [
        SubstackRecord(
            id="1",
            title="On Trust",
            subtitle=None,
            slug="on-trust",
            published_at="2026-04-08T12:00:00Z",
            saved_at="2026-04-08T12:00:00Z",
            url="https://example.com/on-trust",
            author_name="Mario Gabriele",
            author_id="a1",
            publication_name="The Generalist",
            publication_slug="thegeneralist",
            body_html="<p>body</p>",
            is_paywalled=False,
        ),
        SubstackRecord(
            id="2",
            title="Private Note",
            subtitle=None,
            slug="private-note",
            published_at="2026-04-08T12:00:00Z",
            saved_at="2026-04-08T12:00:00Z",
            url="https://example.com/private",
            author_name="Pay Wall",
            author_id="a2",
            publication_name="Example",
            publication_slug="example",
            body_html=None,
            is_paywalled=True,
        ),
    ]
    monkeypatch.setattr("mind.commands.ingest.pull_saved", lambda client, out_dir, today=None: export_path)
    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: records)
    article_page = tmp_path / "memory" / "sources" / "substack" / "thegeneralist" / "2026-04-08-on-trust.md"
    summary_page = tmp_path / "memory" / "summaries" / "summary-on-trust.md"

    def fake_lifecycle(record, client, repo_root, today, saved_urls, **kwargs):
        if record.id == "2":
            raise __import__("scripts.substack.enrich", fromlist=["Paywalled"]).Paywalled(record.url)
        article_page.parent.mkdir(parents=True, exist_ok=True)
        article_page.write_text("x", encoding="utf-8")
        summary_page.parent.mkdir(parents=True, exist_ok=True)
        summary_page.write_text("x", encoding="utf-8")
        return SimpleNamespace(
            propagate={"unsaved_refs": 0},
            envelope={"pass_d": {}},
            materialized={"article": str(article_page), "summary": str(summary_page)},
        )

    monkeypatch.setattr("mind.commands.ingest.substack_enrich.run_substack_record_lifecycle", fake_lifecycle)
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_articles_queue",
        lambda today, repo_root: SimpleNamespace(
            drop_files_processed=0,
            fetched_summarized=1,
            failed=0,
        ),
    )

    assert main(["ingest", "substack", str(export_path), "--today", "2026-04-08"]) == 0
    out = capsys.readouterr().out
    assert "ingest-substack:" in out
    assert (tmp_path / "memory" / "sources" / "substack" / ".ingested-substack-saved-2026-04-08.json").exists()


def test_mind_ingest_substack_attempts_paywalled_export_record_with_auth(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-08.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text('{"posts": []}', encoding="utf-8")

    record = SubstackRecord(
        id="2",
        title="Subscriber Post",
        subtitle=None,
        slug="subscriber-post",
        published_at="2026-04-08T12:00:00Z",
        saved_at="2026-04-08T12:00:00Z",
        url="https://example.com/private",
        author_name="Pay Wall",
        author_id="a2",
        publication_name="Example",
        publication_slug="example",
        body_html=None,
        is_paywalled=True,
    )

    monkeypatch.setattr("mind.commands.ingest.pull_saved", lambda client, out_dir, today=None: export_path)
    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: [record])

    seen: dict[str, object] = {}

    def fake_lifecycle(record, client, repo_root, today, saved_urls, **kwargs):
        seen["called"] = True
        page = tmp_path / "memory" / "sources" / "substack" / "example" / "2026-04-08-subscriber-post.md"
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text("x", encoding="utf-8")
        return SimpleNamespace(
            propagate={"unsaved_refs": 0},
            envelope={"pass_d": {}},
            materialized={"article": str(page)},
        )

    monkeypatch.setattr("mind.commands.ingest.substack_enrich.run_substack_record_lifecycle", fake_lifecycle)

    assert main(["ingest", "substack", str(export_path), "--today", "2026-04-08"]) == 0
    assert seen["called"] is True
    out = capsys.readouterr().out
    assert "ingest-substack:" in out
    assert "posts_written=1" in out
    assert "0 paywalled" in out


def test_mind_ingest_substack_drains_articles_but_does_not_follow_substack_links(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export_path = tmp_path / "raw" / "exports" / "substack-saved-2026-04-08.json"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text('{"posts": []}', encoding="utf-8")

    saved_record = SubstackRecord(
        id="1",
        title="On Trust",
        subtitle=None,
        slug="on-trust",
        published_at="2026-04-08T12:00:00Z",
        saved_at="2026-04-08T12:00:00Z",
        url="https://thegeneralist.substack.com/p/on-trust",
        author_name="Mario Gabriele",
        author_id="a1",
        publication_name="The Generalist",
        publication_slug="thegeneralist",
        body_html="<p>body</p>",
        is_paywalled=False,
    )
    followed_record = SubstackRecord(
        id="follow-1",
        title="What Is Critical AI Literacy?",
        subtitle=None,
        slug="what-is-critical-ai-literacy",
        published_at="2026-02-13T10:01:57+00:00",
        saved_at="2026-04-08T12:00:00Z",
        url="https://theslowai.substack.com/p/what-is-critical-ai-literacy",
        author_name="Dr Sam Illingworth",
        author_id="u1",
        publication_name="Slow AI",
        publication_slug="theslowai",
        body_html="<p>body</p>",
        is_paywalled=False,
    )

    monkeypatch.setattr("mind.commands.ingest.pull_saved", lambda client, out_dir, today=None: export_path)
    monkeypatch.setattr("mind.commands.ingest.substack_auth.build_client", lambda: object())
    monkeypatch.setattr("mind.commands.ingest.substack_parse.parse_export", lambda data: [saved_record])
    saved_page = tmp_path / "memory" / "sources" / "substack" / "thegeneralist" / "2026-04-08-on-trust.md"
    followed_page = tmp_path / "memory" / "sources" / "substack" / "theslowai" / "2026-02-13-what-is-critical-ai-literacy.md"

    def fake_lifecycle(record, client, repo_root, today, saved_urls, discovered_via_page_id=None, discovered_via_url=None, log_unsaved_refs=True):
        saved_page.parent.mkdir(parents=True, exist_ok=True)
        saved_page.write_text("x", encoding="utf-8")
        return SimpleNamespace(
            propagate={
                "unsaved_refs": 0,
                "unsaved_substack_links": [
                    {
                        "url": followed_record.url,
                        "anchor_text": "five questions I published here",
                        "source_page_id": "2026-04-08-on-trust",
                        "source_post_id": "1",
                        "source_post_url": saved_record.url,
                        "discovered_at": saved_record.saved_at,
                    }
                ],
            },
            envelope={"pass_d": {}},
            materialized={"article": str(saved_page)},
        )

    monkeypatch.setattr("mind.commands.ingest.substack_enrich.run_substack_record_lifecycle", fake_lifecycle)
    seen = {"drained": False}
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_articles_queue",
        lambda *args, **kwargs: seen.__setitem__("drained", True)
        or SimpleNamespace(drop_files_processed=0, fetched_summarized=0, failed=0),
    )

    assert main(["ingest", "substack", str(export_path), "--today", "2026-04-08"]) == 0
    out = capsys.readouterr().out
    assert "ingest-substack:" in out
    assert "posts_written=1" in out
    assert seen["drained"] is True
    assert not followed_page.exists()


def test_mind_ingest_audible_chains_into_books(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "audible-library-2026-04-08.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("scripts.audible.pull.main", lambda argv=None: 0)
    seen: dict[str, str] = {}

    def fake_ingest_books(path, **kwargs):
        seen["path"] = str(path)
        seen.update(kwargs)
        from mind.commands.ingest import BooksIngestResult

        return BooksIngestResult(pages_written=0, page_ids=[])

    monkeypatch.setattr("mind.commands.ingest.ingest_books_export", fake_ingest_books)

    assert main(["ingest", "audible"]) == 0
    assert seen["path"] == str(export)
    assert seen["force_deep"] is False
    assert seen["resume"] is True
    assert seen["skip_materialized"] is True


def test_mind_ingest_books_passes_force_deep(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "audible-library-2026-04-08.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")
    seen: dict[str, object] = {}

    def fake_ingest_books(path, **kwargs):
        seen["path"] = str(path)
        seen.update(kwargs)
        from mind.commands.ingest import BooksIngestResult

        return BooksIngestResult(pages_written=0, page_ids=[])

    monkeypatch.setattr("mind.commands.ingest.ingest_books_export", fake_ingest_books)

    assert main(["ingest", "books", str(export), "--force-deep"]) == 0
    assert seen["path"] == str(export)
    assert seen["force_deep"] is True
    assert seen["resume"] is True
    assert seen["skip_materialized"] is True


def test_mind_ingest_inventory_json_reports_page_only_book(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    page = tmp_path / "memory" / "sources" / "books" / "business" / "done-author-done-book.md"
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text(
        "---\n"
        "id: done-author-done-book\n"
        "type: book\n"
        "title: Done Book\n"
        "external_id: audible-123\n"
        "---\n",
        encoding="utf-8",
    )

    assert main(["ingest", "inventory", "--lane", "books", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["counts"]["materialized"] == 1
    assert payload["items"][0]["source_key"] == "book:audible:123"


def test_mind_ingest_registry_rebuild_and_source_show(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    page = tmp_path / "memory" / "sources" / "books" / "business" / "done-author-done-book.md"
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text(
        "---\n"
        "id: done-author-done-book\n"
        "type: book\n"
        "title: Done Book\n"
        "external_id: audible-123\n"
        "---\n",
        encoding="utf-8",
    )

    assert main(["ingest", "registry", "rebuild"]) == 0
    rebuild_out = capsys.readouterr().out
    assert "ingest-registry-rebuild:" in rebuild_out

    assert main(["ingest", "source", "show", "--id", "audible-123"]) == 0
    out = capsys.readouterr().out
    assert "source_key: book:audible:123" in out
    assert "status: materialized" in out


def test_mind_ingest_books_targeted_blocked_selection_exits_nonzero_with_reason(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "audible-library-2026-04-08.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")

    def fake_ingest_books(path, **kwargs):
        from mind.commands.ingest import BooksIngestResult

        return BooksIngestResult(
            pages_written=0,
            page_ids=[],
            selected_count=1,
            skipped_materialized=0,
            resumable=0,
            blocked=1,
            stale=0,
            executed=0,
            failed=0,
            blocked_samples=["Blocked Book: missing required reusable artifacts"],
        )

    monkeypatch.setattr("mind.commands.ingest.ingest_books_export", fake_ingest_books)

    assert main(["ingest", "books", str(export), "--source-id", "book-blocked-book"]) == 1
    out = capsys.readouterr().out
    assert "blocked=1" in out
    assert "Blocked Book: missing required reusable artifacts" in out


def test_mind_ingest_books_progress_goes_to_stderr_not_stdout(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "audible-library-2026-04-08.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")
    monkeypatch.setattr("mind.commands.ingest.progress_for_args", _fake_progress)
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_books_export",
        lambda *args, **kwargs: __import__("mind.commands.ingest", fromlist=["BooksIngestResult"]).BooksIngestResult(
            pages_written=0,
            page_ids=[],
        ),
    )

    assert main(["ingest", "books", str(export)]) == 0
    captured = capsys.readouterr()
    assert "ingest-books:" in captured.out
    assert "[progress] inventorying selected books" in captured.err


def test_mind_ingest_books_prints_failed_samples(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "audible-library-2026-04-08.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")

    def fake_ingest_books(path, **kwargs):
        from mind.commands.ingest import BooksIngestResult

        return BooksIngestResult(
            pages_written=0,
            page_ids=[],
            selected_count=2,
            skipped_materialized=0,
            resumable=2,
            blocked=0,
            stale=0,
            executed=0,
            failed=2,
            failed_items=[
                "Book One: RuntimeError: pass_b failed",
                "Book Two: ValueError: materialize failed",
            ],
        )

    monkeypatch.setattr("mind.commands.ingest.ingest_books_export", fake_ingest_books)

    assert main(["ingest", "books", str(export)]) == 1
    out = capsys.readouterr().out
    assert "failed=2" in out
    assert "failed_samples:" in out
    assert "Book One: RuntimeError: pass_b failed" in out


def test_mind_ingest_books_clears_spinner_before_printing_summary(tmp_path: Path, monkeypatch, capsys):
    from contextlib import contextmanager

    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "audible-library-2026-04-08.json"
    export.parent.mkdir(parents=True, exist_ok=True)
    export.write_text("[]", encoding="utf-8")
    seen = {"cleared": 0}

    @contextmanager
    def _progress(*_args, **_kwargs):
        class _Progress:
            def phase(self, message: str) -> None:
                print(f"[progress] {message}", file=sys.stderr)

            def update(self, message: str) -> None:
                print(f"[progress] {message}", file=sys.stderr)

            def clear(self, *, newline: bool = False) -> None:
                seen["cleared"] += 1

        yield _Progress()

    monkeypatch.setattr("mind.commands.ingest.progress_for_args", _progress)
    monkeypatch.setattr(
        "mind.commands.ingest.ingest_books_export",
        lambda *args, **kwargs: __import__("mind.commands.ingest", fromlist=["BooksIngestResult"]).BooksIngestResult(
            pages_written=0,
            page_ids=[],
        ),
    )

    assert main(["ingest", "books", str(export)]) == 0
    assert seen["cleared"] == 1
    assert "ingest-books:" in capsys.readouterr().out


def test_mind_ingest_inventory_json_suppresses_progress_output(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    page = tmp_path / "memory" / "sources" / "books" / "business" / "done-author-done-book.md"
    page.parent.mkdir(parents=True, exist_ok=True)
    page.write_text(
        "---\n"
        "id: done-author-done-book\n"
        "type: book\n"
        "title: Done Book\n"
        "external_id: audible-123\n"
        "---\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("mind.services.cli_progress.sys.stderr.isatty", lambda: True)

    assert main(["ingest", "inventory", "--lane", "books", "--json"]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["counts"]["materialized"] == 1
    assert captured.err == ""


def test_mind_ingest_audible_passes_force_deep(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)
    seen: dict[str, object] = {}

    def fake_ingest_audible_library(**kwargs):
        seen.update(kwargs)
        from mind.commands.ingest import BooksIngestResult

        return BooksIngestResult(pages_written=0, page_ids=[])

    monkeypatch.setattr("mind.commands.ingest.ingest_audible_library", fake_ingest_audible_library)

    assert main(["ingest", "audible", "--force-deep"]) == 0
    assert seen["library_only"] is False
    assert seen["sleep"] is None
    assert seen["force_deep"] is True
    assert seen["resume"] is True
    assert seen["skip_materialized"] is True


def test_mind_dream_campaign_help_exposes_current_flags():
    parser = build_parser()
    assert "campaign" in subcommand_names(parser, "dream")
    assert option_strings(parser, "dream", "campaign") >= {"--days", "--start-date", "--dry-run", "--resume", "--profile"}


def test_mind_dream_simulate_year_help_exposes_current_flags():
    parser = build_parser()
    assert "simulate-year" in subcommand_names(parser, "dream")
    assert option_strings(parser, "dream", "simulate-year") >= {"--days", "--start-date", "--run-id", "--dry-run"}


def test_mind_dream_weave_help_exposes_current_flags():
    parser = build_parser()
    assert "weave" in subcommand_names(parser, "dream")
    assert option_strings(parser, "dream", "weave") >= {"--dry-run", "--shadow-v2"}
