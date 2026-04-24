from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pytest

from mind.cli import main
from mind.commands.ingest import ingest_file
from mind.dream.light import run_light
from mind.services.onboarding import import_onboarding_bundle, materialize_onboarding_bundle
from mind.runtime_state import RuntimeState
from scripts.atoms.evidence_writer import append_evidence
from scripts.atoms.probationary import create_or_extend
from scripts.atoms.working_set import load_for_source
from scripts.common.frontmatter import split_frontmatter
from tests.paths import EXAMPLES_ROOT, FIXTURES_ROOT
from tests.support import patch_onboarding_llm


THIN_HARNESS = EXAMPLES_ROOT
ONBOARDING_FIXTURE = FIXTURES_ROOT / "synthetic" / "onboarding-seed.json"


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def _enable_dream(root: Path, *, auto_activate: bool = False) -> None:
    cfg = root / "config.yaml"
    text = cfg.read_text(encoding="utf-8")
    text = text.replace("enabled: false", "enabled: true", 1)
    text = text.replace(
        "auto_activate_skills: false",
        f"auto_activate_skills: {'true' if auto_activate else 'false'}",
        1,
    )
    cfg.write_text(text, encoding="utf-8")


def _copy_harness(tmp_path: Path, *, auto_activate: bool = False) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(THIN_HARNESS, target)
    _enable_dream(target, auto_activate=auto_activate)
    return target


def _write_summary(root: Path, name: str, body: str, *, concepts: list[str] | None = None) -> Path:
    path = root / "memory" / "summaries" / f"{name}.md"
    concepts_yaml = "\n".join(f'  - "{item}"' for item in (concepts or []))
    default_concepts = '  - "[[local-first-systems]]"'
    path.write_text(
        "---\n"
        f"id: {name}\n"
        "type: summary\n"
        'title: "Summary"\n'
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-04-09\n"
        "aliases: []\n"
        "tags:\n  - domain/learning\n  - function/summary\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "source_path: raw/drops/example.md\n"
        "source_type: document\n"
        "source_date: 2026-04-09\n"
        "ingested: 2026-04-09\n"
        "entities: []\n"
        f"concepts:\n{concepts_yaml if concepts_yaml else default_concepts}\n"
        "---\n\n"
        f"# Summary\n\n{body}\n",
        encoding="utf-8",
    )
    return path


def test_append_evidence_is_idempotent_and_writes_contradiction_nudge(tmp_path: Path):
    root = _copy_harness(tmp_path)
    appended = append_evidence(
        atom_id="user-owned-ai",
        atom_type="stance",
        date="2026-04-09",
        source_link="[[summary-example-seed]]",
        snippet="challenge to ownership assumptions",
        polarity="against",
        confidence="high",
        evidence_strength="theoretical",
        relation_kind="contradicts",
        source_id="summary-example-seed",
        source_kind="summary",
        source_date="2026-04-09",
        topics=["ownership"],
        entities=["AI"],
        repo_root=root,
    )
    again = append_evidence(
        atom_id="user-owned-ai",
        atom_type="stance",
        date="2026-04-09",
        dedupe_by_source=True,
        recorded_on="2026-04-12",
        source_link="[[summary-example-seed]]",
        snippet="a different contradiction snippet",
        polarity="against",
        repo_root=root,
    )
    assert appended is True
    assert again is False
    nudge = root / "memory" / "inbox" / "nudges" / "2026-04-09-contradiction-user-owned-ai.md"
    assert nudge.exists()
    edge_path = root / "raw" / "evidence-edges" / "summary" / "summary-example-seed.jsonl"
    edges = [json.loads(line) for line in edge_path.read_text(encoding="utf-8").splitlines()]
    assert len(edges) == 1
    assert edges[0]["atom_id"] == "user-owned-ai"
    assert edges[0]["relation_kind"] == "contradicts"
    assert edges[0]["evidence_strength"] == "theoretical"
    assert edges[0]["topics"] == ["ownership"]


def test_append_evidence_dedupe_by_source_blocks_repeated_source_on_new_date(tmp_path: Path):
    root = _copy_harness(tmp_path)
    appended = append_evidence(
        atom_id="user-owned-ai",
        atom_type="stance",
        date="2026-04-09",
        source_link="[[summary-example-seed]]",
        snippet="first supporting mention",
        polarity="for",
        repo_root=root,
    )
    again = append_evidence(
        atom_id="user-owned-ai",
        atom_type="stance",
        date="2026-04-12",
        dedupe_by_source=True,
        recorded_on="2026-04-12",
        source_link="[[summary-example-seed]]",
        snippet="same source on a later simulated date",
        polarity="for",
        repo_root=root,
    )

    assert appended is True
    assert again is False
    stance = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    assert "- 2026-04-12 — [[summary-example-seed]]" not in stance


def test_create_or_extend_reuses_apostrophe_title_pages_without_breaking_frontmatter(tmp_path: Path):
    root = _copy_harness(tmp_path)
    first = create_or_extend(
        type="stance",
        proposed_id="exit-interview-too-expensive",
        title="'Too expensive' in exit interviews is a value-delivery signal",
        description="Treat 'too expensive' as a value-delivery signal, not a pricing signal.",
        snippet="Exit interviews usually reveal value-delivery failures, not pure pricing problems.",
        polarity="for",
        rationale="Repeated SaaS diagnostic pattern",
        date="2026-04-18",
        source_link="[[summary-source-a]]",
        repo_root=root,
    )
    second = create_or_extend(
        type="stance",
        proposed_id="exit-interview-too-expensive",
        title="'Too expensive' in exit interviews is a value-delivery signal",
        description="Treat 'too expensive' as a value-delivery signal, not a pricing signal.",
        snippet="A second source confirms the same stance.",
        polarity="for",
        rationale="Repeated SaaS diagnostic pattern",
        date="2026-04-18",
        source_link="[[summary-source-b]]",
        repo_root=root,
    )

    assert first == second
    frontmatter, body = split_frontmatter(first.read_text(encoding="utf-8"))
    assert frontmatter["type"] == "stance"
    assert frontmatter["title"] == "'Too expensive' in exit interviews is a value-delivery signal"
    assert frontmatter["evidence_count"] == 2
    assert "[[summary-source-a]]" in body
    assert "[[summary-source-b]]" in body


def test_working_set_load_for_source_returns_atoms(tmp_path: Path):
    root = _copy_harness(tmp_path)
    atoms = load_for_source(
        source_topics=["local-first-systems"],
        source_domains=["work"],
        cap=10,
        repo_root=root,
    )
    assert atoms
    assert any(atom.id == "local-first-systems" for atom in atoms)


def test_mind_dream_requires_onboarding(tmp_path: Path, monkeypatch, capsys):
    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n",
        encoding="utf-8",
    )
    (tmp_path / "memory").mkdir(parents=True, exist_ok=True)
    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    _patch_roots(monkeypatch, tmp_path)

    assert main(["dream", "light", "--dry-run"]) == 1
    assert "run mind onboard first" in capsys.readouterr().out


def test_light_dry_and_live_on_thin_harness(tmp_path: Path, monkeypatch, capsys):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_summary(
        root,
        "summary-fresh-signal",
        "This new source revisits [[user-owned-ai]] and however challenges the stance.",
        concepts=["[[local-first-systems]]"],
    )

    assert main(["dream", "light", "--dry-run"]) == 0
    dry_out = capsys.readouterr().out
    assert "would tail-rescan append evidence" in dry_out
    assert "would write polarity-audit nudge" in dry_out
    probationary_dir = root / "memory" / "inbox" / "probationary" / "inquiries"
    assert not probationary_dir.exists()

    assert main(["dream", "light"]) == 0
    live_out = capsys.readouterr().out
    assert "Light Dream processed" in live_out
    stance = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    assert "[[summary-fresh-signal]]" in stance
    assert not probationary_dir.exists()
    nudge_dir = root / "memory" / "inbox" / "nudges"
    assert any("polarity-audit" in path.name for path in nudge_dir.glob("*.md"))
    dream_state = RuntimeState.for_repo_root(root).get_dream_state()
    assert dream_state.last_light == date.today().isoformat()


def test_light_base_exception_marks_run_failed(tmp_path: Path, monkeypatch):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)

    class ForcedInterrupt(BaseException):
        pass

    def interrupting_source_topics(frontmatter: dict, body: str) -> list[str]:
        raise ForcedInterrupt

    monkeypatch.setattr("mind.dream.light._source_topics", interrupting_source_topics)

    with pytest.raises(ForcedInterrupt):
        run_light(dry_run=False)

    state = RuntimeState.for_repo_root(root)
    with state.connect() as conn:
        run_rows = [
            (str(row["kind"]), str(row["status"]), str(row["notes"] or ""))
            for row in conn.execute(
                """
                SELECT kind, status, notes
                FROM runs
                WHERE kind = 'dream.light'
                ORDER BY id
                """
            ).fetchall()
        ]
    assert ("dream.light", "failed", "ForcedInterrupt") in run_rows


def test_deep_promotes_probationary_and_updates_indexes(tmp_path: Path, monkeypatch, capsys):
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    create_or_extend(
        type="inquiry",
        proposed_id="how-should-the-system-evolve",
        title="How should the system evolve",
        description="How should the system evolve?",
        snippet="How should the system evolve?",
        polarity="neutral",
        rationale="question",
        date="2026-04-08",
        source_link="[[summary-example-seed]]",
        repo_root=root,
    )
    nudge_dir = root / "memory" / "inbox" / "nudges"
    nudge_dir.mkdir(parents=True, exist_ok=True)
    nudge = nudge_dir / "2026-04-08-contradiction-user-owned-ai.md"
    nudge.write_text(
        "---\n"
        "id: contradiction\n"
        "type: note\n"
        "title: contradiction\n"
        "status: active\n"
            "created: 2026-04-08\n"
            "last_updated: 2026-04-08\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# contradiction\n\n"
        "- [[user-owned-ai]] vs [[summary-example-seed]] — opposing evidence\n",
        encoding="utf-8",
    )

    assert main(["dream", "deep", "--dry-run"]) == 0
    assert "would promote" in capsys.readouterr().out

    assert main(["dream", "deep"]) == 0
    out = capsys.readouterr().out
    assert "Deep Dream processed" in out
    inquiry = root / "memory" / "inquiries" / "how-should-the-system-evolve.md"
    assert inquiry.exists()
    index_text = (root / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[how-should-the-system-evolve]]" in index_text
    open_inquiries = (root / "memory" / "me" / "open-inquiries.md").read_text(encoding="utf-8")
    assert "[[how-should-the-system-evolve]]" in open_inquiries
    stance_text = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    assert "## Contradictions" in stance_text
    digests = root / "memory" / "me" / "digests"
    assert digests.exists()
    assert list(digests.glob("*.md"))


def test_rem_dry_and_live_generates_monthly_graph_outputs(tmp_path: Path, monkeypatch, capsys):
    root = _copy_harness(tmp_path, auto_activate=True)
    _patch_roots(monkeypatch, root)
    assert not (root / "memory" / "me" / "reflections").exists()
    assert not (root / "memory" / "me" / "timeline.md").exists()
    assert not (root / "skills").exists()

    assert main(["dream", "rem", "--dry-run"]) == 0
    assert "would write monthly REM page" in capsys.readouterr().out

    assert main(["dream", "rem"]) == 0
    out = capsys.readouterr().out
    assert "REM Dream processed" in out
    assert list((root / "memory" / "dreams" / "rem").glob("*.md"))
    reflection_pages = sorted((root / "memory" / "me" / "reflections").glob("*.md"))
    assert reflection_pages
    assert "REM Reflection" in reflection_pages[0].read_text(encoding="utf-8")
    assert not (root / "memory" / "me" / "timeline.md").exists()
    assert not (root / "skills").exists()
    state = RuntimeState.for_repo_root(root)
    assert state.get_dream_state().last_rem == date.today().isoformat()
    assert not state.list_skill_usage()
    inquiries = sorted((root / "memory" / "inquiries").glob("*.md"))
    assert [path.name for path in inquiries] == ["how-to-balance-depth-and-speed.md"]


def test_seeded_memory_after_onboard_runs_light_live(tmp_path: Path, monkeypatch):
    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
        "dream:\n"
        "  enabled: true\n"
        "skills:\n"
        "  auto_activate_skills: false\n",
        encoding="utf-8",
    )
    (tmp_path / "memory").mkdir(parents=True, exist_ok=True)
    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    _patch_roots(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)

    imported = import_onboarding_bundle(tmp_path, from_json=str(ONBOARDING_FIXTURE))
    materialized = materialize_onboarding_bundle(tmp_path, bundle_id=imported.bundle_id)
    assert materialized.readiness["ready"] is True
    raw = tmp_path / "raw" / "web" / "2026-04-08-seeded.md"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text("# Seeded\n\nHow should the system evolve?\n", encoding="utf-8")
    ingest_file(raw.resolve())
    probationary_dir = tmp_path / "memory" / "inbox" / "probationary" / "inquiries"
    before = sorted(path.name for path in probationary_dir.glob("*.md")) if probationary_dir.exists() else []
    result = run_light(dry_run=False)
    assert result.status == "completed"
    after = sorted(path.name for path in probationary_dir.glob("*.md")) if probationary_dir.exists() else []
    assert after == before


def test_mind_dream_blocks_when_current_onboarding_session_is_invalid(tmp_path: Path, monkeypatch, capsys):
    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
        "dream:\n"
        "  enabled: true\n"
        "skills:\n"
        "  auto_activate_skills: false\n",
        encoding="utf-8",
    )
    (tmp_path / "memory").mkdir(parents=True, exist_ok=True)
    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    _patch_roots(monkeypatch, tmp_path)
    patch_onboarding_llm(monkeypatch)

    assert main(["onboard", "--from-json", str(ONBOARDING_FIXTURE)]) == 0
    current = tmp_path / "raw" / "onboarding" / "current.json"
    payload = json.loads(current.read_text(encoding="utf-8"))
    payload["bundle_sha256"] = "corrupted"
    current.write_text(json.dumps(payload), encoding="utf-8")

    assert main(["dream", "light", "--dry-run"]) == 1
    assert "run mind onboard first" in capsys.readouterr().out
