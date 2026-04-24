from __future__ import annotations

import shutil
from pathlib import Path

from mind.cli import main
from mind.dream.common import DreamExecutionContext
from mind.dream.deep import run_deep
from scripts.atoms.probationary import create_or_extend
from tests.paths import EXAMPLES_ROOT


def _copy_harness(tmp_path: Path) -> Path:
    target = tmp_path / "thin-harness"
    shutil.copytree(EXAMPLES_ROOT, target)
    cfg = target / "config.yaml"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace("enabled: false", "enabled: true", 1), encoding="utf-8")
    return target


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.dream.common.project_root", lambda: root)


def _write_contradiction_nudge(root: Path, name: str, *, atom_id: str, hint: str = "") -> None:
    target = root / "memory" / "inbox" / "nudges" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "---\n"
        f"id: {target.stem}\n"
        "type: note\n"
        "title: contradiction\n"
        "status: active\n"
        "created: 2026-04-10\n"
        "last_updated: 2026-04-10\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# contradiction\n\n"
        f"- [[{atom_id}]] vs [[summary-example-seed]] — opposing evidence {hint}\n",
        encoding="utf-8",
    )


def _write_pair_nudge(root: Path, name: str, *, kind: str, left_id: str, right_id: str) -> None:
    target = root / "memory" / "inbox" / "nudges" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        "---\n"
        f"id: {target.stem}\n"
        "type: note\n"
        'title: "pair nudge"\n'
        "status: active\n"
        "created: 2026-04-10\n"
        "last_updated: 2026-04-10\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
        "domains:\n  - meta\n"
        f"kind: {kind}\n"
        f"left_atom: {left_id}\n"
        f"right_atom: {right_id}\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# pair nudge\n\n"
        f"- [[{left_id}]] -> [[{right_id}]]\n",
        encoding="utf-8",
    )


def _archive_copy(root: Path, *, bucket: str, name: str, content: str | None = None) -> Path:
    nudge_dir = root / "memory" / "inbox" / "nudges"
    source = nudge_dir / name
    target = nudge_dir / bucket / name
    target.parent.mkdir(parents=True, exist_ok=True)
    if content is None:
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        target.write_text(content, encoding="utf-8")
    return target


def _replace_frontmatter_field(path: Path, key: str, value: str) -> None:
    lines = []
    replaced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}:"):
            lines.append(f"{key}: {value}")
            replaced = True
        else:
            lines.append(line)
    assert replaced, key
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_summary(root: Path, name: str, body: str) -> None:
    (root / "memory" / "summaries" / f"{name}.md").write_text(
        "---\n"
        f"id: {name}\n"
        "type: summary\n"
        'title: "Summary"\n'
        "status: active\n"
        "created: 2026-04-10\n"
        "last_updated: 2026-04-10\n"
        "aliases: []\n"
        "tags:\n  - domain/learning\n  - function/summary\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "source_path: raw/drops/example.md\n"
        "source_type: document\n"
        "source_date: 2026-04-10\n"
        "ingested: 2026-04-10\n"
        "entities: []\n"
        "concepts:\n  - \"[[local-first-systems]]\"\n"
        "---\n\n"
        f"# Summary\n\n{body}\n",
        encoding="utf-8",
    )


def test_deep_dry_run_reports_holds_and_contradiction_actions(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    create_or_extend(
        type="stance",
        proposed_id="emerging-stance",
        title="Emerging stance",
        description="Emerging stance",
        snippet="Emerging stance",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-example-seed]]",
        repo_root=root,
    )
    _write_contradiction_nudge(root, "2026-04-10-contradiction-user-owned-ai.md", atom_id="user-owned-ai", hint="[dismiss]")
    _replace_frontmatter_field(root / "memory" / "stances" / "user-owned-ai.md", "last_evidence_date", "2025-01-01")

    assert main(["dream", "deep", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert ": hold (" in out
    assert "would dismiss contradiction nudge" in out
    assert "would regenerate INDEX.md" in out


def test_deep_live_applies_dismisses_and_escalates_without_identity_file_rewrites(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    before_values = (root / "memory" / "me" / "values.md").read_text(encoding="utf-8")
    before_profile = (root / "memory" / "me" / "profile.md").read_text(encoding="utf-8")
    before_positioning = (root / "memory" / "me" / "positioning.md").read_text(encoding="utf-8")
    create_or_extend(
        type="inquiry",
        proposed_id="phase5-promotion",
        title="Phase5 promotion",
        description="Phase5 promotion?",
        snippet="Phase5 promotion?",
        polarity="neutral",
        rationale="test",
        date="2026-04-08",
        source_link="[[summary-example-seed]]",
        repo_root=root,
    )
    create_or_extend(
        type="concept",
        proposed_id="merge-left",
        title="Merge candidate",
        description="Merge candidate",
        snippet="Merge candidate",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-example-seed]]",
        repo_root=root,
    )
    create_or_extend(
        type="concept",
        proposed_id="merge-right",
        title="Merge candidate",
        description="Merge candidate",
        snippet="Merge candidate",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-phase5-b]]",
        repo_root=root,
    )
    _write_contradiction_nudge(root, "2026-04-10-contradiction-user-owned-ai.md", atom_id="user-owned-ai")
    _write_contradiction_nudge(root, "2026-04-10-contradiction-local-first-systems.md", atom_id="local-first-systems", hint="[escalate]")
    _write_contradiction_nudge(root, "2026-04-10-contradiction-weekly-review-loop.md", atom_id="weekly-review-loop", hint="[dismiss]")
    _write_pair_nudge(root, "2026-04-10-merge-merge-left-merge-right.md", kind="merge", left_id="merge-left", right_id="merge-right")
    _write_pair_nudge(root, "2026-04-10-cooccurrence-user-owned-ai-weekly-review-loop.md", kind="cooccurrence", left_id="user-owned-ai", right_id="weekly-review-loop")

    assert main(["dream", "deep"]) == 0
    out = capsys.readouterr().out
    assert "Deep Dream processed" in out
    assert (root / "memory" / "inquiries" / "phase5-promotion.md").exists()
    stance = (root / "memory" / "stances" / "user-owned-ai.md").read_text(encoding="utf-8")
    assert "## Contradictions" in stance
    nudge_dir = root / "memory" / "inbox" / "nudges"
    assert (nudge_dir / ".processed" / "2026-04-10-contradiction-user-owned-ai.md").exists()
    assert (nudge_dir / ".dismissed" / "2026-04-10-contradiction-weekly-review-loop.md").exists()
    assert (nudge_dir / ".escalated" / "2026-04-10-contradiction-local-first-systems.md").exists()
    assert (nudge_dir / ".processed" / "2026-04-10-merge-merge-left-merge-right.md").exists()
    assert (nudge_dir / ".processed" / "2026-04-10-cooccurrence-user-owned-ai-weekly-review-loop.md").exists()
    assert (root / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-10-merge-left.md").exists() != (
        root / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-10-merge-right.md"
    ).exists()
    playbook_text = (root / "memory" / "playbooks" / "weekly-review-loop.md").read_text(encoding="utf-8")
    assert "[[user-owned-ai]]" in playbook_text
    assert (root / "memory" / "me" / "values.md").read_text(encoding="utf-8") == before_values
    assert (root / "memory" / "me" / "profile.md").read_text(encoding="utf-8") == before_profile
    assert (root / "memory" / "me" / "positioning.md").read_text(encoding="utf-8") == before_positioning


def test_deep_merge_canonicalizes_existing_active_target(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    target.write_text(
        "---\n"
        "id: local-first-systems\n"
        "type: concept\n"
        "title: Local First Systems\n"
        "status: active\n"
        "created: 2026-04-08\n"
        "last_updated: 2026-04-08\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources:\n  - \"[[summary-example-seed]]\"\n"
        "lifecycle_state: active\n"
        "last_evidence_date: 2026-04-08\n"
        "evidence_count: 1\n"
        "---\n\n"
        "# thin stub\n\n"
        "## Evidence log\n\n"
        "- 2026-04-08 — [[summary-example-seed]] — seed evidence\n",
        encoding="utf-8",
    )
    create_or_extend(
        type="concept",
        proposed_id="local-first-systems",
        title="Local First Systems",
        description="Systems work best when the user keeps the durable record local.",
        tldr="Systems work best when the durable record stays local.",
        snippet="local durable record",
        polarity="neutral",
        rationale="test",
        domains=["work", "identity"],
        in_conversation_with=["user-owned-ai"],
        date="2026-04-10",
        source_link="[[summary-phase5-b]]",
        repo_root=root,
    )
    probationary = root / "memory" / "inbox" / "probationary" / "concepts" / "2026-04-10-local-first-systems.md"
    _replace_frontmatter_field(probationary, "evidence_count", "3")
    _replace_frontmatter_field(probationary, "created", "2026-04-01")

    assert main(["dream", "deep"]) == 0
    _ = capsys.readouterr().out
    text = target.read_text(encoding="utf-8")

    assert "# Local First Systems\n\nSystems work best when the user keeps the durable record local.\n" in text
    assert "## TL;DR" in text
    assert 'relates_to:\n  - "[[user-owned-ai]]"\n' in text
    assert "\n\n\n" not in text[text.index("---\n\n") + 5 : text.index("## Evidence log")]


def test_deep_consumes_light_generated_polarity_audit_nudges(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_summary(root, "summary-light-polarity", "This source revisits [[user-owned-ai]] and however challenges the stance.")

    assert main(["dream", "light"]) == 0
    _ = capsys.readouterr().out
    nudge_dir = root / "memory" / "inbox" / "nudges"
    polarity_nudges = [path for path in nudge_dir.glob("*-polarity-audit-*.md")]
    assert polarity_nudges

    assert main(["dream", "deep"]) == 0
    _ = capsys.readouterr().out
    escalated = list((nudge_dir / ".escalated").glob("*-polarity-audit-*.md"))
    assert escalated
    digest = next((root / "memory" / "me" / "digests").glob("*.md"))
    assert "Polarity reviews: 1" in digest.read_text(encoding="utf-8")


def test_deep_reuses_identical_archived_cooccurrence_nudges(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    nudge_name = "2026-04-10-cooccurrence-user-owned-ai-weekly-review-loop.md"
    _write_pair_nudge(root, nudge_name, kind="cooccurrence", left_id="user-owned-ai", right_id="weekly-review-loop")
    archived = _archive_copy(root, bucket=".processed", name=nudge_name)

    assert main(["dream", "deep"]) == 0
    out = capsys.readouterr().out

    nudge_dir = root / "memory" / "inbox" / "nudges"
    assert not (nudge_dir / nudge_name).exists()
    assert archived.exists()
    assert "reused archived apply co-occurrence nudge" in out


def test_deep_warns_on_archive_collision_for_different_cooccurrence_content(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    nudge_name = "2026-04-10-cooccurrence-user-owned-ai-weekly-review-loop.md"
    _write_pair_nudge(root, nudge_name, kind="cooccurrence", left_id="user-owned-ai", right_id="weekly-review-loop")
    archived = _archive_copy(
        root,
        bucket=".processed",
        name=nudge_name,
        content=(
            "---\n"
            f"id: {Path(nudge_name).stem}\n"
            "type: note\n"
            'title: "pair nudge"\n'
            "status: active\n"
            "created: 2026-04-10\n"
            "last_updated: 2026-04-10\n"
            "aliases: []\n"
            "tags:\n  - domain/meta\n  - function/note\n  - signal/working\n"
            "domains:\n  - meta\n"
            "kind: cooccurrence\n"
            "left_atom: user-owned-ai\n"
            "right_atom: weekly-review-loop\n"
            "relates_to: []\n"
            "sources: []\n"
            "---\n\n"
            "# pair nudge\n\n"
            "- conflicting archived content\n"
        ),
    )
    playbook_before = (root / "memory" / "playbooks" / "weekly-review-loop.md").read_text(encoding="utf-8")

    assert main(["dream", "deep"]) == 0
    out = capsys.readouterr().out

    nudge_dir = root / "memory" / "inbox" / "nudges"
    live_nudge = nudge_dir / nudge_name
    assert live_nudge.exists()
    assert archived.exists()
    assert live_nudge.read_text(encoding="utf-8") != archived.read_text(encoding="utf-8")
    assert "archive collision for" in out
    assert "skipped co-occurrence nudge" in out
    assert (root / "memory" / "playbooks" / "weekly-review-loop.md").read_text(encoding="utf-8") == playbook_before


def test_deep_refreshes_mature_relations_from_active_synthesis(tmp_path: Path, monkeypatch, capsys) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    target = root / "memory" / "concepts" / "local-first-systems.md"
    text = target.read_text(encoding="utf-8")
    text = text.replace("seed_managed: true\n", "")
    text = text.replace("evidence_count: 1\n", "evidence_count: 5\n")
    text = text.replace("relates_to: []\n", 'relates_to:\n  - "[[weekly-review-loop]]"\n')
    text = text.replace(
        "sources: []\n",
        "sources: []\n"
        "typed_relations:\n"
        '  supports:\n    - "[[weekly-review-loop]]"\n',
    )
    target.write_text(text, encoding="utf-8")

    monkeypatch.setattr(
        "mind.dream.active_synthesis.run_active_synthesis",
        lambda **kwargs: __import__("scripts.atoms.synthesis", fromlist=["ActiveSynthesisResult"]).ActiveSynthesisResult(
            intro="A richer local-first concept.",
            tldr="Local-first systems stay inspectable.",
            why_it_matters="Trust survives when the record stays visible.",
            mechanism="The durable graph stays visible to the operator.",
            examples=["Brain repo"],
            in_conversation_with=["user-owned-ai"],
            typed_relations={"supports": ["user-owned-ai"]},
        ),
    )

    assert main(["dream", "deep"]) == 0
    _ = capsys.readouterr().out

    updated = target.read_text(encoding="utf-8")
    assert '"[[user-owned-ai]]"' in updated
    assert '"[[weekly-review-loop]]"' not in updated


def test_deep_campaign_still_holds_low_trust_only_probationary_promotions(tmp_path: Path, monkeypatch) -> None:
    root = _copy_harness(tmp_path)
    _patch_roots(monkeypatch, root)
    _write_summary(root, "summary-campaign-low-trust", "A low-trust video summary.")
    summary_path = root / "memory" / "summaries" / "summary-campaign-low-trust.md"
    summary_path.write_text(
        summary_path.read_text(encoding="utf-8").replace("source_type: document\n", "source_type: video\n"),
        encoding="utf-8",
    )
    probationary_path = create_or_extend(
        type="inquiry",
        proposed_id="campaign-low-trust-inquiry",
        title="Campaign low trust inquiry",
        description="Campaign low trust inquiry?",
        snippet="Campaign low trust inquiry?",
        polarity="neutral",
        rationale="test",
        date="2026-04-10",
        source_link="[[summary-campaign-low-trust]]",
        repo_root=root,
    )
    monkeypatch.setattr(
        "mind.dream.deep.evaluate_and_persist_quality",
        lambda persist, report_key: {"lanes": {"youtube": {"state": "bootstrap-only"}}},
    )

    context = DreamExecutionContext(
        effective_date="2026-04-21",
        mode="campaign",
        lane_relaxation_mode="relation_only",
        campaign_run_id="campaign-test",
    )
    result = run_deep(dry_run=False, context=context)

    assert "hold (trusted_sources=0" in "\n".join(result.mutations)
    assert probationary_path.exists()
    assert not (root / "memory" / "inquiries" / "campaign-low-trust-inquiry.md").exists()
