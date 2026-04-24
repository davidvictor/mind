from __future__ import annotations

from pathlib import Path

from mind.services.graph_registry import GraphRegistry
from mind.services.onboarding import validate_onboarding_readiness
from mind.services.reset_service import reset_brain
from mind.services.seed_service import seed_brain
from scripts import lint
from scripts.common.vault import Vault
from tests.support import write_repo_config


def _prepare_root(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    write_repo_config(root)
    reset_brain(root, apply=True)
    return root


def _created_paths(result) -> set[str]:
    return {path.as_posix() for path in result.created_paths}


def _stub_seed_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        "mind.services.seed_service.atoms_cache.rebuild",
        lambda repo_root: {"atoms": {"count": 5, "by_type": {"concept": 1, "playbook": 1, "stance": 1, "inquiry": 1, "note": 1}}},
    )

    class _Registry:
        def rebuild(self):
            from mind.services.graph_registry import GraphRebuildResult

            return GraphRebuildResult(
                node_count=5,
                alias_count=0,
                edge_count=0,
                document_count=5,
                chunk_count=0,
                built_at="2026-04-17T00:00:00Z",
            )

    monkeypatch.setattr("mind.services.seed_service.GraphRegistry.for_repo_root", lambda repo_root: _Registry())


def test_seed_presets_create_exact_expected_page_sets(tmp_path: Path, monkeypatch) -> None:
    _stub_seed_runtime(monkeypatch)
    expected = {
        "core": {
            "memory/INDEX.md",
            "memory/decisions/brain-structure.md",
            "memory/me/profile.md",
            "memory/me/values.md",
            "memory/me/positioning.md",
            "memory/me/open-inquiries.md",
            "memory/concepts/concepts.md",
            "memory/playbooks/playbooks.md",
            "memory/stances/stances.md",
            "memory/inquiries/inquiries.md",
            "memory/projects/projects.md",
            "memory/people/people.md",
            "memory/sources/sources.md",
        },
        "skeleton": {
            "memory/INDEX.md",
            "memory/decisions/brain-structure.md",
            "memory/me/profile.md",
            "memory/me/values.md",
            "memory/me/positioning.md",
            "memory/me/open-inquiries.md",
            "memory/concepts/concepts.md",
            "memory/playbooks/playbooks.md",
            "memory/stances/stances.md",
            "memory/inquiries/inquiries.md",
            "memory/projects/projects.md",
            "memory/people/people.md",
            "memory/sources/sources.md",
            "memory/playbooks/inbox-intake-flow.md",
            "memory/playbooks/source-to-atom-promotion.md",
            "memory/decisions/graph-conventions.md",
            "memory/projects/brain.md",
            "memory/inquiries/how-should-the-system-evolve.md",
            "memory/concepts/starter-graph.md",
            "memory/stances/local-first-knowledge-should-stay-file-first.md",
        },
        "framework": {
            "memory/INDEX.md",
            "memory/decisions/brain-structure.md",
            "memory/me/profile.md",
            "memory/me/values.md",
            "memory/me/positioning.md",
            "memory/me/open-inquiries.md",
            "memory/concepts/concepts.md",
            "memory/playbooks/playbooks.md",
            "memory/stances/stances.md",
            "memory/inquiries/inquiries.md",
            "memory/projects/projects.md",
            "memory/people/people.md",
            "memory/sources/sources.md",
            "memory/playbooks/inbox-intake-flow.md",
            "memory/playbooks/source-to-atom-promotion.md",
            "memory/decisions/graph-conventions.md",
            "memory/projects/brain.md",
            "memory/inquiries/how-should-the-system-evolve.md",
            "memory/concepts/starter-graph.md",
            "memory/stances/local-first-knowledge-should-stay-file-first.md",
            "memory/channels/channels.md",
            "memory/companies/companies.md",
            "memory/decisions/decision-log.md",
            "memory/inbox/review-conventions.md",
            "memory/projects/current-focus.md",
            "memory/people/relationship-map.md",
            "memory/sources/books-lane.md",
            "memory/sources/articles-lane.md",
            "memory/sources/videos-lane.md",
            "memory/sources/podcasts-lane.md",
            "memory/sources/web-discovery-lane.md",
            "memory/playbooks/contradiction-review.md",
            "memory/decisions/page-family-semantics.md",
        },
    }

    for preset, expected_paths in expected.items():
        root = _prepare_root(tmp_path / preset)
        result = seed_brain(root, preset=preset)  # type: ignore[arg-type]
        assert _created_paths(result) == expected_paths


def test_seed_after_reset_creates_connected_lint_clean_starter_graph(tmp_path: Path) -> None:
    root = _prepare_root(tmp_path)

    result = seed_brain(root, preset="skeleton")
    report = lint.run(Vault.load(root))
    readiness = validate_onboarding_readiness(Vault.load(root))
    status = GraphRegistry.for_repo_root(root).status()

    assert result.atom_count == 5
    assert report.exit_code == 0
    assert report.orphans == 0
    assert report.broken_links == 0
    assert readiness["ready"] is True
    assert not (root / "raw" / "onboarding" / "current.json").exists()
    assert status.node_count > 0
    assert status.document_count > 0


def test_seed_is_idempotent_and_preserves_existing_user_page(tmp_path: Path, monkeypatch) -> None:
    _stub_seed_runtime(monkeypatch)
    root = _prepare_root(tmp_path)
    profile = root / "memory" / "me" / "profile.md"
    profile.parent.mkdir(parents=True, exist_ok=True)
    custom = "\n".join(
        [
            "---",
            "id: profile",
            "type: profile",
            "title: Custom Profile",
            "status: active",
            "created: 2026-04-13",
            "last_updated: 2026-04-13",
            "aliases: []",
            "tags:",
            "  - domain/identity",
            "  - function/identity",
            "  - signal/canon",
            "domains:",
            "  - identity",
            "relates_to: []",
            "sources: []",
            "---",
            "",
            "# Custom Profile",
            "",
            "This profile already belongs to the user and should not be overwritten.",
            "",
            "## Snapshot",
            "",
            "- Keep this text intact.",
            "",
        ]
    )
    profile.write_text(custom, encoding="utf-8")

    first = seed_brain(root, preset="core")
    second = seed_brain(root, preset="core")

    assert profile.read_text(encoding="utf-8") == custom
    assert "memory/me/profile.md" in _created_paths(first) or "memory/me/profile.md" in {
        path.as_posix() for path in first.skipped_paths
    }
    assert "memory/me/profile.md" in {path.as_posix() for path in second.skipped_paths}
    assert second.created_paths == []


def test_seed_presets_upgrade_shared_pages_when_they_are_still_generated(tmp_path: Path, monkeypatch) -> None:
    _stub_seed_runtime(monkeypatch)
    root = _prepare_root(tmp_path)

    core = seed_brain(root, preset="core")
    core_index = (root / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[brain]]" not in core_index

    skeleton = seed_brain(root, preset="skeleton")
    skeleton_index = (root / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[brain]]" in skeleton_index
    assert "memory/INDEX.md" in {path.as_posix() for path in skeleton.updated_paths}
    assert "memory/me/open-inquiries.md" in {path.as_posix() for path in skeleton.updated_paths}
    assert "[[how-should-the-system-evolve]]" in (root / "memory" / "me" / "open-inquiries.md").read_text(encoding="utf-8")

    framework = seed_brain(root, preset="framework")
    framework_index = (root / "memory" / "INDEX.md").read_text(encoding="utf-8")
    assert "[[channels]]" in framework_index
    assert "[[contradiction-review]]" in framework_index
    assert "memory/INDEX.md" in {path.as_posix() for path in framework.updated_paths}
