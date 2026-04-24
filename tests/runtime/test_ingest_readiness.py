from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from mind.services.ingest_readiness import GraphHealthSnapshot, run_ingest_readiness
from mind.services.reingest import ArticleRepairPlan, ArticleRepairResult
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_indexes=True)


def test_run_ingest_readiness_fails_when_dropbox_and_articles_are_not_ready(tmp_path: Path, monkeypatch):
    _write_config(tmp_path)

    monkeypatch.setattr(
        "mind.services.dropbox.sweep_dropbox",
        lambda repo_root, dry_run=True, limit=None: SimpleNamespace(
            predicted_process_count=1,
            predicted_review_count=1,
            predicted_fail_count=0,
            metadata={},
            outcomes=[],
            render=lambda: "dropbox dry-run",
        ),
    )
    monkeypatch.setattr(
        "mind.services.ingest_readiness.build_graph_health",
        lambda repo_root, include_promotion_gate=False: GraphHealthSnapshot(
            graph_built=True,
            node_count=10,
            edge_count=5,
            document_count=10,
            embedding_model="openai/text-embedding-3-small",
            embedding_count=12,
            embedding_backend="sqlite",
            embedding_backend_count=12,
            shadow_mode="advisory-only",
            promotion_gate_passed=None,
            promotion_gate_artifact_json=None,
            promotion_gate_artifact_markdown=None,
            issues=(),
        ),
    )

    def _lane_result(request, repo_root):
        blocked = 2 if request.lane == "articles" else 0
        return SimpleNamespace(
            plan=SimpleNamespace(selected_count=3, blocked_count=blocked),
        )

    monkeypatch.setattr("mind.services.ingest_readiness.run_reingest", _lane_result)
    monkeypatch.setattr(
        "mind.services.ingest_readiness.render_reingest_report",
        lambda run: f"blocked={run.plan.blocked_count}",
    )
    monkeypatch.setattr(
        "mind.services.ingest_readiness.run_article_repair",
        lambda repo_root, limit=None, apply=False: ArticleRepairResult(
            plan=ArticleRepairPlan(
                source_label="all-drop-files",
                items=(),
            ),
            applied=False,
        ),
    )
    monkeypatch.setattr(
        "mind.services.ingest_readiness.render_article_repair_report",
        lambda result: "article repair",
    )

    result = run_ingest_readiness(repo_root=tmp_path)

    assert not result.passed
    assert any("dropbox dry-run predicts 1 review-required files" in issue for issue in result.issues)
    assert any("articles reingest dry-run still has 2 blocked items" in issue for issue in result.issues)
    assert result.report_json_path.exists()
    assert result.report_markdown_path.exists()
