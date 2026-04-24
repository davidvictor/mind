from __future__ import annotations

import os
from pathlib import Path

import yaml

from mind.dream.common import DreamResult
from mind.dream.campaign import CAMPAIGN_ADAPTER
from mind.dream.simulation import run_simulate_year
from mind.runtime_state import RuntimeState
from tests.support import write_repo_config


def test_simulate_year_runs_campaign_inside_isolated_roots(tmp_path: Path, monkeypatch) -> None:
    write_repo_config(tmp_path, create_indexes=True)
    source_page = tmp_path / "memory" / "concepts" / "live-idea.md"
    source_page.parent.mkdir(parents=True, exist_ok=True)
    source_page.write_text(
        "---\n"
        "id: live-idea\n"
        "type: concept\n"
        "title: Live Idea\n"
        "lifecycle_state: active\n"
        "evidence_count: 1\n"
        "---\n\n"
        "# Live Idea\n",
        encoding="utf-8",
    )
    raw_cache = tmp_path / "raw" / "transcripts" / "articles" / "article-live.pass_d.json"
    raw_cache.parent.mkdir(parents=True, exist_ok=True)
    raw_cache.write_text('{"data": {"ok": true}}\n', encoding="utf-8")
    original_env = {key: os.environ.get(key) for key in ("BRAIN_CONFIG_PATH", "BRAIN_MEMORY_ROOT", "BRAIN_RAW_ROOT", "BRAIN_STATE_ROOT")}
    seen: dict[str, object] = {}

    def fake_run_campaign(*, days, start_date, dry_run, resume, profile):
        seen.update(
            {
                "days": days,
                "start_date": start_date,
                "dry_run": dry_run,
                "resume": resume,
                "profile": profile,
                "memory_root": os.environ["BRAIN_MEMORY_ROOT"],
                "raw_root": os.environ["BRAIN_RAW_ROOT"],
                "state_root": os.environ["BRAIN_STATE_ROOT"],
            }
        )
        memory_root = Path(os.environ["BRAIN_MEMORY_ROOT"])
        simulated_page = memory_root / "concepts" / "live-idea.md"
        simulated_page.write_text(
            "---\n"
            "id: live-idea\n"
            "type: concept\n"
            "title: Live Idea\n"
            "lifecycle_state: mature\n"
            "evidence_count: 2\n"
            "---\n\n"
            "# Live Idea\n\nSimulation changed this copy only.\n",
            encoding="utf-8",
        )
        (memory_root / "dreams" / "rem").mkdir(parents=True, exist_ok=True)
        (memory_root / "dreams" / "rem" / "2026-01.md").write_text("# REM\n", encoding="utf-8")
        RuntimeState.for_repo_root(tmp_path).upsert_adapter_state(
            adapter=CAMPAIGN_ADAPTER,
            state={
                "status": "completed",
                "completed_counts": {"light": 1, "deep": 1, "rem": 1},
                "schedule": [{"day_index": 0, "effective_date": "2026-01-01", "stages": ["light", "deep", "rem"]}],
            },
        )
        return DreamResult(stage="campaign", dry_run=False, summary="sim campaign ok")

    monkeypatch.setattr("mind.dream.simulation.run_campaign", fake_run_campaign)

    result = run_simulate_year(
        repo_root=tmp_path,
        start_date="2026-01-01",
        run_id="test-run",
        days=1,
    )

    assert seen["profile"] == "yearly"
    assert seen["days"] == 1
    assert str(result.simulation_root).endswith("local_data/simulations/test-run")
    assert str(seen["memory_root"]).endswith("local_data/simulations/test-run/memory")
    assert source_page.read_text(encoding="utf-8").endswith("# Live Idea\n")
    assert result.stage_counts == {"light": 1, "deep": 1, "rem": 1}
    assert result.deltas["modified"] == ["concepts/live-idea.md"]
    assert result.deltas["dream_outputs"] == ["dreams/rem/2026-01.md"]
    assert (result.simulation_root / "raw" / "transcripts" / "articles" / "article-live.pass_d.json").exists()
    assert result.report_json_path.exists()
    assert result.report_markdown_path.exists()

    overlay = yaml.safe_load(result.config_path.read_text(encoding="utf-8"))
    assert "dream" not in overlay or "weave" not in overlay.get("dream", {})
    assert {key: os.environ.get(key) for key in original_env} == original_env
