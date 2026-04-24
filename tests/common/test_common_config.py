"""Tests for scripts/common/config.py BrainConfig loader."""
from __future__ import annotations

from pathlib import Path
import pytest


def test_brain_config_loads_minimal_yaml(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n"
    )
    cfg = BrainConfig.load(cfg_path)
    assert cfg.vault.wiki_dir == "memory"
    assert cfg.llm.model == "google/gemini-2.5-pro"
    assert cfg.dream.enabled is True  # default
    assert cfg.anti_sales.enabled is True  # default
    assert cfg.anti_sales.allow_brands == []


def test_brain_config_anti_sales_allow_brands_loads(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n  wiki_dir: memory\n  raw_dir: raw\n  owner_profile: me/profile.md\n"
        "llm:\n  model: google/gemini-2.5-pro\n"
        "anti_sales:\n  enabled: true\n  allow_brands: [Notion, Linear]\n"
    )
    cfg = BrainConfig.load(cfg_path)
    assert cfg.anti_sales.allow_brands == ["Notion", "Linear"]


def test_brain_config_ignores_legacy_provider_field(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n  wiki_dir: memory\n  raw_dir: raw\n  owner_profile: me/profile.md\n"
        "llm:\n  provider: cohere\n  model: command-r\n"
    )
    cfg = BrainConfig.load(cfg_path)
    assert cfg.llm.model == "command-r"


def test_brain_config_loads_root_yaml_with_vault_fields_and_skill_knob(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: openai/gpt-5\n"
        "skills:\n"
        "  auto_activate_skills: true\n"
        "atom_promotion:\n"
        "  inquiry:\n"
        "    min_distinct_sources: 2\n"
        "    min_days_observed: 0\n"
    )
    cfg = BrainConfig.load(cfg_path)
    assert cfg.vault.wiki_dir == "memory"
    assert cfg.llm.model == "openai/gpt-5"
    assert cfg.skills.auto_activate_skills is True
    assert cfg.atom_promotion.inquiry.min_distinct_sources == 2


def test_brain_config_normalizes_legacy_open_thread_promotion_key(tmp_path: Path):
    from scripts.common.config import BrainConfig

    legacy_key = "open" + "-thread"
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "atom_promotion:\n"
        f"  {legacy_key}:\n"
        "    min_distinct_sources: 4\n"
        "    min_days_observed: 2\n",
        encoding="utf-8",
    )

    cfg = BrainConfig.load(cfg_path)

    assert cfg.atom_promotion.inquiry.min_distinct_sources == 4
    assert cfg.atom_promotion.inquiry.min_days_observed == 2


def test_brain_config_rejects_legacy_config_files(tmp_path: Path):
    from scripts.common.config import BrainConfig

    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n",
        encoding="utf-8",
    )
    (tmp_path / "mind.toml").write_text(
        "[llm]\nprovider = \"openai\"\nmodel = \"gpt-5\"\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Unsupported legacy config file"):
        BrainConfig.load(tmp_path)


def test_brain_config_campaign_defaults_and_override_load(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "dream:\n"
        "  campaign:\n"
        "    light_interval_days: 2\n"
        "    deep_interval_days: 9\n"
        "    rem_interval_days: 31\n"
        "    lane_relaxation_mode: strict\n"
        "    rem_archive_after_weak_months: 4\n",
        encoding="utf-8",
    )

    cfg = BrainConfig.load(cfg_path)

    assert cfg.dream.campaign.light_interval_days == 2
    assert cfg.dream.campaign.deep_interval_days == 9
    assert cfg.dream.campaign.rem_interval_days == 31
    assert cfg.dream.campaign.lane_relaxation_mode == "strict"
    assert cfg.dream.campaign.rem_archive_after_weak_months == 4
    assert cfg.dream.campaign.rem_decline_after_weak_months == 2
    assert cfg.dream.campaign.yearly.lane_relaxation_mode == "strict"
    assert cfg.dream.campaign.yearly.deep_interval_days == 14
    assert cfg.dream.campaign.yearly.deep_probationary_cap == 250
    assert cfg.dream.campaign.yearly.apply_cap_miss_lifecycle_changes is False
    assert cfg.dream.campaign.yearly.write_audit_nudges is False
    assert cfg.dream.campaign.yearly.emit_verbose_mutations is False
    assert cfg.dream.weave.enabled is True
    assert cfg.dream.weave.run_after_rem is True
    assert cfg.dream.weave.candidate_cap == 400
    assert cfg.dream.weave.cluster_limit == 12


def test_brain_config_weave_override_loads(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "dream:\n"
        "  weave:\n"
        "    enabled: true\n"
        "    run_after_rem: false\n"
        "    candidate_cap: 120\n"
        "    min_cluster_size: 4\n"
        "    hub_link_member_limit: 2\n",
        encoding="utf-8",
    )

    cfg = BrainConfig.load(cfg_path)

    assert cfg.dream.weave.enabled is True
    assert cfg.dream.weave.run_after_rem is False
    assert cfg.dream.weave.candidate_cap == 120
    assert cfg.dream.weave.min_cluster_size == 4
    assert cfg.dream.weave.hub_link_member_limit == 2


def test_brain_config_dream_v2_override_loads(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "dream:\n"
        "  v2:\n"
        "    artifact_root: raw/reports/dream/v2-custom\n"
        "    weave_shadow_enabled: true\n"
        "    weave_window_size: 18\n"
        "    weave_max_local_clusters: 2\n",
        encoding="utf-8",
    )

    cfg = BrainConfig.load(cfg_path)

    assert cfg.dream.v2.artifact_root == "raw/reports/dream/v2-custom"
    assert cfg.dream.v2.weave_shadow_enabled is True
    assert cfg.dream.v2.weave_window_size == 18
    assert cfg.dream.v2.weave_max_local_clusters == 2


def test_brain_config_loads_route_knobs_from_yaml(tmp_path: Path):
    from scripts.common.config import BrainConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  routes:\n"
        "    dream_decision:\n"
        "      model: openai/gpt-5.4-mini\n"
        "      supports_strict_schema: true\n"
        "      top_p: 0.2\n"
        "      truncation: disabled\n"
        "      gateway_options:\n"
        "        only: [vertex]\n"
        "      provider_options:\n"
        "        openai:\n"
        "          reasoningEffort: high\n",
        encoding="utf-8",
    )

    cfg = BrainConfig.load(cfg_path)

    route = cfg.llm.routes["dream_decision"]
    assert route.model == "openai/gpt-5.4-mini"
    assert route.supports_strict_schema is True
    assert route.top_p == 0.2
    assert route.truncation == "disabled"
    assert route.gateway_options == {"only": ["vertex"]}
    assert route.provider_options == {"openai": {"reasoningEffort": "high"}}


def test_brain_config_applies_overlay_from_brain_config_path(tmp_path: Path, monkeypatch):
    from scripts.common.config import BRAIN_CONFIG_PATH_ENV, BrainConfig

    (tmp_path / "config.yaml").write_text(
        "vault:\n"
        "  wiki_dir: memory\n"
        "  raw_dir: raw\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  routes:\n"
        "    dream_decision:\n"
        "      model: google/gemini-3.1-flash-lite-preview\n"
        "      supports_strict_schema: false\n"
        "      timeout_seconds: 480\n",
        encoding="utf-8",
    )
    overlay_path = tmp_path / "dream-eval.yaml"
    overlay_path.write_text(
        "llm:\n"
        "  routes:\n"
        "    dream_decision:\n"
        "      model: openai/gpt-5.4-mini\n"
        "      supports_strict_schema: true\n"
        "      truncation: disabled\n",
        encoding="utf-8",
    )
    monkeypatch.setenv(BRAIN_CONFIG_PATH_ENV, overlay_path.name)

    cfg = BrainConfig.load(tmp_path)

    route = cfg.llm.routes["dream_decision"]
    assert route.model == "openai/gpt-5.4-mini"
    assert route.supports_strict_schema is True
    assert route.timeout_seconds == 480
    assert route.truncation == "disabled"


def test_brain_config_describe_active_config_includes_overlay(tmp_path: Path, monkeypatch):
    from scripts.common.config import BRAIN_CONFIG_PATH_ENV, BrainConfig

    (tmp_path / "config.yaml").write_text("vault:\n  wiki_dir: memory\n", encoding="utf-8")
    overlay_path = tmp_path / "override.yaml"
    overlay_path.write_text("llm:\n  model: openai/gpt-5\n", encoding="utf-8")
    monkeypatch.setenv(BRAIN_CONFIG_PATH_ENV, overlay_path.name)

    description = BrainConfig.describe_active_config(tmp_path)

    assert "overlay" in description
    assert str(overlay_path) in description
