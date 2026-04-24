"""BrainConfig — typed schema for Brain's public/private config surface.

Public defaults live in `config.example.yaml`. Private runtime config lives in
`local_data/config.yaml` or the file/directory named by `BRAIN_CONFIG_PATH`.
Anything opinionated about "how the engine thinks" stays in code or
docs/SYSTEM_RULES.md.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import AliasChoices, BaseModel, Field


BRAIN_CONFIG_PATH_ENV = "BRAIN_CONFIG_PATH"
BRAIN_LOCAL_DATA_ROOT_ENV = "BRAIN_LOCAL_DATA_ROOT"
BRAIN_MEMORY_ROOT_ENV = "BRAIN_MEMORY_ROOT"
BRAIN_RAW_ROOT_ENV = "BRAIN_RAW_ROOT"
BRAIN_DROPBOX_ROOT_ENV = "BRAIN_DROPBOX_ROOT"
BRAIN_STATE_ROOT_ENV = "BRAIN_STATE_ROOT"


class VaultConfig(BaseModel):
    wiki_dir: str = "wiki"
    raw_dir: str = "raw"
    dropbox_dir: Optional[str] = None
    state_dir: Optional[str] = None
    owner_profile: str = "me/profile.md"  # relative to wiki_dir


class PathsConfig(BaseModel):
    local_data_root: str = "local_data"
    memory_root: Optional[str] = None
    raw_root: Optional[str] = None
    dropbox_root: Optional[str] = None
    state_root: Optional[str] = None


class StateConfig(BaseModel):
    runtime_db: Optional[str] = None
    graph_db: Optional[str] = None
    sources_db: Optional[str] = None


class RetrievalConfig(BaseModel):
    backend: Literal["sqlite", "file"] = "sqlite"
    vector_db: Optional[str] = None


class LLMRouteConfig(BaseModel):
    model: Optional[str] = None
    transport: Optional[Literal["ai_gateway"]] = None
    api_family: Optional[Literal["responses"]] = None
    input_mode: Optional[Literal["text", "media", "file"]] = None
    supports_strict_schema: Optional[bool] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None
    timeout_seconds: Optional[int] = None
    truncation: Optional[Literal["auto", "disabled"]] = None
    reasoning_effort: Optional[Literal["low", "medium", "high"]] = None
    gateway_options: Optional[dict[str, Any]] = None
    provider_options: Optional[dict[str, dict[str, Any]]] = None


class LLMTransportConfig(BaseModel):
    mode: Literal["ai_gateway"] = "ai_gateway"


class LLMConfig(BaseModel):
    model: str = "google/gemini-3.1-flash-lite-preview"
    transport: LLMTransportConfig = Field(default_factory=LLMTransportConfig)
    routes: dict[str, LLMRouteConfig] = Field(default_factory=dict)
    backup: Optional[LLMRouteConfig] = None
    min_balance_usd: float = 1.0
    concurrency: dict[str, int] = Field(default_factory=dict)


class IngestorsConfig(BaseModel):
    enabled: list[str] = Field(default_factory=lambda: [
        "substack", "articles", "youtube", "books", "audible"
    ])


class DreamConfig(BaseModel):
    enabled: bool = True
    working_set_cap: int = 300
    merge_window_days: int = 14
    cooccurrence_threshold: int = 5
    tail_rescan_enabled: bool = True
    snippet_max_chars: int = 160
    log_fold_threshold: int = 50
    rem_hotset_cap: int = 120
    rem_cluster_limit: int = 8
    rem_prune_brake_pct: int = 15
    nudge_auto_archive_days: int = 30
    probationary_stale_warn_days: int = 90
    active_synthesis: "DreamActiveSynthesisConfig" = Field(default_factory=lambda: DreamActiveSynthesisConfig())
    external_grounding: "DreamExternalGroundingConfig" = Field(default_factory=lambda: DreamExternalGroundingConfig())
    quality: "DreamQualityConfig" = Field(default_factory=lambda: DreamQualityConfig())
    campaign: "DreamCampaignConfig" = Field(default_factory=lambda: DreamCampaignConfig())
    v2: "DreamV2Config" = Field(default_factory=lambda: DreamV2Config())


class DreamActiveSynthesisThresholds(BaseModel):
    concept: int = 5
    playbook: int = 3
    stance: int = 5
    inquiry: int = 3


class DreamActiveSynthesisConfig(BaseModel):
    enabled: bool = True
    max_atoms_per_run: int = 25
    cooldown_days: int = 14
    synthesis_version: int = 1
    maturity_thresholds: DreamActiveSynthesisThresholds = Field(default_factory=DreamActiveSynthesisThresholds)


class DreamExternalGroundingConfig(BaseModel):
    enabled: bool = True
    max_atoms_per_run: int = 3
    cooldown_days: int = 30
    min_evidence_count: int = 8
    freshness_window_days: int = 90
    max_queries_per_atom: int = 2
    max_results_per_query: int = 3


class DreamQualityConfig(BaseModel):
    persist_receipts: bool = True


class DreamCampaignProfileConfig(BaseModel):
    light_interval_days: Optional[int] = None
    deep_interval_days: Optional[int] = None
    rem_interval_days: Optional[int] = None
    light_working_set_cap: Optional[int] = None
    deep_probationary_cap: Optional[int] = None
    deep_progress_every_probationaries: Optional[int] = None
    deep_active_synthesis_max_atoms_per_run: Optional[int] = None
    deep_active_synthesis_cooldown_days: Optional[int] = None
    deep_external_grounding_max_atoms_per_run: Optional[int] = None
    deep_external_grounding_cooldown_days: Optional[int] = None
    rem_hotset_cap: Optional[int] = None
    rem_cluster_limit: Optional[int] = None
    rem_candidate_multiplier: Optional[int] = None
    lane_relaxation_mode: Optional[Literal["strict", "relation_only"]] = None
    rem_decline_after_weak_months: Optional[int] = None
    rem_archive_after_weak_months: Optional[int] = None
    apply_cap_miss_lifecycle_changes: Optional[bool] = None
    write_audit_nudges: Optional[bool] = None
    emit_verbose_mutations: Optional[bool] = None
    checkpoint_every_sources: Optional[int] = None
    fast_forward_skip_unchanged_light: Optional[bool] = None


class DreamCampaignConfig(BaseModel):
    light_interval_days: int = 1
    deep_interval_days: int = 7
    rem_interval_days: int = 30
    light_working_set_cap: int = 600
    deep_probationary_cap: int = 0
    deep_progress_every_probationaries: int = 100
    deep_active_synthesis_max_atoms_per_run: int = 60
    deep_active_synthesis_cooldown_days: int = 7
    deep_external_grounding_max_atoms_per_run: int = 8
    deep_external_grounding_cooldown_days: int = 14
    rem_hotset_cap: int = 240
    rem_cluster_limit: int = 16
    rem_candidate_multiplier: int = 5
    lane_relaxation_mode: Literal["strict", "relation_only"] = "relation_only"
    rem_decline_after_weak_months: int = 2
    rem_archive_after_weak_months: int = 3
    apply_cap_miss_lifecycle_changes: bool = True
    write_audit_nudges: bool = True
    emit_verbose_mutations: bool = True
    checkpoint_every_sources: int = 50
    fast_forward_skip_unchanged_light: bool = False
    yearly: DreamCampaignProfileConfig = Field(
        default_factory=lambda: DreamCampaignProfileConfig(
            deep_interval_days=14,
            light_working_set_cap=300,
            deep_probationary_cap=250,
            deep_progress_every_probationaries=100,
            deep_active_synthesis_max_atoms_per_run=25,
            deep_active_synthesis_cooldown_days=14,
            deep_external_grounding_max_atoms_per_run=3,
            deep_external_grounding_cooldown_days=30,
            rem_hotset_cap=120,
            rem_cluster_limit=8,
            rem_candidate_multiplier=3,
            lane_relaxation_mode="strict",
            apply_cap_miss_lifecycle_changes=False,
            write_audit_nudges=False,
            emit_verbose_mutations=False,
            checkpoint_every_sources=50,
        )
    )


class DreamV2Config(BaseModel):
    artifact_root: str = "raw/reports/dream/v2"
    kene: "DreamV2KeneConfig" = Field(default_factory=lambda: DreamV2KeneConfig())


class DreamV2KeneConfig(BaseModel):
    enabled: bool = True
    mode: Literal["shadow"] = "shadow"
    max_atoms: int = 80
    max_prior_artifacts: int = 24
    campaign_enabled: bool = False


class AntiSalesConfig(BaseModel):
    enabled: bool = True   # opt-out — anti-sales rule is part of the engine's philosophy
    allow_brands: list[str] = Field(default_factory=list)


class DreamPromotionEntry(BaseModel):
    min_distinct_sources: int
    min_days_observed: int = 0


LEGACY_INQUIRY_PROMOTION_KEY = "open" + "-thread"


class DreamPromotionConfig(BaseModel):
    concept: DreamPromotionEntry = DreamPromotionEntry(min_distinct_sources=3)
    playbook: DreamPromotionEntry = DreamPromotionEntry(min_distinct_sources=2)
    stance: DreamPromotionEntry = DreamPromotionEntry(min_distinct_sources=5, min_days_observed=14)
    inquiry: DreamPromotionEntry = Field(
        default_factory=lambda: DreamPromotionEntry(min_distinct_sources=1),
        # Read-only compatibility for pre-lock configs; canonical serialization stays "inquiry".
        validation_alias=AliasChoices("inquiry", LEGACY_INQUIRY_PROMOTION_KEY),
    )


class SkillsConfig(BaseModel):
    auto_activate_skills: bool = False


class ChromeFirecrawlConfig(BaseModel):
    max_requests_per_run: int = 0
    max_candidates_per_run: int = 25
    crawl_cooldown_days: int = 30
    retry_cooldown_days: int = 7


class ChromeConfig(BaseModel):
    enabled: bool = False
    profile_root: str = ""
    profiles: list[str] = Field(default_factory=list)
    history_days: int = 30
    raw_query_retention_days: int = 90
    triage_confidence_threshold: float = 0.75
    firecrawl: ChromeFirecrawlConfig = Field(default_factory=ChromeFirecrawlConfig)


class UserConfig(BaseModel):
    """Who is using this knowledge base? Drives classification and prompt framing."""
    name: str = "Example User"
    role: str = "local-first knowledge worker"
    business_interests: list[str] = Field(default_factory=list)
    personal_interests: list[str] = Field(default_factory=list)
    exclude_always: list[str] = Field(default_factory=list)
    classification_rules: list[str] = Field(default_factory=list)


class BrainConfig(BaseModel):
    paths: PathsConfig = Field(default_factory=PathsConfig)
    vault: VaultConfig = Field(
        default_factory=VaultConfig,
        validation_alias=AliasChoices("vault", "memory"),
    )
    state: StateConfig = Field(default_factory=StateConfig)
    retrieval: RetrievalConfig = Field(default_factory=RetrievalConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    user: UserConfig = Field(default_factory=UserConfig)
    ingestors: IngestorsConfig = Field(default_factory=IngestorsConfig)
    dream: DreamConfig = Field(default_factory=DreamConfig)
    anti_sales: AntiSalesConfig = Field(default_factory=AntiSalesConfig)
    atom_promotion: DreamPromotionConfig = Field(default_factory=DreamPromotionConfig)
    skills: SkillsConfig = Field(default_factory=SkillsConfig)
    chrome: ChromeConfig = Field(default_factory=ChromeConfig)

    @staticmethod
    def _legacy_paths(root: Path) -> list[Path]:
        return [root / "mind.toml", root / "brain.config.yaml"]

    @classmethod
    def _raise_for_legacy_paths(cls, root: Path) -> None:
        legacy = [path.name for path in cls._legacy_paths(root) if path.exists()]
        if legacy:
            joined = ", ".join(sorted(legacy))
            raise RuntimeError(
                f"Unsupported legacy config file(s): {joined}. "
                f"Migrate to local_data/config.yaml or set {BRAIN_CONFIG_PATH_ENV}."
            )

    @classmethod
    def _resolve_override_path(cls, root: Path) -> Path | None:
        override = os.environ.get(BRAIN_CONFIG_PATH_ENV, "").strip()
        if not override:
            return None
        override_path = Path(override)
        if not override_path.is_absolute():
            override_path = root / override_path
        if override_path.is_dir():
            override_path = override_path / "config.yaml"
        if not override_path.exists():
            raise RuntimeError(
                f"{BRAIN_CONFIG_PATH_ENV} points to a missing config file: {override_path}"
            )
        if override_path.name in {"mind.toml", "brain.config.yaml"}:
            raise RuntimeError(
                f"{BRAIN_CONFIG_PATH_ENV} points to an unsupported legacy config file: {override_path.name}. "
                f"Migrate to local_data/config.yaml or point {BRAIN_CONFIG_PATH_ENV} at a supported YAML overlay."
            )
        return override_path

    @classmethod
    def resolved_config_path(cls, root: Path) -> Path | None:
        if not root.is_dir():
            return root if root.exists() else None
        cls._raise_for_legacy_paths(root)
        override_path = cls._resolve_override_path(root)
        if override_path is not None:
            return override_path
        local_path = root / "local_data" / "config.yaml"
        if local_path.exists():
            return local_path
        yaml_path = root / "config.yaml"
        if yaml_path.exists():
            return yaml_path
        example_path = root / "config.example.yaml"
        return example_path if example_path.exists() else None

    @classmethod
    def describe_active_config(cls, root: Path) -> str:
        if not root.is_dir():
            return str(root) if root.exists() else "<defaults>"
        cls._raise_for_legacy_paths(root)
        base_path = root / "config.yaml"
        local_path = root / "local_data" / "config.yaml"
        override_path = cls._resolve_override_path(root)
        if override_path is not None:
            if local_path.exists():
                base_label = str(local_path)
            elif base_path.exists():
                base_label = str(base_path)
            elif (root / "config.example.yaml").exists():
                base_label = str(root / "config.example.yaml")
            else:
                base_label = "<defaults>"
            return f"{base_label} + overlay {override_path}"
        if local_path.exists():
            return str(local_path)
        if base_path.exists():
            return str(base_path)
        example_path = root / "config.example.yaml"
        return str(example_path) if example_path.exists() else "<defaults>"

    @staticmethod
    def _read_yaml(path: Path) -> dict[str, Any]:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise RuntimeError(f"Expected mapping at config path: {path}")
        return payload

    @classmethod
    def _deep_merge(cls, base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base)
        for key, value in override.items():
            current = merged.get(key)
            if isinstance(current, dict) and isinstance(value, dict):
                merged[key] = cls._deep_merge(current, value)
            else:
                merged[key] = value
        return merged

    @classmethod
    def load(cls, path: Path) -> "BrainConfig":
        """Load config from a repo root or explicit YAML path."""
        if path.is_dir():
            cls._raise_for_legacy_paths(path)
            local_path = path / "local_data" / "config.yaml"
            root_config_path = path / "config.yaml"
            example_path = path / "config.example.yaml"
            yaml_path = (
                local_path
                if local_path.exists()
                else root_config_path
                if root_config_path.exists()
                else example_path
            )
            base_payload = cls._read_yaml(yaml_path) if yaml_path.exists() else {}
            override_path = cls._resolve_override_path(path)
            if override_path is not None:
                override_payload = cls._read_yaml(override_path)
                return cls(**cls._deep_merge(base_payload, override_payload))
            if base_payload:
                return cls(**base_payload)
            return cls()
        if not path.exists():
            return cls()
        if path.name in {"mind.toml", "brain.config.yaml"}:
            raise RuntimeError(
                f"Unsupported legacy config file: {path.name}. "
                f"Migrate to local_data/config.yaml or set {BRAIN_CONFIG_PATH_ENV}."
            )
        return cls(**(yaml.safe_load(path.read_text(encoding="utf-8")) or {}))

    @classmethod
    def defaults(cls) -> "BrainConfig":
        return cls()
