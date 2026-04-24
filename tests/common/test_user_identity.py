"""Tests for scripts.common.user_identity."""
from __future__ import annotations

from scripts.common.config import BrainConfig, UserConfig
from scripts.common.user_identity import build_identity_vars, _cached_vars


def test_build_identity_vars_defaults():
    """Default config produces all expected keys."""
    cfg = BrainConfig.defaults()
    iv = build_identity_vars(cfg)
    assert set(iv.keys()) == {
        "user_name", "user_role", "business_description",
        "personal_description", "exclude_description", "classification_rules",
    }
    assert iv["user_name"] == "Example User"
    assert "local-first knowledge worker" in iv["user_role"]


def test_build_identity_vars_custom():
    """Custom user config propagates correctly."""
    cfg = BrainConfig(user=UserConfig(
        name="Alice",
        role="data scientist",
        business_interests=["ML", "stats"],
        personal_interests=["painting"],
        exclude_always=["sports"],
        classification_rules=["Always prefer business"],
    ))
    iv = build_identity_vars(cfg)
    assert iv["user_name"] == "Alice"
    assert iv["user_role"] == "data scientist"
    assert "ML" in iv["business_description"]
    assert "painting" in iv["personal_description"]
    assert "sports" in iv["exclude_description"]
    assert "Always prefer business" in iv["classification_rules"]


def test_no_hardcoded_identity_in_classify_video_prompt():
    """build_classify_video_prompt should not contain hardcoded identity."""
    from mind.services.prompt_builders import build_classify_video_prompt
    # Use a custom config to verify the prompt uses the config, not hardcoded strings
    cfg = BrainConfig(user=UserConfig(
        name="TestUser",
        role="underwater basket weaver",
    ))
    prompt = build_classify_video_prompt("Test Video", "Test Channel", cfg=cfg)
    assert "underwater basket weaver" in prompt
    assert "design engineer" not in prompt


def test_no_hardcoded_identity_in_classify_book_prompt():
    """build_classify_book_prompt should not contain hardcoded identity."""
    from mind.services.prompt_builders import build_classify_book_prompt
    cfg = BrainConfig(user=UserConfig(
        name="TestUser",
        role="underwater basket weaver",
    ))
    prompt = build_classify_book_prompt("Test Book", "Test Author", cfg=cfg)
    assert "underwater basket weaver" in prompt
    assert "design engineer" not in prompt


def test_no_hardcoded_identity_in_research_book_deep_prompt():
    """build_research_book_deep_prompt should not contain hardcoded identity."""
    from mind.services.prompt_builders import build_research_book_deep_prompt
    cfg = BrainConfig(user=UserConfig(
        name="TestUser",
        role="underwater basket weaver",
    ))
    prompt = build_research_book_deep_prompt("Test Book", "Test Author", cfg=cfg)
    assert "underwater basket weaver" in prompt
    assert "design engineer" not in prompt


def test_cache_invalidates_on_different_config():
    """Different configs should produce different results."""
    _cached_vars.cache_clear()
    cfg1 = BrainConfig(user=UserConfig(name="Alice", role="scientist"))
    cfg2 = BrainConfig(user=UserConfig(name="Bob", role="engineer"))
    iv1 = build_identity_vars(cfg1)
    iv2 = build_identity_vars(cfg2)
    assert iv1["user_name"] == "Alice"
    assert iv2["user_name"] == "Bob"


def test_no_hardcoded_identity_in_applied_to_you_prompt():
    """build_applied_to_you_prompt should use config identity, not hardcoded."""
    from mind.services.prompt_builders import build_applied_to_you_prompt
    cfg = BrainConfig(user=UserConfig(name="TestUser", role="underwater basket weaver"))
    prompt = build_applied_to_you_prompt(
        "Test Book", "Test Author", "profile context",
        {"tldr": "x", "core_argument": "y", "key_frameworks": [], "topics": []},
        cfg=cfg,
    )
    assert "TestUser" in prompt
    assert "underwater basket weaver" in prompt
    assert "design engineer" not in prompt


def test_no_hardcoded_identity_in_applied_to_video_prompt():
    """build_applied_to_video_prompt should use config identity."""
    from mind.services.prompt_builders import build_applied_to_video_prompt
    cfg = BrainConfig(user=UserConfig(name="TestUser", role="underwater basket weaver"))
    prompt = build_applied_to_video_prompt(
        "Test Video", "Test Channel", "profile context",
        {"tldr": "x", "core_argument": "y", "key_claims": [], "topics": []},
        cfg=cfg,
    )
    assert "TestUser" in prompt
    assert "underwater basket weaver" in prompt
    assert "design engineer" not in prompt


def test_no_hardcoded_identity_in_applied_to_article_prompt():
    """build_applied_to_article_prompt should use config identity."""
    from mind.services.prompt_builders import build_applied_to_article_prompt
    cfg = BrainConfig(user=UserConfig(name="TestUser", role="underwater basket weaver"))
    prompt = build_applied_to_article_prompt(
        "Test Article", "https://example.com", "profile context",
        {"tldr": "x", "core_argument": "y", "key_claims": [], "topics": []},
        sitename="Example",
        cfg=cfg,
    )
    assert "TestUser" in prompt
    assert "underwater basket weaver" in prompt
    assert "design engineer" not in prompt


def test_applied_to_video_prompt_structure():
    """build_applied_to_video_prompt includes expected sections."""
    from mind.services.prompt_builders import build_applied_to_video_prompt
    _cached_vars.cache_clear()
    prompt = build_applied_to_video_prompt(
        "Test Video", "Test Channel", "profile context",
        {"tldr": "x", "core_argument": "y", "key_claims": [], "topics": []},
    )
    assert "applied_paragraph" in prompt
    assert "applied_bullets" in prompt
    assert "socratic_questions" in prompt
    assert "thread_links" in prompt
    assert "Person's profile context" in prompt


def test_applied_to_article_prompt_structure():
    """build_applied_to_article_prompt includes expected sections."""
    from mind.services.prompt_builders import build_applied_to_article_prompt
    _cached_vars.cache_clear()
    prompt = build_applied_to_article_prompt(
        "Test Article", "https://example.com", "profile context",
        {"tldr": "x", "core_argument": "y", "key_claims": [], "topics": []},
        sitename="Example",
    )
    assert "applied_paragraph" in prompt
    assert "applied_bullets" in prompt
    assert "socratic_questions" in prompt
    assert "thread_links" in prompt
    assert "Person's profile context" in prompt


def test_applied_to_you_prompt_requires_exact_thread_link_names():
    from mind.services.prompt_builders import build_applied_to_you_prompt
    _cached_vars.cache_clear()
    prompt = build_applied_to_you_prompt(
        "Test Book",
        "Test Author",
        "---\nid: open-inquiries\n---\n# Open Inquiries\n\n---\nid: positioning\n---\n# Positioning\n\n---\nid: values\n---\n# Values\n",
        {"tldr": "x", "core_argument": "y", "key_frameworks": [], "topics": []},
    )
    assert "Use the exact canonical name or id" in prompt
    assert "Good: `open-inquiries`" in prompt
    assert "Good: `Example Studio`" in prompt
    assert "Bad: `Open inquiry:" in prompt
