"""Article Pass A prompt-builder tests.

These exercise the pure prompt-construction path in ``mind.services.prompt_builders``.
The transport layer (LLMService / AI Gateway) is covered elsewhere.
"""
from mind.services.prompt_builders import build_summarize_article_prompt


def test_summarize_article_prompt_includes_metadata():
    prompt = build_summarize_article_prompt(
        title="The Future of Aggregators",
        url="https://stratechery.com/2024/aggregators",
        body_markdown="Aggregators are the dominant business model of the internet.",
        sitename="Stratechery",
    )
    assert "The Future of Aggregators" in prompt
    assert "https://stratechery.com/2024/aggregators" in prompt
    assert "Stratechery" in prompt
    assert "Aggregators are the dominant" in prompt


def test_summarize_article_prompt_handles_missing_optional_fields():
    """sitename should be optional (None) — prompt still renders cleanly."""
    prompt = build_summarize_article_prompt(
        title="Plain Title",
        url="https://example.com/x",
        body_markdown="Body.",
        sitename=None,
    )
    assert "Plain Title" in prompt
    assert "Body." in prompt


def test_summarize_article_prompt_includes_anti_sales_rule():
    out = build_summarize_article_prompt(
        title="Test article",
        url="https://example.com/test",
        body_markdown="hello world",
        sitename="Example",
    )
    assert "%%ANTI_SALES%%" not in out, "placeholder should be substituted"
    assert "ANTI-SALES RULE" in out
