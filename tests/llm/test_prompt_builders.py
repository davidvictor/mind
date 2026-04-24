from __future__ import annotations

from mind.services.prompt_builders import build_applied_to_you_prompt


def test_build_applied_to_you_prompt_accepts_string_frameworks() -> None:
    prompt = build_applied_to_you_prompt(
        title="Superforecasting",
        author="Philip Tetlock",
        profile_context="Founder. Builder.",
        research={
            "tldr": "Forecasting is learnable.",
            "core_argument": "Use probabilities and update often.",
            "key_frameworks": [
                "Bayesian Updating",
                {"name": "Outside View", "summary": "Start from base rates."},
            ],
            "topics": ["decision-science"],
        },
    )

    assert '"name": "Bayesian Updating"' in prompt
    assert '"summary": ""' in prompt
    assert '"name": "Outside View"' in prompt
    assert '"summary": "Start from base rates."' in prompt
