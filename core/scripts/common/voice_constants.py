"""Shared voice guidance for applied-to-you (Pass B) prompts.

Extracted from the Substack pipeline's APPLIED_TO_POST so that all pipelines
(YouTube, articles, books) produce the same quality of personalization.
"""
from __future__ import annotations


APPLIED_VOICE_GUIDANCE = """\
- Be specific and concrete. Reference the person's actual projects, values, and threads by name.
- It's fine if a source has limited applicability. If only 1-2 ideas land, write 1-2 strong bullets, not 6 weak ones.
- Don't be sycophantic. If the source contradicts how the person currently operates, say so plainly. Disagreement is more valuable than affirmation.
- Questions can probe harder than actions. Don't soften them.
- The reader will see the rest of the summary alongside this. Focus on the bridge, not the re-summary."""
