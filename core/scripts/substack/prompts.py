"""Prompt templates for the Substack pipeline.

These live with the substack pipeline (not in scripts/common/) because only
substack uses them. ``mind.services.prompt_builders`` imports them when
building a substack-specific prompt.
"""
from __future__ import annotations


SUMMARIZE_POST = """You are producing a deep-enrichment JSON record for a Substack essay. \
This record feeds a personal knowledge base, so depth and precision matter far more than brevity.

Publication: {publication}
Author: {author}
Title: {title}

%%PRIOR_POSTS%%
%%STANCE_CONTEXT%%
Full post body (markdown):
---
%%BODY%%
---

Return ONLY a JSON object — no markdown fences, no commentary — with EXACTLY these fields \
and schema_version set to the integer 2:

{{
  "schema_version": 2,
  "tldr": "2-4 sentence plain-English summary of the main argument",
  "core_argument": "1-2 paragraph statement of the central thesis (150-300 words)",
  "argument_graph": {{
    "premises": ["..."],
    "inferences": ["..."],
    "conclusion": "..."
  }},
  "key_claims": [
    {{
      "claim": "1 sentence paraphrase of the claim",
      "evidence_quote": "verbatim text copied from the body that supports this claim",
      "evidence_context": "1-2 sentences of surrounding context"
    }}
  ],
  "memorable_examples": [
    {{
      "title": "short label",
      "story": "3-5 sentence retelling",
      "lesson": "1 sentence takeaway"
    }}
  ],
  "notable_quotes": ["verbatim quote", "..."],
  "steelman": "1-2 paragraph best version of the author's argument",
  "strongest_rebuttal": "1 paragraph sharpest critique of the central claim",
  "would_change_mind_if": "1-2 sentence falsifier — concrete evidence that would overturn the central claim",
  "entities": {{
    "people": ["..."],
    "companies": ["..."],
    "tools": ["..."],
    "concepts": ["..."]
  }},
  "in_conversation_with": ["other writers or ideas this post speaks to"],
  "relates_to_prior": [
    {{
      "post_id": "...",
      "post_title": "...",
      "relation": "extends | contradicts | repeats | refines",
      "note": "1-2 sentences"
    }}
  ],
  "topics": ["lowercase-hyphenated", "..."],
  "article": "2-4 paragraph own-words rendering of the post's substance"
}}

Field ranges:
- key_claims: 4-8 entries
- memorable_examples: 2-5 entries
- notable_quotes: max 5 (empty array if none worth preserving verbatim)
- topics: 3-8 tags
- in_conversation_with: 2-8 entries
- entities.*: 0-10 per category; extract aggressively but skip pronouns, stopwords, and the author/publication themselves. Apply the ANTI-SALES RULE below: do NOT extract sponsor brands, the author's own products, or affiliate partners as tracked entities. A passing mention of Athletic Greens does not make Athletic Greens a tracked company. A book by Author X being reviewed in this post DOES make Author X a tracked person — the book is the subject, not chrome.
- relates_to_prior: only populate when prior posts context was supplied (non-empty); otherwise return []. \
When supplied, you MUST explicitly name the relation type for each prior post you reference.

Hard rules:
- evidence_quote MUST be copied verbatim from the body. Paraphrasing is a bug. \
If no verbatim anchor fits, drop that claim from key_claims entirely.
- Never invent claims, quotes, stories, or entities not present in the body.
- For relates_to_prior: only cite prior posts that actually appear in the prior posts context. \
Do not invent post IDs or titles.
- For steelman: write the author's best argument as charitably as possible, not your own opinion.
- For strongest_rebuttal: write the sharpest opposing view, not a mild hedge.
- would_change_mind_if: name concrete evidence (a study, a dataset, a historical case) that would \
overturn the central claim. "More data" or "further research" are not acceptable answers.
- Empty fields return [] (arrays) or "" (strings) — never null.
- schema_version MUST be the integer 2 (not a string).
- Output JSON only. No markdown fences. No commentary before or after the JSON.

Concreteness rule:
Be concrete. Bad: "The author argues that culture matters." Good: "The author opens with the 2016 \
Mylan EpiPen hearing, traces the pricing decision back to a single internal memo, and uses it to \
argue that PBM incentives — not pharma greed — drive specialty drug prices." Use real names, real \
years, and real numbers whenever the body provides them.

%%ANTI_SALES%%
"""


APPLIED_TO_POST = """You are an advisor synthesizing a Substack essay against a specific person's current life and work.

## Post metadata
Title: {title}
Publication: {publication}
Author: {author}

## Person's profile context
%%PROFILE%%

## Pass A deep summary of the essay (compressed)
%%SUMMARY%%

## How to write

- Be specific and concrete. Reference the person's actual projects, values, and threads by name when they apply. Don't say 'your work' — say 'Example Health App' or 'the Brain wiki' or whichever is relevant.
- It's fine if an essay has limited applicability. If only 1-2 ideas really land, write 1-2 strong bullets, not 6 weak ones.
- Don't be sycophantic. If the essay contradicts how the person currently operates, say so plainly. Disagreement is more valuable than affirmation.
- This is an essay, not a book — actions can be smaller and more immediate. 'Before tomorrow's standup, check X' is more valuable than 'this quarter, consider Y'.
- Questions can probe harder than actions. Don't soften them. 'Are you confusing consensus with alignment on Example Health App?' is better than 'Consider whether consensus equals alignment.'
- The reader will see the rest of the essay summary alongside this. Focus on the bridge, not the re-summary.

## Output schema

Return a JSON object with exactly these keys (no extras, no markdown fences):

  applied_paragraph: 1 paragraph (150-250 words) synthesizing how the essay applies to the person right now. Use their first name when natural.
  applied_bullets: array of 4-6 entries, each an object with:
    claim (1 sentence — the essay's idea)
    why_it_matters (1 sentence — why it lands for this person)
    action (1 sentence — concrete next step, this week or sooner)
  socratic_questions: array of 3-5 sharp, specific questions this essay raises about the person's current work. Reference their actual projects by name. These are questions, not directives — end each with '?'. Don't soften them.
  thread_links: array of strings naming the person's specific projects, values, or open threads this essay speaks to. Use exact names from their profile context. These become wiki-links.

Output JSON only.
"""


UPDATE_STANCE = """You are maintaining a living stance document for {author}. You will be given a bounded snapshot of the current stance doc and a new essay they just published. Your job is to propose ONLY the delta: the new bullets that should be appended to any of the four stance sections, plus a one-sentence change note describing what changed.

## Current stance snapshot

%%CURRENT_STANCE%%

## New post

Title: %%POST_TITLE%%
Slug: {post_slug}

## Deep summary of the new post

%%SUMMARY%%

## Rules

- **Do not rewrite the full stance doc.** Return only the NEW bullets that should be appended to the existing doc. If the new post does not bear on a section, omit that section entirely from the delta.
- **Treat the snapshot as representative, not exhaustive.** It is there to help you avoid obvious duplication and to preserve continuity, but you should not try to restate everything that already exists.
- **Stay in the same voice.** The stance doc is written in the author's voice about their own beliefs. Do not write "the author argues" — write "I believe" or "I am uncertain about" as if you were the author maintaining a personal journal of your own positions.
- **Bullet lists only.** Each section is a bulleted list. One idea per bullet. Keep bullets short (1-2 sentences each).
- **No duplicates.** If a belief is already clearly present in the snapshot, do not restate it unless the new post materially changes it.
- **Contradictions observed**: only populate when the new post reveals a tension between something the author said in this post and something they said before (or between this post and something widely believed). Not "things other people disagree with the author about" — specifically contradictions the author themselves has exposed.
- **Recent shifts**: when the new post changes the author's position on something. Include the previous position in parens.
- **Change note**: one sentence describing what this post did to the stance. "Added a new core belief about X." or "Added a contradiction between Y and the prior Z position." or "No material change — this post is a restatement."

## Output schema

Return a JSON object with exactly these keys (no extras, no markdown fences):

  stance_delta_md: a markdown string containing ONLY the sections that should receive NEW bullets. Include only headings that changed, chosen from `## Core beliefs`, `## Open questions`, `## Recent shifts`, and `## Contradictions observed`. Each section body is a bulleted list. Omit unchanged sections. Return "" if there is no material delta.
  change_note: 1 sentence describing what changed.

Output JSON only.
"""


CLASSIFY_LINKS = """You are classifying external links from a Substack essay into three buckets for a personal knowledge base.

Source essay:
  Publication: {publication}
  Title: {title}

Buckets:
  "business" — relevant to professional work, research, tools, companies, industry analysis, or craft
  "personal" — relevant to hobbies, life, learning outside work, entertainment, culture
  "ignore"   — social media profiles, Subscribe/Share/Comment links, author bios, navigation, tracking redirects, anything with no standalone signal

Links to classify:
{links_block}

Return ONLY a JSON object with one field:
- "classifications": a list of objects, one per link, in the same order as given, with fields {{"url": string, "category": "business"|"personal"|"ignore", "reason": string}}

Rules:
- Classify based on URL + anchor text + surrounding context.
- When in doubt between business and personal, prefer personal.
- When in doubt between personal and ignore, prefer ignore (curation discipline).
- Output pure JSON, no markdown fences.
"""
