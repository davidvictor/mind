"""Shared prompt builders for the Brain LLM service seam."""
from __future__ import annotations

import json
from typing import Any

from scripts.substack.prompts import APPLIED_TO_POST, CLASSIFY_LINKS, SUMMARIZE_POST, UPDATE_STANCE

TRANSCRIPT_CHAR_CAP = 80_000
CLASSIFY_VIDEO_PROMPT_VERSION = "youtube.classification.v4"
CLASSIFY_BOOK_PROMPT_VERSION = "books.classification.v3"
SUMMARIZE_TRANSCRIPT_PROMPT_VERSION = "youtube.summary.v2"
RESEARCH_BOOK_PROMPT_VERSION = "books.research.v2"
RESEARCH_BOOK_DEEP_PROMPT_VERSION = "books.research.deep.v2"
SUMMARIZE_BOOK_RESEARCH_PROMPT_VERSION = "books.summary.v2"
APPLIED_TO_YOU_PROMPT_VERSION = "books.personalization.v3"
APPLIED_TO_VIDEO_PROMPT_VERSION = "youtube.personalization.v1"
APPLIED_TO_ARTICLE_PROMPT_VERSION = "articles.personalization.v1"
SUMMARIZE_ARTICLE_PROMPT_VERSION = "articles.summary.v2"
SUMMARIZE_SUBSTACK_PROMPT_VERSION = "substack.summary.v1"
APPLIED_TO_POST_PROMPT_VERSION = "substack.personalization.v1"
UPDATE_AUTHOR_STANCE_PROMPT_VERSION = "substack.stance.v1"
CLASSIFY_LINKS_PROMPT_VERSION = "substack.classification.links.v1"
GENERATE_SKILL_PROMPT_VERSION = "dream.generate-skill.v1"
ONBOARDING_SYNTHESIS_PROMPT_VERSION = "onboarding.synthesis.semantic.v1"
ONBOARDING_GRAPH_PROMPT_VERSION = "onboarding.synthesis.graph.v1"
ONBOARDING_GRAPH_CHUNK_PROMPT_VERSION = "onboarding.synthesis.graph-chunk.v1"
ONBOARDING_MERGE_PROMPT_VERSION = "onboarding.merge.v1"
ONBOARDING_MERGE_CHUNK_PROMPT_VERSION = "onboarding.merge.chunk.v2"
ONBOARDING_MERGE_RELATIONSHIPS_PROMPT_VERSION = "onboarding.merge.relationships.v1"
ONBOARDING_VERIFY_PROMPT_VERSION = "onboarding.verify.v1"
ONBOARDING_MATERIALIZATION_PROMPT_VERSION = "onboarding.materialization-plan.v4"


def build_classify_video_prompt(
    title: str,
    channel: str,
    description: str = "",
    tags: list[str] | None = None,
    cfg: Any = None,
) -> str:
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars(cfg)
    prompt = (
        "You are a strict content classifier for a personal knowledge base. "
        "You will be shown a YouTube video's title and channel and asked to "
        "assign a durable content policy. You will NOT see the video itself.\n\n"
        "## Policy axes\n\n"
        "**retention**\n"
        "- `keep` — the source belongs in the knowledge base\n"
        "- `exclude` — the source should not be materialized or synthesized\n\n"
        "**domains**\n"
        f"- `business` — directly relevant to the user's work as a {iv['user_role']}\n"
        "- `personal` — relevant to the user's broader personal interests or life\n"
        "- use both only when the source clearly informs both work and life\n\n"
        "**synthesis_mode**\n"
        "- `deep` — high-value source eligible for full concept/playbook/stance extraction\n"
        "- `light` — keep and summarize the source, but do not aggressively mine it downstream\n"
        "- `none` — only valid with `retention=exclude`\n\n"
        "## Classification guidance\n\n"
        f"**business** — content related to the user's work as a {iv['user_role']}. "
        f"Includes: {iv['business_description']}, design tools (Figma, etc.), interviews "
        "with builders, conference talks on tech/product/design.\n\n"
        f"**personal** — general intellectual content unrelated to the user's work. "
        f"Includes: {iv['personal_description']}, deep-dive interviews on non-tech subjects, "
        "academic lectures.\n\n"
        f"**exclude** — everything else. Includes (but not limited to): {iv['exclude_description']}, "
        "food/cooking, travel vlogs, fitness routines, "
        "makeup/beauty, entertainment, movie trailers. "
        "Cars are explicitly always excluded.\n\n"
        "## Rules\n\n"
        "- Output exactly one retention value, one synthesis_mode value, and one or two domains.\n"
        f"{iv['classification_rules']}\n"
        "- Use both domains conservatively: only when the source clearly informs both work and life.\n"
        "- Kept business content defaults to `deep`.\n"
        "- Kept personal content defaults to `light`.\n"
        "- Personal content should only be `deep` when it is clearly formative and broadly insightful, not merely interesting.\n"
        "- Exclude documentary-style curiosity content by default when it is mainly about places, disasters, hidden locations, ghost towns, stately homes, tours, abandoned sites, transport history, ships, submarines, buildings, or travel-like exploration.\n"
        "- Exclude geopolitics, national-warning commentary, war explainers, and current-events analysis by default unless they are directly about technology, science, product strategy, or systems the user is likely to apply.\n"
        "- Exclude archaeology/civilization mystery content by default when it is framed as legends, unexplained ancient sites, controversial theories, or historical spectacle rather than rigorous analytical science.\n"
        "- Keep hard science, math, frontier scientific instrumentation, philosophy, contemplative frameworks, and knowledge-systems/meta-intellectual content when the video is primarily analytical rather than scenic or documentary.\n"
        "- If the title is generic and the channel name doesn't help, default to "
        "exclude. The user can recover false negatives manually; false positives "
        "waste API budget on transcription.\n\n"
        "## Output\n\n"
        "Return a JSON object with these exact keys:\n"
        "  retention: 'keep' | 'exclude'\n"
        "  domains: array containing 'business' and/or 'personal'\n"
        "  synthesis_mode: 'deep' | 'light' | 'none'\n"
        "  confidence: one of 'high' | 'medium' | 'low'\n"
        "  reasoning: one sentence explaining the classification\n\n"
        f"## Video metadata\n\nTitle: {title}\nChannel: {channel}\n"
    )
    if description:
        prompt += f"\n## Description (first 2000 chars)\n\n{description[:2000]}\n"
    if tags:
        prompt += f"\n## Tags\n\n{', '.join(tags[:10])}\n"
    return prompt + "\nOutput JSON only."


def build_classify_book_prompt(title: str, author: str, cfg: Any = None) -> str:
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars(cfg)
    return (
        "You are a content classifier for a personal knowledge base. You assign "
        "a durable content policy to each book.\n\n"
        "## Policy axes\n\n"
        "**retention**\n"
        "- `keep` — the book belongs in the knowledge base\n"
        "- `exclude` — the book should not be materialized or synthesized\n\n"
        "**domains**\n"
        f"- `business` — directly relevant to the user's work as a {iv['user_role']}\n"
        "- `personal` — relevant to the user's broader personal interests or life\n"
        "- use both only when the book clearly informs both work and life\n\n"
        "**synthesis_mode**\n"
        "- `deep` — high-value book eligible for full concept/playbook/stance extraction\n"
        "- `light` — keep and summarize the book, but do not aggressively mine it downstream\n"
        "- `none` — only valid with `retention=exclude`\n\n"
        "## Browsing compatibility\n\n"
        "Also return a compatibility `category` for existing storage and browsing:\n"
        "- `business`\n"
        "- `personal`\n"
        "- `fiction`\n"
        "- `ignore`\n\n"
        "## Classification guidance\n\n"
        f"**business** — related to the user's work as a {iv['user_role']}. "
        f"Includes: {iv['business_description']}, "
        "founder/operator memoirs, technology "
        "history, computing history, design history, business strategy, "
        "tech-company histories, aerospace engineering history.\n\n"
        "**personal** — books the user wants to keep for personal reading, curiosity, culture, or life relevance.\n\n"
        "**fiction** — novels, short story collections, narrative storytelling. "
        "Anything that is not a true account or a non-fiction analysis.\n\n"
        "## Optional subcategory (only for compatibility `category=personal`)\n\n"
        "If compatibility category is `personal`, also assign exactly ONE optional subcategory:\n"
        "- **history** — wars, eras, civilizations, specific historical events, "
        "  industrial/scientific history, disasters, place histories\n"
        "- **science** — hard sciences (biology, physics, chemistry, neuroscience, "
        "  math, evolution, paleontology, geology, astronomy)\n"
        "- **biography** — substantive biographies of historical figures (presidents, "
        "  scientists, industrialists, artists). NOT celebrity memoirs.\n"
        "- **politics** — current affairs, political memoirs, political journalism, "
        "  geopolitics, political analysis\n"
        "- **memoir** — celebrity memoirs, personal stories, ghostwritten "
        "  autobiographies, comedy/music/sports memoirs\n"
        "- **self-help** — productivity, personal development, frameworks for "
        "  living, diet/fitness, spiritual self-improvement, popular psychology\n"
        "- **culture** — anthropology, archaeology, ideas books, food/drink "
        "  history, language, religion/mysticism, fringe-history, anything that "
        "  doesn't fit the others but is intellectual non-fiction\n\n"
        "If compatibility category is not `personal`, set subcategory to null.\n\n"
        "## Rules\n\n"
        "- Output exactly one retention value, one synthesis_mode value, and one or two domains.\n"
        "- Kept business books default to `deep`.\n"
        "- Kept personal books default to `light`.\n"
        "- Use both domains conservatively: only when the book clearly informs both work and life.\n"
        "- Books should only be `deep` when they are clearly formative and broadly insightful.\n"
        f"{iv['classification_rules']}\n"
        "- When in doubt between fiction and personal, prefer personal if it's "
        "  ANY kind of true account (memoir, history, journalism). Fiction is "
        "  reserved for actual storytelling/novels.\n"
        "- Books are usually kept. Only use `retention=exclude` for obvious junk, filler, or material the system should deliberately suppress.\n"
        "- Substantive biographies (Carnegie, Da Vinci, Franklin, Tesla) go in "
        "  biography. Celebrity self-told memoirs (Spare, Will, Greenlights) go "
        "  in memoir.\n"
        "- Self-help is its own subcategory — don't try to upgrade it. Subtle "
        "  Art / Power of Now / 4-Hour Workweek = self-help.\n"
        "- Fringe-history (Hancock, Lazar, Oak Island) goes in personal/culture, "
        "  not business and not its own thing.\n\n"
        "## Output\n\n"
        "Return a JSON object with these exact keys:\n"
        "  retention: 'keep' | 'exclude'\n"
        "  domains: array containing 'business' and/or 'personal'\n"
        "  synthesis_mode: 'deep' | 'light' | 'none'\n"
        "  category: 'business' | 'personal' | 'fiction' | 'ignore'\n"
        "  subcategory: one of the personal subcategories above, OR null if not personal\n"
        "  confidence: 'high' | 'medium' | 'low'\n"
        "  reasoning: one sentence explaining the classification\n\n"
        f"## Book\n\nTitle: {title}\nAuthor: {author}\n\n"
        "Use widely-known information about the book. Output JSON only."
    )


def build_summarize_transcript_prompt(
    title: str,
    channel: str,
    transcript: str,
    cfg: Any = None,
    stance_context: str = "",
    prior_sources_context: str = "",
) -> tuple[str, bool]:
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars(cfg)
    truncated = False
    if len(transcript) > TRANSCRIPT_CHAR_CAP:
        transcript = transcript[:TRANSCRIPT_CHAR_CAP]
        truncated = True

    stance_block = ""
    if stance_context and stance_context.strip():
        stance_block = f"## Creator stance context\n\n{stance_context.strip()}\n\n"

    prior_block = ""
    if prior_sources_context and prior_sources_context.strip():
        prior_block = f"## Prior videos from this channel\n\n{prior_sources_context.strip()}\n\n"

    return (
        "You are producing a deep-enrichment JSON record for a YouTube video transcript. "
        "This record feeds a personal knowledge base, so depth and precision matter far more than brevity.\n\n"
        f"## Reader context (for relevance weighting only)\n"
        f"The reader is a {iv['user_role']}. Professional interests: {iv['business_description']}.\n"
        "When the source touches the reader's domains, lean into those angles.\n"
        "Do NOT personalize — that happens in Pass B.\n\n"
        f"{stance_block}"
        f"{prior_block}"
        "Return ONLY a JSON object — no markdown fences, no commentary — with EXACTLY these fields "
        "and schema_version set to the integer 2:\n\n"
        '  schema_version: 2\n'
        '  tldr: "2-4 sentence summary"\n'
        '  core_argument: "1-2 paragraph central thesis (150-300 words)"\n'
        '  argument_graph: { premises: [], inferences: [], conclusion: "" }\n'
        '  key_claims: [{ claim, evidence_quote, evidence_context }]\n'
        '  memorable_examples: [{ title, story, lesson }]\n'
        '  notable_quotes: ["verbatim from transcript"]\n'
        '  steelman: ""\n'
        '  strongest_rebuttal: ""\n'
        '  would_change_mind_if: ""\n'
        '  entities: { people: [], companies: [], tools: [], concepts: [] }\n'
        '  in_conversation_with: []\n'
        '  takeaways: []\n'
        '  topics: []\n'
        '  article: "400-700 word synthesis"\n\n'
        "Field ranges:\n"
        "- key_claims: 4-8 entries\n"
        "- memorable_examples: 2-5 entries\n"
        "- notable_quotes: max 5 (empty array if none worth preserving verbatim)\n"
        "- topics: 3-8 tags (lowercase-hyphenated, no leading #)\n"
        "- in_conversation_with: 2-8 entries\n"
        "- entities.*: 0-10 per category; extract aggressively but skip pronouns, "
        "stopwords, and the speaker themselves. Do NOT extract sponsor brands or "
        "affiliate partners as tracked entities.\n\n"
        "Hard rules:\n"
        "- evidence_quote MUST be copied verbatim from the transcript. "
        "If no verbatim anchor fits, drop that claim from key_claims entirely.\n"
        "- Never invent claims, quotes, stories, or entities not present in the transcript.\n"
        "- For steelman: write the speaker's best argument as charitably as possible.\n"
        "- For strongest_rebuttal: write the sharpest opposing view, not a mild hedge.\n"
        "- would_change_mind_if: name concrete evidence that would overturn the central claim.\n"
        "- Empty fields return [] (arrays) or \"\" (strings) — never null.\n"
        "- schema_version MUST be the integer 2.\n\n"
        "Concreteness rule:\n"
        "Be concrete. Use real names, real years, and real numbers whenever the "
        "transcript provides them.\n\n"
        f"## Video metadata\n\nTitle: {title}\nChannel: {channel}\n\n"
        f"## Transcript\n\n{transcript}\n"
    ), truncated


def build_research_book_prompt(title: str, author: str) -> str:
    return (
        f"You are researching the book '{title}' by {author} for a personal knowledge base. "
        "Use only widely-available public information (publisher summaries, reputable reviews, "
        "author interviews). Do NOT reproduce any copyrighted text from the book itself. "
        "Return a JSON object with these exact keys:\n\n"
        "  tldr: 1-paragraph plain-language summary\n"
        "  key_claims: array of 5-10 entries, each {claim: short phrase, evidence_context: 2-3 sentences}\n"
        "  frameworks_introduced: array of named frameworks/models the book introduces (may be empty)\n"
        "  in_conversation_with: array of 2-5 books/thinkers this book is in dialogue with\n"
        "  notable_quotes: empty array (do NOT reproduce any copyrighted text)\n"
        "  topics: array of 3-7 lowercase-hyphenated topic tags\n\n"
        "Output JSON only, no markdown fences."
    )


def build_research_book_deep_prompt(title: str, author: str, cfg: Any = None) -> str:
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars(cfg)
    return (
        f"You are an expert nonfiction reader writing a deep, structured "
        f"reference summary of '{title}' by {author} for a personal knowledge "
        f"base. The reader is a {iv['user_role']} who "
        "wants substantive, applied takeaways — not a Wikipedia stub.\n\n"
        "Use widely-available public information about the book: publisher "
        "synopsis, reputable book reviews, author interviews, and talks. "
        "Do NOT reproduce long copyrighted passages. Up to 5 famous, widely-"
        "attributed quotes are allowed if confidently known.\n\n"
        "## Output schema\n\n"
        "Return a single JSON object with these exact keys (no extras, "
        "no markdown fences):\n\n"
        "  tldr: 2-4 sentence summary\n"
        "  core_argument: 1-2 paragraph statement of the central thesis\n"
        "  key_frameworks: array of 5-9 entries, each with name, summary, worked_example\n"
        "  memorable_examples: array of 3-6 entries, each with title, story, lesson\n"
        "  counterarguments: array of 2-4 critique strings\n"
        "  notable_quotes: array of up to 5 entries, each with quote and context\n"
        "  in_conversation_with: array of 4-8 strings naming related books or thinkers\n"
        "  topics: array of 5-10 lowercase-hyphenated topic tags\n\n"
        "Output JSON only."
    )


def build_summarize_book_research_prompt(*, title: str, author: str, research: dict[str, Any]) -> str:
    research_json = json.dumps(research, ensure_ascii=False)
    return (
        f"You are transforming a rich research artifact about '{title}' by {author} into a compact, reusable summary artifact "
        "for a personal knowledge base. Preserve the substance, but compress it into a summary that still supports downstream "
        "wiki pages and synthesis.\n\n"
        "Return JSON only with these exact keys:\n"
        "  tldr\n"
        "  core_argument\n"
        "  key_frameworks\n"
        "  memorable_examples\n"
        "  counterarguments\n"
        "  notable_quotes\n"
        "  in_conversation_with\n"
        "  topics\n\n"
        f"## Research artifact\n\n{research_json}\n"
    )


def build_applied_to_you_prompt(
    title: str,
    author: str,
    profile_context: str,
    research: dict[str, Any],
    cfg: Any = None,
) -> str:
    normalized_frameworks: list[dict[str, str]] = []
    for item in (research.get("key_frameworks") or []):
        if isinstance(item, dict):
            normalized_frameworks.append(
                {
                    "name": str(item.get("name", "")),
                    "summary": str(item.get("summary", "")),
                }
            )
            continue
        text = str(item).strip()
        if text:
            normalized_frameworks.append({"name": text, "summary": ""})

    research_summary = json.dumps(
        {
            "tldr": research.get("tldr", ""),
            "core_argument": research.get("core_argument", ""),
            "key_frameworks": normalized_frameworks,
            "topics": research.get("topics", []),
        },
        ensure_ascii=False,
    )
    from scripts.common.voice_constants import APPLIED_VOICE_GUIDANCE
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars(cfg)
    return (
        f"You are an advisor synthesizing a book against {iv['user_name']}'s "
        f"current life and work. {iv['user_name']} is a {iv['user_role']}. "
        "You will be given the person's profile context "
        "and a structured research summary of a book. Your job is to write a "
        "personal advisory note connecting the book to the person.\n\n"
        "This is a book — the ideas have been refined through editing. Actions "
        "can be larger and more strategic than for an essay or video.\n\n"
        f"## How to write\n\n{APPLIED_VOICE_GUIDANCE}\n\n"
        "## Output schema\n\n"
        "Return JSON only with exact keys:\n"
        f"  applied_paragraph: 1 paragraph (150-250 words) using {iv['user_name']}'s first name when natural\n"
        "  applied_bullets: array of 4-6 entries, each {claim, why_it_matters, action}\n"
        "  socratic_questions: array of 3-5 sharp, specific questions this book raises "
        "about the person's current work. Reference their actual projects by name. "
        "End each with '?'. Don't soften them.\n"
        "  thread_links: array of strings naming the person's specific projects, values, or open threads.\n\n"
        "## Thread-link rules\n\n"
        "These thread_links are later rendered as wiki-links, so exact naming matters.\n"
        "- Use the exact canonical name or id that already appears in the profile context.\n"
        "- If the profile context shows a note id in frontmatter, prefer that exact id string when relevant.\n"
        "- For owner-note threads, prefer exact ids like `open-inquiries`, `positioning`, and `values` over descriptive paraphrases.\n"
        "- For named projects, use the exact project name as written in the profile context, such as `Example Product` or `Example Studio`.\n"
        "- Do NOT decorate thread_links with explanations or prefixes.\n"
        "- Bad: `Open inquiry: how far can the one-builder model scale before coherence breaks`\n"
        "- Bad: `Positioning constraint: coherence across brand, product, and technical architecture`\n"
        "- Bad: `Example Product - methodology to platform evolution inquiry`\n"
        "- Good: `open-inquiries`\n"
        "- Good: `positioning`\n"
        "- Good: `values`\n"
        "- Good: `Example Product`\n"
        "- Good: `Example Studio`\n\n"
        f"## Person's profile context\n\n{profile_context}\n\n"
        f"## Book\n\nTitle: {title}\nAuthor: {author}\n\n"
        f"## Book research summary\n\n{research_summary}\n\n"
        "Output JSON only."
    )


def build_applied_to_video_prompt(
    title: str,
    channel: str,
    profile_context: str,
    summary: dict[str, Any],
    cfg: Any = None,
) -> str:
    """Build the applied-to-you prompt for a YouTube video."""
    from scripts.common.voice_constants import APPLIED_VOICE_GUIDANCE
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars(cfg)
    compressed = json.dumps(
        {
            "tldr": summary.get("tldr", ""),
            "core_argument": summary.get("core_argument", ""),
            "key_claims": [
                {"claim": (c.get("claim", "") if isinstance(c, dict) else str(c))}
                for c in (summary.get("key_claims") or [])
            ],
            "topics": summary.get("topics", []),
        },
        ensure_ascii=False,
    )
    return (
        f"You are an advisor synthesizing a YouTube video against {iv['user_name']}'s "
        f"current life and work. {iv['user_name']} is a {iv['user_role']}. "
        "You will be given the person's profile context "
        "and a compressed summary of the video. Your job is to write a "
        "personal advisory note connecting the video to the person.\n\n"
        "This is a video — ideas are often less polished than in writing. "
        "Actions can be smaller and more immediate: 'before tomorrow's standup, check X'.\n\n"
        f"## How to write\n\n{APPLIED_VOICE_GUIDANCE}\n\n"
        "## Output schema\n\n"
        "Return a JSON object with exactly these keys (no extras, no markdown fences):\n\n"
        f"  applied_paragraph: 1 paragraph (150-250 words) using {iv['user_name']}'s first name when natural\n"
        "  applied_bullets: array of 3-6 entries, each {claim, why_it_matters, action}\n"
        "  socratic_questions: array of 3-5 sharp, specific questions this video raises "
        "about the person's current work. Reference their actual projects by name. "
        "End each with '?'.\n"
        "  thread_links: array of strings naming the person's specific projects, values, "
        "or open threads this video speaks to.\n\n"
        f"## Person's profile context\n\n{profile_context}\n\n"
        f"## Video\n\nTitle: {title}\nChannel: {channel}\n\n"
        f"## Video summary (compressed)\n\n{compressed}\n\n"
        "Output JSON only."
    )


def build_applied_to_article_prompt(
    title: str,
    url: str,
    profile_context: str,
    summary: dict[str, Any],
    sitename: str | None = None,
    cfg: Any = None,
) -> str:
    """Build the applied-to-you prompt for a web article."""
    from scripts.common.voice_constants import APPLIED_VOICE_GUIDANCE
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars(cfg)
    compressed = json.dumps(
        {
            "tldr": summary.get("tldr", ""),
            "core_argument": summary.get("core_argument", ""),
            "key_claims": [
                {"claim": (c.get("claim", "") if isinstance(c, dict) else str(c))}
                for c in (summary.get("key_claims") or [])
            ],
            "topics": summary.get("topics", []),
        },
        ensure_ascii=False,
    )
    outlet = sitename or ""
    return (
        f"You are an advisor synthesizing a web article against {iv['user_name']}'s "
        f"current life and work. {iv['user_name']} is a {iv['user_role']}. "
        "You will be given the person's profile context "
        "and a compressed summary of the article. Your job is to write a "
        "personal advisory note connecting the article to the person.\n\n"
        "This is a web article — ideas may be less refined than a book. "
        "Actions should be concrete and near-term.\n\n"
        f"## How to write\n\n{APPLIED_VOICE_GUIDANCE}\n\n"
        "## Output schema\n\n"
        "Return a JSON object with exactly these keys (no extras, no markdown fences):\n\n"
        f"  applied_paragraph: 1 paragraph (150-250 words) using {iv['user_name']}'s first name when natural\n"
        "  applied_bullets: array of 3-6 entries, each {claim, why_it_matters, action}\n"
        "  socratic_questions: array of 3-5 sharp, specific questions this article raises "
        "about the person's current work. Reference their actual projects by name. "
        "End each with '?'.\n"
        "  thread_links: array of strings naming the person's specific projects, values, "
        "or open threads this article speaks to.\n\n"
        f"## Person's profile context\n\n{profile_context}\n\n"
        f"## Article\n\nTitle: {title}\nOutlet: {outlet}\nURL: {url}\n\n"
        f"## Article summary (compressed)\n\n{compressed}\n\n"
        "Output JSON only."
    )


SUMMARIZE_ARTICLE_PROMPT = """You are producing a deep-enrichment JSON record for a web article. \
This record feeds a personal knowledge base, so depth and precision matter far more than brevity.

Source URL: {url}
Outlet: {sitename}
Title: {title}

%%READER_CONTEXT%%

%%STANCE_CONTEXT%%

%%PRIOR_SOURCES%%

Full article body (markdown):
---
{body}
---

Return ONLY a JSON object — no markdown fences, no commentary — with EXACTLY these fields \
and schema_version set to the integer 2:

  schema_version: 2
  tldr: "2-4 sentence summary"
  core_argument: "1-2 paragraph central thesis (150-300 words)"
  argument_graph: {{ premises: [], inferences: [], conclusion: "" }}
  key_claims: [{{ claim, evidence_quote, evidence_context }}]
  memorable_examples: [{{ title, story, lesson }}]
  notable_quotes: ["verbatim from body"]
  steelman: ""
  strongest_rebuttal: ""
  would_change_mind_if: ""
  entities: {{ people: [], companies: [], tools: [], concepts: [] }}
  in_conversation_with: []
  takeaways: []
  topics: []
  article: "400-700 word synthesis"

Field ranges:
- key_claims: 4-8 entries
- memorable_examples: 2-5 entries
- notable_quotes: max 5 (empty array if none worth preserving verbatim)
- topics: 3-8 tags (lowercase-hyphenated, no #)
- in_conversation_with: 2-8 entries
- entities.*: 0-10 per category; extract aggressively but skip pronouns, \
stopwords, and the author themselves. Do NOT extract sponsor brands or affiliate \
partners as tracked entities.

Hard rules:
- evidence_quote MUST be copied verbatim from the body. If no verbatim anchor fits, \
drop that claim from key_claims entirely.
- Never invent claims, quotes, stories, or entities not present in the body.
- For steelman: write the author's best argument as charitably as possible.
- For strongest_rebuttal: write the sharpest opposing view, not a mild hedge.
- would_change_mind_if: name concrete evidence that would overturn the central claim. \
"More data" or "further research" are not acceptable.
- Empty fields return [] (arrays) or "" (strings) — never null.
- schema_version MUST be the integer 2.

Concreteness rule:
Be concrete. Use real names, real years, and real numbers whenever the body provides them.

%%ANTI_SALES%%
"""


def _build_anti_sales_block(vault: Any = None) -> str:
    """Build the anti-sales prompt block, honoring vault config when provided."""
    from scripts.common.anti_sales import ANTI_SALES_RULE_PROMPT
    if vault is None:
        return ANTI_SALES_RULE_PROMPT
    if not vault.config.anti_sales.enabled:
        return ""
    block = ANTI_SALES_RULE_PROMPT
    if vault.config.anti_sales.allow_brands:
        brands = ", ".join(vault.config.anti_sales.allow_brands)
        block += (
            f"\n\nException: the user has opted to track these brands as "
            f"first-class entities even if they appear only in promotional chrome: {brands}."
        )
    return block


def build_summarize_article_prompt(
    *,
    title: str,
    url: str,
    body_markdown: str,
    sitename: str | None,
    vault: Any = None,
    stance_context: str = "",
    prior_sources_context: str = "",
) -> str:
    """Build the substituted SUMMARIZE_ARTICLE prompt for a single article.

    Pure function — no network calls, no I/O.
    """
    from scripts.common.user_identity import build_identity_vars
    iv = build_identity_vars()
    sitename_str = sitename or ""
    reader_context = (
        f"## Reader context (for relevance weighting only)\n"
        f"The reader is a {iv['user_role']}. Professional interests: {iv['business_description']}.\n"
        "When the source touches the reader's domains, lean into those angles.\n"
        "Do NOT personalize — that happens in Pass B."
    )
    stance_block = ""
    if stance_context and stance_context.strip():
        stance_block = f"## Author stance context\n\n{stance_context.strip()}"
    prior_block = ""
    if prior_sources_context and prior_sources_context.strip():
        prior_block = f"## Prior articles from this author/outlet\n\n{prior_sources_context.strip()}"
    prompt = (
        SUMMARIZE_ARTICLE_PROMPT
        .replace("{url}", url)
        .replace("{sitename}", sitename_str)
        .replace("{title}", title)
        .replace("{body}", body_markdown)
    )
    prompt = prompt.replace("%%READER_CONTEXT%%", reader_context)
    prompt = prompt.replace("%%STANCE_CONTEXT%%", stance_block)
    prompt = prompt.replace("%%PRIOR_SOURCES%%", prior_block)
    prompt = prompt.replace("%%ANTI_SALES%%", _build_anti_sales_block(vault))
    return prompt


def build_summarize_substack_prompt(
    *,
    title: str,
    publication: str,
    author: str,
    body_markdown: str,
    prior_posts_context: str = "",
    stance_context: str = "",
    vault: Any = None,
) -> str:
    """Build the substituted SUMMARIZE_POST prompt for a single substack post.

    Pure function — no network calls, no I/O.

    All variable-content fields are injected via marker replacement AFTER .format()
    runs, so that braces (JSX, generics, templates, config syntax) in any of those
    strings can't collide with Python's format mini-language. Only the safe,
    author-controlled metadata fields (title, publication, author) go through .format().
    """
    prompt = SUMMARIZE_POST.format(
        title=title,
        publication=publication,
        author=author,
    )
    prompt = prompt.replace("%%PRIOR_POSTS%%", prior_posts_context)
    prompt = prompt.replace("%%STANCE_CONTEXT%%", stance_context)
    prompt = prompt.replace("%%BODY%%", body_markdown)
    prompt = prompt.replace("%%ANTI_SALES%%", _build_anti_sales_block(vault))
    return prompt


def build_applied_to_post_prompt(
    title: str,
    publication: str,
    author: str,
    profile_context: str,
    summary: dict[str, Any],
) -> str:
    compressed = json.dumps(
        {
            "tldr": summary.get("tldr", ""),
            "core_argument": summary.get("core_argument", ""),
            "key_claims": [{"claim": (item or {}).get("claim", "")} for item in (summary.get("key_claims") or [])],
            "topics": summary.get("topics", []),
        },
        ensure_ascii=False,
    )
    prompt = APPLIED_TO_POST.format(title=title, publication=publication, author=author)
    prompt = prompt.replace("%%PROFILE%%", profile_context)
    prompt = prompt.replace("%%SUMMARY%%", compressed)
    return prompt


def build_update_author_stance_prompt(
    author: str,
    title: str,
    post_slug: str,
    current_stance: str,
    summary: dict[str, Any],
) -> str:
    compressed = json.dumps(
        {
            "tldr": summary.get("tldr", ""),
            "core_argument": summary.get("core_argument", ""),
            "key_claims": [{"claim": (item or {}).get("claim", "")} for item in (summary.get("key_claims") or [])],
            "topics": summary.get("topics", []),
        },
        ensure_ascii=False,
    )
    prompt = UPDATE_STANCE.format(author=author, post_slug=post_slug)
    prompt = prompt.replace("%%POST_TITLE%%", title)
    prompt = prompt.replace(
        "%%CURRENT_STANCE%%",
        current_stance or "(no prior stance doc — this is the first ingest of this author)",
    )
    prompt = prompt.replace("%%SUMMARY%%", compressed)
    return prompt


def _sanitize_for_prompt(value: str) -> str:
    return " ".join(str(value or "").split()).replace("{", "{{").replace("}", "}}").replace('"', "'")


def build_classify_links_prompt(post_title: str, publication: str, links: list[dict[str, str]]) -> str:
    links_block = "\n".join(
        f'{idx + 1}. url={_sanitize_for_prompt(link["url"])}\n'
        f'   anchor="{_sanitize_for_prompt(link["anchor_text"])}"\n'
        f'   context="{_sanitize_for_prompt(link["context_snippet"])}"'
        for idx, link in enumerate(links)
    )
    return CLASSIFY_LINKS.format(title=post_title, publication=publication, links_block=links_block)


def build_generate_skill_prompt(
    *,
    task_description: str,
    context_text: str = "",
) -> str:
    return (
        "You are generating a reusable Codex skill from a concrete workflow description.\n\n"
        "Return markdown only. The result should be a practical SKILL.md draft with:\n"
        "- name\n"
        "- description\n"
        "- when to use\n"
        "- procedure\n"
        "- output format\n\n"
        f"## Task description\n\n{task_description}\n\n"
        f"## Optional context\n\n{context_text}\n"
    )


def build_onboarding_synthesis_instructions(*, bundle_id: str) -> str:
    return (
        "You are the semantic synthesizer for Brain onboarding.\n\n"
        "You will receive:\n"
        "- a normalized onboarding evidence bundle\n"
        "- an onboarding interview transcript\n"
        "- zero or more uploaded files\n\n"
        "Treat all of them as first-class evidence. Read across them, infer the richest grounded model you can, "
        "and return JSON only.\n\n"
        "Rules:\n"
        "- Do not invent entities, relationships, values, or positioning claims that are not grounded in the evidence.\n"
        "- Inference is encouraged, but every inferred item must still cite evidence_refs that point back to the bundle or uploads.\n"
        "- Families for entities must be one of: projects, people, concepts, playbooks, stances, inquiries.\n"
        "- Keep titles human-readable and slugs lowercase-hyphenated.\n"
        "- Use domains such as work, identity, relationships, craft, business, personal, meta only when clearly grounded.\n"
        "- relationships.source_ref and relationships.target_ref must refer to 'owner' or to an entity proposal_id.\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, owner, entities, relationships, synthesis_notes\n\n"
        "owner must contain:\n"
        "name, role, location, summary, values, positioning, open_inquiries\n\n"
        "values entries must contain: text, evidence_refs\n"
        "positioning must contain: summary, work_priorities, life_priorities, constraints, evidence_refs\n"
        "open_inquiries entries must contain: slug, question, evidence_refs\n"
        "entities entries must contain:\n"
        "proposal_id, family, title, slug, summary, domains, aliases, evidence_refs, attributes\n"
        "relationships entries must contain:\n"
        "source_ref, target_ref, relation_type, rationale, evidence_refs\n\n"
        f"Bundle ID: {bundle_id}\n"
        "Output JSON only."
    )


def build_onboarding_graph_prompt(
    *,
    bundle: dict[str, Any],
    semantic_artifact: dict[str, Any],
) -> str:
    return (
        "You are the graph shaper for Brain onboarding.\n\n"
        "Turn the semantic onboarding synthesis into graph proposals.\n"
        "Do not make merge decisions against the existing graph yet. Only shape candidate nodes and edges.\n\n"
        "Rules:\n"
        "- page_type must be one of: project, person, concept, playbook, stance, inquiry.\n"
        "- Keep proposal_id stable from the semantic artifact.\n"
        "- relates_to_refs may only contain 'owner' or proposal_ids from the semantic artifact.\n"
        "- attributes should preserve grounded structured details such as priorities, constraints, position, question, confidence, or role.\n"
        "- Every node and edge must preserve evidence_refs.\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, node_proposals, edge_proposals, notes\n\n"
        "node_proposals entries must contain:\n"
        "proposal_id, page_type, slug, title, summary, domains, aliases, evidence_refs, attributes, relates_to_refs\n"
        "edge_proposals entries must contain:\n"
        "source_ref, target_ref, relation_type, rationale, evidence_refs\n\n"
        "## Normalized bundle\n\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Semantic synthesis artifact\n\n"
        f"{json.dumps(semantic_artifact, ensure_ascii=False, sort_keys=True)}\n"
    )


def build_onboarding_graph_chunk_prompt(
    *,
    bundle: dict[str, Any],
    semantic_chunk: dict[str, Any],
) -> str:
    return (
        "You are the graph shaper for one Brain onboarding chunk.\n\n"
        "Turn only this semantic chunk into graph proposals.\n"
        "Return proposals for the entities in this chunk and any grounded edges that touch them.\n"
        "Do not make merge decisions.\n\n"
        "Rules:\n"
        "- page_type must be one of: project, person, concept, playbook, stance, inquiry.\n"
        "- Keep proposal_id stable from the semantic chunk.\n"
        "- relates_to_refs may only contain 'owner' or proposal_ids present in the chunk relationships.\n"
        "- Preserve evidence_refs on every node and edge.\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, node_proposals, edge_proposals, notes\n\n"
        "## Normalized bundle\n\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Semantic chunk\n\n"
        f"{json.dumps(semantic_chunk, ensure_ascii=False, sort_keys=True)}\n"
    )


def build_onboarding_merge_prompt(
    *,
    bundle: dict[str, Any],
    graph_artifact: dict[str, Any],
    candidate_context: dict[str, Any],
) -> str:
    return (
        "You are the merge planner for Brain onboarding.\n\n"
        "You are given onboarding graph proposals plus candidate matches from the existing graph. "
        "Decide whether each proposal should create a new page, update an existing page, or merge into an existing page. "
        "This is an AI-native decision pass; use the candidate context and proposal evidence, not heuristics.\n\n"
        "Rules:\n"
        "- action must be one of: create, update, merge.\n"
        "- Use create when the proposal is meaningfully new.\n"
        "- Use update or merge only when the candidate context clearly indicates the same underlying node.\n"
        "- target_path, target_page_id, and target_page_type are required for update/merge and must be null for create.\n"
        "- relationship_decisions action must be one of: keep, drop.\n"
        "- Preserve rationale and evidence_refs for every decision.\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, decisions, relationship_decisions, notes\n\n"
        "decisions entries must contain:\n"
        "proposal_id, action, target_page_id, target_page_type, target_path, rationale, evidence_refs\n"
        "relationship_decisions entries must contain:\n"
        "source_ref, target_ref, action, rationale, evidence_refs\n\n"
        "## Normalized bundle\n\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Graph proposal artifact\n\n"
        f"{json.dumps(graph_artifact, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Existing graph candidate context\n\n"
        f"{json.dumps(candidate_context, ensure_ascii=False, sort_keys=True)}\n"
    )


def build_onboarding_merge_chunk_prompt(
    *,
    bundle: dict[str, Any],
    graph_chunk: dict[str, Any],
) -> str:
    return (
        "You are the merge planner for one Brain onboarding node chunk.\n\n"
        "Decide create/update/merge for only the node proposals in this chunk.\n"
        "Do not emit relationship decisions in this pass.\n\n"
        "Rules:\n"
        "- action must be one of: create, update, merge.\n"
        "- source_proposal_id must equal proposal_id.\n"
        "- Copy title, slug, summary, page_type, domains, and relates_to from the source proposal into the decision.\n"
        "- relates_to must contain only proposal_ids from the chunk or the literal 'owner'. Do not emit final wiki-links.\n"
        "- target_path, target_page_id, and target_page_type are required for update/merge and null for create.\n"
        "- Preserve rationale and evidence_refs for every decision.\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, decisions, notes\n\n"
        "decisions entries must contain:\n"
        "proposal_id, source_proposal_id, action, title, slug, summary, page_type, domains, relates_to, target_page_id, target_page_type, target_path, rationale, evidence_refs\n\n"
        "## Normalized bundle\n\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Merge node chunk\n\n"
        f"{json.dumps(graph_chunk, ensure_ascii=False, sort_keys=True)}\n"
    )


def build_onboarding_merge_relationships_prompt(
    *,
    bundle: dict[str, Any],
    kept_nodes: list[dict[str, Any]],
    edge_proposals: list[dict[str, Any]],
) -> str:
    return (
        "You are the relationship merge planner for Brain onboarding.\n\n"
        "Decide which graph edges to keep or drop after node merge decisions have settled.\n\n"
        "Rules:\n"
        "- action must be one of: keep, drop.\n"
        "- Only decide the provided edge proposals.\n"
        "- Preserve rationale and evidence_refs for every relationship decision.\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, relationship_decisions, notes\n\n"
        "## Normalized bundle\n\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Kept nodes\n\n"
        f"{json.dumps(kept_nodes, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Edge proposals\n\n"
        f"{json.dumps(edge_proposals, ensure_ascii=False, sort_keys=True)}\n"
    )


def build_onboarding_verify_prompt(
    *,
    bundle: dict[str, Any],
    semantic_artifact: dict[str, Any],
    graph_artifact: dict[str, Any],
    merge_artifact: dict[str, Any],
) -> str:
    return (
        "You are the verifier for Brain onboarding synthesis.\n\n"
        "Your job is to reject incoherent or overreaching onboarding synthesis before materialization.\n\n"
        "Check for:\n"
        "- unsupported inferences\n"
        "- risky or incorrect merge decisions\n"
        "- contradictions between identity, values, positioning, and inferred entities\n"
        "- relationships that are not grounded in evidence\n"
        "- graph proposals that overfit weak evidence\n"
        "- update plans that target unsupported section headings or imply dropping unknown sections\n\n"
        "Editable section schema for v1 patching:\n"
        "- profile: intro, ## Snapshot\n"
        "- note owner pages: intro, ## Operating Principles, ## Positioning Narrative, ## Work Priorities, ## Life Priorities, ## Constraints, ## Active Inquiries\n"
        "- person: intro, ## Snapshot, ## Relationships, ## Notes\n"
        "- project: intro, ## Project Priorities, ## Constraints, ## Notes\n"
        "- concept: intro, ## TL;DR, ## Evidence log\n"
        "- playbook: intro, ## TL;DR, ## Steps, ## Evidence log\n"
        "- stance: intro, ## TL;DR, ## Evidence log, ## Contradictions\n"
        "- inquiry: intro, ## TL;DR, ## Evidence log\n"
        "- unknown sections on existing pages must always be preserved\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, approved, blocking_issues, warnings, notes\n\n"
        "If approved is false, blocking_issues must contain concrete operator-facing reasons.\n"
        "If approved is true, blocking_issues must be an empty array.\n\n"
        "## Normalized bundle\n\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Semantic artifact\n\n"
        f"{json.dumps(semantic_artifact, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Graph artifact\n\n"
        f"{json.dumps(graph_artifact, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Merge artifact\n\n"
        f"{json.dumps(merge_artifact, ensure_ascii=False, sort_keys=True)}\n"
    )


def build_onboarding_materialization_prompt(
    *,
    bundle: dict[str, Any],
    semantic_artifact: dict[str, Any],
    graph_artifact: dict[str, Any],
    merge_artifact: dict[str, Any],
    verify_artifact: dict[str, Any],
) -> str:
    return (
        "You are the materialization planner for Brain onboarding.\n\n"
        "Produce the exact page plan that deterministic code will write to disk.\n"
        "Do not rely on downstream heuristics. Emit the final content-bearing plan.\n\n"
        "Requirements:\n"
        "- Always include these target kinds exactly once: owner_profile, owner_values, owner_positioning, owner_open_inquiries, owner_person, decision.\n"
        "- Always include these five summary pages exactly once (target_kind=\"summary\"): overview, profile, values, positioning, open-inquiries. Each carries summary_kind set to its specific kind.\n"
        "- CRITICAL: emit exactly one target_kind=\"canonical\" page for EVERY merge-artifact proposal whose action is \"create\", \"update\", or \"merge\". Do not skip any. Do not batch. One canonical page per proposal, one proposal per canonical page. Use the proposal's entity data to populate title, slug, body_markdown, relates_to, etc. Count the proposals and confirm your output has the same number of canonical pages before returning.\n"
        "- For canonical pages, slug/title/body_markdown come from the corresponding entity in the semantic/graph artifact (look up the proposal's ent-id in semantic.entities). body_markdown must be a rich, multi-section Markdown page (~300-800 words) describing the entity with its summary, key facts, and links to related entities via [[wiki-links]].\n"
        "- write_mode must be exactly \"create\" or \"update\" (no other values).\n"
        "- target_kind must be exactly one of: \"owner_profile\", \"owner_values\", \"owner_positioning\", \"owner_open_inquiries\", \"owner_person\", \"canonical\", \"summary\", \"decision\".\n"
        "- page_type rules:\n"
        "  * When target_kind=\"summary\", page_type MUST be the literal string \"summary\" (not \"overview\", \"profile\", etc). The specific kind goes in summary_kind.\n"
        "  * When target_kind=\"canonical\", page_type is the canonical node type (e.g. \"project\", \"concept\", \"playbook\", \"stance\", \"inquiry\", \"person\").\n"
        "  * When target_kind=\"owner_profile\", page_type=\"profile\".\n"
        "  * When target_kind=\"owner_values\", page_type=\"note\".\n"
        "  * When target_kind=\"owner_positioning\", page_type=\"note\".\n"
        "  * When target_kind=\"owner_open_inquiries\", page_type=\"note\".\n"
        "  * When target_kind=\"owner_person\", page_type=\"person\".\n"
        "  * When target_kind=\"decision\", page_type=\"decision\".\n"
        "- summary_kind: REQUIRED exactly when target_kind=\"summary\"; MUST be null or omitted for all other target_kinds. Allowed values: \"overview\", \"profile\", \"values\", \"positioning\", \"open-inquiries\".\n"
        "- For create pages, target_path must be null, and body_markdown must be a non-empty string.\n"
        "- For update pages, target_path must be the exact existing repo-relative path, and body_markdown must be null (use section_operations instead).\n"
        "- extra_frontmatter must stay JSON-serializable.\n"
        "- relates_to and sources should use wiki-link strings when appropriate.\n\n"
        "Patch model for update pages:\n"
        "- Do not emit body_markdown for update pages.\n"
        "- Emit intro_mode, optional intro_markdown, and section_operations.\n"
        "- intro_mode must be exactly one of these three string literals: \"preserve\", \"replace\", \"append\". No other values are legal. Default to \"preserve\" if unsure.\n"
        "- For write_mode=\"create\" pages, set intro_mode to \"preserve\" and section_operations to an empty list [] — these fields are only meaningful for update pages, but must still be valid literals.\n"
        "- section_operations entries must contain heading, mode, content, and optional insert_after.\n"
        "- section mode must be one of replace, append, union, preserve.\n"
        "- Use union only for bullet-list sections.\n"
        "- Unknown sections on existing pages are preserved automatically; do not try to rewrite or delete them.\n\n"
        "Editable section schema for v1 patching:\n"
        "- profile: intro, ## Snapshot\n"
        "- note owner pages: intro, ## Operating Principles, ## Positioning Narrative, ## Work Priorities, ## Life Priorities, ## Constraints, ## Active Inquiries\n"
        "- person: intro, ## Snapshot, ## Relationships, ## Notes\n"
        "- project: intro, ## Project Priorities, ## Constraints, ## Notes\n"
        "- concept: intro, ## TL;DR, ## Evidence log\n"
        "- playbook: intro, ## TL;DR, ## Steps, ## Evidence log\n"
        "- stance: intro, ## TL;DR, ## Evidence log, ## Contradictions\n"
        "- inquiry: intro, ## TL;DR, ## Evidence log\n\n"
        "Return one JSON object with these exact top-level keys:\n"
        "bundle_id, pages, notes\n\n"
        "pages entries must contain:\n"
        "plan_id, target_kind, write_mode, page_type, slug, title, body_markdown, intro_mode, intro_markdown, section_operations, domains, relates_to, sources, extra_frontmatter, target_path, summary_kind\n\n"
        "## Normalized bundle\n\n"
        f"{json.dumps(bundle, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Semantic artifact\n\n"
        f"{json.dumps(semantic_artifact, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Graph artifact\n\n"
        f"{json.dumps(graph_artifact, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Merge artifact\n\n"
        f"{json.dumps(merge_artifact, ensure_ascii=False, sort_keys=True)}\n\n"
        "## Verify artifact\n\n"
        f"{json.dumps(verify_artifact, ensure_ascii=False, sort_keys=True)}\n"
    )


def align_classified_links(
    *,
    links: list[dict[str, str]],
    response: dict[str, Any],
) -> list[dict[str, str]]:
    raw = response.get("classifications", [])
    by_url = {item.get("url"): item for item in raw if isinstance(item, dict)}
    return [
        by_url.get(
            link["url"],
            {"url": link["url"], "category": "ignore", "reason": "missing from model response"},
        )
        for link in links
    ]
