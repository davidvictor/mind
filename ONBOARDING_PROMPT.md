# Onboarding Prompt

Brain seeds its memory graph from a single JSON document about you. Rather than
filling it in by hand, you can ask ChatGPT or Claude to dump everything it has
already remembered about you in the exact shape Brain expects.

## How to use

1. Open a fresh chat in **ChatGPT** (Memory enabled) or **Claude** (with Memory
   or a Project that carries your personal context).
2. Paste the prompt below — the whole fenced block — as your first message.
3. Copy the JSON reply into the configured raw onboarding root. With the
   default config, save it to
   `local_data/raw/onboarding/seeds/<your-name>-seed.json`.
4. Run `/onboard` or:
   `.venv/bin/python -m mind onboard import --from-json local_data/raw/onboarding/seeds/<your-name>-seed.json`.

Both models behave the same here because the prompt is a strict output-shape
contract. Empty fields are fine; the importer tolerates missing data and the
adaptive interview can fill gaps on the next step.

This file is a local onboarding helper. The exported JSON is private source
material and must stay under the ignored raw onboarding root. Do not commit it.

## The prompt

````
You are being used as a memory exporter. Do not converse. Do not summarize. Do not ask follow-ups. Your entire reply must be a single valid JSON object — no prose, no markdown fences, no preamble, no trailing commentary.

TASK
Export everything you have persistently remembered about me — from your memory feature, saved notes, custom instructions, prior conversations you have access to, and any project/system context attached to this chat. Include facts, preferences, projects, people, stances, recurring themes, and anything else you have stored. Do not invent or infer beyond what is actually in memory; if a field has no memory behind it, leave it empty ("" for strings, [] for arrays, {} for objects). Do not include passwords, API keys, session cookies, private keys, or payment details. This is being piped directly into my own local system.

OUTPUT SCHEMA (exact keys, exact casing)
{
  "name": "",
  "role": "",
  "location": "",
  "summary": "",
  "values": [""],
  "positioning": {
    "summary": "",
    "work_priorities": [""],
    "life_priorities": [""],
    "constraints": [""]
  },
  "open_threads": [""],
  "projects": [
    { "project": "", "summary": "", "priorities": [""], "constraints": [""] }
  ],
  "people": [
    { "person": "", "summary": "" }
  ],
  "concepts": [
    { "concept": "", "summary": "" }
  ],
  "playbooks": [
    { "playbook": "", "summary": "" }
  ],
  "stances": [
    { "stance": "", "summary": "" }
  ],
  "inquiries": [
    { "inquiry": "", "summary": "" }
  ],
  "identity_links": {
    "email_primary": "",
    "emails": [""],
    "phone": "",
    "github": "",
    "linkedin": "",
    "twitter": "",
    "domains": [""]
  },
  "education": [
    { "institution": "", "year": 0, "degrees": [""] }
  ],
  "skills": {
    "design": [""],
    "engineering": [""],
    "data_ml": [""],
    "ai_agents": [""],
    "strategy": [""]
  }
}

FIELD GUIDANCE
- name / role / location: whatever you have on file for me; if multiple, pick the most recent.
- summary: 2–4 sentence factual, encyclopedia-style paragraph. Not marketing copy.
- values: operating principles you have seen me hold across conversations. One per entry, "Principle — short justification" style where possible.
- positioning.summary: how I currently frame my work and life in one paragraph.
- positioning.work_priorities / life_priorities / constraints: short bullets, imperative mood.
- open_threads: unresolved questions I keep returning to. Phrase as questions.
- projects: every distinct product, company, or engagement you've seen me work on. Include past and present. `priorities` and `constraints` only when you actually remember them; otherwise omit those keys for that project.
- people: individuals I've talked about repeatedly with enough context to describe their relationship to me.
- concepts: recurring frameworks or mental models I use.
- playbooks: repeatable processes or methodologies I apply.
- stances: positions I hold publicly or argue for.
- inquiries: longer-horizon research questions, distinct from tactical open_threads.
- identity_links: only fields you actually have; leave others empty.
- education: institutions, graduation year as integer, degrees as separate strings.
- skills: categorized lists; a skill can appear in only one category. Prefer the most specific category.

RULES
1. Output ONLY the JSON object. No ```json fences. No explanation before or after.
2. Every key in the schema must be present. Use empty string, empty array, or empty object if you have no memory for it.
3. Remove placeholder template entries (e.g., the empty `{ "project": "", ... }` example row) — emit only real data, or an empty array.
4. Arrays of strings: drop empty strings. Arrays of objects: drop objects where the title field (`project`, `person`, etc.) is empty.
5. Do not hallucinate. If you are unsure whether something is from memory vs. inferred from this session, omit it.
6. Do not include the word "memory" or meta-commentary in any field value.
7. UTF-8, valid JSON, parseable by `json.loads`. No trailing commas.

Begin output now.
````
