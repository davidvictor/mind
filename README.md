# Brain

Brain is a local-first knowledge engine that turns private source material into
an inspectable markdown brain: source pages, atomic evidence, graph
attributions, and recurring Dream passes that synthesize what keeps showing up.

The point is not to give a chatbot a larger context window. The point is to
build a memory system you can inspect, repair, rerun, and own. Brain keeps the
evidence in files, records how each idea entered the graph, and then "dreams"
over that graph so repeated evidence can become better concepts, sharper
stances, useful playbooks, and non-obvious connections.

The public repository contains the reusable engine: schemas, ingestion code,
prompts, Dream runtime, CLI services, tests, and synthetic examples. Your real
memory, raw evidence, credentials, runtime databases, and generated graph output
stay in ignored local storage.

## Why Dreaming Matters

Most personal memory tools store notes. Brain tries to keep the reasoning trail.
Each source can become a durable source page, and the ingestion pipeline can
distill that source into atom-level evidence:

- **Concepts** for recurring ideas.
- **Playbooks** for repeatable procedures.
- **Stances** for positions with evidence for or against them.
- **Inquiries** for unresolved questions worth carrying forward.

Those atoms are not just tags. They carry evidence logs, source links, dates,
polarity, lifecycle state, and graph relations. Dreaming is the maintenance
cycle that revisits this substrate after more evidence accumulates.

### Dreaming: Light, Deep, REM

- **Light Dream** scans recent source pages and the tail of the graph, appends
  low-risk evidence, finds possible links, and creates review nudges.
- **Deep Dream** handles slower editorial work: promotions, holds, merges,
  relationship updates, digest/index regeneration, and external grounding.
- **REM Dream** looks at hot or stale clusters against the owner's identity
  context, writes reflections, and proposes pruning or lifecycle changes.

The user-facing result is a brain that gets more useful because it can notice
recurrence, tension, and structure across sources. Instead of only remembering
"you read this," it can show that several sources are pointing at the same
strategy, that a stance is weakening, or that two projects are connected by the
same underlying idea.

## What It Does

- Imports local files and configured provider-backed sources into private raw
  storage.
- Normalizes source material into stable source records with provenance.
- Runs an enrichment lifecycle: understand, personalize, attribute, distill,
  materialize, and propagate.
- Writes durable markdown pages under the configured memory root.
- Tracks runtime, source registry, graph registry, and optional vector index
  state under the configured state root.
- Exposes one canonical operator surface: `python -m mind`.
- Keeps public code and private knowledge separated by default.

## What A Brain Is

In this implementation, a brain is not a hidden database of embeddings. It is a
private markdown graph plus rebuildable runtime state.

- **Raw evidence** lives under the configured raw root.
- **Source pages** preserve materialized evidence under the configured memory
  root.
- **Atom pages** represent concepts, playbooks, stances, and inquiries that can
  gather evidence across sources.
- **Dream outputs** live as markdown under memory and as operator artifacts
  under raw reports.
- **Runtime state** lives in SQLite under the configured state root and is not
  the durable knowledge product.

The public template points those private roots at:

- `local_data/memory`
- `local_data/raw`
- `local_data/dropbox`
- `local_data/state`

`local_data/config.yaml` is the normal private config. `BRAIN_CONFIG_PATH` can
point at another YAML config or overlay.

## Quick Start

This path verifies a fresh checkout without requiring private data or API keys.

```bash
git clone <repo-url>
cd Brain
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
mkdir -p local_data/{memory,raw,dropbox,state}
cp config.example.yaml local_data/config.yaml
cp .env.example .env
.venv/bin/python -m mind config path
.venv/bin/python -m mind --help
```

To create a tiny local starter brain and query it:

```bash
.venv/bin/python -m mind seed --preset skeleton
.venv/bin/python -m mind graph rebuild
.venv/bin/python -m mind query "What themes keep recurring?"
```

For LLM-backed ingestion, onboarding, Dream, and provider flows, put
`AI_GATEWAY_API_KEY` in `.env` and then run:

```bash
.venv/bin/python -m mind doctor
```

`doctor` is expected to fail until the memory/raw roots exist and gateway
credentials are configured.

## Configuration

The public config template is `config.example.yaml`. Copy it to
`local_data/config.yaml` for local use. Do not commit a real local config.

The current environment surface is:

- `AI_GATEWAY_API_KEY`: required for routed LLM execution through AI Gateway.
- `GEMINI_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`: only needed when
  intentionally bypassing the gateway.
- `BRAIN_CONFIG_PATH`: optional config path or overlay.
- `BRAIN_LOCAL_DATA_ROOT`, `BRAIN_MEMORY_ROOT`, `BRAIN_RAW_ROOT`,
  `BRAIN_DROPBOX_ROOT`, `BRAIN_STATE_ROOT`: optional private-root overrides.
- `BROWSER_FOR_COOKIES`: browser selector for cookie-backed YouTube operations.
- `SUBSTACK_SESSION_COOKIE`: optional full browser cookie header for Substack
  saved-post pulls when that lane is enabled.

Model routes and Dream knobs live in config, not in the README. Inspect the
resolved config with:

```bash
.venv/bin/python -m mind config show
```

## Core Commands

Inspect and initialize:

```bash
.venv/bin/python -m mind config path
.venv/bin/python -m mind doctor
.venv/bin/python -m mind seed --preset skeleton
.venv/bin/python -m mind graph rebuild
.venv/bin/python -m mind graph status
```

Move inbox material into ingest lanes:

```bash
.venv/bin/python -m mind dropbox status
.venv/bin/python -m mind dropbox sweep
.venv/bin/python -m mind ingest readiness
```

Run the staged onboarding backend:

```bash
.venv/bin/python -m mind onboard import --from-json local_data/raw/onboarding/seeds/me.json
.venv/bin/python -m mind onboard normalize --bundle <bundle-id>
.venv/bin/python -m mind onboard synthesize --bundle <bundle-id>
.venv/bin/python -m mind onboard verify --bundle <bundle-id>
.venv/bin/python -m mind onboard materialize --bundle <bundle-id>
.venv/bin/python -m mind onboard status --bundle <bundle-id>
```

Run Dream directly:

```bash
.venv/bin/python -m mind dream light --dry-run
.venv/bin/python -m mind dream deep --dry-run
.venv/bin/python -m mind dream rem --dry-run
.venv/bin/python -m mind digest
.venv/bin/python -m mind state
```

Run operator schedules:

```bash
.venv/bin/python -m mind orchestrate daily
.venv/bin/python -m mind worker run-once
.venv/bin/python -m mind worker drain-until-empty
.venv/bin/python -m mind dream campaign --days 7 --dry-run
.venv/bin/python -m mind dream simulate-year --start-date 2025-01-01 --days 7 --dry-run
```

Some Dream and ingest commands call routed models even in dry-run mode. If
`AI_GATEWAY_API_KEY` is missing, they may fail with a configuration error rather
than producing a local-only preview.

## Synthetic Harness

The public repo includes a small synthetic graph under `examples/synthetic/`.
It is safe to inspect, lint, and test. It is not a sample of private memory.

```bash
.venv/bin/python -m scripts.lint examples/synthetic
.venv/bin/python -m pytest tests/project/test_example_memory_smoke.py
```

## Tests

Run the full suite:

```bash
.venv/bin/python -m pytest
```

Run the public-safety guard:

```bash
.venv/bin/python core/tools/check_no_private_data.py --tracked
```

Run the synthetic graph linter:

```bash
.venv/bin/python -m scripts.lint examples/synthetic
```

## Repository Map

```text
Brain/
|-- core/mind/              # Canonical CLI and runtime services
|-- core/scripts/           # Ingestion, parsing, writing, and atom mechanics
|-- contracts/              # Machine-readable schema contract
|-- docs/                   # Current public docs only
|-- examples/synthetic/     # Safe synthetic harness
|-- tests/                  # Unit, integration, runtime, and safety tests
|-- config.example.yaml     # Public config template
|-- AGENTS.md               # Agent/operator rules for this repo
`-- README.md               # Public overview
```

## Public Safety

These paths must not be public release artifacts:

- `local_data/`
- `memory/`
- `raw/`
- `dropbox/`
- `.obsidian/`
- `.env` and `.env.*`
- root `config.yaml`
- runtime databases and vector indexes
- generated Dream reports and simulations
- local planning archives and agent runtime folders

The repo includes ignore rules, a pre-commit private-data guard, and a tracked
file scanner for private roots, database artifacts, secrets, and owner-specific
markers.

## Limitations

- The public repo ships the engine and synthetic fixtures, not real memory.
- Provider-backed ingestion requires local credentials, browser cookies, or
  exports depending on the lane.
- LLM-backed stages require a configured AI Gateway key unless you deliberately
  override routing.
- Full-year Dream simulation can be compute-heavy on a large private graph.
- The graph is intentionally inspectable, so schema hygiene matters: run lint,
  readiness, and graph checks before trusting a private vault.

## License

MIT. See [`LICENSE`](LICENSE).
