# mind

```text
       _       _
 _____|_|___ _| |
|     | |   | . |
|_|_|_|_|_|_|___|
```

> A local-first knowledge engine that turns private source material into an
> evidence-backed markdown graph.

mind keeps source evidence, derived concepts, and maintenance history in files
you can inspect. It is designed for people who want a durable personal knowledge
base that can ingest material, preserve provenance, and revisit its own graph as
new evidence accumulates.

The repository contains the reusable engine: schemas, ingestion code, prompts,
Dream runtime, retrieval interfaces, CLI services, tests, and synthetic
examples. Your real memory, raw source material, credentials, runtime databases,
vector indexes, and generated graph output stay in ignored local storage.

## How It Works

1. **Ingest sources** from local files, dropbox-style inboxes, or configured
   provider exports.
2. **Normalize evidence** into source records with stable IDs, metadata, and
   provenance.
3. **Distill atoms** from repeated evidence: concepts, playbooks, stances, and
   inquiries.
4. **Materialize markdown** under the configured memory root so the graph stays
   readable outside the runtime.
5. **Run Dream passes** to keep the graph useful as more evidence arrives.

Atoms are more than tags. They carry evidence logs, source links, dates,
polarity, lifecycle state, and graph relations. That structure lets mind show
when sources reinforce the same idea, when a stance is weakening, or when two
projects share an underlying pattern.

## Dream Cycle

Dream is the graph-maintenance loop.

- **Light Dream** scans recent source pages and the tail of the graph. It
  appends low-risk evidence, finds possible links, and creates review nudges.
- **Deep Dream** handles slower consolidation: promotions, holds, merges,
  relationship updates, digest/index regeneration, and external grounding.
- **REM Dream** reviews hot or stale clusters against identity and project
  context, writes reflections, and proposes pruning or lifecycle changes.
- **Kené Dream** is the shadow restructuring pass. It consumes prior Light,
  Deep, and REM outputs, emits auditable structure and relation artifacts under
  raw reports, and blocks canonical markdown or relation writes until apply mode
  is explicitly trusted.

## Local Data Model

A local mind is a private markdown graph plus rebuildable runtime state.

- **Raw evidence** lives under the configured raw root.
- **Source pages** preserve materialized evidence under the configured memory
  root.
- **Atom pages** represent concepts, playbooks, stances, and inquiries that can
  gather evidence across many sources.
- **Evidence edges** are machine-readable JSONL records under configured raw
  `evidence-edges/`; markdown evidence logs are the readable view.
- **Dream artifacts** live under configured raw reports, while durable Dream
  pages live under the configured memory root when a stage is allowed to write.
- **Runtime state** lives in SQLite under the configured state root and can be
  rebuilt or inspected separately from the knowledge graph.

The public config template points local roots at:

- `local_data/memory`
- `local_data/raw`
- `local_data/dropbox`
- `local_data/state`

Use `local_data/config.yaml` for local configuration, or set
`BRAIN_CONFIG_PATH` to another YAML file.

## Quick Start

This path verifies a fresh checkout without private data or API keys.

```bash
git clone <repo-url> mind
cd mind
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
mkdir -p local_data/{memory,raw,dropbox,state}
cp config.example.yaml local_data/config.yaml
cp .env.example .env
.venv/bin/python -m mind config path
.venv/bin/python -m mind --help
```

Create a tiny starter graph and query it:

```bash
.venv/bin/python -m mind seed --preset skeleton
.venv/bin/python -m mind graph rebuild
.venv/bin/python -m mind query "What themes keep recurring?"
```

For LLM-backed ingestion, onboarding, Dream, and provider flows, add
`AI_GATEWAY_API_KEY` to `.env` and run:

```bash
.venv/bin/python -m mind doctor
```

`doctor` reports config, paths, credentials, and local readiness.

## Configuration

The public config template is `config.example.yaml`. Copy it to
`local_data/config.yaml` for local use. Do not commit a real local config.

Common environment variables:

- `AI_GATEWAY_API_KEY`: required for routed LLM execution through AI Gateway.
- `GEMINI_API_KEY`, `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`: optional direct
  provider keys when bypassing the gateway intentionally.
- `BRAIN_CONFIG_PATH`: optional config path or overlay.
- `BRAIN_LOCAL_DATA_ROOT`, `BRAIN_MEMORY_ROOT`, `BRAIN_RAW_ROOT`,
  `BRAIN_DROPBOX_ROOT`, `BRAIN_STATE_ROOT`: optional private-root overrides.
- `BROWSER_FOR_COOKIES`: browser selector for cookie-backed YouTube operations.
- `SUBSTACK_SESSION_COOKIE`: optional full browser cookie header for Substack
  saved-post pulls when that lane is enabled.

Inspect resolved config with:

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
.venv/bin/python -m mind readiness --scope new-user
```

Run staged onboarding:

```bash
.venv/bin/python -m mind onboard import --from-json <path-to-onboarding.json>
.venv/bin/python -m mind onboard normalize --bundle <bundle-id>
.venv/bin/python -m mind onboard synthesize --bundle <bundle-id>
.venv/bin/python -m mind onboard verify --bundle <bundle-id>
.venv/bin/python -m mind onboard validate --bundle <bundle-id>
.venv/bin/python -m mind onboard materialize --bundle <bundle-id>
.venv/bin/python -m mind onboard replay --bundle <bundle-id>
.venv/bin/python -m mind onboard status --bundle <bundle-id>
```

Run Dream directly:

```bash
.venv/bin/python -m mind dream light --dry-run
.venv/bin/python -m mind dream deep --dry-run
.venv/bin/python -m mind dream rem --dry-run
.venv/bin/python -m mind dream kene --dry-run
.venv/bin/python -m mind digest
.venv/bin/python -m mind state
```

Run maintenance:

```bash
.venv/bin/python -m mind repair graph --apply
.venv/bin/python -m mind repair atom-pages --apply
```

Run scheduled or accelerated Dream flows:

```bash
.venv/bin/python -m mind orchestrate daily
.venv/bin/python -m mind worker run-once
.venv/bin/python -m mind worker drain-until-empty
.venv/bin/python -m mind dream campaign --days 7 --dry-run
.venv/bin/python -m mind dream simulate-year --run-id first-year --dry-run
```

Some Dream and ingest commands call routed models even in dry-run mode. Without
`AI_GATEWAY_API_KEY`, those commands may fail during configuration checks.

`mind dream simulate-year` runs an isolated simulation under
`local_data/simulations/<run-id>/`. It emits graph-delta reports without
forward-dating live state. Kené remains a shadow artifact pass.

## Synthetic Harness

The repository includes a small synthetic graph under `examples/synthetic/`.
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
mind/
|-- core/mind/              # CLI and runtime services
|-- core/scripts/           # Ingestion, parsing, writing, and atom mechanics
|-- contracts/              # Machine-readable schema contract
|-- docs/                   # Current public docs
|-- examples/synthetic/     # Safe synthetic harness
|-- tests/                  # Unit, integration, runtime, and safety tests
|-- config.example.yaml     # Public config template
|-- .env.example            # Public env template
|-- AGENTS.md               # Contributor and automation guidance
`-- README.md               # Project overview
```

## Public Safety

These paths must stay out of public commits:

- `local_data/`
- `memory/`
- `raw/`
- `dropbox/`
- `.obsidian/`
- real env files such as `.env`, `.env.local`, and `.env.production`
- root `config.yaml`
- runtime databases and vector indexes
- generated Dream reports, evidence-edge files, and simulations under private
  roots
- planning drafts, local onboarding prompts, and agent runtime folders

The repo includes ignore rules and a tracked-file scanner for private roots,
database artifacts, secrets, and personal markers. `.env.example` and
`config.example.yaml` are public templates; real values belong only in ignored
local files.

## Limitations

- The repository ships the engine and synthetic fixtures, not real memory.
- Provider-backed ingestion requires local credentials, browser cookies, or
  exports depending on the lane.
- LLM-backed stages require a configured AI Gateway key unless routing is
  deliberately overridden.
- Dream trust is evidence-quality aware. A lane can be ready for ingest while
  Dream still treats it as partial-fidelity or bootstrap-only because of low
  quote coverage, low entity yield, or insufficient sample size.
- Year-scale Dream simulation can be compute-heavy on a large graph, so use a
  shorter `--days` smoke run before a full 365-day pass.
- The graph is intentionally inspectable, so schema hygiene matters: run lint,
  readiness, and graph checks before trusting a local vault.

## License

MIT. See [`LICENSE`](LICENSE).
