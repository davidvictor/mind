# Brain

Brain is a local-first knowledge engine that turns private source material into
an inspectable markdown memory graph.

It exists because useful AI memory should be owned by the person using it. The
public repository contains the reusable engine: schemas, ingestion code,
prompts, Dream logic, runtime services, tests, and synthetic examples. Your real
memory, raw evidence, credentials, runtime databases, and generated graph output
stay in ignored local storage.

## Why We Built This

Most personal AI memory systems are opaque. They remember things somewhere, but
you cannot inspect the evidence, repair the graph, rerun the pipeline, or move
the memory somewhere else.

Brain takes the opposite path. It treats memory as files first: source evidence
is normalized, enriched, and written into a durable markdown graph. The graph is
plain enough to open in Obsidian, strict enough to lint, and structured enough
for agents to query and maintain over time.

The goal is not a chatbot with a larger context window. The goal is a living
knowledge base where sources, concepts, playbooks, stances, questions, and
identity notes can accumulate evidence and be reorganized without hiding the
work.

## What It Does

- Imports source material from configured local and provider-backed lanes.
- Normalizes raw inputs into typed source records with stable IDs and
  provenance.
- Runs enrichment passes that understand the source, connect it to the owner,
  attribute creators or channels, and distill candidate atoms.
- Materializes durable markdown pages under the configured memory root.
- Maintains a graph registry, runtime state, source registry, and optional
  vector index outside the public repo.
- Runs Light, Deep, REM, and Weave Dream cycles that consolidate and reorganize
  memory over time.
- Provides one canonical CLI: `python -m mind`.

## How It Works

Brain keeps three surfaces separate:

- **Public core:** code, tests, schemas, prompts, docs, contracts, and synthetic
  fixtures.
- **Raw evidence:** private source material, normalized bundles, provider
  exports, transcripts, caches, and onboarding state.
- **Memory graph:** private markdown pages that become the durable knowledge
  product.

The default public config points private data at `local_data/`:

- `local_data/memory`
- `local_data/raw`
- `local_data/dropbox`
- `local_data/state`

Those paths are ignored by Git. A private config lives at
`local_data/config.yaml`, or in another file selected with `BRAIN_CONFIG_PATH`.

## Graph-Based Ingestion

Ingestion is a graph-building pipeline, not a file dump.

1. **Normalize:** turn a raw item into a stable `NormalizedSource` with source
   truth, timestamps, creators, IDs, and provenance.
2. **Understand:** extract the main argument, examples, entities, and useful
   structure.
3. **Personalize:** connect the source to the owner profile when local context
   is available.
4. **Attribute:** resolve creators, channels, authors, and source lineage.
5. **Distill:** emit candidate concepts, playbooks, stances, and inquiries.
6. **Materialize:** write canonical markdown source pages and atom candidates.
7. **Propagate:** update indexes, queue follow-up work, and record runtime
   receipts.

The maintained graph is made of ordinary markdown pages with typed
frontmatter. Source pages preserve evidence. Atom pages represent the
cross-source ideas that survive repeated observation. Runtime state lives in
SQLite under the configured state root and is rebuildable operational state, not
the knowledge product.

## Dreaming: Light, Deep, REM

Dreaming is Brain's maintenance loop. It lets the graph change shape as evidence
accumulates.

- **Light Dream** runs as the bounded daily consolidator. It scans recent and
  tail evidence, adds low-risk evidence updates, detects possible links, and
  creates review nudges or probationary atoms.
- **Deep Dream** runs as the slower editorial pass. It promotes or holds
  probationary atoms, merges duplicates, handles contradiction nudges,
  regenerates indexes, and can write a digest.
- **REM Dream** runs as the identity-pressure pass. It looks at the graph
  against the owner context, reflects on stale or important clusters, prunes or
  retires weak material, and writes monthly reflection output.
- **Weave Dream** runs as the structural clustering pass. It groups mature atoms
  into durable overlay pages, updates safe cluster references, and reports
  bridge, merge, and split opportunities.

For year-scale testing, `mind dream simulate-year` copies the configured memory
and relevant Dream raw inputs into `local_data/simulations/<run-id>/`, runs an
accelerated Light/Deep/REM yearly schedule there, and writes graph-delta reports
without mutating the live memory graph.

## Quick Start

```bash
git clone <repo-url>
cd Brain
python3.11 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements.txt
mkdir -p local_data
cp config.example.yaml local_data/config.yaml
cp .env.example .env
.venv/bin/python -m mind doctor
```

Fill `.env` with the credentials you need. AI Gateway is the default LLM
transport, so `AI_GATEWAY_API_KEY` is the main key for routed model calls.

## Common Commands

Inspect config:

```bash
.venv/bin/python -m mind config path
.venv/bin/python -m mind config show
```

Create a starter graph:

```bash
.venv/bin/python -m mind seed --preset skeleton
```

Import onboarding evidence:

```bash
.venv/bin/python -m mind onboard import --from-json path/to/onboarding.json
.venv/bin/python -m mind onboard normalize --bundle <bundle-id>
.venv/bin/python -m mind onboard validate --bundle <bundle-id>
.venv/bin/python -m mind onboard materialize --bundle <bundle-id>
```

Run readiness checks:

```bash
.venv/bin/python -m mind readiness --scope new-user
.venv/bin/python -m mind ingest readiness
```

Run the daily operator loop:

```bash
.venv/bin/python -m mind orchestrate daily
```

Drain manual recovery work:

```bash
.venv/bin/python -m mind worker run-once
.venv/bin/python -m mind worker drain-until-empty
```

Run Dream stages directly:

```bash
.venv/bin/python -m mind dream light
.venv/bin/python -m mind dream deep
.venv/bin/python -m mind dream rem
.venv/bin/python -m mind dream weave
```

Run an isolated year simulation:

```bash
.venv/bin/python -m mind dream simulate-year --start-date 2025-01-01 --days 365
```

Write a digest and inspect runtime state:

```bash
.venv/bin/python -m mind digest
.venv/bin/python -m mind state
```

Query the maintained memory graph:

```bash
.venv/bin/python -m mind query "What themes keep recurring?"
```

## Synthetic Harness

The public repo includes a tiny synthetic graph under `examples/synthetic/`.
It is safe to inspect, lint, and copy in tests. It is not a sample of private
memory.

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

## Public Safety

These paths must never be public release artifacts:

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

The repo includes `.gitignore`, a pre-commit private-data guard, and a GitHub
workflow that scans tracked files for private roots, database artifacts,
secrets, and owner-specific markers.

## Repository Map

```text
Brain/
├── core/mind/              # Canonical CLI and runtime services
├── core/scripts/           # Ingestion, parsing, writing, and atom mechanics
├── contracts/              # Machine-readable schema contract
├── docs/                   # Current public docs only
├── examples/synthetic/     # Safe synthetic harness
├── tests/                  # Unit, integration, runtime, and safety tests
├── config.example.yaml     # Public config template
├── AGENTS.md               # Agent/operator rules for this repo
└── README.md               # This public overview
```

## Limitations

- The public repo ships the engine and synthetic fixtures, not real memory.
- Provider-backed ingestion requires local credentials and, for some providers,
  browser cookies or exports.
- Full-year Dream simulation can be compute-heavy on a large private graph.
- The markdown graph is intentionally inspectable, so schema quality matters:
  run lint and repair commands before treating a private vault as healthy.

## License

MIT. See [`LICENSE`](LICENSE) when present in the release package.
