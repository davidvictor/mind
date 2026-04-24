# Brain Agent Operating System

This file is the complete operating manual for agents working in this repository.

If you are an agent in this repo, treat this document as the operating system:

- what the project is
- how the architecture works
- which files are authoritative
- which command surfaces are canonical
- how runtime state works
- how queueing and orchestration work
- how to verify changes
- how to document changes
- how to commit changes
- what belongs in the archive
- what should never be committed

[`CLAUDE.md`](CLAUDE.md) is a thin pointer for compatibility. The complete agent operating guidance lives in [`AGENTS.md`](AGENTS.md).

## 1. Project Purpose

Brain is a public, file-first, agent-maintained knowledge engine that runs against a private local memory store.

The project exists to transform raw personal source material into a durable markdown knowledge graph that can be:

- ingested from local or pulled source material
- maintained by the canonical CLI
- operated on a daily unattended cadence
- queried directly from the maintained wiki
- inspected and recovered via local runtime state

The public product is the reusable core: schemas, prompts, orchestration, ingestion mechanics, Dream logic, adapters, tests, and synthetic examples. The private product is the user's local markdown graph and source evidence, which must never be committed.

## 2. Product Shape

At a high level the system does four things:

1. accept source material
2. transform source material into maintained wiki pages
3. consolidate knowledge on a Dream cadence
4. expose operator/runtime health through the CLI

Current priorities are:

- one public example config plus one private local config
- one execution engine
- one canonical onboarding pipeline with isolated raw evidence and resumable state
- one unattended daily sweep
- one queue worker for manual recovery
- one current documentation surface

## 3. Canonical Authority Map

When deciding which file is authoritative, use this map.

### 3.1 Config

The public config template is:

- [`config.example.yaml`](config.example.yaml)

The private local config is:

- `local_data/config.yaml`, or the path injected through `BRAIN_CONFIG_PATH`

Never commit `config.yaml` or a real local config. Runtime policy that belongs in public should be expressed in `config.example.yaml` or code/docs; machine-specific paths, API keys, provider cookies, owner preferences, and private data roots belong only in ignored local config/env files.

Legacy root config files are not valid active inputs.

### 3.2 Human-facing overview

The human/operator overview is:

- [`README.md`](README.md)

This should explain what the project is, how to launch it, and how to use it.

### 3.3 Agent-facing operating guidance

- [`AGENTS.md`](AGENTS.md)

`AGENTS.md` is the complete and authoritative agent operating manual for this repository. `CLAUDE.md` points here for compatibility with pointer-based agent flows.

### 3.4 Current docs

The current docs surface is intentionally small:

- [`docs/README.md`](docs/README.md)
- [`docs/SYSTEM_RULES.md`](docs/SYSTEM_RULES.md)
- [`docs/OBSIDIAN_THEME.md`](docs/OBSIDIAN_THEME.md)
- [`docs/schema-v2.md`](docs/schema-v2.md)
- [`contracts/brain-contract.yaml`](contracts/brain-contract.yaml)

### 3.5 Historical Context

Historical material may exist in ignored local archives such as `docs/archive/`,
but it is not part of the public release surface.

That includes old:

- specs
- plans
- setup notes
- brainstorming material
- rollout narratives

Historical docs are for reference only. They are not the active source of truth.

## 4. Repository Layout

The important top-level layout is:

```text
Brain/
├── core/
│   ├── mind/
│   ├── scripts/
│   ├── templates/
│   └── tools/
├── config.example.yaml
├── README.md
├── AGENTS.md
├── CLAUDE.md
├── docs/
├── contracts/
├── examples/
├── tests/
├── local_data/                 # ignored private store, optional/default
├── .claude/commands/
└── .codex/prompts/
```

Important directories:

- `core/mind/`
  Canonical CLI/runtime layer.
- `core/scripts/`
  Lower-level mechanical pipeline code.
- `core/templates/`
  Code-owned page templates.
- `core/tools/`
  Public maintenance and data-safety tools.
- `local_data/`
  Default ignored private store for local config, raw inputs, markdown memory, runtime DBs, vector indexes, inbox material, and local UI state.
- `examples/synthetic/`
  Synthetic runnable harness only.
- `.claude/commands/`
  Claude wrapper workflows and prompt-native holdouts.
- `.codex/prompts/`
  Codex prompt wrappers mirroring the thin command surface.
- ignored local planning/archive folders
  Historical material only; not part of the public release tree.

## 5. Live Vault Layout

The live vault roots are defined by private config/env, with `config.example.yaml` as the public template.

Default public template settings:

- `paths.memory_root: local_data/memory`
- `paths.raw_root: local_data/raw`
- `paths.dropbox_root: local_data/dropbox`
- `paths.state_root: local_data/state`

That means:

- private inbox material belongs under the configured dropbox root
- maintained wiki content belongs under the configured memory root
- source-input content belongs under the configured raw root
- runtime, graph, source, and vector DBs belong under the configured state root
- internal machine queue artifacts belong under configured raw `drops/`
- isolated onboarding intake, normalized evidence bundles, validation, manifests, and resumable state belong under configured raw `onboarding/`

Do not hard-code `memory/`, `raw/`, `dropbox/`, `local_data/`, or root `.brain-*` paths. Use `Vault`, `BrainConfig`, and the retrieval/store abstractions.

If you find layout residue:

- prefer converging everything to the configured private roots
- update docs, tests, examples, and runtime assumptions to the configured layout
- avoid leaving “both are okay” ambiguity unless it is explicitly part of the design

## 6. Execution Engine

The CLI is the execution engine.

If a capability exists in both a wrapper workflow and `python -m mind`, the CLI is the source of truth.

Current key operator surfaces include:

- `mind dropbox sweep`
- `mind dropbox status`
- `mind dropbox migrate-legacy`
- `mind seed`
- `mind obsidian theme apply`
- `mind onboard`
- `mind onboard import`
- `mind onboard normalize`
- `mind onboard validate`
- `mind onboard materialize`
- `mind onboard replay`
- `mind onboard status`
- `mind orchestrate daily`
- `mind worker run-once`
- `mind worker drain-until-empty`
- `mind dream light`
- `mind dream deep`
- `mind dream rem`
- `mind digest`
- `mind state`
- `mind state health`
- provider pull commands
- ingest commands
- query and expand commands
- doctor, config, and check commands

Substack operator note:

- when refreshing `SUBSTACK_SESSION_COOKIE`, use the logged-in request to [https://substack.com/api/v1/posts/saved?limit=1](https://substack.com/api/v1/posts/saved?limit=1)
- copy the full request `cookie` header from DevTools, not only the raw `substack.sid`
- if the Substack pull path returns a Cloudflare 403, assume the full browser cookie header is required

## 7. Runtime Architecture

### 7.1 `core/mind/`

The `mind` package lives under `core/mind/` and owns operator-facing runtime behavior. A tiny root `mind/` compatibility shim keeps `python -m mind` and existing imports working from a source checkout.

Important modules:

- [`core/mind/cli.py`](core/mind/cli.py)
  Top-level canonical CLI entrypoint.
- [`core/mind/commands/registry.py`](core/mind/commands/registry.py)
  CLI command registration and public command surface.
- [`core/mind/runtime_state.py`](core/mind/runtime_state.py)
  SQLite-backed operational state.
- [`core/mind/services/orchestrator.py`](core/mind/services/orchestrator.py)
  Daily unattended sweep implementation.
- [`core/mind/services/queue_worker.py`](core/mind/services/queue_worker.py)
  Queue processing seam used by worker flows.
- [`core/mind/services/provider_ops.py`](core/mind/services/provider_ops.py)
  Shared provider pull seams.
- [`core/mind/services/digest_service.py`](core/mind/services/digest_service.py)
  Shared digest writing seam.
- [`core/mind/mcp/server.py`](core/mind/mcp/server.py)
  MCP facade over the runtime and services layer.

### 7.2 `core/scripts/`

The `scripts` package lives under `core/scripts/` and owns lower-level mechanics:

- parsing
- fetching
- source-specific enrichment
- markdown writing helpers
- atom distillation helpers
- linting and migrations

`scripts` should contain mechanics, not a competing operator surface.

## 8. Runtime State

Operational state lives at the configured `state.runtime_db` path, defaulting to:

- `local_data/state/brain-runtime.sqlite3`

This file is runtime state, not durable product state.

It tracks:

- runs
- run events
- errors
- queue state
- locks
- Dream state
- MCP session state

It should not be committed.

Onboarding truth does not live in SQLite. Normalized onboarding bundles, decisions, validation output, materialization manifests, and resumable onboarding state live under the configured raw onboarding root.

## 9. Command Semantics

### 9.0 Onboarding pipeline

The canonical onboarding surface is `mind onboard`.

Current onboarding rules:

- Claude owns the interactive onboarding loop; `mind onboard` is backend-only for import/normalize/validate/materialize/replay/status
- import/normalize writes only to the configured raw onboarding root
- `mind onboard --from-json ...` is compatibility-only; the staged operator path is `import -> status -> normalize -> validate -> materialize -> replay -> status`
- onboarding prompts/responses/uploads are persisted as raw onboarding transcript/input state under the configured raw onboarding root
- materialization is the only onboarding step that writes durable memory pages
- every onboarding session persists raw intake, normalized evidence, decisions, validation, materialization manifest, and resumable state
- Dream readiness depends on the current onboarding session pointer plus successful materialization/readiness, not only on the existence of four files
- optional projects, people, concepts, playbooks, stances, and inquiries are created only through the onboarding decisions artifact

### 9.1 Daily orchestrator

The only unattended daily entrypoint is:

```bash
.venv/bin/python -m mind orchestrate daily
```

Its job is to:

1. load config
2. run `dropbox sweep`
3. run enabled provider pulls
4. drain allowed inbox-style ingest queues
5. run ingest processing
6. attempt cadence-controlled Dream stages
7. record health and outcomes

The orchestrator owns unattended cadence decisions.

### 9.2 Manual worker

The worker is intentionally narrower than the orchestrator.

Available commands:

```bash
.venv/bin/python -m mind worker run-once
.venv/bin/python -m mind worker drain-until-empty
```

Rules:

- queue-only
- no provider orchestration
- no scheduling policy ownership
- `drain-until-empty` continues past failures
- `drain-until-empty` exits nonzero at the end if any item failed

### 9.3 Direct Dream commands

Direct Dream commands are imperative:

```bash
.venv/bin/python -m mind dream light
.venv/bin/python -m mind dream deep
.venv/bin/python -m mind dream rem
```

If a human or agent calls them directly, they run.

Unattended Dream cadence belongs only to the daily orchestrator.

### 9.4 Digest

Digest is available:

- directly via `mind digest`
- indirectly through Deep

## 10. Queue and Boundary Rules

Queue work and orchestration have explicit boundaries.

### 10.1 Orchestrator queue-drain boundary

The daily orchestrator queue-drain phase must not become a generic executor for all queued work.

It is scoped to inbox-style ingest queue families.

If you add new queue families:

- decide whether they belong to unattended daily queue-drain
- update the allowlist intentionally
- update tests so the boundary stays explicit

### 10.2 Dream boundary

Queued Dream work must not silently bypass the orchestrator’s cadence decisions during unattended daily runs.

The orchestrator must remain the owner of unattended Dream cadence.

### 10.3 Locking

Locking is still relatively coarse.

That is acceptable for the current command set, but if you expand queue families or add new concurrent operators:

- revisit lock precedence explicitly
- add concurrency tests
- do not assume the current lock model is future-proof

## 11. Content Rules

### 11.1 Raw vs wiki

The configured dropbox root is the user-facing inbox for ad hoc files and exports.

The configured raw root is the engine-owned raw input/cache side.

Configured raw `drops/` is an internal machine queue, not the user inbox.

The configured memory root is maintained output.

Configured raw `onboarding/` is still raw-side material and state, but it is intentionally isolated from normal ingest lanes such as configured raw `drops/`.

The maintained knowledge graph belongs in the configured memory root, not in runtime state.

### 11.2 Source promotion

Raw source artifacts may be read, imported, or appended as part of pipelines, but the maintained wiki is the durable knowledge product.

### 11.3 Privacy

This repository must be public-safe by construction. Private memory remains local and ignored.

Never:

- publish contents externally without permission
- treat raw material as public-safe by default
- promote secrets or third-party private info into maintained pages
- commit `local_data/`, `memory/`, `raw/`, `dropbox/`, `.obsidian/`, root `config.yaml`, `.env`, runtime DBs, embeddings, provider exports, onboarding bundles, transcripts, or generated private markdown

Branch policy:

- local working branches may have ignored private data in the working tree
- commits intended for `main` must be system-only and public-safe
- if private Brain content lands in tracked files, remove it from tracking before continuing

### 11.4 File Classes

When deciding what may be committed on a mixed branch and what may be promoted to `main`, classify files explicitly.

System files are architecture/codebase/runtime-policy files. These are eligible for promotion to `main` when they are intentionally changed:

- root runtime/config/docs surfaces such as `config.example.yaml`, `README.md`, `AGENTS.md`, `CLAUDE.md`, `.gitignore`, `.env.example`, and current docs under `docs/` and `contracts/`
- implementation code under `core/mind/` and `core/scripts/`
- tests and fixtures under `tests/`
- examples and wrapper surfaces under `examples/`, `.claude/commands/`, and `.codex/prompts/`
- rebuildable local databases and caches are **not** durable system files and still must not be committed

User files are private vault/work-product/content files. These must never be promoted to `main`:

- local config and secrets under `local_data/`, `.env`, `.env.*`, and root `config.yaml`
- maintained graph content under configured memory roots, including root `memory/`
- raw inputs, exports, inbox artifacts, onboarding bundles, and caches under configured raw roots, including root `raw/`
- ad hoc inbox material under configured dropbox roots, including root `dropbox/`
- user-local Obsidian state or repo-local UI state when it reflects the private vault rather than shared architecture intent
- any other file whose primary purpose is to store user-specific knowledge, notes, source material, or generated private outputs

Classification rule:

- if a file changes how Brain works, it is probably a system file
- if a file stores what Brain knows about a real owner or private source material, it is a user file
- if a change mixes both concerns, split it before promoting anything to `main`

## 12. Documentation Policy

The current docs surface must stay small and current.

Current docs:

- [`README.md`](README.md)
- [`AGENTS.md`](AGENTS.md)
- [`docs/README.md`](docs/README.md)
- [`docs/SYSTEM_RULES.md`](docs/SYSTEM_RULES.md)
- [`docs/OBSIDIAN_THEME.md`](docs/OBSIDIAN_THEME.md)
- [`docs/schema-v2.md`](docs/schema-v2.md)
- [`contracts/brain-contract.yaml`](contracts/brain-contract.yaml)

Historical docs are local-only release exclusions.

Rules:

- do not leave obsolete docs in the active surface
- when a doc becomes historical, keep it out of the public release tree
- do not point current users or operators into local archives unless explicitly calling out historical context

## 13. Wrapper Workflow Policy

`.claude/commands/` and `.codex/prompts/` may contain:

1. thin wrappers around canonical CLI commands
2. intentionally prompt-native holdouts

Thin wrappers should stay thin.

Prompt-native holdouts should be explicitly marked as such and should not silently drift into separate implementations that compete with the CLI.

## 14. Cleanup Policy

When cleaning the repo:

- remove tracked generated or runtime files from git
- add ignore rules so they stay out
- archive old docs instead of silently deleting historical context
- finish layout transitions across the entire surface instead of leaving mixed-path ambiguity
- do not leave half-migrated layouts in docs, tests, examples, and runtime code

Generated or local artifacts that should not be tracked include:

- `local_data/`
- `memory/`
- `raw/`
- `dropbox/`
- `.obsidian/`
- `config.yaml`
- `.env`
- `.brain-runtime.sqlite3`
- `.brain-graph.sqlite3`
- `.brain-sources.sqlite3`
- `.logs/`
- `.omx/`
- `.claude/settings.local.json`
- stray generated vault output outside the configured live root

## 15. Commit Policy

Use Lore-style commits.

Each commit should explain why the change exists, not only what changed.

Preferred structure:

- intent line
- short context body
- git-native trailers:
  - `Constraint:`
  - `Rejected:`
  - `Confidence:`
  - `Scope-risk:`
  - `Reversibility:`
  - `Directive:`
  - `Tested:`
  - `Not-tested:`

Commit rules:

- keep commits scoped to one coherent unit of change
- do not mix runtime or local generated junk into real code or doc commits
- if archiving or deleting legacy material, make that action explicit in the commit message

### 15.1 Mixed Branch Workflow

Branches used for private Brain work may contain both system files and user files.

Working rules on mixed branches:

- it is acceptable to do architecture work in a branch that also contains user files
- but architecture/system changes should be kept in their own commits whenever possible
- user-content commits and architecture commits should not be mixed if the architecture work is expected to be promoted to `main`
- if a commit accidentally mixes system files and user files, split or rebuild the clean architecture-only commit before promoting

Promotion rules from a mixed branch to `main`:

1. identify the exact architecture-only commit or commit stack to promote
2. inspect the diff against `main` and confirm it touches only system files
3. do **not** merge the whole branch to `main` if that branch contains user-file history
4. promote by replaying only the clean architecture commits onto `main` (for example via cherry-pick or an equivalent clean-history method)
5. verify that `main` remains free of `memory/`, `raw/`, `dropbox/`, and other user-file changes after promotion

Worktree note:

- using a separate checkout/worktree for `main` is acceptable operationally
- it does **not** change the promotion policy
- a worktree is only a safer place to update `main`; it must still receive only clean architecture-only commits

## 16. Verification Rules

Before claiming work is complete:

- run the relevant tests
- read the output
- verify that the result actually passed
- run syntax or compile checks when appropriate
- run targeted diagnostics when helpful
- keep the evidence concrete

For doc changes:

- run doc or wrapper tests if present
- run reference sweeps for stale paths

For runtime changes:

- run focused regression suites around the changed surfaces
- do not claim success on “should” or “probably”

## 17. Current Rough Edges

Known non-blocking limits of the current architecture:

- `worker drain-until-empty` can still be operationally unbounded if allowed queue work keeps generating more allowed queue work during the same drain window
- locking is still coarse
- health output does not yet present manual Dream runs and orchestrated cadence-affecting Dream runs as especially distinct operator-facing categories

Treat these as known limits, not surprises.

## 18. Quick Mental Model

If you remember only a few things, remember these:

1. `config.example.yaml` is the public template; private config lives in `local_data/config.yaml` or `BRAIN_CONFIG_PATH`.
2. The CLI is the execution engine.
3. `mind orchestrate daily` owns unattended daily behavior.
4. Direct Dream commands run when called: Light, Deep, and REM are the canonical stages.
5. The worker is queue-only.
6. Use `Vault`/config for private roots; never hard-code or commit `dropbox/`, `raw/`, `memory/`, or `local_data/`.
7. Current docs are small; old docs are archived.
8. Do not commit runtime or local generated artifacts.
