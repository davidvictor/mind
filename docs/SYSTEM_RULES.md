# System Rules

These are the current high-level rules for the Brain runtime.

## File Ownership

- `config.example.yaml` is the public config template.
- `local_data/config.yaml` or `BRAIN_CONFIG_PATH` is the private runtime config authority.
- Configured `dropbox_root` is the user-facing inbox for ad hoc files and exports.
- Configured `raw_root` is source material and should be treated as append-only input.
- Configured raw `drops/` is an engine-owned queue/state area for machine-produced drop artifacts.
- Configured raw `onboarding/` is an isolated onboarding intake lane and must not be treated as an ordinary ingest drop queue.
- Configured `memory_root` is maintained output.
- Configured `state_root` stores runtime, graph, source, and vector DBs; these should not be committed.
- Memory `.brain-state.json` is a regenerable atom-cache file, not operational runtime authority.

## Runtime Rules

- The CLI is the execution engine.
- `mind dropbox sweep` is the explicit user inbox ingest command.
- `mind readiness --scope new-user` is the first-run operator gate.
- `mind ingest readiness` is the unattended-ingest rollout gate.
- `mind ingest repair-articles` is the deterministic article cache repair surface.
- `mind graph rebuild`, `mind graph status`, `mind graph health`, and `mind graph resolve` are the operator surfaces for the canonical graph registry.
- `mind reset` is the explicit destructive wipe surface for returning configured memory/raw/dropbox roots to an empty starter layout.
- `mind seed` is the explicit semantic starter-graph initializer and must stay separate from `mind reset`.
- `mind obsidian theme apply` is the canonical surface for regenerating repo-managed Obsidian appearance and graph artifacts inside the configured private memory root.
- Shadow vector matches are advisory only in the current rollout; they may enrich review/evaluation output but do not create canonical edges or auto-resolve nodes.
- `mind orchestrate daily` is the unattended daily entrypoint.
- `mind orchestrate daily` sweeps the configured dropbox root before provider pulls and Dream.
- Dropbox dry-run is a graph-aware preflight, not just an extension-based route preview.
- `mind worker run-once` and `mind worker drain-until-empty` are manual queue tools.
- Direct Dream commands are imperative.
- `mind dream bootstrap` is an explicit operator-only maintenance lane, not part of unattended daily cadence.
- `mind dream campaign` is an explicit operator-only reorg lane, not part of unattended daily cadence.
- In campaign `aggressive`, Light rescans the current source corpus on every scheduled day, Deep keeps day-interval cadence, and REM runs on day 0 then once per calendar month.
- In campaign `yearly`, Light still rescans the current source corpus on every scheduled day, but it keeps strict lane behavior, suppresses campaign-generated inbox nudges, and checkpoints long Light passes for operator resume.
- Campaign resume must reuse the persisted schedule/config snapshot for that run and fail fast if schedule-affecting `dream.campaign` knobs have drifted.
- `mind dream simulate-year` is the accelerated Dream feature. It must run under ignored `local_data/simulations/<run-id>/` roots, seed simulation-local memory plus Dream raw transcript/cache inputs, force separate memory/raw/state/vector DB paths, use Light/Deep/REM only, and emit candidate graph deltas instead of mutating the live timeline or live Dream state.
- Unattended Dream cadence belongs to the orchestrator.
- Dream readiness depends on validated onboarding state plus projected core onboarding outputs, not only on file existence.
- Ingest lanes materialize one canonical durable source page per item under configured memory `sources/...`; summary pages are reserved for synthetic/operator outputs.
- Pass D is the only ordinary per-source encoding layer.
- Successful evidence appends must preserve a structured evidence edge under configured raw `evidence-edges/`; markdown is the readable view, not the whole substrate.
- Light Dream runs through the Dream v2 stage dispatcher and stays a bounded cross-source consolidator over shared distillation selectors.
- Deep Dream runs through the Dream v2 stage dispatcher as the weekly relation editor and may regenerate digest/index/open-inquiries outputs.
- REM Dream runs through the Dream v2 stage dispatcher as the monthly graph-pruning and reflection pass. Its canonical durable outputs live under configured memory `dreams/rem/` and `me/reflections/`.
- Dream v2 artifacts are durable operator evidence under configured raw `reports/dream/v2/`.
- Structural link-weaving belongs inside Light as safe relation hints or inside Deep as bounded consolidation. It is not a standalone Dream stage.
- REM must interpret what the graph is becoming; it must not depend on a separate structural clustering handoff.

## Documentation Rules

- Keep the current doc surface small and current.
- Keep superseded specs, plans, and setup notes out of the public release tree.
- Do not let README, DESIGN, and runtime behavior drift apart.

## Safety Rules

- Do not invent facts in wiki content.
- Do not publish private vault content externally without explicit permission.
- Do not commit private roots (`local_data/`, `memory/`, `raw/`, `dropbox/`, `.obsidian/`) or local config/secrets.
- Do not silently tolerate legacy root config files.
