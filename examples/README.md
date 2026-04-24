# Thin Synthetic Harness

`examples/synthetic/` is the canonical thin synthetic harness for pre-ingest
architecture work in Brain. Tests copy this directory into a temporary root so
Dream stages and other runtime flows can mutate it without touching the
repo-owned seed.

The seeded harness is intentionally minimal. It includes only:

- the canonical split-root config in `config.yaml`
- Dream precondition owner notes under `memory/me/`
- one representative active page in each canonical atom family
- one summary/source pair for Light Dream input
- shared durable scaffold files such as `.brain-state.json`, `INDEX.md`, and
  `CHANGELOG.md`

The seeded `.brain-state.json` is a synthetic atom-cache scaffold. Runtime
coordination still belongs to SQLite state when copied-harness tests run Dream
flows.

Generated runtime outputs are intentionally not pre-seeded here. Copied-harness
tests create probationary atoms, digests, reflections, timeline entries, and
skills dynamically.

The copied-harness suite and harness lint commands are:

```bash
.venv/bin/pytest -q \
  tests/common/test_common_config.py \
  tests/common/test_common_profile.py \
  tests/common/test_common_vault.py \
  tests/project/test_example_memory_smoke.py \
  tests/runtime/test_dream_runtime.py

.venv/bin/python -m scripts.lint examples/synthetic
```

Structure:

- `synthetic/memory/` — the seeded synthetic memory graph
- `synthetic/raw/` — synthetic raw inputs used by copied-harness tests
