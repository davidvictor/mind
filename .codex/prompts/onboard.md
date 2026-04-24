---
description: Codex-owned onboarding workflow over backend-only mind onboard commands
argument-hint: <path-to-onboarding-json>
---

Use this workflow when the owner wants to onboard or resume onboarding.

Codex is the interaction owner. `mind onboard` is the backend state machine.

Workflow:

1. Import the raw intake without writing durable wiki pages.

```bash
.venv/bin/python -m mind onboard import --from-json "$ARGUMENTS"
```

2. Read the returned `bundle=<id>` and inspect backend state.

```bash
.venv/bin/python -m mind onboard status --bundle <bundle-id>
```

3. Ask exactly one backend-emitted question at a time. After each answer or upload, send it back with `normalize`.

```bash
.venv/bin/python -m mind onboard normalize --bundle <bundle-id> --response <question-id>=<answer>
.venv/bin/python -m mind onboard normalize --bundle <bundle-id> --upload /absolute/path/to/file
```

4. Repeat `status -> ask one question -> normalize` until `ready_for_materialization: yes`.

5. Validate before writing durable pages.

```bash
.venv/bin/python -m mind onboard validate --bundle <bundle-id>
```

6. Materialize durable pages only after validation is ready.

```bash
.venv/bin/python -m mind onboard materialize --bundle <bundle-id>
```

7. Re-check final state.

```bash
.venv/bin/python -m mind onboard status --bundle <bundle-id>
```

Notes:

- `.venv/bin/python -m mind onboard --from-json <path>` is compatibility-only and skips the question-by-question Codex workflow.
- Durable `memory/` writes happen only during `materialize`.
- Raw onboarding transcript/history, uploads, normalized evidence, validation, and manifests stay under `raw/onboarding/`.
