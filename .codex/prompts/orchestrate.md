---
description: Thin wrapper for the canonical mind orchestrate surface
argument-hint: daily
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind orchestrate $ARGUMENTS
```

Use `daily` for the unattended sweep. That run now starts by sweeping the configured dropbox inbox before provider pulls, queue drain, and Dream.

Before trusting unattended daily runs after ingestion/platform changes, run:

- `python -m mind graph health --skip-promotion-gate`
- `python -m mind ingest readiness`
- `python -m mind dropbox sweep --dry-run`
