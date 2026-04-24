---
description: Thin wrapper for the unattended-ingest readiness gate
argument-hint: [--dropbox-limit N] [--lane-limit N] [--include-promotion-gate]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind ingest readiness $ARGUMENTS
```

Use this before a supervised `dropbox sweep` or before trusting `orchestrate daily` after ingestion changes.
