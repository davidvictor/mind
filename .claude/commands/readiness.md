---
description: Thin wrapper for the first-run operator readiness check
argument-hint: [--scope new-user] [--include-promotion-gate] [--skip-source-checks]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind readiness $ARGUMENTS
```

Use this when you want one command that checks onboarding, graph/ingest readiness, runtime health, and upstream pull/auth prerequisites for a fresh environment.
