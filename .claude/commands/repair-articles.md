---
description: Thin wrapper for deterministic article cache repair
argument-hint: --dry-run | --apply [--limit N] [--today YYYY-MM-DD] [--source-id ID]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind ingest repair-articles $ARGUMENTS
```

Use `--dry-run` first to classify blocked article replays into acquisition refreshes versus downstream recomputes.
