---
description: Thin wrapper for the canonical dropbox inbox surface
argument-hint: sweep | status | migrate-legacy
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind dropbox $ARGUMENTS
```

Use `sweep` for the normal inbox ingest flow, `status` to inspect pending files, and `migrate-legacy` to move user-like files out of `raw/drops/`.

Recommended rollout flow:

1. `python -m mind ingest readiness`
2. `python -m mind dropbox sweep --dry-run`
3. confirm `would_review=0` and `would_fail=0`
4. `python -m mind dropbox sweep`
