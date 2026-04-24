---
description: Thin wrapper for the canonical mind ingest surface
argument-hint: <path-to-raw-file>
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind ingest file "$ARGUMENTS"
```

Use `/dropbox` with `mind dropbox sweep` for the normal inbox workflow. Use `python -m mind ingest --help` to see the umbrella ingest surface for direct file, books, YouTube, Audible, Substack, articles, and links lanes.

For rollout and recovery, prefer the newer canonical surfaces:

- `python -m mind ingest readiness`
- `python -m mind ingest repair-articles --dry-run`
- `python -m mind ingest reingest --lane <lane> --dry-run`
