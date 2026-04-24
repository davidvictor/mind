---
description: Thin wrapper for the canonical mind ingest youtube command
argument-hint: <path-to-export> [--default-duration-minutes N]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind ingest youtube $ARGUMENTS
```

For raw watch-history acquisition only, the stable compatibility surface remains:

```bash
.venv/bin/python -m mind youtube pull
```
