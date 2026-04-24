---
description: Thin wrapper for the canonical mind ingest substack command
argument-hint: [path-to-export] [--today YYYY-MM-DD]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind ingest substack $ARGUMENTS
```

For raw export acquisition only, the stable compatibility surface remains:

```bash
.venv/bin/python -m mind substack pull
```
