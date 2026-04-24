---
description: Thin wrapper for the canonical mind ingest audible command
argument-hint: [--library-only] [--sleep seconds]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind ingest audible $ARGUMENTS
```

For raw auth verification only, the stable compatibility surface remains:

```bash
.venv/bin/python -m mind check audible-auth
```
