---
description: Thin wrapper for the canonical mind ingest books command
argument-hint: <path-to-export>
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind ingest books $ARGUMENTS
```

For raw Audible export acquisition only, the stable compatibility surface remains:

```bash
.venv/bin/python -m mind audible pull
```
