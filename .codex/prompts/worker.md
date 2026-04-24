---
description: Thin wrapper for the canonical mind worker surface
argument-hint: run-once|drain-until-empty
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind worker $ARGUMENTS
```

Use `run-once` for one queued item or `drain-until-empty` for full manual recovery.
