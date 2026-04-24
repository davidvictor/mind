---
description: Thin wrapper for the canonical graph registry surface
argument-hint: rebuild | status | health | resolve "<text>" | embed ...
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind graph $ARGUMENTS
```

Use `health` to confirm the canonical graph is built and the shadow-vector index is populated. Shadow vector matches remain advisory in the current rollout.
