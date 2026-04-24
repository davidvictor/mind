---
description: Thin wrapper for the canonical mind llm telemetry surface
argument-hint: audit [--today|--date YYYY-MM-DD] [--bundle ID] [--refresh-gateway]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind llm $ARGUMENTS
```

Use `audit` to summarize local `.logs/llm/` telemetry, optionally filtered by date, bundle, task class, or model.
