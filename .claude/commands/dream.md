---
description: Thin wrapper for the canonical mind dream surface
argument-hint: light|deep|rem [--dry-run] | bootstrap [--dry-run] [--force-pass-d] [--checkpoint-every N] [--resume] [--limit N] | campaign --days N [--start-date YYYY-MM-DD] [--dry-run] [--resume] [--profile aggressive|yearly] | simulate-year [--start-date YYYY-MM-DD] [--run-id ID] [--days N] [--dry-run]
---

Run the canonical CLI:

```bash
.venv/bin/python -m mind dream $ARGUMENTS
```

Use `light`, `deep`, `rem`, `bootstrap`, `campaign`, or `simulate-year`. `light`, `deep`, and `rem` support `--dry-run`. `bootstrap` also supports `--force-pass-d`, `--checkpoint-every`, `--resume`, and `--limit`. `campaign` supports `--days`, `--start-date`, `--dry-run`, `--resume`, and `--profile`. `simulate-year` runs against ignored simulation roots.
