# Obsidian Theme

Brain ships a repo-managed Obsidian design system based on Kanagawa. The managed output is private-local: it is written inside the configured memory root, not to a tracked repo-root `.obsidian/` directory.

Canonical command:

```bash
.venv/bin/python -m mind obsidian theme apply
```

The theme system owns:

- `design/obsidian-kanagawa.json` as the token source of truth
- `<configured memory root>/.obsidian/snippets/brain-kanagawa.css` as the generated CSS snippet
- `<configured memory root>/.obsidian/graph.json` as the canonical graph color configuration plus default graph tuning seed
- managed theme keys in `<configured memory root>/.obsidian/appearance.json`

Defaults:

- dark base: `dragon`
- light base: `lotus`
- visual model: default Obsidian base theme plus the managed Brain snippet

## Color Semantics

Graph colors are stable by page family:

- `me`: owner / identity
- `projects`: active work coordination
- `people`: relationships
- `companies`: organizations
- `channels`: creator/reference streams
- `concepts`: abstractions
- `playbooks`: procedures
- `stances`: beliefs
- `inquiries`: open questions
- `decisions`: commitments
- `sources/books`: book sources
- `sources/youtube`: YouTube sources
- `sources/substack`: Substack sources
- remaining `sources`: other evidence
- `summaries`: synthesis, hidden from the default graph filter
- `inbox`: transient review

## Maintenance

To restore the canonical look after local drift:

```bash
.venv/bin/python -m mind obsidian theme apply
```

To rewrite the managed files even if the generated content is unchanged:

```bash
.venv/bin/python -m mind obsidian theme apply --force
```

If you add a new page family or want to rebalance colors, edit `design/obsidian-kanagawa.json` and rerun the apply command.
