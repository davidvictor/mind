# Schema Summary

The machine-readable source of truth for the Brain data model is:

- [`contracts/brain-contract.yaml`](../contracts/brain-contract.yaml)

This file is the short human-readable summary.

## Core Ideas

- Every durable knowledge artifact is markdown in `memory/`.
- Frontmatter carries the typed/queryable metadata.
- The contract file defines the canonical field shapes and validation expectations.

## Important Page Families

- source pages
- synthetic/operator summaries
- concepts
- playbooks
- stances
- inquiries
- identity pages under `memory/me/`

## Minimum Expectations

- Pages should have stable IDs in frontmatter.
- Internal references should use wiki-links where appropriate.
- Runtime and lint expectations should follow the contract, not ad hoc local fields.

Historical schema design notes are intentionally not part of the public docs
surface. The active contract and this summary are the release references.
