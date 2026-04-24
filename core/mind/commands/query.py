from __future__ import annotations

import argparse

from .common import score_pages, vault


def cmd_query(args: argparse.Namespace) -> int:
    v = vault()
    matches = score_pages(args.question, v, limit=args.limit)
    if not matches:
        print("No relevant wiki pages found.")
        print(f"Try: python3.11 -m mind expand \"{args.question}\"")
        return 1
    print(f"Question: {args.question}\n")
    print("Relevant pages:")
    for match in matches:
        annotation = f" — {'; '.join(match.annotations)}" if getattr(match, "annotations", None) else ""
        print(f"- [[{match.page_id}]] ({match.title}){annotation}")
    print("\nAnswer:")
    for match in matches[: min(3, len(matches))]:
        snippet = match.snippet or "(no body excerpt available)"
        if getattr(match, "annotations", None):
            print(f"- [[{match.page_id}]] — {snippet} ({'; '.join(match.annotations)})")
        else:
            print(f"- [[{match.page_id}]] — {snippet}")
    confidence = "high" if matches[0].score >= 5 else "medium" if matches[0].score >= 2 else "low"
    print(f"\nConfidence: {confidence}")
    return 0
