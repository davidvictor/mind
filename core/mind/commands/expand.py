from __future__ import annotations

import argparse

from mind.services.web_research import ingest_web_articles, search_web
from mind.services.cli_progress import progress_for_args
from .common import today_str, vault
from .ingest import ingest_file  # compatibility monkeypatch target
from .query import cmd_query

_search_web = search_web


def cmd_expand(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="expanding with web research", default=True) as progress:
        if not args.force_web:
            from .common import score_pages

            if score_pages(args.question, vault(), limit=3):
                return cmd_query(argparse.Namespace(question=args.question, limit=args.limit))
        progress.phase("searching the web")
        results = _search_web(args.question, limit=args.limit)
        if not results:
            print("No web results found.")
            return 1
        progress.phase("fetching web results")
        progress.phase("saving raw sources")
        grounded = ingest_web_articles(
            repo_root=vault().root,
            queries=[args.question],
            source_label=f"expand:{today_str()}",
            today=today_str(),
            results_per_query=args.limit,
        )
        if not grounded:
            print("No usable web results fetched.")
            return 1
        print("Saved web sources:")
        for item in grounded:
            print(f"- [[{item.article_page_id}]] ({item.url})")
        print("")
        progress.phase("querying local graph")
        return cmd_query(argparse.Namespace(question=args.question, limit=args.limit))
