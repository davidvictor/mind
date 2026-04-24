from __future__ import annotations

from pathlib import Path

from mind.runtime_state import RuntimeState
from mind.services.weave_cleanup import run_weave_cleanup
from tests.support import write_repo_config


def _write_atom(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "---\n"
        "id: old-atom\n"
        "type: concept\n"
        "title: Old Atom\n"
        "relates_to:\n"
        "  - \"[[keep-me]]\"\n"
        "  - \"[[weave-old-cluster]]\"\n"
        "weave_cluster_refs:\n"
        "  - \"[[weave-old-cluster]]\"\n"
        "last_weaved_at: 2026-04-22\n"
        "---\n\n"
        "# Old Atom\n",
        encoding="utf-8",
    )


def test_weave_cleanup_strips_fields_archives_pages_and_clears_runtime_state(tmp_path: Path) -> None:
    write_repo_config(tmp_path)
    atom = tmp_path / "memory" / "concepts" / "old-atom.md"
    _write_atom(atom)
    old_page = tmp_path / "memory" / "dreams" / "weave" / "weave-old-cluster.md"
    old_page.parent.mkdir(parents=True, exist_ok=True)
    old_page.write_text("# Old cluster\n", encoding="utf-8")
    state = RuntimeState.for_repo_root(tmp_path)
    with state.connect() as conn:
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(dream_state)").fetchall()}
        if "last_weave" not in columns:
            conn.execute("ALTER TABLE dream_state ADD COLUMN last_weave TEXT")
        conn.execute(
            """
            UPDATE dream_state
            SET last_weave = '2026-04-22',
                last_lock_holder = 'dream-weave-v2-shadow',
                last_lock_acquired_at = '2026-04-22T20:42:33Z'
            WHERE id = 1
            """
        )
        conn.execute(
            "INSERT INTO locks(name, holder, acquired_at) VALUES (?, ?, ?)",
            ("dream-weave-v2-shadow", "pytest-weave", "2026-04-22T20:42:33Z"),
        )

    dry = run_weave_cleanup(tmp_path, apply=False)
    assert dry.pages_to_update == ["memory/concepts/old-atom.md"]
    assert dry.archived_to
    assert old_page.exists()

    applied = run_weave_cleanup(tmp_path, apply=True)
    text = atom.read_text(encoding="utf-8")
    assert "weave_cluster_refs" not in text
    assert "last_weaved_at" not in text
    assert "[[weave-old-cluster]]" not in text
    assert "[[keep-me]]" in text
    assert not old_page.exists()
    assert (tmp_path / applied.archived_to).exists()
    assert applied.runtime_state_cleared is True
    assert applied.runtime_locks_cleared == 1
    with state.connect() as conn:
        row = conn.execute(
            "SELECT last_weave, last_lock_holder, last_lock_acquired_at FROM dream_state WHERE id = 1"
        ).fetchone()
        lock_count = conn.execute("SELECT COUNT(*) FROM locks WHERE lower(name) LIKE '%weave%'").fetchone()[0]
    assert row["last_weave"] is None
    assert row["last_lock_holder"] is None
    assert row["last_lock_acquired_at"] is None
    assert lock_count == 0
