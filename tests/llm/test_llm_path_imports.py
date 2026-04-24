from __future__ import annotations

from tests.paths import REPO_ROOT


ACTIVE_PATHS = [
    REPO_ROOT / "core" / "scripts" / "articles" / "enrich.py",
    REPO_ROOT / "core" / "scripts" / "youtube" / "enrich.py",
    REPO_ROOT / "core" / "scripts" / "books" / "enrich.py",
    REPO_ROOT / "core" / "scripts" / "substack" / "enrich.py",
]


def test_active_llm_paths_do_not_import_legacy_llm_facades():
    for path in ACTIVE_PATHS:
        text = path.read_text(encoding="utf-8")
        assert "scripts.common.gemini" not in text
        assert "from scripts.common import gemini" not in text
        assert "mind.services.llm_compat" not in text
        assert "gemini_compat" not in text
