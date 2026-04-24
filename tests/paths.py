from __future__ import annotations

from pathlib import Path


TESTS_ROOT = Path(__file__).resolve().parent
REPO_ROOT = TESTS_ROOT.parent
FIXTURES_ROOT = TESTS_ROOT / "fixtures"
EXAMPLES_ROOT = REPO_ROOT / "examples" / "synthetic"
SKILLS_ROOT = REPO_ROOT / "skills"
