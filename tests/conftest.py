from __future__ import annotations

from pathlib import Path
import sys

import pytest

from tests.paths import REPO_ROOT


if sys.version_info[:2] != (3, 11):
    pytest.exit(
        f"Brain tests require Python 3.11.x; found {sys.version.split()[0]}",
        returncode=2,
    )

repo_root = str(REPO_ROOT)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)


def _relative_test_path(item: pytest.Item) -> Path | None:
    path_obj = getattr(item, "path", None)
    path = Path(path_obj).resolve() if path_obj is not None else Path(str(item.fspath)).resolve()
    try:
        return path.relative_to(REPO_ROOT)
    except ValueError:
        return None


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        rel_path = _relative_test_path(item)
        if rel_path is None:
            continue

        rel_text = rel_path.as_posix()
        file_name = rel_path.name

        if "/integration/" in f"/{rel_text}" or "smoketest" in file_name:
            item.add_marker(pytest.mark.integration)

        if "/readiness/" in f"/{rel_text}":
            item.add_marker(pytest.mark.readiness)
            item.add_marker(pytest.mark.slow)

        if "golden" in file_name or "/golden/" in f"/{rel_text}":
            item.add_marker(pytest.mark.golden)

        if rel_path == Path("tests/project/test_contracts.py"):
            item.add_marker(pytest.mark.contract)

        if "smoketest" in file_name or file_name.endswith("integration_deep.py"):
            item.add_marker(pytest.mark.slow)
