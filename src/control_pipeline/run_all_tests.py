"""run_all_tests.py — Standalone test runner for EAA orchestrator block.

Executes all unit and integration test suites directly.
"""

from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    src_dir = Path(__file__).resolve().parent.parent
    repo_root = src_dir.parent
    for p in (str(repo_root), str(src_dir)):
        if p not in sys.path:
            sys.path.insert(0, p)

    test_dir = str(Path(__file__).resolve().parent / "tests")

    try:
        import pytest
        return pytest.main([test_dir, "-v"] + sys.argv[1:])
    except ImportError:
        print("pytest is not installed. Please install pytest and pytest-asyncio.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
