import sys


if sys.version_info[:2] != (3, 11):
    raise SystemExit(
        f"Brain requires Python 3.11.x; found {sys.version.split()[0]}"
    )

from mind.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
