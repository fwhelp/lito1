"""CLI entrypoint for the modular package."""

from __future__ import annotations

from .core import legacy_module


def parse_args():
    return legacy_module().parse_args()


def main() -> None:
    legacy_module().main()


if __name__ == "__main__":
    main()
