"""Loader for the original monolithic implementation.

This keeps behavior stable while modules are split incrementally.
"""

from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path
from types import ModuleType

LEGACY_FILENAME = "lito1.py"


@lru_cache(maxsize=1)
def legacy_module() -> ModuleType:
    base_dir = Path(__file__).resolve().parent.parent
    legacy_path = base_dir / LEGACY_FILENAME
    if not legacy_path.exists():
        raise FileNotFoundError(f"Legacy script not found: {legacy_path}")

    spec = importlib.util.spec_from_file_location("lito1_legacy", legacy_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module spec from {legacy_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
