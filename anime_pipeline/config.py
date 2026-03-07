"""Configuration accessors and constants."""

from __future__ import annotations

from .core import legacy_module

_legacy = legacy_module()

cfg = _legacy._cfg
load_toml_config = _legacy._load_toml_config
write_example_config = _legacy._write_example_config

QBIT_HOST = _legacy.QBIT_HOST
QBIT_PORT = _legacy.QBIT_PORT
QBIT_USER = _legacy.QBIT_USER
QBIT_PASS = _legacy.QBIT_PASS
QBIT_CATEGORY = _legacy.QBIT_CATEGORY

NYAA_BASE = _legacy.NYAA_BASE
MONITOR_INTERVAL = _legacy.MONITOR_INTERVAL
DEFAULT_PAGES = _legacy.DEFAULT_PAGES

HB_DEFAULT_PRESET = _legacy.HB_DEFAULT_PRESET
HS_MARKER = _legacy.HS_MARKER
VIDEO_EXTENSIONS = _legacy.VIDEO_EXTENSIONS

__all__ = [
    "cfg",
    "load_toml_config",
    "write_example_config",
    "QBIT_HOST",
    "QBIT_PORT",
    "QBIT_USER",
    "QBIT_PASS",
    "QBIT_CATEGORY",
    "NYAA_BASE",
    "MONITOR_INTERVAL",
    "DEFAULT_PAGES",
    "HB_DEFAULT_PRESET",
    "HS_MARKER",
    "VIDEO_EXTENSIONS",
]
