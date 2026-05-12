"""Jellyfin setup, validation, and library refresh helpers."""

from __future__ import annotations

from .core import legacy_module

_legacy = legacy_module()

load_jellyfin_config = _legacy.load_jellyfin_config
setup_jellyfin_config = _legacy.setup_jellyfin_config
test_jellyfin_connectivity = _legacy.test_jellyfin_connectivity
trigger_jellyfin_rescan = _legacy.trigger_jellyfin_rescan

__all__ = [
    "load_jellyfin_config",
    "setup_jellyfin_config",
    "test_jellyfin_connectivity",
    "trigger_jellyfin_rescan",
]
