"""qBittorrent integration and torrent lifecycle helpers."""

from __future__ import annotations

from .core import legacy_module

_legacy = legacy_module()

connect_qbit = _legacy.connect_qbit
add_torrent = _legacy.add_torrent
resolve_hash_for_title = _legacy.resolve_hash_for_title
CleanupMonitor = _legacy.CleanupMonitor

__all__ = [
    "connect_qbit",
    "add_torrent",
    "resolve_hash_for_title",
    "CleanupMonitor",
]
