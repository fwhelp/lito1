"""Watch-list and RSS polling workflow."""

from __future__ import annotations

from .core import legacy_module

_legacy = legacy_module()

WATCHLIST_FILENAME = _legacy.WATCHLIST_FILENAME
watchlist_path = _legacy.watchlist_path
load_watchlist = _legacy.load_watchlist
save_watchlist = _legacy.save_watchlist
upsert_watch_entry = _legacy.upsert_watch_entry
print_watchlist_status = _legacy.print_watchlist_status
poll_rss_for_entry = _legacy.poll_rss_for_entry
rebuild_watchlist_from_disk = _legacy.rebuild_watchlist_from_disk
run_watch_mode = _legacy.run_watch_mode

__all__ = [
    "WATCHLIST_FILENAME",
    "watchlist_path",
    "load_watchlist",
    "save_watchlist",
    "upsert_watch_entry",
    "print_watchlist_status",
    "poll_rss_for_entry",
    "rebuild_watchlist_from_disk",
    "run_watch_mode",
]
