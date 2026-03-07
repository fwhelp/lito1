"""Nyaa search, ranking, and episode-selection helpers."""

from __future__ import annotations

from .core import legacy_module

_legacy = legacy_module()

search_nyaa = _legacy.search_nyaa
search_all_pages = _legacy.search_all_pages
extract_sub_group = _legacy.extract_sub_group
rank_sub_groups = _legacy.rank_sub_groups
pick_best_group = _legacy.pick_best_group
extract_season_number = _legacy.extract_season_number
extract_episode_number = _legacy.extract_episode_number
build_season_query = _legacy.build_season_query
filter_episodes = _legacy.filter_episodes
filter_episodes_any_group = _legacy.filter_episodes_any_group
verify_episode_coverage = _legacy.verify_episode_coverage
fetch_batch_file_list = _legacy.fetch_batch_file_list
find_best_batch_for_season = _legacy.find_best_batch_for_season
post_process_batch_download = _legacy.post_process_batch_download

__all__ = [
    "search_nyaa",
    "search_all_pages",
    "extract_sub_group",
    "rank_sub_groups",
    "pick_best_group",
    "extract_season_number",
    "extract_episode_number",
    "build_season_query",
    "filter_episodes",
    "filter_episodes_any_group",
    "verify_episode_coverage",
    "fetch_batch_file_list",
    "find_best_batch_for_season",
    "post_process_batch_download",
]
