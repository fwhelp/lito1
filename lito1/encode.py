"""Encoding tools and track-selection pipeline."""

from __future__ import annotations

from .core import legacy_module

_legacy = legacy_module()

find_handbrake = _legacy.find_handbrake
find_ffmpeg = _legacy.find_ffmpeg
run_scan = _legacy.run_scan
scan_and_autoselect = _legacy.scan_and_autoselect
auto_select_subtitle = _legacy.auto_select_subtitle
auto_select_audio = _legacy.auto_select_audio
build_command = _legacy.build_command
run_encode = _legacy.run_encode
build_ffmpeg_nvenc_command = _legacy.build_ffmpeg_nvenc_command
run_ffmpeg_nvenc_hardsub = _legacy.run_ffmpeg_nvenc_hardsub
process_files = _legacy.process_files
process_files_ffmpeg = _legacy.process_files_ffmpeg
delete_originals = _legacy.delete_originals

__all__ = [
    "find_handbrake",
    "find_ffmpeg",
    "run_scan",
    "scan_and_autoselect",
    "auto_select_subtitle",
    "auto_select_audio",
    "build_command",
    "run_encode",
    "build_ffmpeg_nvenc_command",
    "run_ffmpeg_nvenc_hardsub",
    "process_files",
    "process_files_ffmpeg",
    "delete_originals",
]
