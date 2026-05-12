# lito1

`lito1` is a direct Nyaa-to-Jellyfin anime pipeline.

It searches Nyaa, picks a viable release, sends the torrent to qBittorrent,
routes finished files into your Jellyfin library, normalizes names for library
detection, and triggers a Jellyfin rescan when the run completes.

## What It Does

- Searches Nyaa for season and batch releases
- Scores release groups and prefers healthier swarms
- Downloads through qBittorrent and monitors progress
- Detects stuck torrents and retries with a different source strategy
- Moves episodes and subtitle sidecars into Jellyfin-ready season folders
- Triggers Jellyfin library scans after successful routing
- Supports watch mode for polling currently airing shows

## Current Workflow

- Direct-to-Jellyfin pipeline
- No HandBrake stage
- No subtitle burn-in stage
- Optimized around Jellyfin naming, metadata pickup, and rescans

## Project Layout

- `lito1/config.py` - config loading and constants
- `lito1/nyaa.py` - Nyaa search and source selection helpers
- `lito1/qbit.py` - qBittorrent integration and cleanup monitor
- `lito1/encode.py` - legacy compatibility wrapper
- `lito1/watch.py` - watch-list and RSS polling mode
- `lito1/jellyfin.py` - Jellyfin setup, connectivity, and rescans
- `lito1/cli.py` - CLI entrypoint
- `lito1/core.py` - loader for the monolithic engine

## Transitional Architecture

The modular package currently wraps the main script
`lito1.py` so behavior stays stable while the codebase is gradually split into
smaller pieces.

## Install

```bash
pip install -e .
```

## Run

```bash
lito1
```

or:

```bash
python -m lito1
```

Windows launcher:

```powershell
.\lito1.bat
```

## Common Commands

```bash
lito1
lito1 --watch
lito1 --watch-status
lito1 --setup-jellyfin
```

## Requirements

- Python 3.10+
- qBittorrent with Web UI enabled
- Jellyfin server API access
- Nyaa reachable from the machine running the script

## Dependencies

- requests
- beautifulsoup4
- qbittorrent-api
- anitopy
- guessit
- tomli (Python <3.11)
