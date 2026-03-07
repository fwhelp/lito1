# Anime Pipeline (Modular Project)

This repo contains a modularized layout for the Jellyfin anime automation pipeline.

## Project Layout

- `anime_pipeline/config.py` - config loading and constants
- `anime_pipeline/nyaa.py` - Nyaa search/ranking/selection helpers
- `anime_pipeline/qbit.py` - qBittorrent integration and cleanup monitor
- `anime_pipeline/encode.py` - HandBrake/FFmpeg scan and encode flow
- `anime_pipeline/watch.py` - watch-list and RSS polling mode
- `anime_pipeline/jellyfin.py` - Jellyfin setup/connectivity/rescan
- `anime_pipeline/cli.py` - CLI entrypoint
- `anime_pipeline/core.py` - loader for the existing monolithic engine

## Transitional Architecture

The new modules are wired as facades over the existing script
`Auto_script_for_Jellyfin_anime_v2.0_optimized_.py` so behavior remains stable.

This allows incremental extraction into true per-module implementations without
breaking existing workflow.

## Install

```bash
pip install -e .
```

## Run

```bash
anime-pipeline
```

or:

```bash
python -m anime_pipeline
```

## Dependencies

- requests
- beautifulsoup4
- qbittorrent-api
- anitopy
- guessit
- tomli (Python <3.11)
