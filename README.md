# lito1

This repo contains the `lito1` Jellyfin anime automation pipeline.

## Project Layout

- `lito1/config.py` - config loading and constants
- `lito1/nyaa.py` - Nyaa search/ranking/selection helpers
- `lito1/qbit.py` - qBittorrent integration and cleanup monitor
- `lito1/encode.py` - HandBrake/FFmpeg scan and encode flow
- `lito1/watch.py` - watch-list and RSS polling mode
- `lito1/jellyfin.py` - Jellyfin setup/connectivity/rescan
- `lito1/cli.py` - CLI entrypoint
- `lito1/core.py` - loader for the existing monolithic engine

## Transitional Architecture

The new modules are wired as facades over the existing script
`lito1.py` so behavior remains stable.

This allows incremental extraction into true per-module implementations without
breaking existing workflow.

## Install

```bash
pip install -e .
```

## Run

```bash
anime
```

or:

```bash
python -m lito1
```

## Dependencies

- requests
- beautifulsoup4
- qbittorrent-api
- anitopy
- guessit
- tomli (Python <3.11)

