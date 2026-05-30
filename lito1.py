#!/usr/bin/env python3
"""
lito1.py - Unified Nyaa -> qBittorrent -> Jellyfin pipeline.

Flow:
  1. Auto-discover the Jellyfin Anime directory (non-C: drive, parent = "media")
  2. Search nyaa.si, auto-rank sub groups, prompt for anime title + season
  3. Add torrents to qBittorrent, monitor until complete, auto-cleanup
  4. Route completed downloads directly into the Jellyfin media structure
  5. Trigger Jellyfin library rescan for newly downloaded content


Requirements:
    pip install requests beautifulsoup4 qbittorrent-api anitopy guessit

Configuration:
    Copy config.toml.example -> config.toml and edit to taste.
    Sensitive credentials (qBittorrent password, Jellyfin API key) can be
    supplied via environment variables QBIT_PASS and JELLYFIN_API_KEY to keep
    them out of plain-text config files.

Usage:
    python lito1.py
    python lito1.py --pick-group --season 2
    python lito1.py --no-confirm --dry-run
    python lito1.py --cleanup-files
    python lito1.py --watch
"""

import argparse
import datetime
import json
import logging
import logging.config
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import qbittorrentapi

# -- Python 3.11+ has tomllib in stdlib; fall back to tomli on older versions --
try:
    import tomllib  # type: ignore[import]
except ImportError:
    try:
        import tomli as tomllib  # type: ignore[import]
    except ImportError:
        tomllib = None  # type: ignore[assignment]


# -----------------------------------------------------------------------------
# CONFIG  - loaded from config.toml (alongside this script) at startup.
#           Sensitive secrets are read from environment variables so they never
#           live in a plain-text file:
#             QBIT_PASS          qBittorrent web-UI password
#             JELLYFIN_API_KEY   Jellyfin long-lived API token
# -----------------------------------------------------------------------------

_CONFIG_FILE = Path(__file__).with_name("config.toml")
_CONFIG_EXAMPLE = Path(__file__).with_name("config.toml.example")

def _load_toml_config() -> dict:
    """Load config.toml; silently return {} if tomllib unavailable or file missing."""
    if tomllib is None or not _CONFIG_FILE.exists():
        return {}
    try:
        with open(_CONFIG_FILE, "rb") as fh:
            return tomllib.load(fh)
    except Exception as exc:
        print(f"[WARN] Could not parse config.toml: {exc} - using defaults.")
        return {}

def _write_example_config() -> None:
    """Write a config.toml.example next to the script if it doesn't exist."""
    if _CONFIG_EXAMPLE.exists():
        return
    _CONFIG_EXAMPLE.write_text("""\
# lito1 configuration file
# Copy this file to config.toml and edit to taste.
# NEVER put passwords here - use the QBIT_PASS / JELLYFIN_API_KEY env vars.

[qbittorrent]
host     = "localhost"
port     = 8080
user     = "admin"
category = "anime"

[nyaa]
base_url         = "https://nyaa.si"
default_pages    = 15
monitor_interval = 30   # seconds between completion-check polls

[groups]
preferred = [
    "erai-raws", "subsplease", "horriblesubs", "judas",
    "ember", "yameii", "psa", "akihito", "commie",
]
quality_tolerance = 0.15

[handbrake]
default_preset = "General/Fast 1080p30"

[pipeline]
hs_marker = ".hs"
movie_library_dir = ""
""", encoding="utf-8")

_write_example_config()
_TOML = _load_toml_config()

def _cfg(section: str, key: str, default):
    return _TOML.get(section, {}).get(key, default)

# -- qBittorrent - password comes from env var (never from config file) --------
QBIT_HOST     = _cfg("qbittorrent", "host", "localhost")
QBIT_PORT     = _cfg("qbittorrent", "port", 8080)
QBIT_USER     = _cfg("qbittorrent", "user", "admin")
QBIT_PASS     = os.environ.get("QBIT_PASS") or _cfg("qbittorrent", "pass", "adminadmin")
QBIT_CATEGORY = _cfg("qbittorrent", "category", "anime")

NYAA_BASE        = _cfg("nyaa", "base_url", "https://nyaa.si")
MONITOR_INTERVAL = _cfg("nyaa", "monitor_interval", 30)
STUCK_NO_PROGRESS_SECS = int(_cfg("nyaa", "stuck_no_progress_secs", 900))
STUCK_EPISODE_RETRY_LIMIT = int(_cfg("nyaa", "stuck_episode_retry_limit", 2))
DEFAULT_PAGES    = _cfg("nyaa", "default_pages", 15)

DONE_STATES = {
    "uploading", "stalledUP", "forcedUP",
    "queuedUP",  "checkingUP", "pausedUP",
}

STUCK_TRACK_STATES = {
    "metaDL", "stalledDL", "downloading", "queuedDL", "forcedDL", "checkingDL",
}

_pref_raw = _cfg("groups", "preferred", [
    "erai-raws", "subsplease", "horriblesubs", "judas",
    "ember", "yameii", "psa", "akihito", "commie",
])
PREFERRED_GROUPS  = set(_pref_raw)
QUALITY_TOLERANCE = _cfg("groups", "quality_tolerance", 0.15)

# HandBrake preset - change to suit your quality preference
HB_DEFAULT_PRESET = _cfg("handbrake", "default_preset", "General/Fast 1080p30")

# .hs suffix appended before the file extension to mark hard-subbed outputs
HS_MARKER = _cfg("pipeline", "hs_marker", ".hs")
MOVIE_LIBRARY_DIR = str(_cfg("pipeline", "movie_library_dir", "") or "").strip()

VIDEO_EXTENSIONS = {".mkv", ".mp4"}

WINDOWS_HB_PATHS = [
    r"C:\Program Files\HandBrake\HandBrakeCLI.exe",
    r"C:\Program Files (x86)\HandBrake\HandBrakeCLI.exe",
]

# Common Linux install paths + command names:
# - distro packages (Debian/Ubuntu/Arch/etc.): /usr/bin/HandBrakeCLI
# - local/source installs: /usr/local/bin/HandBrakeCLI
# - Flatpak exports can expose fr.handbrake.HandBrakeCLI on PATH.
LINUX_HB_PATHS = [
    "/usr/bin/HandBrakeCLI",
    "/usr/local/bin/HandBrakeCLI",
    "/app/bin/HandBrakeCLI",
]

LINUX_HB_COMMANDS = [
    "HandBrakeCLI",
    "handbrakecli",
    "fr.handbrake.HandBrakeCLI",
]


# -----------------------------------------------------------------------------
# COLOR SYSTEM
# Two palettes merged: HandBrake (ANSI-safe, TTY-gated) + Fetcher (amber theme)
# -----------------------------------------------------------------------------

def _ansi_supported() -> bool:
    if not sys.stdout.isatty():
        return False
    if sys.platform == "win32":
        try:
            import ctypes
            k32 = ctypes.windll.kernel32          # type: ignore[attr-defined]
            k32.SetConsoleMode(k32.GetStdHandle(-11), 7)
            return True
        except Exception:
            return False
    return True


_COLOR = _ansi_supported()

# Also activate VT on Windows (no-op on POSIX; belt-and-suspenders)
if sys.platform == "win32":
    os.system("")


class C:
    # -- Reset ----------------------------------------------------------------
    RST       = "\033[0m"          if _COLOR else ""
    RESET     = RST                                    # alias

    # -- Amber family ---------------------------------------------------------
    AMBER     = "\033[39m"         if _COLOR else ""   # terminal default foreground
    AMBER_B   = "\033[97m"         if _COLOR else ""   # bright foreground / values
    ORANGE    = "\033[97m"         if _COLOR else ""   # high-contrast foreground
    ORANGE_DIM= "\033[37m"         if _COLOR else ""   # neutral gray secondary
    B_AMBER   = "\033[1;97m"       if _COLOR else ""   # section titles (bold foreground)

    # -- Accent ---------------------------------------------------------------
    WHITE     = "\033[97m"         if _COLOR else ""   # headers / prompts
    WHITE_DIM = "\033[37m"         if _COLOR else ""   # muted text
    B_WHITE   = "\033[1;97m"       if _COLOR else ""   # plan headers (bold)
    CYAN      = "\033[96m"         if _COLOR else ""   # paths / CMD / progress
    CYAN_DIM  = "\033[36m"         if _COLOR else ""   # minor info
    GREEN     = "\033[92m"         if _COLOR else ""   # success / auto-pick
    YELLOW    = "\033[93m"         if _COLOR else ""   # WARN / SKIP / FALLBACK
    RED       = "\033[91m"         if _COLOR else ""   # ERROR / FAIL
    PINK      = "\033[38;5;213m"   if _COLOR else ""   # genre highlight: Ecchi
    VAL_RED   = "\033[38;5;196m"   if _COLOR else ""   # genre highlight: Romance
    COMEDY_BLUE = "\033[38;5;39m"  if _COLOR else ""   # genre highlight: Comedy (readable blue)
    MAGENTA   = "\033[95m"         if _COLOR else ""   # DRY-RUN / * badge
    DIM       = "\033[2m"          if _COLOR else ""   # separators / secondary

    # -- Semantic aliases used by fetcher section -----------------------------
    HEADER    = AMBER_B
    LABEL     = AMBER
    VALUE     = ORANGE
    VALUE2    = AMBER_B
    MUTED     = WHITE_DIM
    SUCCESS   = GREEN
    INFO      = CYAN
    INFO_DIM  = CYAN_DIM
    WARN      = ORANGE_DIM
    BADGE     = MAGENTA
    PROMPT_CLR= AMBER_B
    BANNER    = WHITE

    @staticmethod
    def strip(text: str) -> str:
        return re.sub(r"\033\[[0-9;]*m", "", text)


def co(color: str, text: str) -> str:
    return f"{color}{text}{C.RST}" if _COLOR else text

# Short helpers - used throughout
def amber(t):   return co(C.AMBER,   t)
def white(t):   return co(C.WHITE,   t)
def cyan(t):    return co(C.CYAN,    t)
def green(t):   return co(C.GREEN,   t)
def yellow(t):  return co(C.YELLOW,  t)
def red(t):     return co(C.RED,     t)
def magenta(t): return co(C.MAGENTA, t)
def dim(t):     return co(C.DIM,     t)
def b_amber(t): return co(C.B_AMBER, t)
def b_white(t): return co(C.B_WHITE, t)

# Fetcher-style c() helper
def c(colour: str, text: str) -> str:
    return f"{colour}{text}{C.RESET}" if _COLOR else text

# -- Rainbow cycling for encode stats lines -----------------------------------
# Bright 256-colour codes that pop on dark terminals - wider than the 8-colour
# standard palette so each episode's stats block looks visibly different.
_RAINBOW_CODES: list[str] = [
    "\033[38;5;196m",   # bright red
    "\033[38;5;208m",   # orange
    "\033[38;5;226m",   # yellow
    "\033[38;5;118m",   # lime green
    "\033[38;5;51m",    # cyan
    "\033[38;5;21m",    # blue
    "\033[38;5;129m",   # violet
    "\033[38;5;201m",   # hot pink
    "\033[38;5;214m",   # amber
    "\033[38;5;155m",   # mint
    "\033[38;5;87m",    # sky
    "\033[38;5;171m",   # orchid
] if _COLOR else []
_rainbow_idx = 0

def rainbow_next(text: str) -> str:
    """Return *text* in the next rainbow colour, advancing the global cycle."""
    global _rainbow_idx
    if not _COLOR:
        return text
    code = _RAINBOW_CODES[_rainbow_idx % len(_RAINBOW_CODES)]
    _rainbow_idx += 1
    return f"{code}{text}{C.RST}"

SEP_THIN  = dim("-" * 64)
SEP_THICK = dim("=" * 64)
SEP_HASH  = dim("#" * 64)

SEP_THIN_DEFAULT  = SEP_THIN
SEP_THICK_DEFAULT = SEP_THICK
SEP_HASH_DEFAULT  = SEP_HASH

VISUAL_FUN = False
VISUAL_THEME = "clean"
PIPELINE_START_TS = time.time()
VISUAL_STATS = {
    "seasons": 0,
    "encoded_ok": 0,
    "encoded_skip": 0,
    "encoded_fail": 0,
}

FUN_ORACLE_LINES = [
    "Fansub oracle: trust seeders, but verify subtitles.",
    "Pipeline mood: caffeinated and subtitle-positive.",
    "Nyaa winds are favorable today.",
    "Codec spirits approve this batch.",
]

MILESTONE_LINES = [
    "Combo unlocked: {n} successful encodes.",
    "Batch streak: {n} files cleared.",
    "Achievement: {n} episodes processed cleanly.",
]


def apply_visual_theme(theme: str) -> None:
    """Apply optional output-only separator styling."""
    global SEP_THIN, SEP_THICK, SEP_HASH
    if theme == "minimal":
        SEP_THIN = "-" * 64
        SEP_THICK = "=" * 64
        SEP_HASH = "#" * 64
    elif theme == "retro":
        SEP_THIN = dim("." * 64)
        SEP_THICK = dim("=" * 64)
        SEP_HASH = dim("#" * 64)
    elif theme == "neon":
        SEP_THIN = SEP_THIN_DEFAULT
        SEP_THICK = SEP_THICK_DEFAULT
        SEP_HASH = SEP_HASH_DEFAULT
    else:
        SEP_THIN = SEP_THIN_DEFAULT
        SEP_THICK = SEP_THICK_DEFAULT
        SEP_HASH = SEP_HASH_DEFAULT


def status_badge(kind: str) -> str:
    k = kind.lower()
    if k == "ok":
        return green("[OK]")
    if k == "skip":
        return yellow("[SKIP]")
    if k == "fail":
        return red("[FAIL]")
    if k == "retry":
        return magenta("[RETRY]")
    return white(f"[{kind.upper()}]")


def _maybe_print_milestone(success_count: int) -> None:
    if not VISUAL_FUN:
        return
    if success_count > 0 and success_count % 5 == 0:
        line = random.choice(MILESTONE_LINES).format(n=success_count)
        print(f"  {status_badge('ok')} {magenta(line)}")


def _format_duration(seconds: float) -> str:
    total = int(max(0, seconds))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def print_visual_footer(any_failure: bool) -> None:
    elapsed = _format_duration(time.time() - PIPELINE_START_TS)
    print(f"\n{SEP_THICK}")
    print(b_white("RUN FOOTER"))
    print(SEP_THICK)
    print(f"  Runtime        : {cyan(elapsed)}")
    print(f"  Seasons        : {amber(str(VISUAL_STATS['seasons']))}")
    print(f"  Encoded OK     : {green(str(VISUAL_STATS['encoded_ok']))}")
    print(f"  Encoded Skipped: {yellow(str(VISUAL_STATS['encoded_skip']))}")
    print(f"  Encoded Failed : {red(str(VISUAL_STATS['encoded_fail']))}")
    print(f"  Final status   : {status_badge('fail' if any_failure else 'ok')}")
    if VISUAL_FUN:
        print(f"  {dim(random.choice(FUN_ORACLE_LINES))}")


def print_completion_banner(any_failure: bool) -> None:
    if any_failure:
        colour = C.RED
        title = "DONE WITH WARNINGS"
        subtitle = "One or more seasons need attention."
    else:
        colour = C.GREEN
        title = "DONE"
        subtitle = "Pipeline finished successfully."

    width = 42
    inner = width - 4
    print()
    print(c(colour, "+" + "-" * (width - 2) + "+"))
    print(c(colour, f"| {title:^{inner}} |"))
    print(c(colour, f"| {subtitle:^{inner}} |"))
    print(c(colour, "+" + "-" * (width - 2) + "+"))


def finish_run(any_failure: bool = False, exit_code: int | None = None) -> None:
    print()
    print(c(C.AMBER, "=" * 64))
    print_completion_banner(any_failure)
    print(c(C.AMBER, "=" * 64))
    print()
    print_visual_footer(any_failure)
    if exit_code is not None:
        sys.exit(exit_code)


# -----------------------------------------------------------------------------
# STRUCTURED LOGGING - logging.config.dictConfig
#
# All internal log records go to lito1.log (JSON lines) AND to
# stderr at WARNING+.  Print/colour output is kept for the interactive UI.
# disable_existing_loggers is explicitly False so qbittorrentapi / requests
# library logs propagate normally to the root logger.
# -----------------------------------------------------------------------------

_LOG_FILE = Path(__file__).with_name("lito1.log")

logging.config.dictConfig({
    "version":                 1,
    "disable_existing_loggers": False,   # preserve third-party library loggers
    "formatters": {
        "json": {
            "()":      "logging.Formatter",
            "fmt":     '{"ts":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":%(message)s}',
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        },
        "console": {
            "format": "[%(levelname)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "file": {
            "class":     "logging.handlers.RotatingFileHandler",
            "filename":  str(_LOG_FILE),
            "maxBytes":  10 * 1024 * 1024,   # 10 MB
            "backupCount": 3,
            "formatter": "json",
            "encoding":  "utf-8",
        },
        "console": {
            "class":     "logging.StreamHandler",
            "stream":    "ext://sys.stderr",
            "level":     "WARNING",
            "formatter": "console",
        },
    },
    "root": {
        "level":    "DEBUG",
        "handlers": ["file", "console"],
    },
})

log = logging.getLogger("lito1")


def divider(title: str = "") -> None:
    """Amber section divider (fetcher style)."""
    width = 64
    if title:
        pad   = (width - len(title) - 2) // 2
        left  = "-" * pad
        right = "-" * (width - pad - len(title) - 2)
        print(c(C.AMBER, f"{left} {title} {right}"))
    else:
        print(c(C.AMBER, "-" * width))


# Keep connection-refused noise from urllib3 out of the user-facing terminal
# while we probe qBittorrent; we print a friendlier explanation ourselves.
def _set_qbit_probe_logger_level(level: int) -> dict[str, int]:
    targets = [
        "urllib3.connectionpool",
        "requests.packages.urllib3.connectionpool",
        "qbittorrentapi",
    ]
    previous: dict[str, int] = {}
    for name in targets:
        logger = logging.getLogger(name)
        previous[name] = logger.level
        logger.setLevel(level)
    return previous


def _restore_logger_levels(levels: dict[str, int]) -> None:
    for name, level in levels.items():
        logging.getLogger(name).setLevel(level)


def prompt(label: str, default: str = "") -> str:
    suffix = f" [{c(C.ORANGE_DIM, default)}]" if default else ""
    return (input(f"{c(C.PROMPT_CLR, label)}{suffix}: ").strip()) or default


# -----------------------------------------------------------------------------
# PHASE 1 - JELLYFIN ANIME DIRECTORY DISCOVERY
# -----------------------------------------------------------------------------

def find_jellyfin_anime_dir() -> Path:
    """
    Search all non-C: drives for a directory named 'Anime' (case-insensitive)
    whose immediate parent is named 'media' (case-insensitive).

    Prints what it finds, errors and exits if nothing is found.
    """
    print(f"\n{SEP_THICK}")
    print(b_white("PHASE 1 - Locating Jellyfin Anime Directory"))
    print(SEP_THICK)

    if sys.platform == "win32":
        import string
        import ctypes
        drives = []
        bitmask = ctypes.windll.kernel32.GetLogicalDrives()  # type: ignore[attr-defined]
        for letter in string.ascii_uppercase:
            if letter == "C":
                continue
            if bitmask & 1:
                drives.append(Path(f"{letter}:\\"))
            bitmask >>= 1
        if not drives:
            sys.exit(red("[ERROR] No non-C: drives detected. Cannot locate Jellyfin library."))
    else:
        # Linux/macOS: walk common mount points, skip the root/system drives
        drives = []
        for mp in [Path("/mnt"), Path("/media"), Path("/run/media")]:
            if mp.exists():
                drives.extend(p for p in mp.iterdir() if p.is_dir())
        if not drives:
            drives = [Path("/")]   # last resort

    candidates: list[Path] = []
    searched: list[str]    = []

    for drive in drives:
        print(f"  {dim('Scanning:')} {cyan(str(drive))}")
        searched.append(str(drive))
        try:
            for dirpath, dirnames, _ in os.walk(drive, followlinks=False):
                dp = Path(dirpath)
                # Skip hidden / system directories to keep scanning fast
                dirnames[:] = [
                    d for d in dirnames
                    if not d.startswith("$")
                    and not d.startswith(".")
                    and d.lower() not in ("windows", "system32", "program files",
                                          "program files (x86)", "programdata",
                                          "recovery", "boot", "efi")
                ]
                if (dp.name.lower() == "anime"
                        and dp.parent.name.lower() == "media"):
                    candidates.append(dp)
        except PermissionError:
            pass

    if not candidates:
        print(f"\n{red('[ERROR]')} Could not find an 'Anime' directory "
              f"under a 'media' parent on any non-C: drive.")
        print(f"  {dim('Drives searched:')} {', '.join(searched)}")
        sys.exit(1)

    if len(candidates) > 1:
        print(f"\n{yellow('[WARN]')} Multiple 'media/Anime' directories found:")
        for p in candidates:
            print(f"  {cyan(str(p))}")
        print(f"  {dim('Using the first match.')}")

    found = candidates[0]
    print(f"\n  {green('[FOUND]')} {white(str(found))}")
    print(f"  {dim('Parent confirmed:')} {cyan(found.parent.name)}  "
          f"{dim('→')}  {cyan(found.name)}")
    return found


# -----------------------------------------------------------------------------
# PHASE 2 - NYAA SEARCH + QBITTORRENT
# -----------------------------------------------------------------------------

# -- Nyaa scraping ------------------------------------------------------------

def search_nyaa(query: str, page: int = 1) -> list[dict]:
    params = {
        "f": "0", "c": "1_2", "q": query,
        "p": page, "s": "seeders", "o": "desc",
    }
    headers = {"User-Agent": "Mozilla/5.0 (compatible; anime-pipeline/1.0)"}

    # Retry up to 3 times with exponential backoff for transient server errors
    for attempt in range(1, 4):
        try:
            resp = requests.get(NYAA_BASE, params=params, headers=headers, timeout=15)
            if resp.status_code in (502, 503, 504):
                if attempt < 3:
                    wait = 2 ** attempt
                    print(f"  {yellow(f'[HTTP {resp.status_code}]')} "
                          f"{c(C.DIM, f'Nyaa page {page} - retrying in {wait}s ({attempt}/3)')}")
                    time.sleep(wait)
                    continue
                print(f"  {yellow('[SKIP]')} "
                      f"{c(C.DIM, f'Nyaa page {page} unavailable (HTTP {resp.status_code}) - skipping.')}")
                return []
            resp.raise_for_status()
            break
        except requests.exceptions.ConnectionError:
            if attempt < 3:
                time.sleep(2 ** attempt)
                continue
            print(f"  {yellow('[SKIP]')} {c(C.DIM, f'Nyaa page {page} unreachable - skipping.')}")
            return []
        except requests.exceptions.Timeout:
            if attempt < 3:
                time.sleep(2 ** attempt)
                continue
            print(f"  {yellow('[SKIP]')} {c(C.DIM, f'Nyaa page {page} timed out - skipping.')}")
            return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.select("table.torrent-list tbody tr")
    results = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue
        title_col  = cols[1]
        links      = title_col.find_all("a", href=True)
        title_link = next((l for l in links if not l.get("class")), None)
        if not title_link:
            continue
        title       = title_link.get("title") or title_link.text.strip()
        detail_href = title_link["href"]
        action_col  = cols[2]
        torrent_lnk = action_col.find("a", href=re.compile(r"\.torrent"))
        magnet_lnk  = action_col.find("a", href=re.compile(r"^magnet:"))
        torrent_url = (NYAA_BASE + torrent_lnk["href"]) if torrent_lnk else None
        magnet_url  = magnet_lnk["href"] if magnet_lnk else None
        seeders   = cols[5].text.strip()
        leechers  = cols[6].text.strip() if len(cols) > 6 else "0"
        completed = cols[7].text.strip() if len(cols) > 7 else "0"
        results.append({
            "title":       title,
            "detail_url":  NYAA_BASE + detail_href,
            "torrent_url": torrent_url,
            "magnet_url":  magnet_url,
            "size":        cols[3].text.strip(),
            "seeders":     int(seeders)   if seeders.isdigit()   else 0,
            "leechers":    int(leechers)  if leechers.isdigit()  else 0,
            # "completed" = total number of times this torrent has been downloaded
            # to 100%.  This is the most durable signal for obscure/older series
            # that have few active seeders but are historically well-distributed.
            "completed":   int(completed) if completed.isdigit() else 0,
        })
    return results


def search_all_pages(query: str, max_pages: int = DEFAULT_PAGES) -> list[dict]:
    cache_key = (query.strip().lower(), int(max_pages))
    if cache_key in _SEARCH_CACHE:
        cached = _SEARCH_CACHE[cache_key]
        print(f"  {c(C.INFO_DIM, 'Cached search:')} {c(C.VALUE2, str(len(cached)))} results")
        return [dict(r) for r in cached]

    all_results = []
    for page in range(1, max_pages + 1):
        page_results = search_nyaa(query, page)
        if not page_results:
            break
        all_results.extend(page_results)
        # Flag each result as a batch candidate during the scrape (pure
        # title/size heuristics, no extra HTTP requests).
        for r in page_results:
            r["batch_candidate"] = _is_batch_title(r["title"], r.get("size", ""))
        print(f"  {c(C.INFO_DIM, f'Page {page}:')} "
              f"{c(C.VALUE2, str(len(page_results)))} results")
    _SEARCH_CACHE[cache_key] = [dict(r) for r in all_results]
    return all_results


# =============================================================================
# BATCH TORRENT DETECTION, INSPECTION, AND FILE ROUTING
# =============================================================================
#
# Two-stage approach:
#
#   Stage 1 (title heuristics, zero HTTP):
#     _is_batch_title() runs during search_all_pages() on every result.
#     It uses regex patterns and file-size thresholds to flag candidates.
#     "Candidate" means "worth inspecting" not "confirmed batch".
#
#   Stage 2 (detail page scrape, one HTTP request per candidate):
#     inspect_batch_candidate() fetches the nyaa detail page to get the real
#     file list.  Only called when a candidate is actually about to be used.
#     Confirmed batch = >= 2 video files in the torrent.
#
#   Post-download routing:
#     post_process_batch_download() moves TV episodes to Season NN and
#     OVA/Special/Encore files to Season 00, matching Jellyfin spec exactly.
#
#   Single-episode protection:
#     A torrent with 1 video file is never classified as a batch, so movies
#     and standalone OVAs are never routed through the batch path.
#
# =============================================================================

_BATCH_TITLE_PATTERNS = [
    # Explicit keywords
    re.compile(r"\b(batch|complete|complete[_ ]series|complete[_ ]pack)\b", re.IGNORECASE),
    re.compile(r"\b(bd[_ ]?box|dvd[_ ]?box|bd[_ ]complete|dvd[_ ]complete)\b", re.IGNORECASE),
    re.compile(r"\b(all[_ ]episodes?|full[_ ]series)\b", re.IGNORECASE),
    # Episode range patterns: "01-12", "E01-E24", "01~13", "Ep.1-13"
    re.compile(r"\b(?:E|Ep\.?|Vol\.?)?\d{1,3}[-~]\d{1,3}\b"),
    # Multi-content separators, e.g. "TV+OVA", "TV+OVA+Encore"
    re.compile(r"\b(TV|BD|DVD)\s*\+\s*(OVA|ONA|OAD|Special|SP|Encore|Extra)\b", re.IGNORECASE),
    re.compile(r"\+(OVA|ONA|OAD|Encore|Extra|Specials?)\b", re.IGNORECASE),
    # BD/DVDRip source tag without episode number implies full-series release
    re.compile(r"\b(BDRip|BDremux|BD[_ ]Remux|DVDRip|DVD[_ ]Rip)\b", re.IGNORECASE),
]

_BATCH_SIZE_GIB_THRESHOLD = 1.5


def _parse_size_to_gib(size_str: str) -> float:
    """Parse nyaa size strings like '3.6 GiB', '850 MiB' to float GiB."""
    m = re.match(r"([\d.]+)\s*(GiB|MiB|KiB|GB|MB|KB)", size_str, re.IGNORECASE)
    if not m:
        return 0.0
    val  = float(m.group(1))
    unit = m.group(2).upper()
    if unit in ("MIB", "MB"):
        return val / 1024
    if unit in ("KIB", "KB"):
        return val / (1024 * 1024)
    return val


def _is_batch_title(title: str, size_str: str = "") -> bool:
    """
    Fast zero-HTTP heuristic: returns True if the torrent looks like a
    multi-episode pack.  A True result means "inspect the detail page" --
    not a guarantee.  Actual confirmation requires >= 2 video files.
    """
    for pat in _BATCH_TITLE_PATTERNS:
        if pat.search(title):
            return True
    if size_str and _parse_size_to_gib(size_str) >= _BATCH_SIZE_GIB_THRESHOLD:
        return True
    return False


_DETAIL_CACHE: dict[str, list[str]] = {}
_SEARCH_CACHE: dict[tuple[str, int], list[dict]] = {}
_ANILIST_SEASON_INFO_CACHE: dict[str, dict[int, dict]] = {}
_ANILIST_RELATED_MOVIES_CACHE: dict[str, list[dict]] = {}


def _collect_torrent_leaf_entries(node, parents: list[str] | None = None) -> list[str]:
    """
    Walk Nyaa's nested torrent file tree and preserve relative folder paths.

    Example output:
      "[Judas] Gintama S05 - Gintama'/[Judas] Gintama - 202.mkv(1.2 GiB)"
    """
    parents = parents or []
    if getattr(node, "name", None) != "ul":
        top_ul = node.find("ul")
        if top_ul is None:
            return []
        node = top_ul
    entries: list[str] = []
    for li in node.find_all("li", recursive=False):
        child_ul = li.find("ul", recursive=False)
        text = li.get_text(" ", strip=True)
        if child_ul:
            label_parts = []
            for child in li.contents:
                if child == child_ul:
                    break
                part = getattr(child, "get_text", lambda *a, **k: str(child))(" ", strip=True)
                part = str(part).strip()
                if part:
                    label_parts.append(part)
            folder_name = " ".join(label_parts).strip().rstrip("/").strip()
            next_parents = parents + ([folder_name] if folder_name else [])
            entries.extend(_collect_torrent_leaf_entries(child_ul, next_parents))
            continue

        if not text or text.endswith("/"):
            continue
        if "." not in text:
            continue
        rel_path = "/".join([*parents, text]) if parents else text
        entries.append(rel_path)
    return entries


def fetch_batch_file_list(detail_url: str) -> list[str]:
    """
    Scrape the nyaa detail page and return every filename in the torrent.

    Returns a list of raw strings in the form "filename.mkv (389.6 MiB)" so
    that callers can extract both the filename AND the expected size.
    Use _split_filename_and_size() to parse each entry.

    Results are cached so repeat calls for the same URL are free.
    Returns [] on any failure; callers handle gracefully.
    """
    if detail_url in _DETAIL_CACHE:
        return _DETAIL_CACHE[detail_url]
    try:
        resp = requests.get(
            detail_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; anime-pipeline/1.0)"},
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning("fetch_batch_file_list: HTTP %d for %s", resp.status_code, detail_url)
            return []
        soup      = BeautifulSoup(resp.text, "html.parser")
        container = soup.select_one(".torrent-file-list")
        if not container:
            return []
        entries = _collect_torrent_leaf_entries(container)
        _DETAIL_CACHE[detail_url] = entries
        log.debug("fetch_batch_file_list: %d entries at %s", len(entries), detail_url)
        return entries
    except Exception as exc:
        log.warning("fetch_batch_file_list failed for %s: %s", detail_url, exc)
        return []


_SIZE_ANNOTATION = re.compile(r"\((\d[\d.]*)\s*(GiB|MiB|KiB|GB|MB|KB)\)\s*$", re.IGNORECASE)


def _split_filename_and_size(entry: str) -> tuple[str, int]:
    """
    Parse a nyaa file-list entry like:
      "[GS] Ichigo Mashimaro - 01 (BD 1080p 8bit FLAC) [7D35A0A5].mkv(389.6 MiB)"

    Returns (filename, expected_bytes).
    expected_bytes is 0 if no size annotation is present.
    """
    m = _SIZE_ANNOTATION.search(entry)
    if not m:
        return entry.strip(), 0
    size_str = entry[m.start():].strip()
    filename = entry[:m.start()].strip()
    val  = float(m.group(1))
    unit = m.group(2).upper()
    if unit in ("GIB", "GB"):
        expected = int(val * 1024 * 1024 * 1024)
    elif unit in ("MIB", "MB"):
        expected = int(val * 1024 * 1024)
    elif unit in ("KIB", "KB"):
        expected = int(val * 1024)
    else:
        expected = 0
    return filename, expected


_OVA_PAT     = re.compile(r"\bOVA\b",                   re.IGNORECASE)
_ONA_PAT     = re.compile(r"\bONA\b",                   re.IGNORECASE)
_OAD_PAT     = re.compile(r"\bOAD\b",                   re.IGNORECASE)
_ENCORE_PAT  = re.compile(r"\bEncore\b",                re.IGNORECASE)
_EXTRA_PAT   = re.compile(r"\b(Extra|Bonus)\b",         re.IGNORECASE)
_SPECIAL_PAT = re.compile(r"\b(SP|Special|Specials)\b", re.IGNORECASE)
_NCOP_PAT    = re.compile(r"\b(NCOP|NCED|OP|ED)\b",     re.IGNORECASE)
_VIDEO_EXT   = {".mkv", ".mp4", ".avi", ".m4v", ".ts", ".m2ts"}
_SUB_EXT     = {".srt", ".ass", ".ssa", ".vtt", ".sub"}
_SUB_LANG_HINTS: list[tuple[str, str]] = [
    ("english", "eng"), ("eng", "eng"),
    ("japanese", "jpn"), ("jpn", "jpn"), ("jp", "jpn"),
    ("spanish", "spa"), ("spa", "spa"), ("es", "spa"),
]


def _guess_sub_lang_suffix(name: str) -> str:
    lower = name.lower()
    for token, code in _SUB_LANG_HINTS:
        if re.search(rf"(?<!\w){re.escape(token)}(?!\w)", lower):
            return code
    return "und"


def _find_matching_subtitles(folder: Path, video_stem: str) -> list[Path]:
    if not folder.is_dir():
        return []
    out: list[Path] = []
    for p in folder.iterdir():
        if not p.is_file() or p.suffix.lower() not in _SUB_EXT:
            continue
        if p.stem == video_stem or p.stem.startswith(video_stem + "."):
            out.append(p)
    return sorted(out)


def _route_subtitle_sidecars(
    src_video: Path,
    target_video: Path,
    target_dir: Path,
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """
    Move/rename subtitle sidecars to match a routed video.

    Returns:
      (moved_count, skipped_count, detected_count)
    """
    sidecars = _find_matching_subtitles(src_video.parent, src_video.stem)
    if not sidecars:
        return 0, 0, 0

    moved = 0
    skipped = 0
    detected = len(sidecars)

    for sub_src in sidecars:
        lang = _guess_sub_lang_suffix(sub_src.stem)
        suffix = sub_src.suffix.lower()
        base_name = f"{target_video.stem}.{lang}"
        lower_stem = sub_src.stem.lower()
        if "forced" in lower_stem:
            base_name += ".forced"
        elif "sdh" in lower_stem or lower_stem.endswith(".cc"):
            base_name += ".sdh"

        sub_target = target_dir / f"{base_name}{suffix}"
        dedupe_n = 2
        while sub_target.exists() and sub_target != sub_src:
            sub_target = target_dir / f"{base_name}.{dedupe_n}{suffix}"
            dedupe_n += 1

        if dry_run:
            moved += 1
            continue

        try:
            shutil.move(str(sub_src), str(sub_target))
            moved += 1
            log.info("Batch subtitle routed: %s -> %s", sub_src.name, sub_target.name)
        except OSError as exc:
            skipped += 1
            log.warning("Subtitle move failed %s -> %s: %s", sub_src, sub_target, exc)

    return moved, skipped, detected


def _is_movie_like_format(fmt: str) -> bool:
    return str(fmt or "").upper() in {"MOVIE", "SPECIAL", "OVA", "ONA", "OAD"}


def _fallback_batch_episode_number(stem: str) -> int | None:
    """
    Conservative episode fallback for batch file names.

    We only accept obvious episode-like endings and avoid treating resolution
    markers such as 1080p or 1920x1080 as episode numbers.
    """
    if re.fullmatch(r"\d{1,3}", stem):
        return int(stem)
    m = re.search(r"(?:^|[\s._-])(?:ep|e)?(\d{1,3})(?:v\d+)?$", stem, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _match_related_movie(filename: str, related_movies: list[dict]) -> dict | None:
    name_norm = _normalize_title_for_match(Path(filename).stem)
    best: dict | None = None
    best_len = 0
    for movie in related_movies:
        for alias in movie.get("titles") or []:
            alias_norm = _normalize_title_for_match(alias)
            if not alias_norm or len(alias_norm) < 4:
                continue
            if alias_norm in name_norm and len(alias_norm) > best_len:
                best = movie
                best_len = len(alias_norm)
    return best


def _classify_batch_file(
    filename: str,
    anime_name: str = "",
    default_format: str = "",
    default_year: int | None = None,
    related_movies: list[dict] | None = None,
) -> dict:
    """
    Classify one entry from a batch torrent detail page.

    *entry* is a raw string like:
      "[GS] Ichigo Mashimaro - 01 (BD 1080p 8bit FLAC) [7D35].mkv(389.6 MiB)"

    Returns { "filename", "expected_bytes", "is_video", "category", "ep_num" }

    category:
      "tv"       -> Season 01 (the current TV season)
      all others -> Season 00 (Specials)
    """
    filename, expected_bytes = _split_filename_and_size(filename)
    rel_path = Path(filename)
    base_name = rel_path.name
    stem     = Path(base_name).stem
    ext      = Path(base_name).suffix.lower()
    is_video = ext in _VIDEO_EXT

    matched_movie = _match_related_movie(filename, related_movies or [])
    is_explicit_movie = bool(re.search(r"\b(movie|film|gekijouban|the movie)\b", stem, re.IGNORECASE))

    if _OVA_PAT.search(stem):
        category = "ova"
    elif _ONA_PAT.search(stem):
        category = "ona"
    elif _OAD_PAT.search(stem):
        category = "oad"
    elif _ENCORE_PAT.search(stem):
        category = "encore"
    elif _SPECIAL_PAT.search(stem):
        category = "special"
    elif _NCOP_PAT.search(stem):
        category = "ncop"
    elif _EXTRA_PAT.search(stem):
        category = "extra"
    else:
        category = "tv" if is_video else "other"

    # Use the anitopy-backed parser first - it correctly ignores years (2024),
    # resolutions (1080), and hash codes when finding the episode number.
    # Fall back to the first bare number only if anitopy returns nothing.
    ep_num = extract_episode_number(base_name)
    if ep_num is None:
        ep_num = extract_episode_number(filename)
    if ep_num is None:
        ep_num = _fallback_batch_episode_number(stem)

    # Also extract the season number from the filename so multi-season batches
    # (e.g. "Season 1+2" packs) can be routed to the correct season directory.
    # We intentionally inspect the preserved relative path, not just the leaf
    # filename, so folder names like "[Judas] Gintama S05 - Gintama'" count.
    season_num = extract_explicit_season_number(filename)
    if season_num is None:
        season_num = extract_explicit_season_number(base_name)

    movie_title = None
    movie_year = None
    if is_video and category == "tv":
        if matched_movie:
            category = "movie"
            movie_title = matched_movie.get("title")
            movie_year = matched_movie.get("year")
        elif ep_num is None and season_num is None and (_is_movie_like_format(default_format) or is_explicit_movie):
            category = "movie"
            movie_title = sanitise_name(anime_name or stem)
            movie_year = default_year

    return {"filename": base_name, "relative_path": filename, "expected_bytes": expected_bytes,
            "is_video": is_video, "category": category,
            "ep_num": ep_num, "season_num": season_num,
            "movie_title": movie_title, "movie_year": movie_year}


def classify_batch_files(
    filenames: list[str],
    anime_name: str = "",
    default_format: str = "",
    default_year: int | None = None,
    related_movies: list[dict] | None = None,
) -> dict:
    """
    Classify all files in a batch torrent and return a summary dict.

    {
      "files", "tv_episodes", "specials",
      "video_count", "tv_count", "special_count",
      "is_confirmed_batch",
      "seasons_present"   # sorted list of season numbers found in TV files
                          # e.g. [1, 2] for a Season 1+2 pack, [1] for single
    }
    """
    classified = [
        _classify_batch_file(
            f,
            anime_name=anime_name,
            default_format=default_format,
            default_year=default_year,
            related_movies=related_movies,
        )
        for f in filenames
    ]
    video    = [f for f in classified if f["is_video"]]
    tv       = sorted([f for f in video if f["category"] == "tv"],
                      key=lambda f: f["ep_num"] or 0)
    movies   = [f for f in video if f["category"] == "movie"]
    specials = sorted([f for f in video if f["category"] not in {"tv", "movie"}],
                      key=lambda f: f["ep_num"] or 0)

    # Determine which distinct seasons are present in the TV files
    season_nums = sorted(set(
        f["season_num"] for f in tv
        if f.get("season_num") is not None
    ))

    return {
        "files":              classified,
        "tv_episodes":        tv,
        "movies":             movies,
        "specials":           specials,
        "video_count":        len(video),
        "tv_count":           len(tv),
        "movie_count":        len(movies),
        "special_count":      len(specials),
        "is_confirmed_batch": len(video) >= 2,
        "is_single_video":    len(video) == 1,
        "seasons_present":    season_nums,
    }


def inspect_batch_candidate(
    result: dict,
    anime_name: str = "",
    default_format: str = "",
    default_year: int | None = None,
    related_movies: list[dict] | None = None,
) -> bool:
    """
    Fetch the detail page for *result*, classify its files, and attach
    result["batch_info"] in place.

    Returns True if confirmed as a multi-file batch (>= 2 video files).
    Returns False for single-file torrents or if the page fetch failed.
    Only call when the torrent is actually about to be used.
    """
    if "batch_info" in result:
        info = result.get("batch_info")
        return bool(info and info.get("is_confirmed_batch"))
    filenames = fetch_batch_file_list(result["detail_url"])
    if not filenames:
        result["batch_info"] = None
        return False
    info = classify_batch_files(
        filenames,
        anime_name=anime_name,
        default_format=default_format,
        default_year=default_year,
        related_movies=related_movies,
    )
    result["batch_info"] = info
    return info["is_confirmed_batch"]


def print_batch_info(result: dict, tv_season: int = 1) -> None:
    """Pretty-print the classified file breakdown for a confirmed batch."""
    info = result.get("batch_info")
    if not info:
        return
    tv_eps          = info["tv_episodes"]
    movies          = info.get("movies", [])
    specials        = info["specials"]
    seasons_present = info.get("seasons_present", [])

    print(f"  {c(C.AMBER, 'Batch contents:')}")
    print(f"    {c(C.VALUE2, str(info['tv_count']))} TV episode(s)  "
          f"{c(C.VALUE2, str(info.get('movie_count', 0)))} movie file(s)  "
          f"{c(C.VALUE2, str(info['special_count']))} special/OVA file(s)  "
          f"{c(C.DIM, f'({info["video_count"]} video files total)')}")

    if tv_eps:
        if len(seasons_present) > 1:
            # Multi-season batch - show per-season breakdown
            for sn in seasons_present:
                sn_eps = sorted(
                    f["ep_num"] for f in tv_eps
                    if f.get("season_num") == sn and f["ep_num"] is not None
                )
                sn_eps = sorted(set(sn_eps))
                if sn_eps and sn_eps == list(range(sn_eps[0], sn_eps[-1] + 1)):
                    ep_str = f"E{sn_eps[0]}-E{sn_eps[-1]}"
                elif sn_eps:
                    ep_str = ", ".join(f"E{n}" for n in sn_eps[:8])
                    if len(sn_eps) > 8:
                        ep_str += f" ... ({len(sn_eps)} total)"
                else:
                    ep_str = "?"
                print(f"    {c(C.DIM, f'TV  S{sn:02d}  -> Season {sn:02d}:')} {c(C.VALUE, ep_str)}")
            # Files with no season tag
            untagged = sorted(
                f["ep_num"] for f in tv_eps
                if f.get("season_num") is None and f["ep_num"] is not None
            )
            if untagged:
                ep_str = ", ".join(f"E{n}" for n in untagged)
                print(f"    {c(C.DIM, f'TV  (no S-tag) -> Season {tv_season:02d}:')} {c(C.WARN, ep_str)}")
        else:
            nums = sorted(f["ep_num"] for f in tv_eps if f["ep_num"] is not None)
            if nums and nums == list(range(nums[0], nums[-1] + 1)):
                ep_str = f"E{nums[0]}-E{nums[-1]}"
            elif nums:
                ep_str = ", ".join(f"E{n}" for n in nums[:8])
                if len(nums) > 8:
                    ep_str += f" ... ({len(nums)} total)"
            else:
                ep_str = "?"
            print(f"    {c(C.DIM, f'TV       -> Season {tv_season:02d}:')} {c(C.VALUE, ep_str)}")

    if movies:
        movie_labels = []
        for f in movies[:6]:
            mt = sanitise_name(f.get("movie_title") or Path(f["filename"]).stem)
            my = f.get("movie_year")
            movie_labels.append(f"{mt} ({my})" if my else mt)
        movie_str = ", ".join(movie_labels)
        if len(movies) > 6:
            movie_str += f" ... ({len(movies)} total)"
        print(f"    {c(C.DIM, 'MOVIE    -> movie library:')} {c(C.VALUE, movie_str)}")

    if specials:
        by_cat: dict[str, list] = defaultdict(list)
        for f in specials:
            by_cat[f["category"]].append(f["ep_num"])
        for cat, nums in sorted(by_cat.items()):
            nums_str = ", ".join(str(n) for n in sorted(set(nums)) if n is not None) or "?"
            print(f"    {c(C.DIM, f'{cat.upper():8} -> Season 00:')} {c(C.CYAN_DIM, nums_str)}")



def find_best_batch_for_season(
    results:      list[dict],
    anime_name:   str,
    season:       int | None,
    prefer_group: str = "",
    allowed_seasons: set[int] | None = None,
    season_info:  dict[int, dict] | None = None,
    priority_groups: set[str] | None = None,
    related_movies: list[dict] | None = None,
) -> dict | None:
    """
    Search *results* for the best batch torrent for *season*.

    A result qualifies if it was flagged by _is_batch_title AND the detail
    page confirms >= 2 video files.  Detail page inspection is only done for
    candidates (title-heuristic pass), and within those, highest-priority
    candidates (preferred group, composite score) are checked first so we
    fail fast.

    Returns None if no qualifying batch found.
    If allowed_seasons is provided, reject confirmed batches that contain TV
    seasons outside that set.
    """
    candidates = [
        r for r in results
        if r.get("batch_candidate")
        and (season is None
             or get_result_season_number(r) == season
             or extract_explicit_season_number(r["title"]) is None)
    ]
    if not candidates:
        return None

    _fmt = str(((season_info or {}).get(season) or {}).get("format") or "").upper()
    _allow_single_video = _fmt in {"MOVIE", "SPECIAL", "OVA", "ONA", "OAD"}

    def _priority(r):
        grp     = extract_sub_group(r["title"]) or ""
        is_priority = grp in (priority_groups or set())
        is_pref = (grp == prefer_group or grp in PREFERRED_GROUPS)
        return (
            0 if is_priority else 1,
            0 if is_pref else 1,
            -_title_resolution_rank(r["title"]),
            -_torrent_score(r),
        )

    candidates.sort(key=_priority)

    for r in candidates:
        print(f"  {c(C.DIM, 'Inspecting batch candidate:')} {c(C.MUTED, r['title'][:60])}")
        if not _batch_title_matches_series_family(r["title"], anime_name, season_info):
            print(f"  {c(C.WARN, '[SKIP]')} {c(C.DIM, 'batch title looks like a spinoff / unrelated side entry')}")
            continue
        if inspect_batch_candidate(
            r,
            anime_name=anime_name,
            default_format=_fmt,
            default_year=((season_info or {}).get(season) or {}).get("year"),
            related_movies=related_movies,
        ):
            if allowed_seasons is not None:
                _binfo = r.get("batch_info") or {}
                _present = set(_binfo.get("seasons_present") or [])
                _extra = sorted(sn for sn in _present if sn not in allowed_seasons)
                if _extra:
                    _extra_s = ", ".join(f"S{sn:02d}" for sn in _extra)
                    _allowed_s = ", ".join(f"S{sn:02d}" for sn in sorted(allowed_seasons))
                    print(f"  {c(C.WARN, '[SKIP]')} {c(C.DIM, f'batch includes unrequested seasons: {_extra_s}')} "
                          f"{c(C.DIM, f'(allowed: {_allowed_s})')}")
                    continue
            return r
        if _allow_single_video:
            _binfo = r.get("batch_info") or {}
            if _binfo.get("is_single_video"):
                return r
        print(f"  {c(C.DIM, '  -> single-file or no file list, skipping')}")

    return None


def post_process_batch_download(
    download_dir: Path,
    batch_info:   dict,
    series_dir:   Path,
    movie_root:   Path,
    tv_season:    int = 1,
    dry_run:      bool = False,
) -> dict[str, list[Path]]:
    """
    Route downloaded batch files to the correct Jellyfin season directories.

    TV episodes    -> <series_dir>/Season NN/   per-file season tag takes
                                                precedence over tv_season;
                                                tv_season is the fallback for
                                                files with no season tag
    OVA/specials   -> <series_dir>/Season 00/
    Movies         -> <movie_root>/Title (Year)/Title (Year).mkv

    Handles multi-season packs (e.g. Season 1+2) by routing each file
    individually based on its own season number extracted by anitopy.
    Returns {"tv": [...], "movies": [...], "specials": [...], "skipped": [...]}.
    Never raises; errors collected in "skipped".
    """
    season_sp_dir = series_dir / "Season 00"
    moved: dict[str, list[Path]] = {"tv": [], "movies": [], "specials": [], "skipped": []}
    routed_tv_by_season: dict[int, list[int | None]] = defaultdict(list)
    sub_moved = 0
    sub_skipped = 0
    sub_detected = 0

    print()
    divider("Batch Post-Processing -- Routing Files")

    for file_info in batch_info["files"]:
        if not file_info["is_video"]:
            continue

        # Find the file on disk (may be inside a subdirectory of download_dir)
        rel_path = file_info.get("relative_path") or file_info["filename"]
        rel_path_norm = Path(str(rel_path).replace("/", os.sep))
        candidates = []
        rel_candidate = download_dir / rel_path_norm
        if rel_candidate.exists():
            candidates = [rel_candidate]
        if not candidates:
            candidates = list(download_dir.rglob(file_info["filename"]))
        if not candidates:
            stem = Path(file_info["filename"]).stem
            candidates = [
                p for p in download_dir.rglob("*")
                if p.suffix.lower() in _VIDEO_EXT and p.stem == stem
            ]
        if not candidates:
            print(f"  {yellow('[SKIP]')} {c(C.DIM, file_info['filename'])} "
                  f"-- not found on disk")
            moved["skipped"].append(Path(file_info["filename"]))
            continue

        src = candidates[0]
        cat = file_info["category"]

        if cat == "tv":
            # Use per-file season tag if available, otherwise fall back to tv_season
            file_season = file_info.get("season_num") or tv_season
            dest_dir    = series_dir / f"Season {file_season:02d}"
            bucket      = "tv"
            dest        = dest_dir / src.name
        elif cat == "movie":
            file_season = None
            bucket = "movies"
            movie_title = file_info.get("movie_title") or _series_title_from_dir(series_dir)
            movie_year = file_info.get("movie_year")
            dest = build_movie_target_path(movie_root, movie_title, src.suffix, movie_year, create_dirs=not dry_run)
            dest_dir = dest.parent
        else:
            dest_dir = season_sp_dir
            bucket   = "specials"
            file_season = 0
            dest = dest_dir / src.name

        if dest.exists():
            print(f"  {yellow('[EXISTS]')} {c(C.DIM, src.name)}")
            _m, _s, _d = _route_subtitle_sidecars(src, dest, dest_dir, dry_run=dry_run)
            sub_moved += _m
            sub_skipped += _s
            sub_detected += _d
            moved[bucket].append(dest if not dry_run else src)
            continue

        if not dry_run:
            dest_dir.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(src), str(dest))
                log.info("Batch routed: %s -> %s", src.name, dest_dir.name)
            except OSError as exc:
                print(f"  {red('[ERROR]')} Cannot move {src.name}: {exc}")
                log.exception("post_process_batch_download move failed: %s", src)
                moved["skipped"].append(src)
                continue

        tag   = magenta("[DRY-RUN]") if dry_run else green("ok")
        if cat == "tv":
            label = c(C.DIM, f"Season {file_season:02d}")
        elif cat == "movie":
            label = c(C.DIM, f"Movie Library ({dest.parent.name})")
        else:
            label = c(C.DIM, "Season 00 (Specials)")
        print(f"  {c(C.SUCCESS, tag)} {c(C.MUTED, src.name[:52])} {c(C.DIM, '->')} {label}")
        _m, _s, _d = _route_subtitle_sidecars(src, dest if not dry_run else src, dest_dir, dry_run=dry_run)
        sub_moved += _m
        sub_skipped += _s
        sub_detected += _d
        moved[bucket].append(dest if not dry_run else src)
        if bucket == "tv":
            routed_tv_by_season[file_season].append(file_info.get("ep_num"))

    tv_n = len(moved["tv"])
    mv_n = len(moved["movies"])
    sp_n = len(moved["specials"])
    sk_n = len(moved["skipped"])
    print(f"\n  {c(C.SUCCESS, str(tv_n))} TV  "
          f"{c(C.VALUE, str(mv_n))} movie  "
          f"{c(C.CYAN_DIM, str(sp_n))} special/OVA  "
          f"{c(C.WARN if sk_n else C.DIM, str(sk_n) + ' skipped')}")
    if sub_detected:
        print(f"  {c(C.DIM, 'Subtitles:')} {c(C.SUCCESS, str(sub_moved))} moved/renamed"
              f"  {c(C.WARN if sub_skipped else C.DIM, str(sub_skipped) + ' failed')}")
    else:
        print(f"  {c(C.DIM, 'Subtitles:')} {c(C.DIM, 'no external sidecar subtitles detected (embedded subs likely).')}")

    if routed_tv_by_season:
        print(f"  {c(C.DIM, 'Routed seasons:')}")
        for sn in sorted(routed_tv_by_season):
            nums = sorted(n for n in routed_tv_by_season[sn] if n is not None)
            if nums and nums == list(range(nums[0], nums[-1] + 1)):
                ep_str = f"E{nums[0]}-E{nums[-1]}"
            elif nums:
                ep_str = ", ".join(f"E{n}" for n in nums[:10])
                if len(nums) > 10:
                    ep_str += f" ... ({len(nums)} total)"
            else:
                ep_str = "unknown episode tags"
            print(f"    {c(C.DIM, f'Season {sn:02d}:')} {c(C.VALUE, ep_str)}")

    # Remove empty staging subdirs recursively. Keep only if leftovers remain.
    if not dry_run and download_dir.is_dir():
        for _sub in sorted(download_dir.rglob("*"), reverse=True):
            if _sub.is_dir():
                try:
                    _sub.rmdir()
                except OSError:
                    pass
        try:
            download_dir.rmdir()
            log.info("Removed empty batch staging dir: %s", download_dir)
            print(f"  {c(C.DIM, 'Staging directory removed.')}")
            _parent = download_dir.parent
            if _parent.name == "_batch_staging":
                try:
                    _parent.rmdir()
                    log.info("Removed empty staging parent dir: %s", _parent)
                except OSError:
                    pass
        except OSError:
            remaining = list(download_dir.iterdir())
            if remaining:
                print(f"  {c(C.WARN, '[WARN]')} {c(C.DIM, str(len(remaining)))} "
                      f"{c(C.DIM, 'item(s) remain in staging (were not routed) - directory kept:')}")
                print(f"  {c(C.DIM, str(download_dir))}")
                for leftover in sorted(remaining):
                    print(f"    {c(C.DIM, '-')} {c(C.MUTED, leftover.name)}")
            log.debug("batch staging rmdir skipped: %s", download_dir)

    return moved

# anitopy handles the full range of nyaa release title formats:
#   "01v2", "S01E05", "- 5", batch ranges, OVA/SP/NCOP, resolution tags, etc.
try:
    import anitopy as _anitopy
    _ANITOPY_AVAILABLE = True
except ImportError:
    _ANITOPY_AVAILABLE = False

# guessit is a heuristics-based matcher used as a fallback parse source.
try:
    from guessit import guessit as _guessit
    _GUESSIT_AVAILABLE = True
except ImportError:
    _GUESSIT_AVAILABLE = False

_parse_cache: dict[str, dict] = {}


def _parse_title(title: str) -> dict:
    """Parse a nyaa release title; anitopy first, guessit fallback, with caching."""
    if title in _parse_cache:
        return _parse_cache[title]
    result: dict = {}
    if _ANITOPY_AVAILABLE:
        result = _anitopy.parse(title)
    elif _GUESSIT_AVAILABLE:
        # guessit returns a GuessIt dict - map keys to anitopy field names so
        # the rest of the code doesn't need to change.
        gi = dict(_guessit(title))
        result = {
            "release_group":  gi.get("release_group", ""),
            "anime_season":   gi.get("season", ""),
            "episode_number": gi.get("episode", ""),
            "anime_type":     gi.get("type", ""),
            "video_resolution": gi.get("screen_size", ""),
        }
        log.debug("guessit fallback parsed %r -> %s", title, json.dumps(result))
    _parse_cache[title] = result
    return result


def _normalize_title_for_match(title: str) -> str:
    title = title.lower()
    title = re.sub(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}", " ", title)
    title = re.sub(r"[^a-z0-9]+", " ", title)
    return re.sub(r"\s+", " ", title).strip()


def extract_explicit_season_number(title: str) -> int | None:
    if _ANITOPY_AVAILABLE:
        season = _parse_title(title).get("anime_season")
        if isinstance(season, list):
            for item in season:
                if str(item).isdigit():
                    return int(item)
        elif season and str(season).isdigit():
            return int(season)
    m = re.search(r"\b(\d+)(?:st|nd|rd|th)\s+[Ss]eason\b", title)
    if m:
        return int(m.group(1))
    m = re.search(r"\b[Ss]eason\s+(\d+)\b", title)
    if m:
        return int(m.group(1))
    roman = {"II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6}
    m = re.search(r"\b[Ss]eason\s+(II|III|IV|V|VI)\b", title)
    if m:
        return roman[m.group(1)]
    m = re.search(r"\bS(\d{1,2})(?:E\d+)?\b", title, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(?:Part|Cour)\s*(\d+)\b", title, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _inferred_season_confident(
    season_num: int,
    episodes_found: set[int],
    season_info: dict[int, dict] | None,
) -> bool:
    """
    Gate inferred seasons behind enough evidence to avoid one-off false positives.

    This keeps broad franchise searches like "Gintama" from surfacing synthetic
    seasons based on a single movie/spinoff/special hit that happened to share
    a title fragment with a sequel alias.
    """
    ep_count = len(episodes_found)
    if ep_count >= 3:
        return True
    total = ((season_info or {}).get(season_num) or {}).get("total")
    if not total:
        return False
    return ep_count >= max(2, min(6, math.ceil(total * 0.25)))


def _batch_title_matches_series_family(
    title: str,
    anime_name: str,
    season_info: dict[int, dict] | None = None,
) -> bool:
    """
    Reject obvious spinoff packs during main-series batch selection.

    We allow generic batch descriptors after the matched alias, but reject
    titles that introduce a large unrelated subtitle payload such as
    "Ginpachi-sensei" when the user searched for "Gintama".
    """
    title_norm = _normalize_title_for_match(title)
    aliases = [anime_name]
    for sinfo in (season_info or {}).values():
        aliases.extend(sinfo.get("titles") or [])

    seen: set[str] = set()
    alias_norms: list[str] = []
    for alias in aliases:
        alias_norm = _normalize_title_for_match(alias)
        if not alias_norm or alias_norm in seen:
            continue
        seen.add(alias_norm)
        alias_norms.append(alias_norm)
    alias_norms.sort(key=len, reverse=True)

    generic_tokens = {
        "batch", "bd", "bdrip", "bluray", "dvd", "remux", "complete",
        "collection", "season", "seasons", "part", "cour", "tv", "movie",
        "movies", "special", "specials", "ova", "ovas", "ona", "oad",
        "sp", "episode", "episodes", "ep", "uncensored", "uncut", "dual",
        "audio", "multi", "subs", "sub", "dub", "hevc", "x264", "x265",
        "10bit", "8bit", "aac", "flac", "opus", "v2", "v3",
    }

    for alias_norm in alias_norms:
        if alias_norm not in title_norm:
            continue
        remainder = re.sub(rf"\b{re.escape(alias_norm)}\b", " ", title_norm, count=1)
        leftover = [
            tok for tok in remainder.split()
            if not tok.isdigit()
            and not re.fullmatch(r"s\d{1,2}|e\d{1,3}|[sp]\d{1,3}|v\d+", tok, re.IGNORECASE)
            and tok not in generic_tokens
        ]
        if len(leftover) <= 3:
            return True
    return False


def enrich_season_map_from_batch_candidates(
    results: list[dict],
    anime_name: str,
    season_info: dict[int, dict] | None = None,
    inspect_limit: int = 3,
) -> tuple[dict[int, set[int]], list[str]]:
    """
    Inspect a small number of high-value batch candidates and extract explicit
    season/episode structure from their file lists.

    This is especially useful for long franchises where torrent collectors use
    clear S01/S02/... naming that is more actionable for file routing than
    AniList/MAL's sequel graph.
    """
    candidates = [
        r for r in results
        if r.get("batch_candidate")
        and _batch_title_matches_series_family(r["title"], anime_name, season_info)
    ]
    if not candidates:
        return {}, []

    def _priority(r: dict) -> tuple[float, int, int]:
        return (-_torrent_score(r), -r.get("completed", 0), -r.get("seeders", 0))

    enriched: dict[int, set[int]] = {}
    inspected_titles: list[str] = []

    for r in sorted(candidates, key=_priority)[:inspect_limit]:
        if not inspect_batch_candidate(r):
            continue
        info = r.get("batch_info") or {}
        seasons_present = info.get("seasons_present") or []
        if len(seasons_present) < 2:
            continue
        tv_eps = info.get("tv_episodes") or []
        season_buckets: dict[int, set[int]] = defaultdict(set)
        for f in tv_eps:
            sn = f.get("season_num")
            ep = f.get("ep_num")
            if sn is None or ep is None:
                continue
            season_buckets[int(sn)].add(int(ep))
        if len(season_buckets) < 2:
            continue
        for sn, eps in season_buckets.items():
            enriched.setdefault(sn, set()).update(eps)
        inspected_titles.append(r["title"])

    return enriched, inspected_titles


def infer_anilist_season_number(title: str, anime_name: str, season_info: dict[int, dict] | None) -> int:
    explicit = extract_explicit_season_number(title)
    if explicit is not None:
        return explicit
    if not season_info:
        return 1

    title_norm = _normalize_title_for_match(title)
    base_norm = _normalize_title_for_match(anime_name)
    best_match_season = 1
    best_match_len = 0

    for season_num, sinfo in season_info.items():
        if season_num <= 1:
            continue
        for alias in sinfo.get("titles") or []:
            alias_norm = _normalize_title_for_match(alias)
            if not alias_norm or alias_norm == base_norm:
                continue
            if len(alias_norm) < 6:
                continue
            if alias_norm in title_norm and len(alias_norm) > best_match_len:
                best_match_season = season_num
                best_match_len = len(alias_norm)

    return best_match_season


def annotate_results_with_anilist_seasons(
    results: list[dict],
    anime_name: str,
    season_info: dict[int, dict] | None = None,
) -> dict[int, dict]:
    season_info = season_info or fetch_anilist_season_info(anime_name)
    for r in results:
        resolved = infer_anilist_season_number(r["title"], anime_name, season_info)
        r["_season_num"] = resolved
        r["_season_inferred"] = (extract_explicit_season_number(r["title"]) is None and resolved != 1)
    return season_info


def get_result_season_number(result: dict) -> int:
    return int(result.get("_season_num") or extract_season_number(result["title"]))


def extract_sub_group(title: str) -> str | None:
    if _ANITOPY_AVAILABLE:
        grp = _parse_title(title).get("release_group", "")
        return grp.strip().lower() if grp else None
    # Regex fallback
    m = re.match(r"^\[([^\]]+)\]", title.strip())
    return m.group(1).strip().lower() if m else None


def _torrent_score(r: dict) -> float:
    """
    Composite per-torrent swarm-health score.

    Two signals only - both answer "can I download this right now?":
      * seeders  (weight 1.0) - primary: how many peers can upload to you
      * leechers (weight 0.4) - secondary: confirms swarm is alive and active

    The completed (all-time download) count is intentionally excluded from this
    formula.  It measures historical popularity, not current downloadability, and
    is heavily biased toward mainstream releases - meaning it would actively hurt
    scoring for obscure groups on niche series, which is exactly the fallback case
    this function needs to handle well.  Completed count is still scraped and
    shown in the group table as informational context.

    Episode coverage (did the group sub every episode of the season?) is handled
    separately via the coverage_ratio bonus in rank_sub_groups, which is the
    correct layer for that dimension.
    """
    return r.get("seeders", 0) * 1.0 + r.get("leechers", 0) * 0.4


def _title_resolution_rank(title: str) -> int:
    """
    Higher is better: 2160p > 1080p > 720p > 480p > 360p > unknown.
    """
    m = re.search(r"\b(2160|1080|720|480|360)p\b", title, re.IGNORECASE)
    if not m:
        return 0
    return int(m.group(1))


def rank_sub_groups(results: list[dict]) -> list[dict]:
    """
    Rank all sub groups found in *results* using a composite score.

    Score per group:
      total_score      = sum of _torrent_score() across all individual torrents
      coverage_bonus   = (ep_count / max_ep_count_any_group) x total_score x 0.5

    This means a group that has covered every episode of a season with moderate
    health beats a group that has one ultra-seeded episode but nothing else.
    Episode coverage is the dominant tiebreaker for groups with similar scores.
    """
    group_score:    dict[str, float] = defaultdict(float)
    group_seeders:  dict[str, int]   = defaultdict(int)
    group_leechers: dict[str, int]   = defaultdict(int)
    group_complete: dict[str, int]   = defaultdict(int)
    group_episodes: dict[str, set]   = defaultdict(set)

    for r in results:
        group = extract_sub_group(r["title"])
        if not group:
            continue
        ep = extract_episode_number(r["title"])
        if ep is not None:
            group_episodes[group].add(ep)
        ts = _torrent_score(r)
        group_score[group]    += ts
        group_seeders[group]  += r.get("seeders",   0)
        group_leechers[group] += r.get("leechers",  0)
        group_complete[group] += r.get("completed", 0)

    if not group_score:
        return []

    max_coverage = max((len(eps) for eps in group_episodes.values()), default=0) or 1
    ranked = []
    for group, total_ts in group_score.items():
        ep_count       = len(group_episodes[group])
        coverage_ratio = ep_count / max_coverage
        coverage_bonus = coverage_ratio * total_ts * 0.5
        final_score    = total_ts + coverage_bonus
        ranked.append({
            "group":           group,
            "score":           final_score,
            "raw_score":       total_ts,
            "coverage_ratio":  coverage_ratio,
            "total_seeders":   group_seeders[group],
            "total_leechers":  group_leechers[group],
            "total_completed": group_complete[group],
            "episode_count":   ep_count,
            "avg_seeders":     round(group_seeders[group] / ep_count) if ep_count else 0,
        })
    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


def pick_best_group(ranked: list[dict]) -> tuple[str, bool, str]:
    """
    Select the best sub group from *ranked*.

    Returns (group_name, is_preferred, reason) where:
      * is_preferred - True if the chosen group is in PREFERRED_GROUPS
      * reason       - human-readable explanation for the UI

    Selection logic:
      1. If the top-ranked group is a preferred group -> use it directly.
      2. Scan within QUALITY_TOLERANCE of the top score for any preferred group.
      3. If NO preferred group exists anywhere in the results -> fall back to the
         highest-scored available group (complete fallback path - the preferred
         list is irrelevant for this series).
    """
    if not ranked:
        return "", False, "no groups found"

    # Groups with 0 individual episodes only have batch torrents - skip them
    # for primary group selection since per-episode logic can't work with them.
    ranked_with_eps = [e for e in ranked if e["episode_count"] > 0]
    if not ranked_with_eps:
        ranked_with_eps = ranked  # everything is batches - allow through

    top = ranked_with_eps[0]

    # Fast path: top group is already preferred
    if top["group"] in PREFERRED_GROUPS:
        return top["group"], True, "top-ranked preferred group"

    # Search within quality tolerance for a preferred group
    threshold = top["score"] * (1 - QUALITY_TOLERANCE)
    for entry in ranked_with_eps[1:]:
        if entry["score"] < threshold:
            break
        if entry["group"] in PREFERRED_GROUPS:
            reason = (f"preferred group within {QUALITY_TOLERANCE*100:.0f}% "
                      f"of top score (score {entry['score']:.0f} vs {top['score']:.0f})")
            return entry["group"], True, reason

    # Full fallback - no preferred group subbed this series at all.
    cov_pct  = f"{top['coverage_ratio']*100:.0f}%"
    reason   = (f"no preferred group available - best available by composite score "
                f"(seeds={top['total_seeders']}, leechers={top['total_leechers']}, "
                f"completed={top['total_completed']}, coverage={cov_pct})")
    return top["group"], False, reason


def pick_retry_group(ranked: list[dict], excluded_groups: set[str]) -> tuple[str, bool, str]:
    """
    Retry selector used after a stuck-torrent event.

    Priorities:
      1) not excluded
      2) has episode coverage
      3) has live swarm signal (seeders > 0 or leechers > 0)
      4) highest existing composite rank
    """
    pool = [e for e in ranked if e["group"] not in excluded_groups]
    if not pool:
        return "", False, "no alternative groups available"

    with_eps = [e for e in pool if e.get("episode_count", 0) > 0] or pool
    live = [e for e in with_eps if (e.get("total_seeders", 0) > 0 or e.get("total_leechers", 0) > 0)]
    cand = live[0] if live else with_eps[0]
    is_pref = cand["group"] in PREFERRED_GROUPS
    reason = (
        "retry mode: selected best non-blacklisted group with live swarm signal"
        if live else
        "retry mode: no live swarm groups left; selected best non-blacklisted fallback"
    )
    return cand["group"], is_pref, reason


# -- Season / episode parsing -------------------------------------------------

def extract_season_number(title: str) -> int:
    return extract_explicit_season_number(title) or 1


def extract_episode_number(title: str) -> int | None:
    if _ANITOPY_AVAILABLE:
        info = _parse_title(title)
        # Skip specials and non-episode entries entirely
        anime_type = info.get("anime_type", "")
        if isinstance(anime_type, list):
            anime_type = " ".join(anime_type)
        if anime_type.upper() in ("OVA", "ONA", "OAD", "SP", "SPECIAL",
                                   "NCOP", "NCED", "MOVIE", "BATCH"):
            return None
        ep = info.get("episode_number")
        if ep is not None:
            # Skip batch packs - anitopy returns a list for "01-03"
            if isinstance(ep, list):
                return None
            ep_str = str(ep)
            # Skip version-suffixed non-numeric values and specials like "OVA"
            if re.match(r"^\d+$", ep_str):
                return int(ep_str)
    # Regex fallback
    m = re.search(r"[--]\s*(\d{1,3})(?:\s*[\[\(v\s]|$)", title)
    if m: return int(m.group(1))
    m = re.search(r"\bE(?:P)?(\d{1,3})\b", title, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r"\bEpisode[\s._-]*(\d{1,3})\b", title, re.IGNORECASE)
    if m: return int(m.group(1))
    return None


def build_season_query(anime_name: str, season: int | None, season_info: dict[int, dict] | None = None) -> str:
    if not season or season <= 1:
        return anime_name
    if season_info:
        sinfo = season_info.get(season) or {}
        for alias in sinfo.get("titles") or []:
            alias = str(alias).strip()
            if alias and alias.lower() != anime_name.lower():
                return alias
    return f"{anime_name} S{season:02d}"


def detect_group_handoffs(
    results:      list[dict],
    primary:      str,
    season:       int | None,
    episode_range: tuple[int, int] | None,
    all_ranked:   list[dict],
) -> list[dict]:
    """
    Proactively detect mid-season group handoffs and build a complete episode
    list from the best available source for each episode.

    A "handoff" is when group A subbed episodes 1-6 and then stopped, and group B
    (or C, or whoever) picked up 7-12.  This happens frequently when:
      * A fansub group drops a series mid-season due to licensing or burnout
      * A simulcast group takes over from a fansub partway through
      * Season X was covered by group A but they never returned for season Y

    Algorithm:
      1. Collect all episodes the primary group has for this season.
      2. Find the episode range that any group covers (the "total pool").
      3. For each episode NOT covered by the primary group, find the best
         alternative torrent using the composite score.
      4. Return the merged episode list sorted by episode number, with each
         item tagged with which group provided it.

    Returns a list of torrent dicts (same format as results), possibly sourced
    from multiple groups.  Each dict gets a "_source_group" tag for display.
    """
    # Primary group's coverage for this season
    primary_eps: dict[int, dict] = {}
    all_pool_eps: dict[int, list[dict]] = defaultdict(list)

    for r in results:
        if season is not None and get_result_season_number(r) != season:
            continue
        ep = extract_episode_number(r["title"])
        if ep is None:
            continue
        if episode_range and not (episode_range[0] <= ep <= episode_range[1]):
            continue

        all_pool_eps[ep].append(r)

        grp = extract_sub_group(r["title"])
        if grp == primary:
            # Keep highest composite score version for this ep from primary group
            existing = primary_eps.get(ep)
            if existing is None or _torrent_score(r) > _torrent_score(existing):
                primary_eps[ep] = r

    all_ep_nums = sorted(all_pool_eps)
    if not all_ep_nums:
        return []

    # Never fill episodes ABOVE the primary group's highest episode.
    # Groups with absolute/global numbering (e.g. E264 from the 1989 Ranma
    # series, or ToonsHub E29 for OPM S3) would otherwise inject spurious
    # high-numbered episodes that don't belong in this season.
    primary_max = max(primary_eps) if primary_eps else None
    if primary_max is not None:
        all_ep_nums = [ep for ep in all_ep_nums if ep <= primary_max]
    if not all_ep_nums:
        return []

    # Build ranked lookup: group -> score (for tie-breaking non-primary episodes)
    group_rank: dict[str, float] = {e["group"]: e["score"] for e in all_ranked}

    merged: list[dict] = []
    handoffs_detected: list[tuple[int, str]] = []   # (ep_num, group_that_filled)

    for ep in all_ep_nums:
        if ep in primary_eps:
            item = dict(primary_eps[ep])
            item["_source_group"] = primary
            merged.append(item)
        else:
            # Primary group doesn't have this episode - find best alternative
            candidates = all_pool_eps[ep]
            # Sort by: (1) preferred group membership, (2) group rank score,
            # (3) composite torrent score
            def _fill_key(r):
                grp = extract_sub_group(r["title"]) or ""
                return (
                    0 if grp in PREFERRED_GROUPS else 1,   # preferred first
                    -group_rank.get(grp, 0),               # higher ranked group first
                    -_torrent_score(r),                    # better torrent first
                )
            best = min(candidates, key=_fill_key)
            fill_grp = extract_sub_group(best["title"]) or "unknown"
            item = dict(best)
            item["_source_group"] = fill_grp
            merged.append(item)
            handoffs_detected.append((ep, fill_grp))

    if handoffs_detected:
        # Group contiguous handoff ranges for a compact display message
        ranges: list[tuple[int, int, str]] = []
        for ep_num, grp in handoffs_detected:
            if ranges and ranges[-1][2] == grp and ep_num == ranges[-1][1] + 1:
                ranges[-1] = (ranges[-1][0], ep_num, grp)
            else:
                ranges.append((ep_num, ep_num, grp))
        for lo, hi, grp in ranges:
            rng = f"E{lo}" if lo == hi else f"E{lo}-E{hi}"
            print(f"  {c(C.AMBER, '[HANDOFF]')} {rng}: "
                  f"{c(C.MUTED, f'[{primary}]')} {c(C.DIM, 'never released')} -> "
                  f"filled by {c(C.VALUE, f'[{grp}]')}")
        log.info("Handoffs detected for season %s primary=[%s]: %s",
                 season, primary, handoffs_detected)

    return sorted(merged, key=lambda r: extract_episode_number(r["title"]) or 0)


def filter_episodes(results: list[dict], sub_group: str,
                    season: int | None,
                    episode_range: tuple[int, int] | None) -> list[dict]:
    ep_map: dict[int, dict] = {}
    for r in results:
        if extract_sub_group(r["title"]) != sub_group:
            continue
        if season is not None and get_result_season_number(r) != season:
            continue
        ep = extract_episode_number(r["title"])
        if ep is None:
            continue
        if episode_range and not (episode_range[0] <= ep <= episode_range[1]):
            continue
        # Use composite score instead of bare seeders to pick the best version
        if ep not in ep_map or _torrent_score(r) > _torrent_score(ep_map[ep]):
            ep_map[ep] = r
    return [ep_map[ep] for ep in sorted(ep_map)]


def filter_episodes_any_group(
    results:         list[dict],
    season:          int | None,
    episode_range:   tuple[int, int] | None,
    preferred_group: str,
    missing_nums:    list[int],
) -> list[dict]:
    """
    For each episode number in *missing_nums*, find the best available torrent
    from ANY sub group in *results*.

    Priority order:
      1. Preferred group (from PREFERRED_GROUPS) with highest composite score
      2. Any group - highest composite score wins

    "Composite score" = seedersx1.0 + leechersx0.4 + completedx0.15
    Using completed count as a tiebreaker ensures that an obscure group's
    well-distributed torrent beats a sparsely-seeded release from a bigger name.
    """
    ep_candidates: dict[int, dict] = {}
    for r in results:
        if season is not None and get_result_season_number(r) != season:
            continue
        ep = extract_episode_number(r["title"])
        if ep is None or ep not in missing_nums:
            continue
        if episode_range and not (episode_range[0] <= ep <= episode_range[1]):
            continue

        existing  = ep_candidates.get(ep)
        new_grp   = extract_sub_group(r["title"]) or ""
        new_score = _torrent_score(r)

        if existing is None:
            ep_candidates[ep] = r
            continue

        exist_grp   = extract_sub_group(existing["title"]) or ""
        exist_score = _torrent_score(existing)

        # A preferred group always beats a non-preferred group
        exist_is_pref = exist_grp in PREFERRED_GROUPS
        new_is_pref   = new_grp in PREFERRED_GROUPS

        if new_is_pref and not exist_is_pref:
            ep_candidates[ep] = r
        elif exist_is_pref and not new_is_pref:
            pass   # keep existing
        else:
            # Both preferred or both non-preferred - composite score decides
            if new_score > exist_score:
                ep_candidates[ep] = r

    return [ep_candidates[ep] for ep in sorted(ep_candidates)]


def select_retry_episode_result(
    results: list[dict],
    season: int,
    ep_num: int,
    excluded_groups: set[str] | None = None,
    excluded_detail_urls: set[str] | None = None,
) -> tuple[dict | None, str]:
    """
    Pick a replacement torrent candidate for one stuck episode.

    Prefer a different group first. If none exists, fall back to any other
    release for the same episode that uses a different detail page.
    """
    excluded_groups = excluded_groups or set()
    excluded_detail_urls = excluded_detail_urls or set()

    pool = [
        r for r in results
        if get_result_season_number(r) == season
        and extract_episode_number(r["title"]) == ep_num
        and r.get("detail_url") not in excluded_detail_urls
    ]
    if not pool:
        return None, "no matching torrents found for this episode"

    alt_groups = [r for r in pool if (extract_sub_group(r["title"]) or "") not in excluded_groups]
    candidate_pool = alt_groups or pool

    def _key(r: dict) -> tuple[int, int, float, int]:
        grp = extract_sub_group(r["title"]) or ""
        live = (r.get("seeders", 0) > 0 or r.get("leechers", 0) > 0)
        pref = grp in PREFERRED_GROUPS
        return (
            0 if live else 1,
            0 if pref else 1,
            -_torrent_score(r),
            -int(r.get("completed", 0)),
        )

    best = min(candidate_pool, key=_key)
    grp = extract_sub_group(best["title"]) or "unknown"
    reason = (
        "switched to a different group for this episode"
        if grp not in excluded_groups else
        "reused same group with a different release candidate"
    )
    return best, reason



# -- AniList episode-count verification ---------------------------------------

ANILIST_API = "https://graphql.anilist.co"

_ANILIST_QUERY = """
query ($search: String) {
  Page(page: 1, perPage: 5) {
    media(search: $search, type: ANIME, sort: SEARCH_MATCH) {
      id
      format
      title { romaji english native }
      synonyms
      episodes
      status
      meanScore
      genres
      season
      seasonYear
      studios(isMain: true) { nodes { name } }
      nextAiringEpisode { episode airingAt }
      relations {
        edges {
          relationType
          node {
            id
            format
            title { romaji english native }
            synonyms
            episodes
            status
            meanScore genres season seasonYear
            nextAiringEpisode { episode airingAt }
          }
        }
      }
    }
  }
}
"""

# Return type: {season: {"total": int|None, "aired": int|None, "airing": bool}}
def fetch_anilist_season_info(anime_name: str) -> dict[int, dict]:
    """
    Query AniList for episode info per season.

    Returns a dict keyed by season number, each value containing:
      "total"  - total planned episodes (None if unknown / still airing)
      "aired"  - episodes aired so far (= total for finished, <total if airing)
      "airing" - True if the season is currently RELEASING

    For currently airing seasons, "aired" = nextAiringEpisode.episode - 1,
    which is the highest episode that has actually been broadcast.
    Falls back silently on network/parse errors (returns {}).
    """
    cache_key = _normalize_title_for_match(anime_name)
    if cache_key in _ANILIST_SEASON_INFO_CACHE:
        return _ANILIST_SEASON_INFO_CACHE[cache_key]
    try:
        resp = requests.post(
            ANILIST_API,
            json={"query": _ANILIST_QUERY, "variables": {"search": anime_name}},
            timeout=10,
        )
        if resp.status_code != 200:
            _ANILIST_SEASON_INFO_CACHE[cache_key] = {}
            return {}
        data    = resp.json()
        entries = data.get("data", {}).get("Page", {}).get("media", [])
        if not entries:
            _ANILIST_SEASON_INFO_CACHE[cache_key] = {}
            return {}

        def _entry_titles(node: dict) -> list[str]:
            title_obj = node.get("title") or {}
            raw_titles = [
                title_obj.get("english"),
                title_obj.get("romaji"),
                title_obj.get("native"),
            ]
            raw_titles.extend(node.get("synonyms") or [])
            seen: set[str] = set()
            titles: list[str] = []
            for t in raw_titles:
                if not t:
                    continue
                ts = str(t).strip()
                key = ts.lower()
                if not ts or key in seen:
                    continue
                seen.add(key)
                titles.append(ts)
            return titles

        def _entry_info(node: dict) -> dict:
            status  = (node.get("status") or "").upper()
            airing  = status == "RELEASING"
            total   = node.get("episodes")
            nae     = node.get("nextAiringEpisode")
            if airing and nae:
                aired = nae["episode"] - 1
            elif total:
                aired = total
            else:
                aired = None

            # Metadata fields - present on the root entry, may be absent on sequel nodes
            score   = node.get("meanScore")          # 0-100 int or None
            genres  = node.get("genres") or []       # list of strings
            season  = node.get("season") or ""       # "WINTER", "SPRING", etc.
            year    = node.get("seasonYear") or ""

            # Studios only present on root node (not sequel relation nodes)
            studios_data = node.get("studios") or {}
            studio_nodes = studios_data.get("nodes") or []
            studio = studio_nodes[0]["name"] if studio_nodes else ""

            return {
                "total":   total,
                "aired":   aired,
                "airing":  airing,
                "format":  node.get("format") or "",
                "score":   score,
                "genres":  genres,
                "season":  season,
                "year":    year,
                "studio":  studio,
                "titles":  _entry_titles(node),
            }

        best       = entries[0]
        season_num = 1
        seasons: dict[int, dict] = {season_num: _entry_info(best)}

        for edge in best.get("relations", {}).get("edges", []):
            if edge.get("relationType") == "SEQUEL":
                node = edge.get("node") or {}
                if (node.get("format") or "").upper() not in ("TV", "TV_SHORT"):
                    continue
                season_num += 1
                seasons[season_num] = _entry_info(node)

        _ANILIST_SEASON_INFO_CACHE[cache_key] = seasons
        return seasons
    except Exception:
        log.exception("fetch_anilist_season_info failed for %r", anime_name)
        _ANILIST_SEASON_INFO_CACHE[cache_key] = {}
        return {}


def fetch_anilist_related_movies(anime_name: str) -> list[dict]:
    """
    Return directly related AniList movie entries for a franchise title.

    Result items:
      {"title": str, "year": int|None, "titles": [aliases...]}
    """
    cache_key = _normalize_title_for_match(anime_name)
    if cache_key in _ANILIST_RELATED_MOVIES_CACHE:
        return _ANILIST_RELATED_MOVIES_CACHE[cache_key]
    try:
        resp = requests.post(
            ANILIST_API,
            json={"query": _ANILIST_QUERY, "variables": {"search": anime_name}},
            timeout=10,
        )
        if resp.status_code != 200:
            _ANILIST_RELATED_MOVIES_CACHE[cache_key] = []
            return []
        data = resp.json()
        entries = data.get("data", {}).get("Page", {}).get("media", [])
        if not entries:
            _ANILIST_RELATED_MOVIES_CACHE[cache_key] = []
            return []

        def _entry_titles(node: dict) -> list[str]:
            title_obj = node.get("title") or {}
            raw_titles = [
                title_obj.get("english"),
                title_obj.get("romaji"),
                title_obj.get("native"),
            ]
            raw_titles.extend(node.get("synonyms") or [])
            seen: set[str] = set()
            titles: list[str] = []
            for t in raw_titles:
                if not t:
                    continue
                ts = str(t).strip()
                key = ts.lower()
                if not ts or key in seen:
                    continue
                seen.add(key)
                titles.append(ts)
            return titles

        movies: list[dict] = []
        seen_keys: set[str] = set()
        nodes = [entries[0]] + [
            edge.get("node") or {}
            for edge in (entries[0].get("relations", {}) or {}).get("edges", [])
        ]
        for node in nodes:
            if (node.get("format") or "").upper() != "MOVIE":
                continue
            titles = _entry_titles(node)
            if not titles:
                continue
            title = titles[0]
            year = node.get("seasonYear") or None
            key = f"{_normalize_title_for_match(title)}::{year or ''}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            movies.append({
                "title": title,
                "year": int(year) if str(year).isdigit() else None,
                "titles": titles,
            })
        _ANILIST_RELATED_MOVIES_CACHE[cache_key] = movies
        return movies
    except Exception:
        log.exception("fetch_anilist_related_movies failed for %r", anime_name)
        _ANILIST_RELATED_MOVIES_CACHE[cache_key] = []
        return []


# Thin compatibility wrapper used elsewhere in the script
def fetch_anilist_episode_counts(anime_name: str) -> dict[int, int | None]:
    info = fetch_anilist_season_info(anime_name)
    return {s: v["total"] for s, v in info.items()}


def print_show_card(sinfo: dict) -> None:
    """
    Print a compact metadata card for a season using AniList data.
    Shows score, genres, studio, and air season/year.
    Silently skips any field that is missing.
    """
    score   = sinfo.get("score")
    genres  = sinfo.get("genres") or []
    studio  = sinfo.get("studio") or ""
    season  = (sinfo.get("season") or "").capitalize()
    year    = sinfo.get("year") or ""

    if score is not None:
        score_f = score / 10

        # Tiered colour + label based on AniList mean score (0-100)
        if score >= 80:
            color     = C.GREEN
            indicator = "*****"
        elif score >= 70:
            color     = C.GREEN
            indicator = "****."
        elif score >= 60:
            color     = C.YELLOW
            indicator = "***.."
        elif score >= 50:
            color     = C.YELLOW
            indicator = "**..."
        else:
            color     = C.RED
            indicator = "*...."

        score_str = c(color, f"{score_f:.1f}/10  {indicator}")
        print(f"  {c(C.LABEL, 'Rating:')}       {score_str} "
              f"{c(C.DIM, '(AniList)')}")
    parts = []
    if genres:
        def _genre_label(g: str) -> str:
            key = g.strip().lower()
            if key == "ecchi":
                return c(C.PINK, g)
            if key == "romance":
                return c(C.VAL_RED, g)
            if key == "comedy":
                return c(C.COMEDY_BLUE, g)
            return c(C.VALUE2, g)

        genre_str = c(C.DIM, " | ").join(_genre_label(g) for g in genres[:6])
        print(f"  {c(C.LABEL, 'Genres:')}       {genre_str}")

    if studio:
        parts.append(c(C.VALUE, studio))
    if season and year:
        parts.append(c(C.DIM, f"{season} {year}"))
    elif year:
        parts.append(c(C.DIM, str(year)))
    if parts:
        print(f"  {c(C.LABEL, 'Studio:')}       {c(C.DIM, '  |  ').join(parts)}")


def verify_episode_coverage(
    found_episodes:    list[dict],
    anime_name:        str,
    season:            int,
    raw:               list[dict],
    chosen_group:      str,
    args,
    episode_range:     tuple[int, int] | None,
) -> list[dict]:
    """
    Cross-check found_episodes against AniList expected count.
    If there are gaps, runs targeted fill searches and appends missing eps.
    Returns the (potentially augmented) episode list.
    """
    season_info  = fetch_anilist_season_info(anime_name)
    sinfo        = season_info.get(season, {})
    expected     = sinfo.get("total")
    aired        = sinfo.get("aired")
    is_airing    = sinfo.get("airing", False)

    # For gap checking purposes, use "aired" as the ceiling - never flag
    # episodes that haven't been broadcast yet as missing.
    # For a finished show:  aired == total
    # For a airing show:    aired = last broadcast episode, total may be unknown
    effective_expected = aired if aired is not None else expected

    found_nums = sorted(e for r in found_episodes
                        if (e := extract_episode_number(r["title"])) is not None
                        and e > 0)   # E00 prologues kept in download list but
                                     # excluded from gap/count checks
    found_count = len(found_nums)

    # -- AniList split-cour guard ----------------------------------------------
    # AniList lists split-cour shows as separate entries (~12 ep each).
    # If we found MORE episodes than AniList's total, it's a split-cour entry.
    if effective_expected and found_count > effective_expected:
        print(f"\n  {c(C.DIM, 'AniList:')} expected {c(C.VALUE2, str(effective_expected))} ep(s) "
              f"but found {c(C.AMBER_B, str(found_count))} - "
              f"{c(C.DIM, 'split-cour listing detected, skipping AniList count.')}")
        effective_expected = None
        expected           = None
    elif is_airing and aired is not None:
        status_str = c(C.AMBER, f"airing - {aired} ep(s) aired so far")
        if expected:
            status_str += c(C.DIM, f" of {expected} total")
        print(f"\n  {c(C.LABEL, 'AniList check:')} {status_str}")
        print(f"  {c(C.DIM, 'Currently airing - only checking for gaps up to E'+ str(aired) + '. '
              'Future episodes are not counted as missing.')}")
    elif effective_expected:
        print(f"\n  {c(C.LABEL, 'AniList check:')} "
              f"expected {c(C.AMBER_B, str(effective_expected))} ep(s), "
              f"found {c(C.VALUE2, str(found_count))} in pool")
    else:
        print(f"\n  {c(C.DIM, 'AniList: no episode count for Season')} "
              f"{c(C.VALUE2, str(season))}")

    # -- Gap detection ---------------------------------------------------------
    # Only check within the aired range - never flag unaired eps as missing.
    # For currently airing shows, cap the expected range at `aired`.
    if found_nums:
        # Cap the check range at effective_expected so we don't flag future eps
        check_max  = effective_expected if effective_expected else found_nums[-1]
        full_range = range(found_nums[0], check_max + 1)
        gaps       = sorted(set(full_range) - set(found_nums))
        below_start: list[int] = []
        if effective_expected and found_nums[0] > 1:
            candidate_below = list(range(1, found_nums[0]))
            if len(candidate_below) <= effective_expected // 2:
                below_start = candidate_below
    else:
        gaps        = []
        below_start = list(range(1, (effective_expected or 1) + 1)) if effective_expected else []

    missing_nums = sorted(set(below_start + gaps))

    # -- Check for episodes before the group's start (cross-group handoff) ------
    # If the chosen group starts at E13, episodes E1-12 may exist in the pool
    # under a different group (e.g. HorribleSubs -> SubsPlease handoff).
    pre_start_missing: list[int] = []
    if found_nums and found_nums[0] > 1:
        all_season_eps = sorted({
            e for r in raw
            if (get_result_season_number(r) == season
                and (e := extract_episode_number(r["title"])) is not None)
        })
        pre_start_missing = [e for e in all_season_eps if e < found_nums[0]]

    if pre_start_missing:
        print(f"  {c(C.LABEL, 'Cross-group fill:')} "
              f"E{pre_start_missing[0]}-E{pre_start_missing[-1]} exist in pool "
              f"under other groups - adding best available torrents ...")
        fill_eps = filter_episodes_any_group(
            raw, season, episode_range, chosen_group, pre_start_missing
        )
        if fill_eps:
            fill_groups = sorted({extract_sub_group(r["title"]) for r in fill_eps})
            print(f"  {c(C.SUCCESS, '✓')} Found E{pre_start_missing[0]}-E{pre_start_missing[-1]} "
                  f"via: {c(C.VALUE, ', '.join(f'[{g}]' for g in fill_groups))}")
            found_episodes = fill_eps + found_episodes
            found_nums     = sorted(e for r in found_episodes
                                    if (e := extract_episode_number(r["title"])) is not None)
            found_count    = len(found_nums)

    if not missing_nums:
        if found_count > 0:
            print(f"  {c(C.SUCCESS, '✓')} No in-range gaps detected "
                  f"(E{found_nums[0]}-E{found_nums[-1]}, {found_count} ep(s)).")
        return found_episodes

    # -- Fill search - genuine in-range gaps ----------------------------------
    print(f"  {yellow('[GAP]')} Missing episode(s) within covered range: "
          f"{c(C.AMBER_B, ', '.join(str(n) for n in missing_nums[:20]))}"
          + (f" ... (+{len(missing_nums)-20} more)" if len(missing_nums) > 20 else ""))
    print(f"  {c(C.LABEL, 'Running fill search ...')}")

    fill_queries = [build_season_query(anime_name, season, season_info)]
    if len(missing_nums) <= 6:
        for n in missing_nums:
            fill_queries.append(f"{anime_name} - {n:02d}")

    seen = {r["detail_url"] for r in raw}
    for q in fill_queries:
        extra = search_all_pages(q, max(args.pages, 3))
        annotate_results_with_anilist_seasons(extra, anime_name, season_info)
        new   = [r for r in extra if r["detail_url"] not in seen]
        raw  += new
        seen |= {r["detail_url"] for r in new}

    # Try same group first, then cross-group for anything still missing
    updated_eps = filter_episodes(raw, chosen_group, season, episode_range)
    found_nums3 = sorted(e for r in updated_eps
                         if (e := extract_episode_number(r["title"])) is not None)
    still_missing = sorted(set(missing_nums) - set(found_nums3))

    if still_missing:
        cross_fill = filter_episodes_any_group(
            raw, season, episode_range, chosen_group, still_missing
        )
        if cross_fill:
            cross_groups = sorted({extract_sub_group(r["title"]) for r in cross_fill})
            print(f"  {c(C.SUCCESS, '✓')} Cross-group fill via: "
                  f"{c(C.VALUE, ', '.join(f'[{g}]' for g in cross_groups))}")
            updated_eps = sorted(updated_eps + cross_fill,
                                 key=lambda r: extract_episode_number(r["title"]) or 0)
            truly_missing = sorted(
                set(still_missing) -
                {extract_episode_number(r["title"]) for r in cross_fill}
            )
            if truly_missing:
                print(f"  {yellow('[WARN]')} Still missing: "
                      f"{c(C.AMBER_B, ', '.join(str(n) for n in truly_missing[:20]))}")
        else:
            print(f"  {yellow('[WARN]')} Still missing after fill: "
                  f"{c(C.AMBER_B, ', '.join(str(n) for n in still_missing[:20]))}")
    else:
        print(f"  {c(C.SUCCESS, '✓')} Fill successful - now have "
              f"{c(C.VALUE2, str(len(updated_eps)))} episode(s).")

    # Merge with any pre-start fill we already did
    ep_nums_already = {extract_episode_number(r["title"]) for r in found_episodes
                       if extract_episode_number(r["title"]) is not None}
    for r in updated_eps:
        if extract_episode_number(r["title"]) not in ep_nums_already:
            found_episodes.append(r)
    found_episodes = sorted(found_episodes,
                            key=lambda r: extract_episode_number(r["title"]) or 0)
    return found_episodes


# -- Directory helpers ---------------------------------------------------------

_ILLEGAL_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

# Substitute before stripping - preserves meaning rather than silently dropping
_CHAR_SUBS: list[tuple[str, str]] = [
    ("½", "1-2"),   # Ranma ½  ->  Ranma 1-2
    ("⅓", "1-3"),
    ("¼", "1-4"),
    ("¾", "3-4"),
    ("/", "-"),     # forward slash is a Windows path separator
    (":", " "),    # full-width colon
    ("?", ""),
    ("Ã¯¼Å ", ""),
]


def sanitise_name(name: str) -> str:
    for src, dst in _CHAR_SUBS:
        name = name.replace(src, dst)
    cleaned = _ILLEGAL_CHARS.sub("", name)
    cleaned = re.sub(r" {2,}", " ", cleaned).strip().rstrip(". ")
    return cleaned or "Unknown"


def _series_title_from_dir(series_dir: Path) -> str:
    """
    Derive a clean display title from a Jellyfin series directory name.
    Strips an optional trailing provider tag: "Title [anidb-1234]" -> "Title".
    """
    name = re.sub(r"\s+\[[^\]]+\]\s*$", "", series_dir.name).strip()
    return sanitise_name(name)


def find_jellyfin_movie_dir(anime_dir: Path, create_dirs: bool = True) -> Path:
    """
    Resolve the Jellyfin movie-library root used for anime films.

    Defaults to a sibling `anime_movies` directory next to the anime TV root
    unless explicitly overridden in config.toml.
    """
    if MOVIE_LIBRARY_DIR:
        movie_dir = Path(MOVIE_LIBRARY_DIR)
    else:
        movie_dir = anime_dir.parent / "anime_movies"
    if create_dirs:
        movie_dir.mkdir(parents=True, exist_ok=True)
    return movie_dir


def normalize_series_filenames_for_jellyfin(series_dir: Path, dry_run: bool = False) -> dict[str, int]:
    """
    Normalize video filenames to Jellyfin-friendly patterns:
      TV:      "<Series Title> - S01E01.ext"
      Specials "<Series Title> - S00E01.ext"

    External subtitle sidecars are normalized alongside videos to:
      "<Series Title> - S01E01.<lang>.srt" (and equivalent subtitle ext)

    Returns aggregate counters:
      {"renamed": N, "already": N, "skipped": N, "collisions": N,
       "sub_renamed": N, "sub_skipped": N}
    """
    series_title = _series_title_from_dir(series_dir)
    season_pat = re.compile(r"^Season (\d{2})$", re.IGNORECASE)
    out = {
        "renamed": 0,
        "already": 0,
        "skipped": 0,
        "collisions": 0,
        "sub_renamed": 0,
        "sub_skipped": 0,
    }

    def _normalize_subtitles_for_video(season_dir: Path, source_stem: str, target_stem: str) -> None:
        sidecars = _find_matching_subtitles(season_dir, source_stem)
        for sub_src in sidecars:
            lang = _guess_sub_lang_suffix(sub_src.stem)
            suffix = sub_src.suffix.lower()
            base_name = f"{target_stem}.{lang}"
            lower_stem = sub_src.stem.lower()
            if "forced" in lower_stem:
                base_name += ".forced"
            elif "sdh" in lower_stem or lower_stem.endswith(".cc"):
                base_name += ".sdh"

            sub_target = season_dir / f"{base_name}{suffix}"
            dedupe_n = 2
            while sub_target.exists() and sub_target != sub_src:
                sub_target = season_dir / f"{base_name}.{dedupe_n}{suffix}"
                dedupe_n += 1

            if sub_src.name == sub_target.name:
                continue

            if dry_run:
                out["sub_renamed"] += 1
                continue

            try:
                sub_src.rename(sub_target)
                out["sub_renamed"] += 1
                log.info("Subtitle normalize rename: %s -> %s", sub_src.name, sub_target.name)
            except OSError as exc:
                out["sub_skipped"] += 1
                log.warning("Subtitle normalize failed %s -> %s: %s", sub_src, sub_target, exc)

    for season_dir in sorted(p for p in series_dir.iterdir() if p.is_dir()):
        m = season_pat.match(season_dir.name)
        if not m:
            continue
        season_num = int(m.group(1))
        if season_num < 0 or season_num > 99:
            continue

        files = sorted(
            p for p in season_dir.iterdir()
            if p.is_file() and p.suffix.lower() in _VIDEO_EXT
        )
        if not files:
            continue

        used_eps: set[int] = set()
        for p in files:
            if (ep := extract_episode_number(p.name)) is not None:
                used_eps.add(ep)
            else:
                m_e = re.search(rf"S{season_num:02d}E(\d{{2,3}})", p.stem, re.IGNORECASE)
                if m_e:
                    used_eps.add(int(m_e.group(1)))
        next_special = 1
        while next_special in used_eps:
            next_special += 1

        for src in files:
            ep_num = extract_episode_number(src.name)
            if ep_num is None:
                # Parser fallback for scene/fansub naming styles that still include SxxEyy.
                m_se = re.search(rf"S{season_num:02d}E(\d{{1,3}})", src.stem, re.IGNORECASE)
                if m_se:
                    ep_num = int(m_se.group(1))
                else:
                    m_e = re.search(r"\bE(?:P)?(\d{1,3})\b", src.stem, re.IGNORECASE)
                    if m_e:
                        ep_num = int(m_e.group(1))
            if ep_num is None:
                if season_num == 0:
                    ep_num = next_special
                    next_special += 1
                    while next_special in used_eps:
                        next_special += 1
                else:
                    out["skipped"] += 1
                    continue

            used_eps.add(ep_num)
            target_name = f"{series_title} - S{season_num:02d}E{ep_num:02d}{src.suffix.lower()}"
            target = season_dir / target_name
            if src.name == target_name:
                out["already"] += 1
                _normalize_subtitles_for_video(season_dir, src.stem, src.stem)
                continue
            if target.exists() and target != src:
                out["collisions"] += 1
                log.warning("Filename normalize collision: %s -> %s", src.name, target.name)
                continue

            if dry_run:
                out["renamed"] += 1
                _normalize_subtitles_for_video(season_dir, src.stem, target.stem)
                continue
            try:
                src.rename(target)
                out["renamed"] += 1
                log.info("Renamed for Jellyfin: %s -> %s", src.name, target.name)
                _normalize_subtitles_for_video(season_dir, src.stem, target.stem)
            except OSError as exc:
                out["skipped"] += 1
                log.warning("Rename failed %s -> %s: %s", src, target, exc)

    return out

def build_save_path(anime_dir: Path, anime_name: str, season: int | None,
                    provider_id: str = "") -> str:
    """
    Construct a Jellyfin-compliant save path.

    Series folder format:  Anime Name [provider-XXXX]
    Season folder format:  Season 01  (zero-padded, space-separated - not dots)

    Using "Season.01" or "Series S" breaks Jellyfin's sequence-parsing logic.
    The optional *provider_id* parameter appends an explicit scraper tag such as
    "[anidb-1234]" or "[tvdbid-5678]", which forces Jellyfin's metadata plugin
    to bind to the exact database entry and bypass heuristic text-matching.
    """
    series_name = sanitise_name(anime_name)
    if provider_id:
        series_name = f"{series_name} [{sanitise_name(provider_id)}]"
    # Jellyfin requires "Season NN" - NOT "Season.NN" or raw season names
    season_folder = f"Season {season:02d}" if season else "Season 01"
    save_path = anime_dir / series_name / season_folder
    save_path.mkdir(parents=True, exist_ok=True)
    log.debug("build_save_path -> %s", save_path)
    return str(save_path)


def build_movie_target_path(
    movie_root: Path,
    movie_title: str,
    ext: str,
    year: int | None = None,
    provider_id: str = "",
    create_dirs: bool = True,
) -> Path:
    """
    Build a Jellyfin-friendly movie target path:
      <movie_root>/Title (Year)/Title (Year).ext
    """
    base_title = sanitise_name(movie_title)
    folder_name = f"{base_title} ({year})" if year else base_title
    if provider_id:
        folder_name = f"{folder_name} [{sanitise_name(provider_id)}]"
    target_dir = movie_root / folder_name
    if create_dirs:
        target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / f"{folder_name}{ext}"


# -- qBittorrent helpers -------------------------------------------------------

def connect_qbit() -> qbittorrentapi.Client:
    client = qbittorrentapi.Client(
        host=QBIT_HOST, port=QBIT_PORT,
        username=QBIT_USER, password=QBIT_PASS,
    )
    _saved_levels = _set_qbit_probe_logger_level(logging.ERROR)
    try:
        client.auth_log_in()
    except Exception as exc:
        _restore_logger_levels(_saved_levels)
        err = str(exc)
        err_l = err.lower()
        looks_like_vlc = (
            "/api/v2/auth/login" in err_l
            and ("404 not found" in err_l or "videolan" in err_l)
        )
        looks_offline = (
            "10061" in err_l
            or "actively refused" in err_l
            or "connection refused" in err_l
            or "failed to establish a new connection" in err_l
        )
        if looks_like_vlc:
            print()
            print(c(C.WARN, "  [qBittorrent API check hit the wrong app]"))
            print(c(C.DIM,  "     /\\"))
            print(c(C.DIM,  "    /  \\"))
            print(c(C.DIM,  "   /____\\"))
            print(c(C.DIM,  "   \\    /"))
            print(c(C.DIM,  "    \\  /   cone says: this looks like VLC answered that port"))
            print(c(C.DIM,  "     \\/"))
            print(c(C.DIM,  f"  Try closing VLC, then verify qBittorrent Web UI is on {QBIT_HOST}:{QBIT_PORT}."))
            log.warning("qBittorrent auth appears routed to VLC/other service on %s:%s", QBIT_HOST, QBIT_PORT)
            sys.exit(red("  [ERROR] qBittorrent Web API unavailable on configured host/port."))
        if looks_offline:
            print()
            print(c(C.WARN, "  [qBittorrent isn't answering right now]"))
            print(c(C.DIM,  "        .--------."))
            print(c(C.DIM,  "       /  ____    \\"))
            print(c(C.DIM,  "      /  / __ \\    \\"))
            print(c(C.DIM,  "      | | /  \\ |   |"))
            print(c(C.DIM,  "      | | \\__/ |   |"))
            print(c(C.DIM,  "      \\  \\____/   /"))
            print(c(C.DIM,  "       '--------'"))
            print(c(C.DIM,  f"  I couldn't reach qBittorrent on {QBIT_HOST}:{QBIT_PORT}."))
            print(c(C.DIM,  "  Please check that qBittorrent is open and its Web UI is enabled, then try again."))
            log.warning("qBittorrent appears offline/unreachable on %s:%s", QBIT_HOST, QBIT_PORT)
            sys.exit(red("  [ERROR] qBittorrent Web API did not respond."))
        log.exception("qBittorrent auth failed")
        sys.exit(red(f"  [ERROR] Could not connect to qBittorrent: {exc}"))
    else:
        _restore_logger_levels(_saved_levels)

    log.info("Connected to qBittorrent %s (Web API %s)",
             client.app.version, client.app.web_api_version)
    print(c(C.SUCCESS,
            f"  Connected to qBittorrent {client.app.version} "
            f"(Web API {client.app.web_api_version})"))
    return client


def add_torrent(client: qbittorrentapi.Client, item: dict,
                save_path: str = "", category: str = "") -> str | None:
    kwargs: dict = {}
    if save_path: kwargs["save_path"] = save_path
    if category:  kwargs["category"]  = category

    if item.get("magnet_url"):
        m = re.search(r"urn:btih:([a-fA-F0-9]{40})", item["magnet_url"])
        info_hash = m.group(1).lower() if m else None
        client.torrents_add(urls=item["magnet_url"], **kwargs)
        return info_hash

    if item.get("torrent_url"):
        resp = requests.get(item["torrent_url"], timeout=15)
        resp.raise_for_status()
        client.torrents_add(torrent_files=resp.content, **kwargs)
        return "__lookup__"

    return None


def resolve_hash_for_title(client: qbittorrentapi.Client, nyaa_title: str,
                            retries: int = 6, delay: float = 5.0) -> str | None:
    for _ in range(retries):
        time.sleep(delay)
        for torrent in client.torrents_info():
            t_name = torrent.name or ""
            if t_name and (t_name in nyaa_title or nyaa_title[:len(t_name)] in t_name):
                return torrent.hash.lower()
            if t_name and nyaa_title[:30].lower() in t_name.lower():
                return torrent.hash.lower()
    return None


class CleanupMonitor:
    """Background thread: removes torrents from qBittorrent once they finish."""

    def __init__(self, client: qbittorrentapi.Client, delete_files: bool = False):
        self.client       = client
        self.delete_files = delete_files
        self._watched: dict[str, str] = {}
        self._lock        = threading.Lock()
        self._stop_event  = threading.Event()
        self._thread      = threading.Thread(
            target=self._run, daemon=True, name="CleanupMonitor"
        )
        self.cleaned: list[str] = []

    def start(self)  -> None: self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join(timeout=MONITOR_INTERVAL + 5)

    def watch(self, info_hash: str, label: str) -> None:
        if info_hash and info_hash != "__lookup__":
            with self._lock:
                self._watched[info_hash.lower()] = label

    def pending_count(self) -> int:
        with self._lock:
            return len(self._watched)

    def watched_snapshot(self) -> dict[str, str]:
        with self._lock:
            return dict(self._watched)

    def unwatch(self, info_hash: str) -> None:
        with self._lock:
            self._watched.pop(info_hash.lower(), None)

    def _run(self) -> None:
        mode = "torrent + files" if self.delete_files else "torrent entry only"
        print(f"\n{c(C.AMBER, '[Monitor]')} Started - "
              f"polling every {c(C.VALUE2, str(MONITOR_INTERVAL))}s  "
              f"| on completion: remove {c(C.ORANGE, mode)}")
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=MONITOR_INTERVAL)
            self._check()
        self._check()
        remaining = self.pending_count()
        if remaining:
            print(f"\n{c(C.AMBER, '[Monitor]')} Stopped - "
                  f"{c(C.WARN, str(remaining))} torrent(s) still pending.")
        else:
            print(f"\n{c(C.AMBER, '[Monitor]')} "
                  f"{c(C.SUCCESS, 'All tracked torrents cleaned up.')}")

    def _check(self) -> None:
        with self._lock:
            if not self._watched:
                return
            snapshot = dict(self._watched)
        try:
            torrents = {t.hash.lower(): t for t in self.client.torrents_info()}
        except Exception as exc:
            print(f"\n{c(C.AMBER, '[Monitor]')} "
                  f"Could not reach qBittorrent: {c(C.WARN, str(exc))}")
            return
        to_remove = []
        for info_hash, label in snapshot.items():
            torrent = torrents.get(info_hash)
            tag     = c(C.AMBER, "[Monitor]")
            if torrent is None:
                print(f"\n{tag} {c(C.ORANGE, label)} - "
                      f"{c(C.WARN, 'not found (already removed?)')}")
                to_remove.append(info_hash)
                continue
            state    = torrent.state
            progress = torrent.progress * 100
            if state in DONE_STATES or (torrent.completion_on and torrent.completion_on > 0):
                action = ("removing torrent + files"
                          if self.delete_files else "removing torrent entry")
                print(f"\n{tag} {c(C.SUCCESS, '✓')} "
                      f"{c(C.ORANGE, label)} finished "
                      f"{c(C.INFO_DIM, f'({state})')} - {c(C.AMBER, action)}")
                try:
                    # delete_data=True permanently deletes files from disk
                    # (including residual .rar archives, samples, .nfo files).
                    # This is preferable to the legacy torrent_hashes API which
                    # can leave junk data behind.
                    self.client.torrents_delete(
                        delete_files=self.delete_files,
                        torrent_hashes=info_hash,
                    )
                    log.info("Removed torrent %s (delete_files=%s)", info_hash, self.delete_files)
                    self.cleaned.append(label)
                except Exception as exc:
                    print(f"\n{tag}   Failed to remove "
                          f"{c(C.ORANGE, label)}: {c(C.WARN, str(exc))}")
                    log.exception("Failed to delete torrent %s", info_hash)
                to_remove.append(info_hash)
            else:
                print(f"\n{tag} {c(C.INFO_DIM, '...')} "
                      f"{c(C.ORANGE, label)} - "
                      f"{c(C.CYAN_DIM, state)} "
                      f"{c(C.VALUE2, f'({progress:.1f}%)')}")
        with self._lock:
            for h in to_remove:
                self._watched.pop(h, None)


def wait_for_downloads_or_stuck(
    client: qbittorrentapi.Client,
    monitor: CleanupMonitor,
    no_progress_secs: int = STUCK_NO_PROGRESS_SECS,
    poll_secs: int = 5,
    last_seen: dict[str, tuple[str, float]] | None = None,
    stagnant_since: dict[str, float] | None = None,
) -> dict:
    """
    Wait until monitor reaches zero pending torrents OR detect a stuck torrent.

    A torrent is considered stuck when its (state, progress) pair does not
    change for no_progress_secs while in a download-related state.
    """
    last_seen = last_seen if last_seen is not None else {}
    stagnant_since = stagnant_since if stagnant_since is not None else {}

    while monitor.pending_count() > 0:
        time.sleep(poll_secs)
        watched = monitor.watched_snapshot()
        if not watched:
            break
        watched_hashes = {h.lower() for h in watched}
        for stale_hash in list(last_seen.keys()):
            if stale_hash not in watched_hashes:
                last_seen.pop(stale_hash, None)
                stagnant_since.pop(stale_hash, None)
        try:
            torrents = {t.hash.lower(): t for t in client.torrents_info()}
        except Exception:
            continue

        now = time.time()
        for info_hash, label in watched.items():
            t = torrents.get(info_hash)
            if t is None:
                continue
            state = (t.state or "").strip()
            prog = round(float(t.progress or 0.0), 4)
            key = (state, prog)

            if key != last_seen.get(info_hash):
                last_seen[info_hash] = key
                stagnant_since[info_hash] = now
                continue

            if state in DONE_STATES:
                continue
            if state not in STUCK_TRACK_STATES:
                continue

            idle_for = now - stagnant_since.get(info_hash, now)
            if idle_for >= no_progress_secs:
                return {
                    "stuck": True,
                    "hash": info_hash,
                    "label": label,
                    "state": state,
                    "progress": prog,
                    "idle_for": idle_for,
                }

    return {"stuck": False}


def collect_current_stuck_downloads(
    client: qbittorrentapi.Client,
    monitor: CleanupMonitor,
    no_progress_secs: int,
    last_seen: dict[str, tuple[str, float]],
    stagnant_since: dict[str, float],
) -> list[dict]:
    """
    Return every currently overdue stuck torrent without waiting for another poll.
    """
    watched = monitor.watched_snapshot()
    if not watched:
        return []
    try:
        torrents = {t.hash.lower(): t for t in client.torrents_info()}
    except Exception:
        return []

    now = time.time()
    out: list[dict] = []
    for info_hash, label in watched.items():
        t = torrents.get(info_hash)
        if t is None:
            continue
        state = (t.state or "").strip()
        prog = round(float(t.progress or 0.0), 4)
        if state in DONE_STATES or state not in STUCK_TRACK_STATES:
            continue
        if last_seen.get(info_hash) != (state, prog):
            continue
        idle_for = now - stagnant_since.get(info_hash, now)
        if idle_for >= no_progress_secs:
            out.append({
                "stuck": True,
                "hash": info_hash,
                "label": label,
                "state": state,
                "progress": prog,
                "idle_for": idle_for,
            })
    out.sort(key=lambda x: (-float(x.get("idle_for", 0)), str(x.get("label", ""))))
    return out


def _snapshot_files(root: Path) -> set[str]:
    if not root.is_dir():
        return set()
    out: set[str] = set()
    for p in root.rglob("*"):
        if p.is_file():
            try:
                out.add(str(p.relative_to(root)))
            except Exception:
                continue
    return out


def _remove_new_files_since(root: Path, before: set[str]) -> int:
    if not root.is_dir():
        return 0
    removed = 0
    for p in sorted(root.rglob("*"), reverse=True):
        if p.is_file():
            try:
                rel = str(p.relative_to(root))
            except Exception:
                continue
            if rel not in before:
                try:
                    p.unlink()
                    removed += 1
                except OSError:
                    pass
        elif p.is_dir():
            try:
                p.rmdir()
            except OSError:
                pass
    return removed


# -----------------------------------------------------------------------------
# PHASE 3 - HANDBRAKE HARD-SUB BURNING
# -----------------------------------------------------------------------------

# -- HandBrakeCLI discovery ---------------------------------------------------

def find_handbrake(override: str | None = None) -> str:
    if override:
        if not Path(override).exists():
            sys.exit(red(f"[ERROR] HandBrakeCLI not found at: {override}"))
        return override
    if sys.platform == "win32":
        for p in WINDOWS_HB_PATHS:
            if Path(p).exists():
                return p
    if sys.platform.startswith("linux"):
        for p in LINUX_HB_PATHS:
            if Path(p).exists():
                return p
        for exe in LINUX_HB_COMMANDS:
            found = shutil.which(exe)
            if found:
                return found
    found = shutil.which("HandBrakeCLI") or shutil.which("handbrakecli")
    if found:
        return found
    sys.exit(red(
        "[ERROR] HandBrakeCLI not found. "
        "Install HandBrakeCLI (or Flatpak HandBrake with exported CLI) or pass --hb /path/to/HandBrakeCLI"
    ))


# -- File helpers -------------------------------------------------------------

def _is_hs_output(p: Path) -> bool:
    return p.stem.endswith(HS_MARKER)


def collect_video_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for p in sorted(root.iterdir()):
        if (p.is_file()
                and p.suffix.lower() in VIDEO_EXTENSIONS
                and not _is_hs_output(p)):
            files.append(p)
    return files


def scan_episodes_on_disk(save_path: str) -> set[int]:
    """
    Scan the season directory and return the set of episode numbers
    that are already present on disk - counting BOTH raw source files
    AND .hs encoded outputs.

    This is the filesystem-as-source-of-truth check used before polling RSS.
    A file counts as "present" if:
      - It is a recognised video extension (.mkv, .mp4)
      - anitopy / regex can extract a valid integer episode number from its name
      - Its size is > 10 MB (guards against 0-byte placeholders or corrupt partials)

    Returns an empty set if the directory doesn't exist yet.
    """
    root = Path(save_path)
    if not root.is_dir():
        return set()

    present: set[int] = set()
    MIN_SIZE = 10 * 1024 * 1024   # 10 MB

    for p in root.iterdir():
        if not p.is_file():
            continue
        # p.suffix is always the real extension (.mkv, .mp4) regardless of
        # whether the stem contains .hs - never re-derive it from the stem
        suffix = p.suffix.lower()
        if suffix not in VIDEO_EXTENSIONS:
            continue
        try:
            if p.stat().st_size < MIN_SIZE:
                continue
        except OSError:
            continue
        # Pass the full original filename to the episode parser -
        # anitopy handles .hs.mkv filenames correctly
        ep = extract_episode_number(p.name)
        if ep is not None:
            present.add(ep)

    return present


def wait_for_files_stable(files: list[Path],
                           poll_interval: int = 8,
                           stable_rounds: int = 2) -> None:
    """
    Block until every file in *files* has a stable size across
    *stable_rounds* consecutive polls spaced *poll_interval* seconds apart.

    Guards against a race condition where qBittorrent reports a torrent as
    finished (and the monitor removes it) while the OS is still flushing write
    buffers.  HandBrake will fail with 'unrecognized file type' if it opens a
    file that is still being written.
    """
    print(f"\n{cyan('[INFO]')} Waiting for all files to be fully written to disk ...")

    prev_sizes: dict[Path, int] = {f: -1 for f in files}
    stable:     dict[Path, int] = {f: 0  for f in files}

    while True:
        for f in files:
            try:
                size = f.stat().st_size
            except OSError:
                size = -1
            if size == prev_sizes[f] and size > 0:
                stable[f] += 1
            else:
                stable[f]  = 0
            prev_sizes[f] = size

        still_waiting = [f for f in files if stable[f] < stable_rounds]
        if still_waiting:
            print(f"  {dim('Still stabilising:')} "
                  f"{yellow(str(len(still_waiting)))} file(s) - "
                  f"{dim('re-checking in')} {amber(str(poll_interval))}s")
            time.sleep(poll_interval)
        else:
            print(f"  {green('[OK]')} All {amber(str(len(files)))} file(s) "
                  f"confirmed stable on disk.")
            break


def output_path(p: Path) -> Path:
    return p.with_name(p.stem + HS_MARKER + p.suffix)


# -- Unicode script detection -------------------------------------------------

_SCRIPT_RANGES: list[tuple[int, int, str]] = [
    (0x3040, 0x309F, "Japanese"),
    (0x30A0, 0x30FF, "Japanese"),
    (0xAC00, 0xD7AF, "Korean"),
    (0x1100, 0x11FF, "Korean"),
    (0x4E00, 0x9FFF, "Chinese/Japanese/Korean"),
    (0x3400, 0x4DBF, "Chinese/Japanese/Korean"),
    (0x0600, 0x06FF, "Arabic"),
    (0x0750, 0x077F, "Arabic"),
    (0x0900, 0x097F, "Hindi"),
    (0x0980, 0x09FF, "Bengali"),
    (0x0A80, 0x0AFF, "Gujarati"),
    (0x0400, 0x04FF, "Russian/Cyrillic"),
    (0x0370, 0x03FF, "Greek"),
    (0x0E00, 0x0E7F, "Thai"),
    (0x0590, 0x05FF, "Hebrew"),
    (0x1200, 0x137F, "Ethiopic"),
    (0x0700, 0x074F, "Syriac"),
    (0x1000, 0x109F, "Burmese"),
    (0xFF65, 0xFF9F, "Japanese"),  # Halfwidth katakana (older encodes / some fansubs)
]


# iso639-2 -> human-readable language name, used to resolve ambiguous CJK blocks
_ISO639_MAP: dict[str, str] = {
    "jpn": "Japanese",
    "zho": "Chinese", "chi": "Chinese",
    "kor": "Korean",
    "eng": "English",
    "fre": "French", "fra": "French",
    "ger": "German", "deu": "German",
    "spa": "Spanish",
    "ita": "Italian",
    "por": "Portuguese",
    "rus": "Russian",
    "ara": "Arabic",
    "hin": "Hindi",
    "tha": "Thai",
    "heb": "Hebrew",
    "und": "Unknown",
}


def _safe_lang(text: str, iso639_hint: str | None = None) -> str:
    """
    Return a readable English language label for *text*.

    If *text* is pure ASCII/Latin it is returned unchanged.
    For non-Latin text the dominant Unicode script block is detected.
    The ambiguous CJK block (kanji shared between JP/ZH/KO) is resolved
    using *iso639_hint* (a raw iso639-2 code from the HandBrake scan line)
    when available, falling back to script detection otherwise.
    """
    # Fast path: honour explicit iso639 hint - resolves kanji ambiguity
    if iso639_hint:
        mapped = _ISO639_MAP.get(iso639_hint.lower().strip())
        if mapped:
            return mapped

    counts: dict[str, int] = {}
    has_non_ascii = False
    for ch in text:
        cp = ord(ch)
        if cp < 0x0080:
            continue
        has_non_ascii = True
        for lo, hi, name in _SCRIPT_RANGES:
            if lo <= cp <= hi:
                counts[name] = counts.get(name, 0) + 1
                break
    if not has_non_ascii:
        return text
    if not counts:
        return "Unknown (non-Latin)"
    best = max(counts, key=lambda k: counts[k])
    # If best is still ambiguous CJK and we have no hint, leave as-is so
    # the caller can see it needs a hint rather than silently mislabelling.
    return best


# -- HB scan helpers ----------------------------------------------------------

def run_scan(hb: str, video_file: Path) -> str:
    cmd = [hb, "--input", str(video_file), "--scan", "--title", "0"]
    result = subprocess.run(cmd, capture_output=True, text=True, errors="replace")
    return result.stdout + result.stderr


def extract_raw_section(scan_output: str, section_keyword: str) -> list[str]:
    lines = scan_output.splitlines()
    collecting = False
    raw: list[str] = []
    for line in lines:
        if re.search(re.escape(section_keyword) + r"\s*:", line, re.IGNORECASE):
            collecting = True
            continue
        if collecting:
            if re.match(r"\s{2}\+\s+\w", line) and not re.match(r"\s{4}\+", line):
                break
            if re.match(r"\+ title\s+\d+", line, re.IGNORECASE):
                break
            if re.match(r"\s{4}\+", line):
                raw.append(line.rstrip())
    return raw


# -- Subtitle parsing ---------------------------------------------------------

def _parse_sub_line(line: str) -> dict | None:
    m = re.match(r"\s+\+\s+(\d+),\s+(.+)$", line)
    if not m:
        return None
    idx  = int(m.group(1))
    rest = m.group(2)
    lang_m = re.match(r"(.+?)\s+\((.+)$", rest)
    if not lang_m:
        return {"index": idx, "language": _safe_lang(rest.strip(), None),
                "format": "unknown", "title": "", "flags": []}
    lang       = _safe_lang(lang_m.group(1).strip())
    after_lang = "(" + lang_m.group(2)
    fmt_m = re.match(r"\(([^)]+)\)(.*)", after_lang)
    fmt   = fmt_m.group(1).strip() if fmt_m else "unknown"
    tail  = fmt_m.group(2).strip() if fmt_m else after_lang
    # Extract iso639-2 hint before stripping, for _safe_lang disambiguation
    iso_sub_m = re.search(r"iso639-2:\s*([a-z]{3})", after_lang, re.IGNORECASE)
    iso_sub_hint = iso_sub_m.group(1).lower() if iso_sub_m else None
    tail  = re.sub(r"\(iso639-2:[^)]+\)\s*", "", tail).strip()
    sq_title = ""
    sq_m     = re.search(r"\[([^\]]+)\]", tail)
    if sq_m:
        sq_title = sq_m.group(1).strip()
        tail     = (tail[:sq_m.start()] + tail[sq_m.end():]).strip()
    flags: list[str] = []
    rp_title = ""
    for paren in re.findall(r"\(([^)]+)\)", tail):
        p = paren.strip()
        if re.fullmatch(r"Default", p, re.IGNORECASE):
            flags.append("Default")
        elif re.fullmatch(r"Forced", p, re.IGNORECASE):
            flags.append("Forced")
        elif p and not re.match(r"iso639", p, re.IGNORECASE):
            rp_title = p
    return {"index": idx, "language": lang, "format": fmt,
            "title": sq_title or rp_title, "flags": flags}


def parse_subtitle_tracks(scan_output: str) -> tuple[list[dict], list[str]]:
    raw_lines = extract_raw_section(scan_output, "subtitle tracks")
    tracks    = [t for line in raw_lines if (t := _parse_sub_line(line))]
    return tracks, raw_lines


# -- Audio parsing ------------------------------------------------------------

def _parse_audio_line(line: str) -> dict | None:
    """
    Parse one HandBrake audio track line into a dict.

    Handles all real-world formats:

      Format A - separate paren groups (BD / remux):
        + 1, Japanese (DTS-HD MA) (5.1 ch) (iso639-2: jpn)

      Format B - comma-separated single paren (SubsPlease / web-dl):
        + 1, Ã¦-Â¥Ã¦Å“Â¬Ã¨ÂªÅ¾ (AAC LC, 2.0 ch) (iso639-2: jpn)
        + 1, æ—¥æœ¬èªž (AAC LC, 2.0 ch iso639-2: jpn)   â† iso inline, no own paren

    The iso639-2 code is always extracted first (before any stripping) and
    passed to _safe_lang() so that non-Latin track names like "Ã¦-Â¥Ã¦Å“Â¬Ã¨ÂªÅ¾"
    resolve definitively to "Japanese" regardless of where the tag appears.
    """
    m = re.match(r"\s+\+\s+(\d+),\s+(.+)$", line)
    if not m:
        return None

    idx  = int(m.group(1))
    rest = m.group(2).strip()

    # -- Step 1: extract iso639-2 hint from anywhere on the line, before cleaning
    iso_m    = re.search(r"iso639-2:\s*([a-z]{3})", rest, re.IGNORECASE)
    iso_hint = iso_m.group(1).lower() if iso_m else None

    # -- Step 2: scrub the iso tag in ALL forms so it never leaks into fields
    #   Form A - own paren:   (iso639-2: jpn)
    #   Form B - in codec:    (AAC LC, 2.0 ch iso639-2: jpn)
    #   Form C - bare inline: AAC LC 2.0 ch iso639-2: jpn
    rest = re.sub(r"\s*\(iso639-2:[^)]+\)", "", rest)          # Form A
    rest = re.sub(r"\s*,?\s*iso639-2:\s*[a-z]{3}", "", rest,   # Form B/C
                  flags=re.IGNORECASE)
    rest = rest.strip()

    # -- Step 3: split language name from the paren block
    lang_m = re.match(r"(.+?)\s+\((.*)$", rest)
    if not lang_m:
        # No paren at all - just a language name
        return {
            "index":    idx,
            "language": _safe_lang(rest, iso_hint),
            "codec":    "unknown",
            "channels": "",
        }

    lang_raw   = lang_m.group(1).strip()
    lang       = _safe_lang(lang_raw, iso_hint)
    after_lang = lang_m.group(2)   # everything after the first opening "("

    # -- Step 4: collect remaining paren groups and classify codec / channels
    parens = re.findall(r"\(([^)]*)\)", "(" + after_lang)

    codec    = "unknown"
    channels = ""

    for paren in parens:
        paren = paren.strip()
        if not paren:
            continue

        if "," in paren:
            # Format B: "AAC LC, 2.0 ch"  /  "Opus, 2.0 ch"
            parts    = [p.strip() for p in paren.split(",", 1)]
            codec    = parts[0]
            channels = parts[1] if len(parts) > 1 else ""
        else:
            # Format A: first group = codec, second = channels
            if codec == "unknown":
                codec = paren
            elif not channels:
                channels = paren

    # Belt-and-suspenders: strip any residual iso tag that snuck through
    channels = re.sub(r"\s*iso639-2:\s*[a-z]{3}.*", "", channels,
                      flags=re.IGNORECASE).strip()
    codec    = re.sub(r"\s*iso639-2:\s*[a-z]{3}.*", "", codec,
                      flags=re.IGNORECASE).strip()

    return {
        "index":    idx,
        "language": lang,
        "codec":    codec or "unknown",
        "channels": channels,
    }


def parse_audio_tracks(scan_output: str) -> tuple[list[dict], list[str]]:
    raw_lines = extract_raw_section(scan_output, "audio tracks")
    tracks    = [t for line in raw_lines if (t := _parse_audio_line(line))]
    return tracks, raw_lines


# -- Language helpers ---------------------------------------------------------

def _is_english(lang: str) -> bool:
    return lang.lower() in ("english", "eng")


def _is_japanese(lang: str) -> bool:
    return lang.lower() in (
        "japanese", "jpn",
        "japanese/chinese/korean", "chinese/japanese/korean",
    )


# -- Subtitle scoring ---------------------------------------------------------

_DIALOGUE_BOOST = [
    r"\bdialogue\b", r"\bdialog\b", r"\bfull\b", r"\bmain\b",
    r"\bsubs?\b", r"\bsubtitles?\b", r"\btranslation\b",
]
_NONDIALOGUE_PENALTY = [
    r"\bsigns?\b", r"\bsongs?\b", r"\bop\b", r"\bed\b",
    r"\bop/ed\b", r"\boped\b", r"\bopening\b", r"\bending\b",
    r"\bkaraoke\b", r"\blyrics?\b", r"\binsert\b",
    r"\bsigns\s*(?:&|and)\s*songs\b", r"\bcommentary\b",
    r"\bsdh\b", r"\bdub\b",
]


def _dialogue_score(track: dict) -> int:
    text  = (track.get("title") or "").lower()
    score = 0
    for pat in _DIALOGUE_BOOST:
        if re.search(pat, text, re.IGNORECASE):
            score += 10
    for pat in _NONDIALOGUE_PENALTY:
        if re.search(pat, text, re.IGNORECASE):
            score -= 20
    if "Forced"  in track.get("flags", []): score -= 15
    if "Default" in track.get("flags", []): score += 5
    if not text:                             score += 1
    return score


# -- Auto-selection ------------------------------------------------------------

def auto_select_subtitle(tracks: list[dict]) -> tuple[dict | None, str]:
    candidates = [t for t in tracks if _is_english(t["language"])]
    if not candidates:
        all_langs = sorted({t["language"] for t in tracks})
        msg = f"no English subtitle tracks (available: {all_langs})" if all_langs \
              else "no subtitle tracks found"
        return None, msg
    if len(candidates) == 1:
        t = candidates[0]
        return t, f"only one English track - track {t['index']}"
    scored  = sorted(candidates, key=_dialogue_score, reverse=True)
    best    = scored[0]
    rejects = ["track {} '{}' (score {})".format(
                   t['index'], t.get('title') or 'untitled', _dialogue_score(t))
               for t in scored[1:]]
    reason  = (f"score {_dialogue_score(best)} - highest among "
               f"{len(candidates)} English tracks; "
               f"rejected: {', '.join(rejects)}")
    if _dialogue_score(best) < 0:
        reason += f"  {yellow('[WARN] score is negative - may still be Signs/Songs')}"
    return best, reason


def auto_select_audio(tracks: list[dict]) -> tuple[dict | None, str]:
    japanese = [t for t in tracks if _is_japanese(t["language"])]
    if japanese:
        t  = japanese[0]
        ch = f" {t['channels']}" if t["channels"] else ""
        return t, f"first Japanese track - {t['codec']}{ch}"
    if tracks:
        t  = tracks[0]
        ch = f" {t['channels']}" if t["channels"] else ""
        return t, (f"no Japanese audio found; "
                   f"using first available - {t['language']} {t['codec']}{ch}")
    return None, "no audio tracks in scan"


# -- Display helpers -----------------------------------------------------------

def _fmt_sub(t: dict) -> str:
    title_s = cyan(f'  "{t["title"]}"') if t.get("title") else ""
    flags_s = yellow(f" [{', '.join(t['flags'])}]") if t.get("flags") else ""
    return f"{amber(t['language'])} {dim('(' + t['format'] + ')')}{title_s}{flags_s}"


def _fmt_aud(t: dict) -> str:
    ch = f" {t['channels']}" if t.get("channels") else ""
    return f"{amber(t['language'])} {dim('(' + t['codec'] + ch + ')')}"


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _show_subtitle_tracks(tracks: list[dict], raw_lines: list[str],
                           chosen: dict | None) -> None:
    if raw_lines:
        print(f"\n  {dim('Raw HandBrake subtitle section:')}")
        for rl in raw_lines:
            print(dim(f"    {rl.strip()}"))
    print(f"\n{b_amber('=== Subtitle Tracks ===')}")
    if not tracks:
        print(f"  {dim('(none parsed - see raw lines above)')}")
        return
    for t in tracks:
        marker  = green("  > AUTO") if (chosen and t["index"] == chosen["index"]) else "       "
        score_s = dim(f"  [{_dialogue_score(t):+d}]")
        idx_s   = cyan(f"{t['index']:2d}.")
        print(f"  {idx_s} {_fmt_sub(t)}{score_s}{marker}")


def _show_audio_tracks(tracks: list[dict], chosen: dict | None) -> None:
    print(f"\n{b_amber('=== Audio Tracks ===')}")
    if not tracks:
        print(f"  {yellow('[WARN]')} No audio tracks found in scan.")
        return
    for t in tracks:
        marker = green("  > AUTO") if (chosen and t["index"] == chosen["index"]) else "       "
        idx_s  = cyan(f"{t['index']:2d}.")
        print(f"  {idx_s} {_fmt_aud(t)}{marker}")


# -- Scan + auto-select (fully automatic - no override prompts) ---------------

def scan_and_autoselect(hb: str, sample: Path) -> tuple[dict | None, dict | None]:
    """
    Scan *sample*, auto-select JP audio + EN dialogue subs.
    Displays all tracks and selection reasoning but does NOT prompt for overrides.
    Prints a warning if audio tracks are missing (file will be skipped downstream).
    """
    print(f"\n{cyan('[SCAN]')} {white(sample.name)}")
    scan_output = run_scan(hb, sample)

    sub_tracks, sub_raw = parse_subtitle_tracks(scan_output)
    aud_tracks, _       = parse_audio_tracks(scan_output)

    auto_sub, sub_reason = auto_select_subtitle(sub_tracks)
    auto_aud, aud_reason = auto_select_audio(aud_tracks)

    _show_subtitle_tracks(sub_tracks, sub_raw, auto_sub)
    _show_audio_tracks(aud_tracks, auto_aud)

    print(f"\n{SEP_THIN}")
    print(b_amber("  AUTO-SELECTION"))
    print(SEP_THIN)
    if auto_sub:
        print(f"  {green('OK Subtitle')} : track {cyan(str(auto_sub['index']))}  {_fmt_sub(auto_sub)}")
        print(f"    {dim('Reason: ' + sub_reason)}")
    else:
        print(f"  {yellow('[WARN] Subtitle')} : {yellow('none')}  {dim(sub_reason)}")

    if auto_aud:
        print(f"  {green('OK Audio')}    : track {cyan(str(auto_aud['index']))}  {_fmt_aud(auto_aud)}")
        print(f"    {dim('Reason: ' + aud_reason)}")
    else:
        print(f"  {yellow('[WARN] Audio')}    : {yellow('none')}  {dim(aud_reason)}")
    print(SEP_THIN)

    return auto_sub, auto_aud


# -- HandBrakeCLI command + encode --------------------------------------------

def build_command(hb: str, input_file: Path, output_file: Path,
                  preset: str, subtitle_track: dict | None,
                  audio_track: dict | None, allow_crop: bool) -> list[str]:
    cmd = [
        hb, "--input", str(input_file), "--output", str(output_file),
        "--preset", preset, "--display-width", "0", "--keep-display-aspect",
    ]
    if not allow_crop:
        cmd += ["--crop-mode", "none"]
    if audio_track is not None:
        cmd += ["--audio", str(audio_track["index"])]
    if subtitle_track is not None:
        cmd += ["--subtitle", str(subtitle_track["index"]),
                "--subtitle-burned", str(subtitle_track["index"])]
    return cmd


def output_path_ffmpeg(video: Path) -> Path:
    """Output path for an FFmpeg-encoded file (mirrors the HandBrake naming convention)."""
    return video.with_suffix("").with_suffix(f"{HS_MARKER}.mkv")


def run_encode(cmd: list[str]) -> int:
    """
    Run a HandBrakeCLI encode command, streaming its output line-by-line.

    HandBrake writes progress to stderr.  We merge stdout+stderr via
    subprocess.STDOUT so a single read loop captures everything.
    Returns the process exit code (0 = success).
    """
    log.info("HandBrake encode starting: %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            # Progress lines look like "Encoding: task 1 of 1, 12.34 %"
            if re.search(r"Encoding:|Muxing:|task \d+ of \d+", line):
                print(amber(f"  {line}"), flush=True)
            elif re.search(r"\[ERROR\]|\[WARNING\]", line, re.IGNORECASE):
                print(yellow(f"  {line}"), flush=True)
            elif re.search(
                r"x264 \[info\]:|x265 \[info\]:|kb/s:|mux: track|"
                r"Encode done|HandBrake has exited|libhb: work|"
                r"vfr:|sync:|aac-decoder|h264-decoder|hevc-decoder|"
                r"Finished work at",
                line
            ):
                # Encode-summary stats - cycle rainbow so each episode's
                # completion block is a different vivid colour at a glance
                print(rainbow_next(f"  {line}"), flush=True)
            else:
                print(dim(f"  {line}"), flush=True)
        return proc.wait()
    except Exception as exc:
        print(red(f"  [ERROR] Failed to launch HandBrakeCLI: {exc}"))
        log.exception("run_encode launch failed")
        return 1


def find_ffmpeg() -> str:
    """Locate the ffmpeg binary on PATH. Raises FileNotFoundError if absent."""
    ff = shutil.which("ffmpeg")
    if ff:
        return ff
    raise FileNotFoundError(
        "ffmpeg not found on PATH.  Install FFmpeg (compiled with NVENC support) "
        "or switch to --engine handbrake."
    )


def build_ffmpeg_nvenc_command(
    ffmpeg:         str,
    input_file:     Path,
    output_file:    Path,
    subtitle_index: int | None = None,
    audio_index:    int | None = None,
    output_1080p:   Path | None = None,
    output_720p:    Path | None = None,
) -> list[str]:
    """
    Build an FFmpeg NVENC hybrid filterchain command for hardsub burning.

    Architecture (from the optimisation document):
      * h264_cuvid  - decodes directly to system RAM (avoids PCIe stall on decode)
      * subtitles   - CPU renders .ass subtitle exactly once (software, unavoidable)
      * hwupload_cuda - uploads the rendered frame to VRAM in one DMA transfer
      * split=N     - the GPU splits the frame for each output resolution
      * scale_npp   - hardware NPP scaler handles each output independently
      * h264_nvenc  - hardware encoder on the NVENC silicon

    If both output_1080p and output_720p are provided the subtitle is rendered
    exactly once and the GPU handles both scale operations simultaneously - a
    significant efficiency gain over running two separate encode passes.

    If subtitle_index is None the subtitles filter is omitted (audio-only fix).
    """
    # -- Input / decode --------------------------------------------------------
    cmd = [
        ffmpeg, "-y",
        "-vsync", "0",
        "-hwaccel", "cuvid",          # use CUVID for decode
        "-c:v", "h264_cuvid",         # decode stays in system RAM (not VRAM)
        "-i", str(input_file),
    ]

    # -- Audio map -------------------------------------------------------------
    audio_map = ["-map", f"0:a:{audio_index - 1}"] if audio_index else ["-map", "0:a:0"]

    # -- Build filtergraph -----------------------------------------------------
    dual_output = output_1080p is not None and output_720p is not None

    if subtitle_index is not None:
        # Subtitle path must be escaped for the FFmpeg filter syntax
        sub_path = str(input_file).replace("\\", "/").replace(":", "\\:")
        sub_filter = f"subtitles='{sub_path}':si={subtitle_index - 1}"
    else:
        sub_filter = None

    if dual_output:
        # Apply subtitle once on CPU, upload to VRAM, split for two resolutions
        if sub_filter:
            fg = (
                f"[0:v]{sub_filter},hwupload_cuda,split=2[c0][c1];"
                f"[c0]scale_npp=1920:1080[o1080];"
                f"[c1]scale_npp=1280:720[o720]"
            )
        else:
            fg = (
                "[0:v]hwupload_cuda,split=2[c0][c1];"
                "[c0]scale_npp=1920:1080[o1080];"
                "[c1]scale_npp=1280:720[o720]"
            )
        cmd += ["-filter_complex", fg]
        cmd += [
            "-map", "[o1080]", *audio_map,
            "-c:v", "h264_nvenc", "-preset", "p4", "-cq", "20",
            "-c:a", "aac", "-b:a", "192k",
            str(output_1080p),
            "-map", "[o720]", *audio_map,
            "-c:v", "h264_nvenc", "-preset", "p4", "-cq", "23",
            "-c:a", "aac", "-b:a", "128k",
            str(output_720p),
        ]
    else:
        # Single-output path
        if sub_filter:
            fg = f"[0:v]{sub_filter},hwupload_cuda,scale_npp=1920:1080[out]"
        else:
            fg = "[0:v]hwupload_cuda,scale_npp=1920:1080[out]"
        cmd += ["-filter_complex", fg]
        cmd += [
            "-map", "[out]", *audio_map,
            "-c:v", "h264_nvenc", "-preset", "p4", "-cq", "20",
            "-c:a", "aac", "-b:a", "192k",
            str(output_file),
        ]

    return cmd


def run_ffmpeg_nvenc_hardsub(
    video:          Path,
    subtitle_index: int | None,
    audio_index:    int | None,
    dry_run:        bool,
    force:          bool,
    dual_output:    bool = False,
) -> tuple[list[Path], list[Path]]:
    """
    Hardsub a single *video* using the FFmpeg NVENC hybrid filterchain.

    Returns (successes, failures) - lists of Path objects.
    """
    try:
        ffmpeg = find_ffmpeg()
    except FileNotFoundError as exc:
        print(red(f"  [ERROR] {exc}"))
        log.error("FFmpeg not found: %s", exc)
        return [], [video]

    out_single  = output_path_ffmpeg(video)
    out_1080p   = video.with_suffix("").with_suffix(".1080p.hs.mkv") if dual_output else None
    out_720p    = video.with_suffix("").with_suffix(".720p.hs.mkv")  if dual_output else None
    output_file = out_1080p if dual_output else out_single

    # Skip if output exists and not forced
    primary_out = output_file or out_single
    if primary_out.exists() and not force:
        print(yellow(f"  [SKIP] {primary_out.name} already exists. Use --force to overwrite."))
        return [], []

    cmd = build_ffmpeg_nvenc_command(
        ffmpeg, video, out_single,
        subtitle_index=subtitle_index,
        audio_index=audio_index,
        output_1080p=out_1080p,
        output_720p=out_720p,
    )

    print(f"  {dim('CMD:')} {cyan(' '.join(cmd))}")
    if dry_run:
        print(magenta("  [DRY-RUN] Skipping FFmpeg execution."))
        log.debug("DRY-RUN ffmpeg cmd: %s", " ".join(cmd))
        return [], []

    log.info("Starting FFmpeg NVENC encode: %s", video.name)
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, errors="replace",
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            if re.search(r"frame=|fps=|speed=|time=|bitrate=", line):
                print(amber(line), flush=True)
            else:
                print(dim(line), flush=True)
        rc = proc.wait()
    except Exception as exc:
        print(red(f"  [ERROR] Failed to launch FFmpeg: {exc}"))
        log.exception("FFmpeg launch failed for %s", video)
        return [], [video]

    outputs = [p for p in [out_1080p, out_720p, out_single] if p is not None]
    successes: list[Path] = []
    failures:  list[Path] = []

    if rc != 0:
        print(red(f"  [FAIL] FFmpeg exited with code {rc}."))
        log.error("FFmpeg rc=%d for %s", rc, video)
        for p in outputs:
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass
        return [], [video]

    for p in outputs:
        if p.exists() and p.stat().st_size >= 1_048_576:
            size_mb = p.stat().st_size // 1_048_576
            print(green(f"  [OK] {p.name}  ({size_mb} MB)"))
            successes.append(video)
            _maybe_print_milestone(len(successes))
            log.info("FFmpeg encode OK: %s (%d MB)", p.name, size_mb)
        else:
            sz = p.stat().st_size if p.exists() else 0
            print(red(f"  [FAIL] {p.name} suspiciously small ({sz} bytes)."))
            log.error("FFmpeg output suspect: %s (%d bytes)", p.name, sz)
            failures.append(video)
            if p.exists():
                try:
                    p.unlink()
                except OSError:
                    pass

    return successes, failures


def process_files_ffmpeg(
    files:          list[Path],
    chosen_sub:     dict | None,
    chosen_aud:     dict | None,
    force:          bool,
    dry_run:        bool,
    dual_output:    bool,
    successes:      list[Path],
    failures:       list[Path],
    skipped:        list[Path],
) -> None:
    """Batch-process files through the FFmpeg NVENC hardsub pipeline."""
    sub_idx = chosen_sub["index"] if chosen_sub else None
    aud_idx = chosen_aud["index"] if chosen_aud else None
    for video in files:
        print(f"\n{SEP_THIN}")
        print(f"{amber('[FILE]')} {white(str(video))}")
        s, f = run_ffmpeg_nvenc_hardsub(
            video, sub_idx, aud_idx,
            dry_run=dry_run, force=force, dual_output=dual_output,
        )
        successes.extend(s)
        _maybe_print_milestone(len(successes))
        failures.extend(f)



    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, errors="replace",
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            text = line.rstrip()
            if not text:
                continue
            if "%" in text or re.search(r"Encoding|fps|ETA", text, re.IGNORECASE):
                print(amber(text), flush=True)
            else:
                print(dim(text), flush=True)
        proc.wait()
        return proc.returncode
    except Exception as exc:
        print(red(f"[ERROR] Failed to launch HandBrakeCLI: {exc}"))
        return 1


# -- Per-file fallback --------------------------------------------------------

def _find_by_language(tracks: list[dict], language: str) -> dict | None:
    for t in tracks:
        if t["language"].lower() == language.lower():
            return t
    return None


# -- Batch processing ---------------------------------------------------------

def process_files(files: list[Path], hb: str,
                  chosen_sub: dict | None, chosen_aud: dict | None,
                  preset: str, force: bool, dry_run: bool,
                  allow_crop: bool, fallback_by_language: bool,
                  successes: list[Path], failures: list[Path],
                  skipped: list[Path]) -> None:
    for video in files:
        out = output_path(video)
        print(f"\n{SEP_THIN}")
        print(f"{amber('[FILE]')} {white(str(video))}")
        print(f"  {dim('→')} {cyan(str(out))}")

        if out.exists() and not force:
            print(yellow("  [SKIP] Output already exists. Use --force to overwrite."))
            skipped.append(video)
            continue

        sub_for_file = chosen_sub
        aud_for_file = chosen_aud

        if fallback_by_language and (chosen_sub or chosen_aud):
            file_scan      = run_scan(hb, video)
            file_subs, _sr = parse_subtitle_tracks(file_scan)
            file_auds, _ar = parse_audio_tracks(file_scan)

            if chosen_sub:
                if chosen_sub["index"] not in [t["index"] for t in file_subs]:
                    fb = (_find_by_language(file_subs, chosen_sub["language"])
                          or auto_select_subtitle(file_subs)[0])
                    if fb:
                        fb_title = fb.get('title', '')
                        print(yellow(
                            f"  [FALLBACK] Sub track {chosen_sub['index']} absent; "
                            f"using track {fb['index']} "
                            f"({fb['language']} '{fb_title}')"
                        ))
                        sub_for_file = fb
                    else:
                        print(yellow(
                            f"  [WARN] Sub track {chosen_sub['index']} absent, "
                            f"no fallback found. Skipping file."
                        ))
                        skipped.append(video)
                        continue

            if chosen_aud:
                if chosen_aud["index"] not in [t["index"] for t in file_auds]:
                    fb = (_find_by_language(file_auds, chosen_aud["language"])
                          or auto_select_audio(file_auds)[0])
                    if fb:
                        print(yellow(
                            f"  [FALLBACK] Audio track {chosen_aud['index']} absent; "
                            f"using track {fb['index']} ({fb['language']})"
                        ))
                        aud_for_file = fb
                    else:
                        print(yellow(
                            f"  [WARN] Audio track {chosen_aud['index']} absent; "
                            f"letting HandBrake decide."
                        ))
                        aud_for_file = None

        cmd = build_command(hb, video, out, preset,
                            sub_for_file, aud_for_file, allow_crop)
        print(f"  {dim('CMD:')} {cyan(' '.join(cmd))}")

        if dry_run:
            print(magenta("  [DRY-RUN] Skipping execution."))
            continue

        rc = run_encode(cmd)
        if rc == 0:
            # Sanity-check: output must exist and be larger than 1 MB
            out_size = out.stat().st_size if out.exists() else 0
            if out_size < 1_048_576:
                print(red(f"  [FAIL] Output is suspiciously small ({out_size} bytes) - "
                          f"treating as failed encode."))
                failures.append(video)
                if out.exists():
                    try:
                        out.unlink()
                        print(yellow(f"  [INFO] Removed suspect output: {out.name}"))
                    except OSError:
                        pass
            else:
                print(green(f"  [OK] Encoded successfully.  "
                            f"({out_size // 1_048_576} MB)"))
                successes.append(video)
                _maybe_print_milestone(len(successes))
        else:
            print(red(f"  [FAIL] HandBrakeCLI exited with code {rc}."))
            failures.append(video)
            if out.exists():
                try:
                    out.unlink()
                    print(yellow(f"  [INFO] Removed partial output: {out.name}"))
                except OSError:
                    pass


# -- Cleanup - delete originals ------------------------------------------------

def delete_originals(successes: list[Path],
                     dry_run: bool) -> tuple[list[Path], list[Path]]:
    deleted:       list[Path] = []
    failed_to_del: list[Path] = []
    print(f"\n{SEP_THICK}")
    print(b_white("CLEANUP - Deleting originals"))
    print(SEP_THICK)
    for src_file in successes:
        out    = output_path(src_file)
        prefix = f"  {dim(src_file.name)}"
        if src_file == out:
            print(f"{prefix}  {red('[SKIP]')} source and output are the same path")
            failed_to_del.append(src_file)
            continue
        if not out.exists():
            print(f"{prefix}  {red('[SKIP]')} output {out.name} not found")
            failed_to_del.append(src_file)
            continue
        out_size = out.stat().st_size
        if out_size < 1_048_576:
            print(f"{prefix}  {red('[SKIP]')} output is only {out_size:,} bytes - "
                  f"suspiciously small, NOT deleting")
            failed_to_del.append(src_file)
            continue
        if dry_run:
            print(f"{prefix}  {magenta('[DRY-RUN]')} would delete  "
                  f"{dim('(' + _fmt_size(src_file.stat().st_size) + ')')}")
            deleted.append(src_file)
            continue
        try:
            src_size = src_file.stat().st_size
            src_file.unlink()
            print(f"{prefix}  {green('[DELETED]')}  "
                  f"{dim('(' + _fmt_size(src_size) + ' freed)')}")
            deleted.append(src_file)
        except OSError as exc:
            print(f"{prefix}  {red('[ERROR]')} could not delete: {exc}")
            failed_to_del.append(src_file)
    return deleted, failed_to_del


# -----------------------------------------------------------------------------
# ARGUMENT PARSER
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Unified lito1 pipeline: Nyaa -> qBittorrent -> Jellyfin.\n"
            "Auto-discovers Jellyfin Anime directory, downloads, monitors, then rescans."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python lito1.py
  python lito1.py --pick-group --season 2
  python lito1.py --no-confirm --dry-run
  python lito1.py --cleanup-files
""",
    )
    # -- Fetcher flags -------------------------------------------------------
    p.add_argument("--pick-group",    action="store_true",
                   help="Prompt to choose sub group (default: auto-rank)")
    p.add_argument("--season",        type=int, default=None,
                   help="Season number (skips season prompt)")
    p.add_argument("--episodes",      type=str, default=None, metavar="N-M",
                   help="Episode range e.g. 1-13 (default: all)")
    p.add_argument("--pages",         type=int, default=DEFAULT_PAGES,
                   help=f"Max nyaa search pages (default: {DEFAULT_PAGES})")
    p.add_argument("--no-confirm",    action="store_true",
                   help="Skip the 'Add to qBittorrent?' confirmation")
    p.add_argument("--cleanup-files", action="store_true",
                   help=(
                       "WARNING: tells qBittorrent to DELETE downloaded files from disk "
                       "as soon as the torrent finishes. "
                       "Use this only if your qBittorrent path is temporary and you do "
                       "not want completed media files to remain on disk."
                   ))

    # -- Pipeline flags ---------------------------------------------------------
    p.add_argument("--dry-run",      action="store_true",
                   help="Preview download/routing actions without changing files")
    p.add_argument("--setup-jellyfin", action="store_true",
                   help="Run one-time Jellyfin URL + API key setup, then exit")
    p.add_argument("--watch",          action="store_true",
                   help="Poll RSS for all active watch list entries and download new episodes")
    p.add_argument("--watch-status",   action="store_true",
                   help="Print the current watch list and exit")
    p.add_argument("--reconcile-library", action="store_true",
                   help="Audit existing media and move obvious movies/specials into the correct Jellyfin locations")
    p.add_argument("--fun", dest="fun", action="store_true", default=True,
                   help="Enable fun visual output/easter eggs (default: on; cosmetic only)")
    p.add_argument("--no-fun", dest="fun", action="store_false",
                   help="Disable fun visual output/easter eggs")
    p.add_argument("--theme", choices=["clean", "retro", "neon", "minimal"],
                   default="clean",
                   help="Visual output theme (cosmetic only)")

    return p.parse_args()


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Per-season worker - called once per season in the main loop
# -----------------------------------------------------------------------------

def run_one_season(
    season:           int,
    raw:              list[dict],
    ranked:           list[dict],
    anime_name:       str,
    args:             argparse.Namespace,
    jellyfin_anime_dir: Path,
    client:           "qbittorrentapi.Client",
    delete_files:     bool,
    auto_confirm:     bool = False,
    force_no_batch:   bool = False,
    excluded_groups:  set[str] | None = None,
    allowed_seasons:  set[int] | None = None,
    season_info:      dict[int, dict] | None = None,
) -> bool:
    """
    Execute the full pipeline for a single season:
      group selection -> episode filter -> qBit add -> monitor -> routing.

    Returns True if the season completed without download/routing failures, False otherwise.
    """
    season_label = f"Season {season}"
    excluded_groups = excluded_groups or set()
    season_info = season_info or fetch_anilist_season_info(anime_name)
    related_movies = fetch_anilist_related_movies(anime_name)
    movie_root = find_jellyfin_movie_dir(jellyfin_anime_dir, create_dirs=not args.dry_run)
    routed_batch: dict[str, list[Path]] | None = None

    print(f"\n{SEP_HASH}")
    print(magenta(f"  >  {season_label}"))
    print(SEP_HASH)

    # -- Ensure season has data in the pool ------------------------------------
    season_pool_for_rank = [r for r in raw
                            if get_result_season_number(r) == season]

    if not season_pool_for_rank:
        print(f"  {c(C.WARN, f'Season {season} not in current pool - running targeted search ...')}")
        extra = search_all_pages(build_season_query(anime_name, season, season_info), args.pages)
        annotate_results_with_anilist_seasons(extra, anime_name, season_info)
        seen  = {r["detail_url"] for r in raw}
        raw  += [r for r in extra if r["detail_url"] not in seen]
        season_pool_for_rank = [r for r in raw
                                if get_result_season_number(r) == season]
        if not season_pool_for_rank:
            print(c(C.WARN, f"  No results found for {season_label}. Skipping."))
            return True   # non-fatal - just skip

    # -- Re-rank scoped to this season -----------------------------------------
    season_ranked = rank_sub_groups(season_pool_for_rank)
    if not season_ranked:
        print(c(C.WARN, f"  No rankable sub groups for {season_label}. Skipping."))
        return True

    top_n = min(5, len(season_ranked))
    divider(f"Top {top_n} Sub Groups ({season_label})")

    # Check whether ANY preferred group appears in the results at all
    any_preferred_present = any(e["group"] in PREFERRED_GROUPS for e in season_ranked)
    if not any_preferred_present:
        print(f"  {c(C.YELLOW, '[FALLBACK]')} No preferred groups found for this series - "
              f"scoring all available groups by composite metric.")

    batch_only_priority_groups = {
        e["group"] for e in season_ranked
        if e.get("episode_count", 0) == 0
        and (e.get("total_seeders", 0) > 0 or e.get("total_leechers", 0) > 0)
        and _title_resolution_rank(next(
            (r["title"] for r in season_pool_for_rank if extract_sub_group(r["title"]) == e["group"]),
            ""
        )) >= 720
    }
    batch_priority_summaries: list[str] = []
    batch_priority_entries = [
        e for e in season_ranked if e["group"] in batch_only_priority_groups
    ]
    batch_priority_entries.sort(
        key=lambda e: (
            -e.get("total_seeders", 0),
            -e.get("total_leechers", 0),
            -_title_resolution_rank(next(
                (r["title"] for r in season_pool_for_rank if extract_sub_group(r["title"]) == e["group"]),
                "",
            )),
            e["group"],
        )
    )
    for entry in batch_priority_entries:
        grp = entry["group"]
        sample_title = next(
            (r["title"] for r in season_pool_for_rank if extract_sub_group(r["title"]) == grp),
            "",
        )
        res = _title_resolution_rank(sample_title)
        seeders = entry.get("total_seeders", 0)
        batch_priority_summaries.append(f"[{grp}] {res}p {seeders}S")

    for i, entry in enumerate(season_ranked[:top_n], 1):
        is_pref     = entry["group"] in PREFERRED_GROUPS
        badge       = f" {c(C.BADGE, '*')}" if is_pref else ""
        grp         = entry["group"]
        cov_pct     = f"{entry['coverage_ratio']*100:.0f}%"
        completed_s = c(C.AMBER_B, str(entry['total_completed']))
        leechers_s  = c(C.VALUE2,  str(entry['total_leechers']))
        print(f"  {c(C.ORANGE_DIM, f'{i}.')} "
              f"{c(C.VALUE, f'[{grp}]')}"
              f"  {c(C.VALUE2, str(entry['total_seeders']))} {c(C.AMBER, 'seeds')}"
              f"  {c(C.AMBER_B, '|')}"
              f"  {leechers_s} {c(C.AMBER, 'leeches')}"
              f"  {c(C.AMBER_B, '|')}"
              f"  {completed_s} {c(C.AMBER, 'DLs')}"
              f"  {c(C.AMBER_B, '|')}"
              f"  {c(C.VALUE2, str(entry['episode_count']))} {c(C.AMBER, f'ep(s) [{cov_pct}]')}"
              f"  {c(C.AMBER_B, '|')}"
              f"  {c(C.VALUE2, str(entry['avg_seeders']))} {c(C.AMBER, 'avg/ep')}{badge}")

    if batch_priority_summaries:
        print(f"  {c(C.INFO_DIM, '[Batch-priority]')} "
              f"{c(C.DIM, 'Strong batch-only source(s) detected and will be inspected first:')} "
              f"{c(C.VALUE, '; '.join(batch_priority_summaries[:4]))}")

    if args.pick_group and not excluded_groups:
        divider("Select Sub Group")
        raw_choice = prompt(f"Group number (1-{top_n}) or type a name", "1")
        if raw_choice.isdigit() and 1 <= int(raw_choice) <= top_n:
            chosen_group = season_ranked[int(raw_choice) - 1]["group"]
        else:
            chosen_group = raw_choice.strip().lower()
        is_preferred = chosen_group in PREFERRED_GROUPS
        print(f"  {c(C.LABEL, 'Using:')} {c(C.VALUE, f'[{chosen_group}]')}")
    else:
        if excluded_groups:
            chosen_group, is_preferred, pick_reason = pick_retry_group(season_ranked, excluded_groups)
            if not chosen_group:
                print(c(C.WARN, f"  No alternative sub groups available for {season_label}. Skipping."))
                return True
        else:
            chosen_group, is_preferred, pick_reason = pick_best_group(season_ranked)
        if is_preferred:
            print(f"  {c(C.LABEL, 'Auto-selected:')} {c(C.VALUE, f'[{chosen_group}]')}  "
                  f"{c(C.SUCCESS, '* preferred')}  {c(C.DIM, pick_reason)}")
        else:
            print(f"  {c(C.LABEL, 'Auto-selected:')} {c(C.VALUE, f'[{chosen_group}]')}  "
                  f"{c(C.YELLOW, '-> fallback')}  {c(C.DIM, pick_reason)}")
        log.info("Season %s group selected: [%s] preferred=%s - %s",
                 season, chosen_group, is_preferred, pick_reason)

    # -- Episode range ---------------------------------------------------------
    episode_range: tuple[int, int] | None = None
    if args.episodes:
        parts = args.episodes.split("-")
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            episode_range = (int(parts[0]), int(parts[1]))

    # -- Filter episodes ---------------------------------------------------------
    # Step 1: get what the primary chosen_group has for this season
    episodes = filter_episodes(raw, chosen_group, season, episode_range)

    if not episodes:
        # Fallback: re-search and re-rank for this season specifically
        print(f"  {c(C.WARN, f'[{chosen_group}]: no individual episode torrents for {season_label} - re-searching (batch check will follow) ...')}")
        season_raw   = search_all_pages(build_season_query(anime_name, season, season_info), args.pages)
        annotate_results_with_anilist_seasons(season_raw, anime_name, season_info)
        seen         = {r["detail_url"] for r in raw}
        raw         += [r for r in season_raw if r["detail_url"] not in seen]
        season_pool2 = [r for r in raw if get_result_season_number(r) == season]
        season_ranked2 = rank_sub_groups(season_pool2)
        if season_ranked2:
            chosen_group, is_preferred, pick_reason = pick_best_group(season_ranked2)
            print(f"  {c(C.LABEL, 'Fallback group:')} {c(C.VALUE, f'[{chosen_group}]')}  "
                  f"{c(C.DIM, pick_reason)}")
            episodes = filter_episodes(raw, chosen_group, season, episode_range)
        # NOTE: do NOT bail out here even if episodes is still empty.
        # The batch check block below handles _have == 0 and will scan for
        # a complete-pack torrent (e.g. "Ichigo Mashimaro TV+OVA+Encore").
        # Returning early was the bug: it prevented batch detection from running.

    # Step 2: proactive handoff detection
    # Build the handoff-aware episode list.  detect_group_handoffs() checks every
    # episode in the pool and fills gaps from the primary group with the best
    # available alternative rather than waiting for verify_episode_coverage to
    # discover them reactively.
    print()
    divider(f"Checking Group Continuity - {season_label}")
    pool_for_season = [r for r in raw if get_result_season_number(r) == season]
    episodes = detect_group_handoffs(
        pool_for_season, chosen_group, season, episode_range, season_ranked
    )
    if not episodes:
        # detect_group_handoffs returns [] only if pool_for_season is empty
        episodes = filter_episodes(raw, chosen_group, season, episode_range)

    # -- Batch torrent check ---------------------------------------------------
    # Decision tree:
    #
    #   FINISHED series (AniList status != RELEASING):
    #     Always look for a batch.  If one is found, compare its composite score
    #     against the total score of all per-episode torrents we collected.
    #     Use the batch if it wins OR if per-episode coverage is < 75%.
    #     Rationale: for a completed series, one well-seeded batch torrent is
    #     strictly better than 24 individually tracked torrents at 15 seeds each.
    #
    #   CURRENTLY AIRING series (AniList status == RELEASING):
    #     Only fall back to batch if per-episode coverage is critically thin
    #     (< 75% of aired episodes).  Per-episode is correct while a show is
    #     airing because the batch doesn't exist yet for unwatched episodes.
    #
    # A batch is normally CONFIRMED only when the nyaa detail page shows >= 2
    # video files. The one exception is movie-like AniList formats, where a
    # healthy single-video source is allowed through the same routing path so
    # it can be named and placed into the movie library cleanly.
    # --------------------------------------------------------------------------
    _batch_torrent: dict | None = None
    _using_batch   = False

    _al_info   = season_info
    _sinfo     = _al_info.get(season, {})
    _is_airing = _sinfo.get("airing", False)
    _expected  = _sinfo.get("aired") or _sinfo.get("total")

    _have = len(episodes)

    # Score of the per-episode pool we currently have:
    # sum of composite scores of every individual episode torrent
    _per_ep_pool_score = sum(_torrent_score(ep) for ep in episodes)

    # Determine whether to look for a batch at all
    _coverage_thin = _have == 0 or (_expected and _have < int(_expected * 0.75))
    _check_batch   = (not force_no_batch) and (_coverage_thin or (not _is_airing))
    if force_no_batch:
        print(f"  {c(C.DIM, 'Batch selection disabled for this retry (stuck torrent fallback).')}")

    if _check_batch:
        print()
        divider(f"Batch Check - {season_label}")
        if _coverage_thin:
            _why = ("no per-episode results" if _have == 0
                    else f"{_have}/{_expected} eps found (<75% coverage)")
            print(f"  {c(C.LABEL, 'Reason:')} {c(C.DIM, _why)}")
        else:
            print(f"  {c(C.DIM, 'Series is finished - checking for a complete-pack ...')}")

        _batch_torrent = find_best_batch_for_season(raw, anime_name, season,
                                                     prefer_group=chosen_group,
                                                     allowed_seasons=allowed_seasons,
                                                     season_info=season_info,
                                                     priority_groups=batch_only_priority_groups,
                                                     related_movies=related_movies)
        if _batch_torrent:
            _btag        = extract_sub_group(_batch_torrent["title"]) or "?"
            _batch_score = _torrent_score(_batch_torrent)
            print(f"  {c(C.SUCCESS, 'ok')} {c(C.VALUE, f'[{_btag}]')}  "
                  f"{c(C.MUTED, _batch_torrent['title'][:60])}")

            # Decide whether the batch actually wins
            if _coverage_thin:
                # No real choice -- per-episode coverage is inadequate
                _using_batch = True
                print(f"  {c(C.AMBER, 'Using batch')}  "
                      f"{c(C.DIM, '(per-episode coverage inadequate)')}")
            else:
                # Both paths viable.  Compare health scores.
                # Batch score is per-torrent (seeders + 0.4*leechers).
                # Per-ep pool score is the sum across all per-episode torrents.
                # We normalise the batch score by episode count so the comparison
                # is fair: a single batch torrent vs N individual ones.
                _ep_count_norm = max(_have, 1)
                _batch_norm    = _batch_score * _ep_count_norm   # scale up to match pool
                _margin_pct    = ((_batch_norm - _per_ep_pool_score)
                                  / max(_per_ep_pool_score, 1) * 100)
                if _batch_norm >= _per_ep_pool_score:
                    _using_batch = True
                    print(f"  {c(C.AMBER, 'Batch preferred')}  "
                          f"{c(C.DIM, f'batch normalised score {_batch_norm:.0f} >= ')} "
                          f"{c(C.DIM, f'per-ep pool {_per_ep_pool_score:.0f} ')} "
                          f"{c(C.VALUE2, f'(+{_margin_pct:.0f}%)')}")
                else:
                    print(f"  {c(C.DIM, 'Per-episode preferred')}  "
                          f"{c(C.DIM, f'pool score {_per_ep_pool_score:.0f} > ')} "
                          f"{c(C.DIM, f'batch normalised {_batch_norm:.0f}')}")
        else:
            print(f"  {c(C.DIM, 'No qualifying batch found - using per-episode results.')}")

    # -- AniList verification + gap fill  (skipped when using a batch) ---------
    if not _using_batch:
        episodes = verify_episode_coverage(
            episodes, anime_name, season, raw, chosen_group, args, episode_range
        )

    # -- Episode / batch plan display ------------------------------------------
    print()
    if _using_batch:
        divider(f"Batch Plan - {season_label}")
        _binfo = _batch_torrent["batch_info"]
        print(f"  {c(C.AMBER_B, 'Mode:')}   {c(C.VALUE, 'BATCH TORRENT')}  "
              f"{c(C.DIM, f"({_binfo['tv_count']} TV  +  {_binfo.get('movie_count', 0)} movies  +  {_binfo['special_count']} specials)")}")
        print_batch_info(_batch_torrent)
    else:
        divider(f"Episode List - {season_label}")
        print(f"  {c(C.LABEL, 'Primary group:')}  {c(C.VALUE, f'[{chosen_group}]')}   "
              f"{c(C.LABEL, 'Total:')}  {c(C.VALUE2, str(len(episodes)))}")
        print()

        sources_used: dict[str, int] = defaultdict(int)
        for ep in episodes:
            sg = ep.get("_source_group") or extract_sub_group(ep["title"]) or "?"
            sources_used[sg] += 1
        if len(sources_used) > 1:
            parts = [f"{c(C.VALUE, f'[{g}]')} {c(C.DIM, f'x{n}')}"
                     for g, n in sources_used.items()]
            print(f"  {c(C.AMBER, 'Sources:')} {c(C.DIM, ' + ').join(parts)}")
            print()

        for ep in episodes:
            ep_num  = extract_episode_number(ep["title"])
            ep_seas = extract_season_number(ep["title"])
            ep_id   = c(C.ORANGE, f"S{ep_seas:02d}E{ep_num:>3}")
            seeds   = c(C.VALUE2, f"{ep.get('seeders', 0):>4}")
            leech   = c(C.CYAN_DIM, f"{ep.get('leechers', 0):>3}L")
            dl      = c(C.AMBER_B, f"{ep.get('completed', 0):>5}v")
            src_grp = ep.get("_source_group") or extract_sub_group(ep["title"]) or "?"
            grp_tag = (c(C.DIM,    f"[{src_grp}]") if src_grp == chosen_group
                       else c(C.YELLOW, f"[{src_grp}]"))
            title_s = c(C.MUTED, ep["title"][:48])
            print(f"  {ep_id}  [{seeds}{c(C.AMBER,' S')} {leech} {dl}]  {grp_tag}  {title_s}")

    # Final guard: if we have no per-episode torrents AND no batch was found,
    # there is genuinely nothing to download.
    if not _using_batch and not episodes:
        print(c(C.WARN, f"  No episodes and no qualifying batch found for {season_label}. Skipping."))
        return True

    # -- Confirm - skipped in all-seasons mode (user already committed) ---------
    if not args.no_confirm and not auto_confirm:
        print()
        mode_label = "batch torrent" if _using_batch else f"{len(episodes)} episode(s)"
        confirm = input(
            f"{c(C.PROMPT_CLR, f'Add {season_label} ({mode_label}) to qBittorrent?')} "
            f"{c(C.AMBER_B, '[y/N]')} "
        ).strip().lower()
        if confirm != "y":
            print(c(C.WARN, f"  Skipping {season_label}."))
            return True

    # -- Save path -------------------------------------------------------------
    print()
    divider(f"qBittorrent - {season_label}")
    save_path = build_save_path(jellyfin_anime_dir, anime_name, season)
    print(f"  {c(C.LABEL, 'Save path:')} {c(C.VALUE, save_path)}")
    print(f"  {c(C.SUCCESS, '✓')} {c(C.INFO_DIM, 'Directory created / verified.')}")

    # -- Add torrent(s) --------------------------------------------------------
    monitor = CleanupMonitor(client, delete_files=delete_files)
    monitor.start()

    print()
    divider(f"Adding Torrents - {season_label}")
    added, dl_failed = 0, 0
    deferred: list[tuple[str, str]] = []

    # -- Batch path: check disk state before touching qBittorrent --------------
    # Resume logic: three states to detect so re-running the script is safe.
    #
    #   State A - encode already finished:
    #     Season 01 contains .hs.mkv files  ->  skip everything, return True
    #
    #   State B - routing already finished (download done, encode not started):
    #     Season 01 contains source .mkv files but no .hs.mkv  ->  skip download
    #     and monitor, go straight to encode phase
    #
    #   State C - download finished but routing not done:
    #     _batch_staging/ contains .mkv files  ->  skip qBittorrent, go straight
    #     to post_process_batch_download then encode
    #
    #   State D - fresh run:
    #     Nothing on disk  ->  normal qBittorrent add + monitor flow
    # -------------------------------------------------------------------------
    if _using_batch and _batch_torrent:
        series_dir    = Path(build_save_path(jellyfin_anime_dir, anime_name, season)).parent
        batch_staging = series_dir / "_batch_staging" / f"Season {(season or 1):02d}"
        season_tv_dir = series_dir / f"Season {(season or 1):02d}"

        # -- Integrity check helpers --------------------------------------------
        # We use a two-layer check rather than torrent piece hashes:
        #
        #   1. File existence  - does the file exist at all?
        #   2. Size proximity  - is it within 2% of the expected size from nyaa?
        #
        # nyaa reports sizes in MiB rounded to one decimal place, so there is
        # inherent rounding error.  2% tolerance handles that comfortably while
        # still catching zero-byte partials, 1-KB incomplete downloads, and
        # files truncated mid-transfer.  A piece-hash check would be the only
        # more accurate option, but requires downloading and parsing the .torrent
        # file - substantial extra complexity for marginal real-world gain.
        #
        # If batch_info is unavailable (detail page fetch failed), we fall back
        # to a 10 MB minimum-size heuristic so we are still protected against
        # zero-byte stubs even without expected sizes.
        # ----------------------------------------------------------------------

        _SIZE_TOLERANCE  = 0.02   # 2% - covers MiB rounding + filesystem metadata
        _MIN_VIDEO_BYTES = 10 * 1024 * 1024   # 10 MB fallback floor

        # Build a lookup: stem -> expected_bytes from batch_info (may be 0 if
        # nyaa didn't report a size for that file, which is rare but possible)
        _expected_sizes: dict[str, int] = {}
        _bi = _batch_torrent.get("batch_info") if _batch_torrent else None
        if _bi:
            for fi in _bi["files"]:
                stem = Path(fi["filename"]).stem
                _expected_sizes[stem] = fi.get("expected_bytes", 0)

        def _file_is_complete(p: Path) -> bool:
            """
            Return True if *p* exists and its size passes the integrity check.

            With an expected size: actual must be within SIZE_TOLERANCE of expected.
            Without an expected size (0): actual must be >= MIN_VIDEO_BYTES.
            """
            if not p.is_file():
                return False
            try:
                actual = p.stat().st_size
            except OSError:
                return False
            expected = _expected_sizes.get(p.stem, 0)
            if expected > 0:
                ratio = actual / expected
                ok = (1 - _SIZE_TOLERANCE) <= ratio <= (1 + _SIZE_TOLERANCE)
                if not ok:
                    log.debug("Size check FAIL %s: actual=%d expected=%d ratio=%.3f",
                              p.name, actual, expected, ratio)
                return ok
            # No expected size available - fall back to minimum floor
            return actual >= _MIN_VIDEO_BYTES

        def _scan_dir(d: Path, require_hs: bool = False) -> tuple[int, int]:
            """
            Scan *d* for video files.  Returns (complete, incomplete) counts.
            require_hs=True counts only .hs. encoded outputs.
            require_hs=False counts only raw source files (no .hs. in name).
            """
            if not d.is_dir():
                return 0, 0
            complete = incomplete = 0
            for p in d.iterdir():
                if not p.is_file():
                    continue
                if p.suffix.lower() not in _VIDEO_EXT:
                    continue
                has_hs = ".hs." in p.name
                if require_hs and not has_hs:
                    continue
                if not require_hs and has_hs:
                    continue
                if _file_is_complete(p):
                    complete += 1
                else:
                    incomplete += 1
            return complete, incomplete

        def _scan_movie_targets() -> tuple[int, int]:
            if not _bi:
                return 0, 0
            complete = incomplete = 0
            for fi in _bi.get("movies", []):
                target = build_movie_target_path(
                    movie_root,
                    fi.get("movie_title") or anime_name,
                    Path(fi["filename"]).suffix,
                    fi.get("movie_year"),
                    create_dirs=False,
                )
                if _file_is_complete(target):
                    complete += 1
                elif target.exists():
                    incomplete += 1
            return complete, incomplete

        _enc_ok,   _enc_bad   = _scan_dir(season_tv_dir, require_hs=True)
        _raw_ok,   _raw_bad   = _scan_dir(season_tv_dir, require_hs=False)
        _stg_ok,   _stg_bad   = _scan_dir(batch_staging, require_hs=False)
        _movie_ok, _movie_bad = _scan_movie_targets()

        # Log the full picture for debugging
        log.debug("Resume check - encoded: %d ok / %d bad | routed tv: %d ok / %d bad "
                  "| routed movies: %d ok / %d bad | staging: %d ok / %d bad",
                  _enc_ok, _enc_bad, _raw_ok, _raw_bad, _movie_ok, _movie_bad, _stg_ok, _stg_bad)

        # Warn the user if we found incomplete files on disk
        _bad_total = _enc_bad + _raw_bad + _movie_bad + _stg_bad
        if _bad_total > 0:
            print(f"  {c(C.WARN, f'[WARN] {_bad_total} incomplete/zero-byte file(s) found on disk '
                         f'(wrong size vs nyaa manifest) - will not be treated as complete.')}")

        _encoded_count = _enc_ok
        _routed_count  = _raw_ok + _movie_ok
        _staging_count = _stg_ok

        # -- State A: encode already done --------------------------------------
        if _encoded_count > 0:
            print(f"  {c(C.SUCCESS, 'RESUME')}  {c(C.DIM, f'{_encoded_count} encoded file(s) already in')} "
                  f"{c(C.VALUE, season_tv_dir.name)}  {c(C.DIM, '- encode phase complete, skipping.')}")
            log.info("Resume State A: %d encoded files found, skipping Season %s entirely",
                     _encoded_count, season)
            monitor.stop()
            return True

        # -- State B: routing done, encode not started --------------------------
        elif _routed_count > 0:
            print(f"  {c(C.SUCCESS, 'RESUME')}  {c(C.DIM, f'{_routed_count} source file(s) already routed to')} "
                  f"{c(C.VALUE, season_tv_dir.name)}  {c(C.DIM, '- skipping download, going to encode.')}")
            log.info("Resume State B: %d routed files found, skipping download for Season %s",
                     _routed_count, season)
            monitor.stop()
            # Jump straight to encode - save_path already set correctly above
            save_path = str(season_tv_dir)
            # fall through to encode phase

        # -- State C: download done, routing not done ---------------------------
        elif _staging_count > 0:
            print(f"  {c(C.SUCCESS, 'RESUME')}  {c(C.DIM, f'{_staging_count} file(s) found in staging')} "
                  f"{c(C.DIM, '- skipping download, running routing.')}")
            log.info("Resume State C: %d staged files found, skipping download for Season %s",
                     _staging_count, season)
            monitor.stop()
            batch_staging.mkdir(parents=True, exist_ok=True)
            routed_batch = post_process_batch_download(
                download_dir = batch_staging,
                batch_info   = _batch_torrent["batch_info"],
                series_dir   = series_dir,
                movie_root   = movie_root,
                tv_season    = season or 1,
                dry_run      = args.dry_run,
            )
            save_path = str(season_tv_dir)
            # fall through to encode phase

        # -- State D: fresh run -------------------------------------------------
        else:
            batch_staging.mkdir(parents=True, exist_ok=True)
            _pre_stage_files = _snapshot_files(batch_staging)
            _pre_season_files = _snapshot_files(season_tv_dir)
            label = f"BATCH {season_label}"
            try:
                info_hash = add_torrent(client, _batch_torrent,
                                        save_path=str(batch_staging),
                                        category=QBIT_CATEGORY)
                if info_hash == "__lookup__":
                    deferred.append((_batch_torrent["title"], label))
                    print(f"  {c(C.SUCCESS, '\u2713')} {c(C.AMBER, label)}  "
                          f"{c(C.INFO_DIM, '(hash lookup pending)')}")
                    added += 1
                elif info_hash:
                    monitor.watch(info_hash, label)
                    print(f"  {c(C.SUCCESS, '\u2713')} {c(C.AMBER, label)}  "
                          f"{c(C.CYAN_DIM, info_hash)}")
                    added += 1
                else:
                    print(f"  {c(C.WARN, '\u2717')} {c(C.AMBER, label)}  "
                          f"{c(C.WARN, 'no URL found')}")
                    dl_failed += 1
            except Exception as exc:
                print(f"  {c(C.WARN, '\u2717')} {c(C.AMBER, label)}  {c(C.WARN, str(exc))}")
                dl_failed += 1

            if deferred:
                print(f"\n  {c(C.LABEL, 'Resolving hashes ...')}")
                for nyaa_title, label in deferred:
                    h = resolve_hash_for_title(client, nyaa_title)
                    if h:
                        monitor.watch(h, label)
                        print(f"  {c(C.SUCCESS, '\u2713')} {c(C.AMBER, label)} \u2192 {c(C.CYAN_DIM, h)}")
                    else:
                        print(f"  {c(C.WARN, '\u2717')} {c(C.AMBER, label)} \u2014 hash not resolved")

            print()
            divider(f"Download Summary \u2014 {season_label}")
            print(f"  {c(C.SUCCESS, '\u2713')} {c(C.VALUE2, str(added))} added   "
                  f"{c(C.WARN if dl_failed else C.AMBER, '\u2717')} {c(C.VALUE2, str(dl_failed))} failed")

            if added == 0:
                monitor.stop()
                print(c(C.WARN, f"  No torrents added for {season_label}."))
                return True

            mode_str = "torrent + files" if delete_files else "torrent entry only"
            print()
            print(f"  {c(C.LABEL, 'Monitoring')} "
                  f"{c(C.VALUE2, str(monitor.pending_count()))} torrent(s) ...")
            print(f"  {c(C.LABEL, 'On completion:')} {c(C.ORANGE, mode_str)} will be removed.")
            print(f"  {c(C.AMBER_B, 'Press Ctrl-C')} "
                  f"{c(C.AMBER, 'to stop monitoring (downloads continue in qBittorrent).')}")
            print()

            try:
                _stuck_last_seen: dict[str, tuple[str, float]] = {}
                _stuck_since: dict[str, float] = {}
                stuck = wait_for_downloads_or_stuck(
                    client=client,
                    monitor=monitor,
                    no_progress_secs=STUCK_NO_PROGRESS_SECS,
                    poll_secs=5,
                    last_seen=_stuck_last_seen,
                    stagnant_since=_stuck_since,
                )
            except KeyboardInterrupt:
                stuck = {"stuck": False}

            if stuck.get("stuck"):
                idle_s = int(stuck.get("idle_for", 0))
                stuck_label = stuck.get("label", "BATCH")
                stuck_state = stuck.get("state", "unknown")
                stuck_prog = float(stuck.get("progress", 0.0)) * 100.0
                print()
                print(f"  {c(C.WARN, '[STUCK]')} "
                      f"{c(C.ORANGE, stuck_label)} {c(C.DIM, f'state={stuck_state} progress={stuck_prog:.1f}%')} "
                      f"{c(C.DIM, f'no movement for {idle_s}s')}")
                print(f"  {c(C.DIM, 'Aborting this batch source, cleaning partial files, and retrying with per-episode sources ...')}")

                for h in monitor.watched_snapshot().keys():
                    try:
                        client.torrents_delete(delete_files=True, torrent_hashes=h)
                    except Exception:
                        log.exception("Failed to delete stuck torrent %s", h)
                monitor.stop()
                _removed_stage = _remove_new_files_since(batch_staging, _pre_stage_files)
                _removed_season = _remove_new_files_since(season_tv_dir, _pre_season_files)
                print(f"  {c(C.DIM, 'Cleanup:')} "
                      f"{c(C.VALUE2, str(_removed_stage))} staging file(s), "
                      f"{c(C.VALUE2, str(_removed_season))} season file(s) removed.")

                _stuck_group = (extract_sub_group(_batch_torrent["title"]) if _batch_torrent else None) or chosen_group
                _retry_excluded = {g for g in {_stuck_group, chosen_group} if g}
                print(f"  {c(C.DIM, 'Retry source policy:')} "
                      f"{c(C.DIM, 'excluding')} {c(C.VALUE, ', '.join(f'[{g}]' for g in sorted(_retry_excluded)))}")

                return run_one_season(
                    season=season,
                    raw=raw,
                    ranked=ranked,
                    anime_name=anime_name,
                    args=args,
                    jellyfin_anime_dir=jellyfin_anime_dir,
                    client=client,
                    delete_files=delete_files,
                    auto_confirm=True,
                    force_no_batch=True,
                    excluded_groups=_retry_excluded,
                    allowed_seasons=allowed_seasons,
                    season_info=season_info,
                )
            else:
                monitor.stop()

            if monitor.cleaned:
                print()
                divider(f"Cleaned Up \u2014 {season_label}")
                for lbl in monitor.cleaned:
                    print(f"  {c(C.SUCCESS, '\u2713')} {c(C.ORANGE, lbl)}")

            # Route files from staging to Season NN / Season 00
            routed_batch = post_process_batch_download(
                download_dir = batch_staging,
                batch_info   = _batch_torrent["batch_info"],
                series_dir   = series_dir,
                movie_root   = movie_root,
                tv_season    = season or 1,
                dry_run      = args.dry_run,
            )
            save_path = str(season_tv_dir)

    # ---- PER-EPISODE PATH ----------------------------------------------------
    else:
        episode_retry_meta: dict[str, dict] = {}
        pending_label_meta: dict[str, dict] = {}

        for ep in episodes:
            ep_num  = extract_episode_number(ep["title"])
            ep_seas = extract_season_number(ep["title"])
            label   = f"S{ep_seas:02d}E{ep_num:>3}"
            grp     = extract_sub_group(ep["title"]) or ""
            meta = {
                "label": label,
                "season": ep_seas,
                "ep_num": ep_num,
                "excluded_groups": ({grp} if grp else set()),
                "excluded_detail_urls": ({ep.get("detail_url")} if ep.get("detail_url") else set()),
                "retry_count": 0,
            }
            try:
                info_hash = add_torrent(client, ep,
                                        save_path=save_path,
                                        category=QBIT_CATEGORY)
                if info_hash == "__lookup__":
                    deferred.append((ep["title"], label))
                    pending_label_meta[label] = meta
                    print(f"  {c(C.SUCCESS, '\u2713')} {c(C.ORANGE, label)}  "
                          f"{c(C.INFO_DIM, '(hash lookup pending)')}")
                    added += 1
                elif info_hash:
                    monitor.watch(info_hash, label)
                    episode_retry_meta[info_hash.lower()] = meta
                    print(f"  {c(C.SUCCESS, '\u2713')} {c(C.ORANGE, label)}  "
                          f"{c(C.CYAN_DIM, info_hash)}")
                    added += 1
                else:
                    print(f"  {c(C.WARN, '\u2717')} {c(C.ORANGE, label)}  "
                          f"{c(C.WARN, 'no URL found')}")
                    dl_failed += 1
            except Exception as exc:
                print(f"  {c(C.WARN, '\u2717')} {c(C.ORANGE, label)}  {c(C.WARN, str(exc))}")
                dl_failed += 1

        if deferred:
            print(f"\n  {c(C.LABEL, 'Resolving hashes ...')}")
            for nyaa_title, label in deferred:
                h = resolve_hash_for_title(client, nyaa_title)
                if h:
                    monitor.watch(h, label)
                    _meta = pending_label_meta.get(label)
                    if _meta is None:
                        _m = re.search(r"S(\d{2})E\s*(\d{1,3})", label)
                        if _m:
                            _meta = {
                                "label": label,
                                "season": int(_m.group(1)),
                                "ep_num": int(_m.group(2)),
                                "excluded_groups": set(),
                                "excluded_detail_urls": set(),
                                "retry_count": 0,
                            }
                    if _meta is not None:
                        episode_retry_meta[h.lower()] = _meta
                    print(f"  {c(C.SUCCESS, '\u2713')} {c(C.ORANGE, label)} \u2192 {c(C.CYAN_DIM, h)}")
                else:
                    print(f"  {c(C.WARN, '\u2717')} {c(C.ORANGE, label)} \u2014 hash not resolved")

        print()
        divider(f"Download Summary \u2014 {season_label}")
        print(f"  {c(C.SUCCESS, '\u2713')} {c(C.VALUE2, str(added))} added   "
              f"{c(C.WARN if dl_failed else C.AMBER, '\u2717')} {c(C.VALUE2, str(dl_failed))} failed")

        if added == 0:
            monitor.stop()
            print(c(C.WARN, f"  No torrents added for {season_label}."))
            return True

        mode_str = "torrent + files" if delete_files else "torrent entry only"
        print()
        print(f"  {c(C.LABEL, 'Monitoring')} "
              f"{c(C.VALUE2, str(monitor.pending_count()))} torrent(s) ...")
        print(f"  {c(C.LABEL, 'On completion:')} {c(C.ORANGE, mode_str)} will be removed.")
        print(f"  {c(C.AMBER_B, 'Press Ctrl-C')} "
              f"{c(C.AMBER, 'to stop monitoring (downloads continue in qBittorrent).')}")
        print()

        dead_labels: list[str] = []
        _stuck_last_seen: dict[str, tuple[str, float]] = {}
        _stuck_since: dict[str, float] = {}
        try:
            while monitor.pending_count() > 0:
                stuck = wait_for_downloads_or_stuck(
                    client=client,
                    monitor=monitor,
                    no_progress_secs=STUCK_NO_PROGRESS_SECS,
                    poll_secs=5,
                    last_seen=_stuck_last_seen,
                    stagnant_since=_stuck_since,
                )
                if not stuck.get("stuck"):
                    break
                stuck_batch = [stuck] + [
                    s for s in collect_current_stuck_downloads(
                        client=client,
                        monitor=monitor,
                        no_progress_secs=STUCK_NO_PROGRESS_SECS,
                        last_seen=_stuck_last_seen,
                        stagnant_since=_stuck_since,
                    )
                    if str(s.get("hash", "")).lower() != str(stuck.get("hash", "")).lower()
                ]

                for stuck_item in stuck_batch:
                    stuck_hash = str(stuck_item.get("hash", "")).lower()
                    stuck_label = stuck_item.get("label", "episode")
                    stuck_state = stuck_item.get("state", "unknown")
                    stuck_prog = float(stuck_item.get("progress", 0.0)) * 100.0
                    idle_s = int(stuck_item.get("idle_for", 0))
                    meta = episode_retry_meta.pop(stuck_hash, None)

                    print()
                    print(f"  {c(C.WARN, '[STUCK]')} "
                          f"{c(C.ORANGE, stuck_label)} {c(C.DIM, f'state={stuck_state} progress={stuck_prog:.1f}%')} "
                          f"{c(C.DIM, f'no movement for {idle_s}s')}")

                    try:
                        client.torrents_delete(delete_files=True, torrent_hashes=stuck_hash)
                    except Exception:
                        log.exception("Failed to delete stuck episode torrent %s", stuck_hash)
                    monitor.unwatch(stuck_hash)
                    _stuck_last_seen.pop(stuck_hash, None)
                    _stuck_since.pop(stuck_hash, None)

                    if not meta:
                        dead_labels.append(stuck_label)
                        print(f"  {c(C.WARN, '[DEAD]')} {c(C.DIM, 'No retry metadata for this torrent; leaving episode missing for now.')}")
                        continue

                    if meta["retry_count"] >= STUCK_EPISODE_RETRY_LIMIT:
                        dead_labels.append(stuck_label)
                        print(f"  {c(C.WARN, '[DEAD]')} {c(C.DIM, f'retry limit reached ({STUCK_EPISODE_RETRY_LIMIT}) - leaving this episode for manual follow-up.')}")
                        continue

                    retry_item, retry_reason = select_retry_episode_result(
                        raw,
                        season=meta["season"],
                        ep_num=meta["ep_num"],
                        excluded_groups=set(meta["excluded_groups"]),
                        excluded_detail_urls=set(meta["excluded_detail_urls"]),
                    )
                    if not retry_item:
                        dead_labels.append(stuck_label)
                        print(f"  {c(C.WARN, '[DEAD]')} {c(C.DIM, retry_reason)}")
                        continue

                    retry_grp = extract_sub_group(retry_item["title"]) or ""
                    if retry_grp:
                        meta["excluded_groups"].add(retry_grp)
                    if retry_item.get("detail_url"):
                        meta["excluded_detail_urls"].add(retry_item["detail_url"])
                    meta["retry_count"] += 1

                    print(f"  {c(C.DIM, 'Retrying:')} {c(C.VALUE, retry_reason)} "
                          f"{c(C.DIM, '->')} {c(C.VALUE, '[' + retry_grp + ']') if retry_grp else c(C.DIM, '[unknown]')}")

                    try:
                        retry_hash = add_torrent(client, retry_item,
                                                 save_path=save_path,
                                                 category=QBIT_CATEGORY)
                        if retry_hash == "__lookup__":
                            resolved = resolve_hash_for_title(client, retry_item["title"])
                            if resolved:
                                retry_hash = resolved
                            else:
                                dead_labels.append(stuck_label)
                                print(f"  {c(C.WARN, '[DEAD]')} {c(C.DIM, 'Retry hash could not be resolved.')}")
                                continue
                        if retry_hash:
                            monitor.watch(retry_hash, meta["label"])
                            episode_retry_meta[retry_hash.lower()] = meta
                            _stuck_last_seen.pop(retry_hash.lower(), None)
                            _stuck_since.pop(retry_hash.lower(), None)
                            print(f"  {c(C.SUCCESS, '\u2713')} {c(C.ORANGE, meta['label'])} "
                                  f"{c(C.DIM, 'retry added')} {c(C.CYAN_DIM, retry_hash)}")
                        else:
                            dead_labels.append(stuck_label)
                            print(f"  {c(C.WARN, '[DEAD]')} {c(C.DIM, 'Retry candidate had no usable URL.')}")
                    except Exception as exc:
                        dead_labels.append(stuck_label)
                        print(f"  {c(C.WARN, '[DEAD]')} {c(C.DIM, f'Retry add failed: {exc}')}")
        except KeyboardInterrupt:
            pass

        monitor.stop()

        if monitor.cleaned:
            print()
            divider(f"Cleaned Up \u2014 {season_label}")
            for lbl in monitor.cleaned:
                print(f"  {c(C.SUCCESS, '\u2713')} {c(C.ORANGE, lbl)}")
        if dead_labels:
            print(f"  {c(C.WARN, 'Dead torrents:')} "
                  f"{c(C.WARN, ', '.join(dead_labels))}")

        # Per-episode OVA / Special reclassification
        _sp_dir    = Path(build_save_path(jellyfin_anime_dir, anime_name, season)).parent / "Season 00"
        _tv_dir    = Path(save_path)
        _reclassed = 0
        for _p in list(_tv_dir.iterdir()):
            if not _p.is_file() or _p.suffix.lower() not in _VIDEO_EXT:
                continue
            _cl = _classify_batch_file(_p.name)
            if _cl["category"] != "tv":
                if not args.dry_run:
                    _sp_dir.mkdir(parents=True, exist_ok=True)
                    _dest = _sp_dir / _p.name
                    if not _dest.exists():
                        try:
                            shutil.move(str(_p), str(_dest))
                            log.info("Reclassed %s -> Season 00", _p.name)
                            _reclassed += 1
                        except OSError as exc:
                            log.warning("Reclass move failed: %s: %s", _p.name, exc)
                else:
                    _reclassed += 1
        if _reclassed:
            print(f"  {c(C.CYAN_DIM, str(_reclassed))} file(s) reclassified to Season 00")

    # Encoding/transcoding phase removed: files go directly from qBittorrent to Jellyfin.
    # Continue with watch list update + Jellyfin rescan only.
    _series_dir = Path(save_path).parent
    _has_series_content = True
    _has_movie_content = False
    if routed_batch is not None:
        _has_series_content = bool(routed_batch.get("tv") or routed_batch.get("specials"))
        _has_movie_content = bool(routed_batch.get("movies"))
    elif _using_batch and _batch_torrent and _batch_torrent.get("batch_info"):
        _binfo = _batch_torrent["batch_info"]
        _has_series_content = bool(_binfo.get("tv_count", 0) or _binfo.get("special_count", 0))
        _has_movie_content = bool(_binfo.get("movie_count", 0))

    if _has_series_content:
        _norm = normalize_series_filenames_for_jellyfin(_series_dir, dry_run=args.dry_run)
        print(f"  {c(C.DIM, 'Filename normalize:')} "
              f"{c(C.SUCCESS, str(_norm['renamed']))} renamed, "
              f"{c(C.DIM, str(_norm['already']))} already canonical, "
              f"{c(C.WARN if _norm['collisions'] else C.DIM, str(_norm['collisions']) + ' collisions')}, "
              f"{c(C.WARN if _norm['skipped'] else C.DIM, str(_norm['skipped']) + ' skipped')}")
        print(f"  {c(C.DIM, 'Subtitle normalize:')} "
              f"{c(C.SUCCESS, str(_norm.get('sub_renamed', 0)))} renamed, "
              f"{c(C.WARN if _norm.get('sub_skipped', 0) else C.DIM, str(_norm.get('sub_skipped', 0)) + ' skipped')}")
    else:
        print(f"  {c(C.DIM, 'Filename normalize:')} {c(C.DIM, 'skipped (movie-only content)')}")
        print(f"  {c(C.DIM, 'Subtitle normalize:')} {c(C.DIM, 'skipped (movie-only content)')}")
    # -- Update watch list -----------------------------------------------------
    # Re-query AniList to get current airing status for this season.
    # Reuse the already-fetched season_info for this title/run.
    try:
        sinfo       = season_info.get(season, {})
        _is_airing  = sinfo.get("airing", False)
        _total      = sinfo.get("total")
        _ep_nums    = sorted(e for r in episodes
                             if (e := extract_episode_number(r["title"])) is not None)
        _last_ep    = _ep_nums[-1] if _ep_nums else 0
        if _has_series_content:
            upsert_watch_entry(
                anime_dir      = jellyfin_anime_dir,
                anime_name     = anime_name,
                season         = season,
                group          = chosen_group,
                last_episode   = _last_ep,
                save_path      = save_path,
                total_episodes = _total,
                is_airing      = _is_airing,
            )
    except Exception as exc:
        print(f"  {c(C.DIM, f'Watch list update skipped ({exc})')}")

    # -- Jellyfin library rescan -----------------------------------------------
    print()
    if _has_series_content:
        trigger_jellyfin_rescan(season_label, jellyfin_anime_dir)
    if _has_movie_content:
        trigger_jellyfin_rescan(f"{season_label} / Movies", movie_root)

    VISUAL_STATS["seasons"] += 1
    VISUAL_STATS["encoded_ok"] += 0
    VISUAL_STATS["encoded_skip"] += 0
    VISUAL_STATS["encoded_fail"] += 0

    return True


# -----------------------------------------------------------------------------
# Watch list - tracks currently airing seasons for RSS polling
# Lives in the Jellyfin anime root dir, not next to the script
# -----------------------------------------------------------------------------

WATCHLIST_FILENAME = "anime_watchlist.json"


def watchlist_path(anime_dir: Path) -> Path:
    return anime_dir / WATCHLIST_FILENAME


def load_watchlist(anime_dir: Path) -> list[dict]:
    """Load the watch list from the anime root directory. Returns [] if missing."""
    path = watchlist_path(anime_dir)
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []


def rebuild_watchlist_from_disk(anime_dir: Path) -> None:
    """
    Scan the anime root directory and reconstruct the watch list from what's
    actually on disk.  Called automatically when no watch list file exists.

    Logic:
      - Each subdirectory of anime_dir is treated as an anime title
      - Each "Season XX" subdirectory inside it is a season
      - AniList is queried to check if the season is currently RELEASING
      - Disk is scanned to find the highest episode present
      - An entry is written for every season that is still airing

    Seasons that AniList says are FINISHED are skipped - nothing to track.
    Seasons where AniList cannot be reached get a warning entry so the user
    can review, but are not added to active polling.
    """
    print()
    divider("Watch List - Rebuilding from Disk")
    print(f"  {c(C.DIM, 'No watch list found - scanning library to reconstruct ...')}")
    print(f"  {c(C.DIM, str(anime_dir))}")
    print()

    season_pat = re.compile(r"^Season (\d{2})$", re.IGNORECASE)
    entries: list[dict] = []
    found_airing = 0

    # Walk one level: anime_dir / <Title> / Season XX
    try:
        title_dirs = sorted(
            p for p in anime_dir.iterdir()
            if p.is_dir() and not p.name.startswith(".")
            and p.name != WATCHLIST_FILENAME
        )
    except Exception as exc:
        print(f"  {yellow('[WARN]')} Could not read anime directory: {exc}")
        return

    for title_dir in title_dirs:
        anime_name = _series_title_from_dir(title_dir)
        # Find season subdirectories
        try:
            season_dirs = sorted(
                p for p in title_dir.iterdir()
                if p.is_dir() and season_pat.match(p.name)
            )
        except Exception:
            continue

        if not season_dirs:
            continue

        # Query AniList once per title
        try:
            season_info = fetch_anilist_season_info(anime_name)
        except Exception:
            season_info = {}

        for season_dir in season_dirs:
            m = season_pat.match(season_dir.name)
            if not m:
                continue
            season_num = int(m.group(1))

            sinfo      = season_info.get(season_num, {})
            is_airing  = sinfo.get("airing", False)
            total      = sinfo.get("total")
            aired      = sinfo.get("aired")

            # Skip finished seasons - nothing to poll
            if not is_airing:
                continue

            # Scan disk for episodes present
            on_disk      = scan_episodes_on_disk(str(season_dir))
            disk_highest = max(on_disk) if on_disk else 0

            # Best-effort group detection: look at filenames and use
            # extract_sub_group on the most common result
            group = "unknown"
            try:
                files = list(season_dir.iterdir())
                groups_found: dict[str, int] = {}
                for f in files:
                    if f.suffix.lower() in VIDEO_EXTENSIONS:
                        g = extract_sub_group(f.name)
                        if g:
                            groups_found[g] = groups_found.get(g, 0) + 1
                if groups_found:
                    group = max(groups_found, key=lambda k: groups_found[k])
            except Exception:
                pass

            rss_url = (
                f"https://nyaa.si/?page=rss"
                f"&q={requests.utils.quote(anime_name)}"
                f"+{requests.utils.quote(group)}"
                f"&c=1_2&f=0"
            )

            entry = {
                "anime_name":     anime_name,
                "season":         season_num,
                "group":          group,
                "last_episode":   disk_highest,
                "total_episodes": total,
                "save_path":      str(season_dir),
                "rss_url":        rss_url,
                "completed":      False,
                "added":          datetime.date.today().isoformat(),
                "updated":        datetime.date.today().isoformat(),
            }
            entries.append(entry)
            found_airing += 1

            ep_str = f"E{disk_highest}" if disk_highest else "no eps on disk"
            total_str = f"/{total}" if total else "/?"
            print(f"  {c(C.SUCCESS, '✓')} {c(C.VALUE, anime_name)} "
                  f"{c(C.ORANGE, f'S{season_num:02d}')}  "
                  f"{c(C.DIM, f'{ep_str}{total_str}')}  "
                  f"{c(C.VALUE2, f'[{group}]')}  "
                  f"{c(C.AMBER, 'AIRING')}")

    if not entries:
        print(f"  {c(C.DIM, 'No currently airing seasons found in library.')}")
        print(f"  {c(C.DIM, 'Watch list will be populated next time you run the pipeline on an airing show.')}")
    else:
        save_watchlist(anime_dir, entries)
        print()
        print(f"  {c(C.SUCCESS, '✓')} Watch list rebuilt - "
              f"{c(C.VALUE2, str(found_airing))} airing season(s) registered.")
        print(f"  {c(C.DIM, str(watchlist_path(anime_dir)))}")


def save_watchlist(anime_dir: Path, entries: list[dict]) -> None:
    """
    Write the watch list to disk. Silently warns on failure - never raises.
    A failed write is logged but never stops the pipeline.
    """
    try:
        path = watchlist_path(anime_dir)
        path.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        print(f"  {yellow('[WARN]')} Could not save watch list: {exc}")
        print(f"  {c(C.DIM, 'Continuing - watch list update will need to be performed manually.')}")


def upsert_watch_entry(
    anime_dir:      Path,
    anime_name:     str,
    season:         int,
    group:          str,
    last_episode:   int,
    save_path:      str,
    total_episodes: int | None,
    is_airing:      bool,
) -> None:
    """
    Add or update a watch list entry for this anime/season.
    Only writes an entry if the season is currently airing.
    Marks the entry completed if last_episode >= total_episodes.
    """
    if not is_airing:
        # Finished show - remove entry if it exists (it's done)
        entries = load_watchlist(anime_dir)
        entries = [e for e in entries
                   if not (e["anime_name"] == anime_name and e["season"] == season)]
        save_watchlist(anime_dir, entries)
        return

    entries  = load_watchlist(anime_dir)
    existing = next((e for e in entries
                     if e["anime_name"] == anime_name and e["season"] == season), None)

    completed = (total_episodes is not None and last_episode >= total_episodes)

    rss_url = (
        f"https://nyaa.si/?page=rss"
        f"&q={requests.utils.quote(anime_name)}+{requests.utils.quote(group)}"
        f"&c=1_2&f=0"
    )

    entry = {
        "anime_name":     anime_name,
        "season":         season,
        "group":          group,
        "last_episode":   last_episode,
        "total_episodes": total_episodes,
        "save_path":      save_path,
        "rss_url":        rss_url,
        "completed":      completed,
        "updated":        datetime.date.today().isoformat(),
    }
    if existing is None:
        entry["added"] = datetime.date.today().isoformat()
        entries.append(entry)
        print(f"  {c(C.SUCCESS, '✓')} Watch list: added "
              f"{c(C.VALUE, anime_name)} Season {season} "
              f"{c(C.DIM, f'({anime_dir / WATCHLIST_FILENAME})')}")
    else:
        entry["added"] = existing.get("added", entry["updated"])
        entries = [e if not (e["anime_name"] == anime_name and e["season"] == season)
                   else entry for e in entries]
        if completed:
            print(f"  {c(C.SUCCESS, '✓')} Watch list: Season {season} marked complete.")
        else:
            print(f"  {c(C.DIM, f'Watch list updated - last ep: E{last_episode}')}")

    save_watchlist(anime_dir, entries)


def print_watchlist_status(anime_dir: Path) -> None:
    """Print current watch list contents. Used by --watch-status."""
    entries = load_watchlist(anime_dir)
    print()
    divider(f"Watch List  ({watchlist_path(anime_dir)})")
    if not entries:
        print(f"  {c(C.DIM, 'No entries. Run the pipeline on an airing show to populate.')}")
        return
    active    = [e for e in entries if not e.get("completed")]
    completed = [e for e in entries if e.get("completed")]
    if active:
        print(f"  {c(C.AMBER_B, 'Currently watching:')}")
        for e in active:
            total = f"/{e['total_episodes']}" if e.get("total_episodes") else "/??"
            print(f"    {c(C.VALUE, e['anime_name'])} "
                  f"{c(C.ORANGE, f'S{e["season"]:02d}')}  "
                  f"{c(C.DIM, f'E{e["last_episode"]}{total}')}  "
                  f"{c(C.VALUE2, f'[{e["group"]}]')}  "
                  f"{c(C.DIM, f'updated {e.get("updated","?")}')} ")
    if completed:
        print(f"\n  {c(C.DIM, 'Completed (will not be polled):')}")
        for e in completed:
            print(f"    {c(C.MUTED, e['anime_name'])} "
                  f"{c(C.ORANGE, f'S{e["season"]:02d}')}  "
                  f"{c(C.DIM, f'[{e["group"]}]')}")


def reconcile_existing_media_library(anime_dir: Path, dry_run: bool = False) -> dict[str, int]:
    """
    Audit the existing anime TV library and reconcile obvious movie/special files.

    Rules:
      - movie-like files move to the separate movie library
      - non-TV files in numbered season folders move to Season 00
      - TV files stay in place; this mode does not invent season structure
    """
    movie_root = find_jellyfin_movie_dir(anime_dir, create_dirs=not dry_run)
    summary = {
        "series_scanned": 0,
        "videos_scanned": 0,
        "movies_moved": 0,
        "specials_moved": 0,
        "already_ok": 0,
        "skipped": 0,
        "errors": 0,
    }

    print()
    divider("Library Reconcile")
    print(f"  {c(C.LABEL, 'Anime root:')} {c(C.VALUE, str(anime_dir))}")
    print(f"  {c(C.LABEL, 'Movie root:')} {c(C.VALUE, str(movie_root))}")
    print(f"  {c(C.DIM, 'Mode:')} {c(C.VALUE, 'DRY-RUN' if dry_run else 'APPLY CHANGES')}")

    season_pat = re.compile(r"^Season (\d{2})$", re.IGNORECASE)
    series_dirs = sorted(
        p for p in anime_dir.iterdir()
        if p.is_dir() and p.name != movie_root.name
    )

    for series_dir in series_dirs:
        season_dirs = sorted(
            p for p in series_dir.iterdir()
            if p.is_dir() and season_pat.match(p.name)
        )
        if not season_dirs:
            continue

        summary["series_scanned"] += 1
        anime_name = _series_title_from_dir(series_dir)
        season_info = fetch_anilist_season_info(anime_name)
        related_movies = fetch_anilist_related_movies(anime_name)

        print()
        print(f"  {c(C.AMBER, anime_name)}")

        season00 = series_dir / "Season 00"
        for season_dir in season_dirs:
            season_match = season_pat.match(season_dir.name)
            if not season_match:
                continue
            season_num = int(season_match.group(1))
            default_format = str((season_info.get(season_num) or {}).get("format") or "")
            default_year = (season_info.get(season_num) or {}).get("year")

            for video in sorted(season_dir.iterdir()):
                if not video.is_file() or video.suffix.lower() not in _VIDEO_EXT:
                    continue
                summary["videos_scanned"] += 1
                info = _classify_batch_file(
                    video.name,
                    anime_name=anime_name,
                    default_format=default_format,
                    default_year=default_year,
                    related_movies=related_movies,
                )
                cat = info["category"]

                if cat == "movie":
                    movie_title = info.get("movie_title") or anime_name
                    movie_year = info.get("movie_year")
                    dest = build_movie_target_path(
                        movie_root,
                        movie_title,
                        video.suffix,
                        movie_year,
                        create_dirs=not dry_run,
                    )
                    if dest.exists():
                        summary["skipped"] += 1
                        print(f"    {c(C.WARN, '[SKIP]')} {c(C.MUTED, video.name)} {c(C.DIM, '-> movie target already exists')}")
                        continue
                    try:
                        if not dry_run:
                            shutil.move(str(video), str(dest))
                        _route_subtitle_sidecars(video, dest if not dry_run else video, dest.parent)
                        summary["movies_moved"] += 1
                        print(f"    {c(C.SUCCESS, 'movie')} {c(C.MUTED, video.name[:48])} {c(C.DIM, '->')} {c(C.VALUE, dest.parent.name)}")
                    except OSError as exc:
                        summary["errors"] += 1
                        print(f"    {c(C.WARN, '[ERROR]')} {c(C.MUTED, video.name)} {c(C.DIM, str(exc))}")
                    continue

                if cat != "tv" and season_num != 0:
                    dest = season00 / video.name
                    if dest.exists():
                        summary["skipped"] += 1
                        print(f"    {c(C.WARN, '[SKIP]')} {c(C.MUTED, video.name)} {c(C.DIM, '-> Season 00 target already exists')}")
                        continue
                    try:
                        if not dry_run:
                            season00.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(video), str(dest))
                        _route_subtitle_sidecars(video, dest if not dry_run else video, season00)
                        summary["specials_moved"] += 1
                        print(f"    {c(C.CYAN_DIM, 'special')} {c(C.MUTED, video.name[:48])} {c(C.DIM, '->')} {c(C.VALUE, 'Season 00')}")
                    except OSError as exc:
                        summary["errors"] += 1
                        print(f"    {c(C.WARN, '[ERROR]')} {c(C.MUTED, video.name)} {c(C.DIM, str(exc))}")
                    continue

                summary["already_ok"] += 1

        normalize_series_filenames_for_jellyfin(series_dir, dry_run=dry_run)

    print()
    divider("Reconcile Summary")
    print(f"  {c(C.DIM, 'Series scanned:')} {c(C.VALUE2, str(summary['series_scanned']))}")
    print(f"  {c(C.DIM, 'Videos scanned:')} {c(C.VALUE2, str(summary['videos_scanned']))}")
    print(f"  {c(C.DIM, 'Movies moved:')} {c(C.SUCCESS, str(summary['movies_moved']))}")
    print(f"  {c(C.DIM, 'Specials moved:')} {c(C.CYAN_DIM, str(summary['specials_moved']))}")
    print(f"  {c(C.DIM, 'Already OK:')} {c(C.DIM, str(summary['already_ok']))}")
    print(f"  {c(C.DIM, 'Skipped:')} {c(C.WARN if summary['skipped'] else C.DIM, str(summary['skipped']))}")
    print(f"  {c(C.DIM, 'Errors:')} {c(C.WARN if summary['errors'] else C.DIM, str(summary['errors']))}")

    if not dry_run:
        print()
        trigger_jellyfin_rescan("Library Reconcile / TV", anime_dir)
        trigger_jellyfin_rescan("Library Reconcile / Movies", movie_root)

    return summary


def poll_rss_for_entry(entry: dict) -> list[dict]:
    """
    Fetch the RSS feed for a watch entry and return ONE torrent per new episode.

    Resolution preference: 1080p -> 720p -> 480p (or whatever is highest available).
    Within the same resolution, the torrent with the most seeders+leechers wins.
    Uses nyaa:seeders / nyaa:leechers / nyaa:infoHash namespace fields from the feed.
    """
    import xml.etree.ElementTree as ET

    NYAA_NS   = "https://nyaa.si/xmlns/nyaa"
    RES_ORDER = ["1080p", "720p", "480p", "360p"]   # preference high -> low

    def _res(title: str) -> str:
        """Extract resolution string from title, e.g. '1080p'."""
        m = re.search(r"\b(2160|1080|720|480|360)p\b", title, re.IGNORECASE)
        return m.group(0).lower() if m else "unknown"

    def _res_rank(res: str) -> int:
        """Lower number = more preferred."""
        try:
            return RES_ORDER.index(res)
        except ValueError:
            return len(RES_ORDER)   # unknown resolution ranked last

    def _popularity(item_dict: dict) -> int:
        return item_dict.get("seeders", 0) + item_dict.get("leechers", 0)

    try:
        resp = requests.get(entry["rss_url"], timeout=15,
                            headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return []

        root  = ET.fromstring(resp.content)
        items = root.findall(".//item")

        # Collect all qualifying candidates keyed by episode number
        # candidates[ep] = list of dicts, one per resolution/release found
        candidates: dict[int, list[dict]] = {}

        for item in items:
            title = (item.findtext("title") or "").strip()
            if not title:
                continue

            # -- Group filter --------------------------------------------------
            grp = extract_sub_group(title)
            if grp and grp != entry["group"].lower():
                continue

            # -- Season filter - skip if season tag mismatches -----------------
            ep_season = extract_season_number(title)
            if ep_season != entry["season"]:
                continue

            # -- Episode number ------------------------------------------------
            ep = extract_episode_number(title)
            if ep is None or ep <= entry["last_episode"]:
                continue

            # -- Torrent URL ---------------------------------------------------
            link        = item.findtext("link") or ""
            enc         = item.find("enclosure")
            torrent_url = (enc.get("url") if enc is not None else None) or link
            if torrent_url and not torrent_url.endswith(".torrent"):
                torrent_url = torrent_url.replace("/view/", "/download/") + ".torrent"

            # -- Nyaa namespace fields (seeders, leechers, infoHash) -----------
            def _nyaa(tag: str) -> str:
                return (item.findtext(f"{{{NYAA_NS}}}{tag}") or "").strip()

            try:
                seeders  = int(_nyaa("seeders"))
            except ValueError:
                seeders  = 0
            try:
                leechers = int(_nyaa("leechers"))
            except ValueError:
                leechers = 0
            info_hash = _nyaa("infoHash").lower() or None

            resolution = _res(title)

            # Parse pubDate - RSS format: "Thu, 27 Feb 2026 14:30:00 +0000"
            pub_date_raw = (item.findtext("pubDate") or "").strip()
            try:
                from email.utils import parsedate_to_datetime
                pub_dt   = parsedate_to_datetime(pub_date_raw)
                pub_date = pub_dt.strftime("%Y-%m-%d %H:%M UTC")
            except Exception:
                pub_date = pub_date_raw[:16] if pub_date_raw else "unknown"

            candidate = {
                "title":       title,
                "torrent_url": torrent_url,
                "magnet_url":  (f"magnet:?xt=urn:btih:{info_hash}" if info_hash
                                else None),
                "detail_url":  link,
                "seeders":     seeders,
                "leechers":    leechers,
                "resolution":  resolution,
                "pub_date":    pub_date,
                "episode_num": ep,
            }
            candidates.setdefault(ep, []).append(candidate)

        if not candidates:
            return []

        # -- Per-episode: pick best resolution then highest popularity ---------
        chosen: list[dict] = []
        for ep in sorted(candidates):
            options = candidates[ep]

            # Find the best available resolution across all options for this ep
            best_res_rank = min(_res_rank(c["resolution"]) for c in options)
            best_res      = RES_ORDER[best_res_rank] if best_res_rank < len(RES_ORDER) else "unknown"

            # Filter to only that resolution, then pick most popular
            same_res = [c for c in options if c["resolution"] == best_res]
            winner   = max(same_res, key=_popularity)
            chosen.append(winner)

            if len(options) > 1:
                seeds  = winner["seeders"]
                leech  = winner["leechers"]
                wtitle = winner["title"][:45]
                print(f"  {c(C.DIM, f'E{ep:>3}: {len(options)} releases - '
                      f'{best_res} [{seeds}S/{leech}L] {wtitle}')}")

        return chosen

    except Exception as exc:
        print(f"  {yellow('[WARN]')} RSS poll failed for {entry['anime_name']}: {exc}")
        return []


def run_watch_mode(args: argparse.Namespace, anime_dir: Path) -> None:
    """
    --watch mode: poll RSS for all active watch list entries,
    download new episodes, route to media path, update last_episode.
    """
    entries = load_watchlist(anime_dir)
    active  = [e for e in entries if not e.get("completed")]

    print()
    divider("Watch Mode - Polling RSS")

    if not active:
        print(f"  {c(C.DIM, 'No active watch entries.')}")
        print(f"  {c(C.DIM, 'Run the pipeline on an airing show first to register it.')}")
        return

    print(f"  {c(C.LABEL, 'Entries to check:')} {c(C.VALUE2, str(len(active)))}")
    print(f"  {c(C.LABEL, 'Watch list:')} {c(C.VALUE, str(watchlist_path(anime_dir)))}")


    try:
        client = connect_qbit()
    except Exception as exc:
        print(red(f"[ERROR] Could not connect to qBittorrent: {exc}"))
        return

    delete_files = not hasattr(args, "cleanup_files") or args.cleanup_files

    any_new = False
    any_new = False
    for entry in active:
        print()
        divider(f"{entry['anime_name']} - Season {entry['season']}")
        print(f"  {c(C.LABEL, 'Group:')}        {c(C.VALUE, f'[{entry["group"]}]')}")

        # -- Show metadata card from AniList -----------------------------------
        try:
            _sinfo = fetch_anilist_season_info(entry["anime_name"])
            _smeta = _sinfo.get(entry["season"], {})
            if _smeta:
                print_show_card(_smeta)
        except Exception:
            pass

        ep_total = (f"{entry['total_episodes']}" if entry.get("total_episodes")
                    else "unknown")
        print(f"  {c(C.LABEL, 'Total eps:')}    {c(C.VALUE2, ep_total)}")
        # -- Filesystem scan - source of truth ---------------------------------
        # Always check disk before trusting the JSON last_episode value.
        # Files may have been manually added, deleted, or JSON may be stale.
        on_disk      = scan_episodes_on_disk(entry["save_path"])
        disk_highest = max(on_disk) if on_disk else 0
        json_highest = entry.get("last_episode", 0)

        if on_disk:
            print(f"  {c(C.LABEL, 'On disk:')}      "
                  f"{c(C.VALUE2, str(len(on_disk)))} ep(s)  "
                  f"{c(C.DIM, f'(highest: E{disk_highest})')}")
        else:
            print(f"  {c(C.LABEL, 'On disk:')}      "
                  f"{c(C.DIM, 'none found in ')} "
                  f"{c(C.VALUE, entry['save_path'])}")

        # Reconcile disk vs JSON
        effective_last = max(disk_highest, json_highest)
        if disk_highest > json_highest:
            print(f"  {c(C.AMBER, '[SYNC]')} Disk has E{disk_highest}, "
                  f"JSON said E{json_highest} - updating JSON to match disk.")
            entry["last_episode"] = disk_highest
            entry["updated"]      = datetime.date.today().isoformat()
            save_watchlist(anime_dir, entries)
        elif json_highest > disk_highest and disk_highest > 0:
            print(f"  {c(C.YELLOW, '[SYNC]')} JSON said E{json_highest} "
                  f"but only E{disk_highest} on disk - resetting to disk state.")
            entry["last_episode"] = disk_highest
            entry["updated"]      = datetime.date.today().isoformat()
            save_watchlist(anime_dir, entries)
            effective_last        = disk_highest

        print(f"  {c(C.LABEL, 'Last episode:')} "
              f"{c(C.VALUE2, str(effective_last))} "
              f"{c(C.DIM, '(disk-verified)')}")
        print(f"  {c(C.DIM, 'Polling RSS ...')}")

        # Pass the disk-reconciled last_episode to the RSS poller
        polling_entry                  = dict(entry)
        polling_entry["last_episode"]  = effective_last

        new_eps = poll_rss_for_entry(polling_entry)
        if not new_eps:
            print(f"  {c(C.SUCCESS, '✓')} No new episodes.")
            continue

        new_eps.sort(key=lambda r: r["episode_num"])
        print(f"  {c(C.AMBER_B, f'{len(new_eps)} new episode(s) found:')}")
        for ep in new_eps:
            pub     = ep.get("pub_date", "")
            pub_str = f"  {c(C.DIM, pub)}" if pub else ""
            print(f"    {c(C.ORANGE, f'E{ep["episode_num"]:>3}')}  "
                  f"{c(C.MUTED, ep['title'][:50])}"
                  f"{pub_str}")

        any_new   = True
        save_path = entry["save_path"]
        Path(save_path).mkdir(parents=True, exist_ok=True)


        # Add to qBittorrent
        monitor = CleanupMonitor(client, delete_files=delete_files)
        monitor.start()
        added = 0
        for ep in new_eps:
            label = f"S{entry['season']:02d}E{ep['episode_num']:>3}"
            try:
                info_hash = add_torrent(client, ep,
                                        save_path=save_path,
                                        category=QBIT_CATEGORY)
                if info_hash and info_hash != "__lookup__":
                    monitor.watch(info_hash, label)
                    print(f"  {c(C.SUCCESS, '✓')} {c(C.ORANGE, label)}  "
                          f"{c(C.CYAN_DIM, info_hash)}")
                    added += 1
                elif info_hash == "__lookup__":
                    print(f"  {c(C.SUCCESS, '✓')} {c(C.ORANGE, label)}  "
                          f"{c(C.INFO_DIM, '(hash pending)')}")
                    added += 1
            except Exception as exc:
                print(f"  {c(C.WARN, 'X')} {c(C.ORANGE, label)}  {c(C.WARN, str(exc))}")

        if added == 0:
            monitor.stop()
            continue

        print(f"\n  {c(C.LABEL, 'Monitoring')} {c(C.VALUE2, str(monitor.pending_count()))} "
              f"torrent(s) ...")
        try:
            while monitor.pending_count() > 0:
                time.sleep(5)
        except KeyboardInterrupt:
            pass
        monitor.stop()

        season_label = f"{entry['anime_name']} Season {entry['season']}"
        series_dir = Path(save_path).parent
        _norm = normalize_series_filenames_for_jellyfin(series_dir, dry_run=args.dry_run)
        print(f"  {c(C.DIM, 'Filename normalize:')} "
              f"{c(C.SUCCESS, str(_norm['renamed']))} renamed, "
              f"{c(C.DIM, str(_norm['already']))} already canonical, "
              f"{c(C.WARN if _norm['collisions'] else C.DIM, str(_norm['collisions']) + ' collisions')}, "
              f"{c(C.WARN if _norm['skipped'] else C.DIM, str(_norm['skipped']) + ' skipped')}")
        print(f"  {c(C.DIM, 'Subtitle normalize:')} "
              f"{c(C.SUCCESS, str(_norm.get('sub_renamed', 0)))} renamed, "
              f"{c(C.WARN if _norm.get('sub_skipped', 0) else C.DIM, str(_norm.get('sub_skipped', 0)) + ' skipped')}")

        # Update last_episode in watch list
        highest_new = max(ep["episode_num"] for ep in new_eps)
        entry["last_episode"] = max(entry["last_episode"], highest_new)
        entry["updated"]      = datetime.date.today().isoformat()
        if (entry.get("total_episodes") and
                entry["last_episode"] >= entry["total_episodes"]):
            entry["completed"] = True
            print(f"  {c(C.SUCCESS, '✓')} Season complete - removing from active watch.")
        save_watchlist(anime_dir, entries)

        trigger_jellyfin_rescan(season_label, anime_dir)

    if not any_new:
        print()
        print(f"  {c(C.SUCCESS, '✓')} All entries up to date - nothing to download.")


# -----------------------------------------------------------------------------
# Jellyfin library rescan
# -----------------------------------------------------------------------------

_JELLY_CONFIG_FILE = Path(__file__).with_name("lito1_jellyfin.json")


def load_jellyfin_config() -> dict | None:
    """
    Load Jellyfin connection details from lito1_jellyfin.json.

    Returns a config dict if the file exists and contains a valid URL + API key.
    Returns None silently in all other cases - missing file, opted-out, malformed.

    To set up Jellyfin integration, run:
        python lito1.py --setup-jellyfin
    """
    if not _JELLY_CONFIG_FILE.exists():
        return None   # not configured - silently skip, no prompt
    try:
        cfg = json.loads(_JELLY_CONFIG_FILE.read_text(encoding="utf-8"))
        if cfg.get("skip"):
            return None
        if cfg.get("url") and cfg.get("api_key"):
            return cfg
    except Exception:
        pass
    return None


def _validate_jellyfin_url(url: str) -> tuple[bool, str]:
    """
    Sanitize and validate a Jellyfin base URL.
    Returns (is_valid, sanitized_url_or_error_message).
    """
    url = url.strip().rstrip("/")

    if not url:
        return False, "URL cannot be empty"

    # Must start with http:// or https://
    if not re.match(r"^https?://", url, re.IGNORECASE):
        return False, "URL must start with http:// or https://"

    # Must include a port number - Jellyfin default is 8096
    if not re.search(r":\d{2,5}$", url):
        return False, "URL must include a port number (e.g. :8096)"

    # Port must be in valid range
    port_match = re.search(r":(\d+)$", url)
    if port_match:
        port = int(port_match.group(1))
        if not (1 <= port <= 65535):
            return False, f"Port {port} is out of valid range (1-65535)"

    # Should not have a path component beyond the root
    from urllib.parse import urlparse
    parsed = urlparse(url)
    if parsed.path and parsed.path != "/":
        # Strip any accidental path - Jellyfin always runs at the root
        url = f"{parsed.scheme}://{parsed.netloc}"

    return True, url


def test_jellyfin_connectivity(url: str, api_key: str) -> tuple[bool, str]:
    """
    Attempt to reach the Jellyfin API and confirm the API key is valid.
    Returns (success, message).
    """
    headers = {
        "Authorization": f'MediaBrowser Token="{api_key}"',
        "Content-Type":  "application/json",
    }
    # /System/Info/Public doesn't require auth - confirms the server is up
    # /System/Info requires auth - confirms the API key is valid
    try:
        # Step 1: confirm server is reachable at all
        pub_resp = requests.get(f"{url}/System/Info/Public",
                                headers=headers, timeout=8)
        if pub_resp.status_code == 404:
            return False, "Server responded but /System/Info/Public not found - is this a Jellyfin server?"
        if pub_resp.status_code not in (200, 401):
            return False, f"Unexpected response from server: HTTP {pub_resp.status_code}"

        # Step 2: confirm API key works
        auth_resp = requests.get(f"{url}/System/Info",
                                 headers=headers, timeout=8)
        if auth_resp.status_code == 401:
            return False, "Server is reachable but API key was rejected - check the key in Jellyfin Dashboard -> API Keys"
        if auth_resp.status_code == 200:
            info = auth_resp.json()
            server_name = info.get("ServerName", "Jellyfin")
            version     = info.get("Version", "?")
            return True, f"Connected to {c(C.VALUE, server_name)} (v{version})"

        return False, f"Auth check returned HTTP {auth_resp.status_code}"

    except requests.exceptions.ConnectionError:
        return False, f"Connection refused - is Jellyfin running at {url}?"
    except requests.exceptions.Timeout:
        return False, f"Connection timed out - server did not respond within 8s"
    except Exception as exc:
        return False, f"Unexpected error: {exc}"


def show_jellyfin_startup_status(prompt_on_failure: bool = True) -> None:
    """Print a consistent Jellyfin status line across run modes."""
    _jcfg = load_jellyfin_config()
    if _jcfg:
        _jok, _jmsg = test_jellyfin_connectivity(_jcfg["url"], _jcfg["api_key"])
        if _jok:
            print(f"  {c(C.SUCCESS, '✓')} Jellyfin: {_jmsg}")
            return

        print(f"  {yellow('[WARN]')} Jellyfin unreachable: {_jmsg}")
        print(f"  {c(C.DIM, 'The pipeline will run normally but library rescan will be skipped.')}")
        if not prompt_on_failure:
            print()
            return

        print()
        _ans = input(
            f"  {c(C.PROMPT_CLR, 'Continue without Jellyfin rescan?')} "
            f"{c(C.DIM, '[Y/n]')} : "
        ).strip().lower()
        if _ans in ("n", "no"):
            print(f"  {c(C.DIM, 'Aborted. Fix Jellyfin connectivity and re-run.')}")
            finish_run(any_failure=False, exit_code=0)
        return

    print(f"  {c(C.DIM, 'Jellyfin: not configured  (run: lito1 --setup-jellyfin)')}")


def setup_jellyfin_config() -> None:
    """
    Interactive one-time setup for Jellyfin integration.
    Validates URL format, tests connectivity, and confirms API key before saving.
    Called only when --setup-jellyfin is passed.
    """
    print()
    divider("Jellyfin - Setup")
    print(f"  {c(C.DIM, 'Config will be saved to:')} {c(C.VALUE, str(_JELLY_CONFIG_FILE))}")
    print(f"  {c(C.DIM, 'Get your API key: Jellyfin -> Dashboard -> API Keys -> + Add')}")
    print(f"  {c(C.DIM, 'Leave URL blank to disable/clear Jellyfin rescan.')}")
    print()

    # -- URL input + validation ------------------------------------------------
    while True:
        raw_url = input(
            f"  {c(C.PROMPT_CLR, 'Jellyfin URL')} "
            f"{c(C.AMBER_B, '[e.g. http://192.168.1.50:8096]')} : "
        ).strip()

        if not raw_url:
            if _JELLY_CONFIG_FILE.exists():
                _JELLY_CONFIG_FILE.unlink()
                print(f"  {c(C.DIM, 'Jellyfin config removed - rescan will be skipped.')}")
            else:
                print(f"  {c(C.DIM, 'No URL entered - Jellyfin rescan remains disabled.')}")
            return

        valid, result = _validate_jellyfin_url(raw_url)
        if not valid:
            print(f"  {yellow('[INVALID]')} {c(C.WARN, result)}")
            print(f"  {c(C.DIM, 'Please re-enter the URL.')}")
            continue

        url = result
        if url != raw_url.rstrip("/"):
            print(f"  {c(C.DIM, f'URL normalised to: {url}')}")
        break

    # -- API key input ---------------------------------------------------------
    api_key = input(f"  {c(C.PROMPT_CLR, 'API Key')} : ").strip()
    if not api_key:
        print(f"  {yellow('[WARN]')} No API key entered - setup cancelled.")
        return

    # -- Connectivity test -----------------------------------------------------
    print()
    print(f"  {c(C.DIM, f'Testing connection to {url} ...')}")
    ok, message = test_jellyfin_connectivity(url, api_key)
    if ok:
        print(f"  {c(C.SUCCESS, '✓')} {message}")
    else:
        print(f"  {c(C.WARN, 'X')} {message}")
        print()
        retry = input(
            f"  {c(C.PROMPT_CLR, 'Save anyway?')} "
            f"{c(C.DIM, '[y/N]')} : "
        ).strip().lower()
        if retry != "y":
            print(f"  {c(C.DIM, 'Setup cancelled - config not saved.')}")
            return

    # -- Save ------------------------------------------------------------------
    cfg = {"url": url, "api_key": api_key, "skip": False}
    _JELLY_CONFIG_FILE.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print(f"  {c(C.SUCCESS, '✓')} Saved to {c(C.VALUE, _JELLY_CONFIG_FILE.name)}")
    print(f"  {c(C.DIM, 'Jellyfin rescan will now run automatically after each completed download batch.')}")


def trigger_jellyfin_rescan(season_label: str, library_dir: Path | None = None) -> None:
    """
    Fire a targeted Jellyfin library refresh rather than a full global scan.

    A global POST to /Library/Refresh rescans the ENTIRE library which, for
    large anime collections, can take hours.  Instead this function:

      1. Authenticates with the stored API key.
      2. Queries /Library/VirtualFolders to resolve the ItemId of the target
         virtual library (matched by the configured folder path when available).
      3. POSTs to /Items/{ItemId}/Refresh - rescanning only that library.
      4. Falls back to the global endpoint if VirtualFolders lookup fails.

    Never raises; degrades gracefully with a user-visible warning on failure.
    """
    try:
        cfg = load_jellyfin_config()
        if not cfg:
            return

        # Use the URL exactly as configured - Jellyfin is often on a different
        # machine on the LAN, so never substitute localhost.
        base_url = cfg["url"]

        api_key = os.environ.get("JELLYFIN_API_KEY") or cfg.get("api_key", "")
        headers = {
            "Authorization": f'MediaBrowser Token="{api_key}"',
            "Content-Type":  "application/json",
        }

        # -- Step 1: resolve target library ItemId via VirtualFolders ----------
        target_item_id: str | None = None
        target_library_name: str | None = None
        _library_dir_norm = str(library_dir).replace("\\", "/").rstrip("/").lower() if library_dir else ""
        try:
            vf_resp = requests.get(
                f"{base_url}/Library/VirtualFolders",
                headers=headers,
                timeout=10,
            )
            if vf_resp.status_code == 200:
                folders = vf_resp.json()
                for folder in folders:
                    folder_name = (folder.get("Name") or "").lower()
                    locations = folder.get("Locations") or []
                    locations_norm = {
                        str(loc).replace("\\", "/").rstrip("/").lower()
                        for loc in locations
                    }
                    if _library_dir_norm and _library_dir_norm in locations_norm:
                        target_item_id = folder.get("ItemId")
                        target_library_name = folder.get("Name")
                        log.info("Jellyfin: matched library path %r -> ItemId %s",
                                 library_dir, target_item_id)
                        break
                    if not _library_dir_norm and "anime" in folder_name:
                        target_item_id = folder.get("ItemId")
                        target_library_name = folder.get("Name")
                        log.info("Jellyfin: matched library %r -> ItemId %s",
                                 folder.get("Name"), target_item_id)
                        break
            else:
                log.warning("Jellyfin VirtualFolders returned HTTP %d",
                            vf_resp.status_code)
        except Exception as vf_exc:
            log.warning("Jellyfin VirtualFolders lookup failed: %s", vf_exc)

        # -- Step 2: targeted item refresh -------------------------------------
        if target_item_id:
            resp = requests.post(
                f"{base_url}/Items/{target_item_id}/Refresh",
                headers=headers,
                params={
                    "Recursive":                True,
                    "ImageRefreshMode":         "Default",
                    "MetadataRefreshMode":      "Default",
                    "ReplaceAllImages":         False,
                    "ReplaceAllMetadata":       False,
                },
                timeout=15,
            )
            if resp.status_code in (200, 204):
                _jf_sep = c(C.GREEN, "  " + "-" * 50)
                print()
                print(_jf_sep)
                print(f"  {c(C.GREEN, 'OK  JELLYFIN LIBRARY SCAN TRIGGERED')}")
                print(f"     {c(C.WHITE, season_label)}"
                      f"  {c(C.DIM, f' |  {target_library_name or "library"} |  ItemId {target_item_id}')}")
                print(_jf_sep)
                print()
                log.info("Jellyfin targeted refresh OK for ItemId=%s", target_item_id)
                return
            elif resp.status_code == 400:
                log.warning("Jellyfin /Items/%s/Refresh -> HTTP 400: %s",
                            target_item_id, resp.text[:200])
                # Fall through to global refresh
        else:
            log.info("Jellyfin: no matching anime library found, falling back to global refresh")

        # -- Step 3: fallback - global refresh (still beats no scan at all) ----
        resp = requests.post(
            f"{base_url}/Library/Refresh",
            headers=headers,
            timeout=10,
        )
        if resp.status_code in (200, 204):
            _jf_sep = c(C.GREEN, "  " + "-" * 50)
            print()
            print(_jf_sep)
            print(f"  {c(C.GREEN, 'OK  JELLYFIN LIBRARY SCAN TRIGGERED')}"
                  f"  {c(C.DIM, '(global fallback)')}")
            print(f"     {c(C.WHITE, season_label)}")
            print(_jf_sep)
            print()
            log.info("Jellyfin global refresh fallback OK")
        else:
            print(f"\n  {yellow('[WARN]')} Jellyfin rescan returned HTTP {resp.status_code} - "
                  f"please trigger a library scan manually in Jellyfin.\n")
            log.warning("Jellyfin rescan HTTP %d", resp.status_code)

    except requests.exceptions.ConnectionError:
        print(f"\n  {yellow('[WARN]')} Could not reach Jellyfin "
              f"{c(C.DIM, '(connection refused)')} - "
              f"please trigger a library scan manually.\n")
        log.warning("Jellyfin connection refused")
    except Exception as exc:
        print(f"\n  {yellow('[WARN]')} Jellyfin rescan skipped ({exc}) - "
              f"please trigger a library scan manually.\n")
        log.exception("Jellyfin rescan unexpected error")


def main() -> None:
    args = parse_args()
    global VISUAL_FUN, VISUAL_THEME
    VISUAL_FUN = getattr(args, "fun", False)
    VISUAL_THEME = getattr(args, "theme", "clean")
    apply_visual_theme(VISUAL_THEME)

    # -- Jellyfin setup mode ---------------------------------------------------
    if args.setup_jellyfin:
        setup_jellyfin_config()
        finish_run(any_failure=False, exit_code=0)

    # -- Watch status / watch mode - need anime dir first ---------------------
    if args.watch or args.watch_status:
        jellyfin_anime_dir = find_jellyfin_anime_dir()
        show_jellyfin_startup_status(prompt_on_failure=True)
        # Auto-rebuild watch list from disk if it doesn't exist or is empty
        if not watchlist_path(jellyfin_anime_dir).exists() or not load_watchlist(jellyfin_anime_dir):
            rebuild_watchlist_from_disk(jellyfin_anime_dir)
        if args.watch_status:
            print_watchlist_status(jellyfin_anime_dir)
            finish_run(any_failure=False, exit_code=0)
        if args.watch:
            run_watch_mode(args, jellyfin_anime_dir)
            finish_run(any_failure=False, exit_code=0)

    # -- Banner ----------------------------------------------------------------
    width = 76
    inner = width - 2
    title_str = "ANIME PIPELINE"
    pipe_str = "Nyaa  ->  qBittorrent  ->  Jellyfin"
    top_fill = "▓"
    side = "█"

    def _center_line(text: str, fill: str = " ") -> str:
        text = f" {text} "
        if len(text) >= inner:
            return text[:inner]
        pad_total = inner - len(text)
        left = pad_total // 2
        right = pad_total - left
        return (fill * left) + text + (fill * right)

    print(c(C.AMBER, top_fill * width))
    print(c(C.AMBER, side) + c(C.BANNER, _center_line("", " ")) + c(C.AMBER, side))
    print(c(C.AMBER, side) + c(C.BANNER, _center_line(title_str, " ")) + c(C.AMBER, side))
    print(c(C.AMBER, side) + c(C.BANNER, _center_line(pipe_str, "·")) + c(C.AMBER, side))
    print(c(C.AMBER, side) + c(C.BANNER, _center_line("", " ")) + c(C.AMBER, side))
    print(c(C.AMBER, top_fill * width))
    if VISUAL_FUN:
        print(f"  {magenta('[FUN]')} {dim(random.choice(FUN_ORACLE_LINES))}")
    print(f"  {dim('Theme:')} {amber(VISUAL_THEME)}")

    if not _ANITOPY_AVAILABLE:
        print(f"\n  {yellow('[WARN]')} anitopy not installed - using regex fallback for title parsing.")
        if _GUESSIT_AVAILABLE:
            print(f"  {green('[INFO]')} guessit detected - will be used as secondary parser.")
        else:
            print(f"  {c(C.DIM, 'For best results: pip install anitopy guessit')}\n")
    else:
        if _GUESSIT_AVAILABLE:
            print(f"  {dim('[INFO]')} anitopy + guessit both available (anitopy primary).")

    log.info("Pipeline startup - anitopy=%s guessit=%s config_toml=%s",
             _ANITOPY_AVAILABLE, _GUESSIT_AVAILABLE, _CONFIG_FILE.exists())

    # -------------------------------------------------------------------------
    # PHASE 1 - Locate Jellyfin Anime directory
    # -------------------------------------------------------------------------
    jellyfin_anime_dir = find_jellyfin_anime_dir()

    # -- Jellyfin connectivity check -------------------------------------------
    # Quick ping at startup so connectivity problems surface immediately rather
    # than silently failing hours later at the end of an encode run.
    _jcfg = load_jellyfin_config()
    if _jcfg:
        _jok, _jmsg = test_jellyfin_connectivity(_jcfg["url"], _jcfg["api_key"])
        if _jok:
            print(f"  {c(C.SUCCESS, '✓')} Jellyfin: {_jmsg}")
        else:
            print(f"  {yellow('[WARN]')} Jellyfin unreachable: {_jmsg}")
            print(f"  {c(C.DIM, 'The pipeline will run normally but library rescan will be skipped.')}")
            print()
            _ans = input(
                f"  {c(C.PROMPT_CLR, 'Continue without Jellyfin rescan?')} "
                f"{c(C.DIM, '[Y/n]')} : "
            ).strip().lower()
            if _ans in ("n", "no"):
                print(f"  {c(C.DIM, 'Aborted. Fix Jellyfin connectivity and re-run.')}")
                finish_run(any_failure=False, exit_code=0)
    else:
        print(f"  {c(C.DIM, 'Jellyfin: not configured  (run: lito1 --setup-jellyfin)')}")

    # Auto-rebuild watch list from disk if it doesn't exist or is empty
    if not watchlist_path(jellyfin_anime_dir).exists() or not load_watchlist(jellyfin_anime_dir):
        rebuild_watchlist_from_disk(jellyfin_anime_dir)

    if args.reconcile_library:
        reconcile_existing_media_library(jellyfin_anime_dir, dry_run=args.dry_run)
        finish_run(any_failure=False, exit_code=0)

    # -------------------------------------------------------------------------
    # PHASE 2 - Nyaa search + qBittorrent
    # -------------------------------------------------------------------------
    print(f"\n{SEP_THICK}")
    print(b_white("PHASE 2 - Nyaa Search + qBittorrent"))
    print(SEP_THICK)

    # -- Inputs ---------------------------------------------------------------
    anime_name = prompt("Anime series name")
    if not anime_name:
        sys.exit(red("[ERROR] No anime title provided."))

    # Normalise capitalisation for directory display - title-case the input
    # so "isekai quartet" becomes "Isekai Quartet", but preserve already-cased
    # input like "Re:ZERO" (only apply if the input is entirely lower-case).
    if anime_name == anime_name.lower():
        anime_name = anime_name.title()

    delete_files = args.cleanup_files

    # Safety check: --cleanup-files removes completed files from disk.
    # Keep it off unless you intentionally want ephemeral downloads.
    if delete_files:
        print()
        print(yellow("[WARN] --cleanup-files is enabled; completed media files will be removed by qBittorrent."))
        print(yellow("[WARN] Disable this unless your qBittorrent path is temporary."))

    # -- Broad nyaa search ----------------------------------------------------
    print()
    divider("Searching Nyaa.si")
    print(f"  {c(C.LABEL, 'Query:')} {c(C.VALUE, anime_name)}  "
          f"{c(C.INFO_DIM, f'(up to {args.pages} page(s))')}")
    print()
    raw = search_all_pages(anime_name, args.pages)
    print(f"\n  {c(C.LABEL, 'Total results:')} {c(C.VALUE2, str(len(raw)))}")

    if not raw:
        sys.exit(c(C.WARN, "\n[ERROR] No results found. Try a different title."))

    # -- Rank sub groups -------------------------------------------------------
    ranked = rank_sub_groups(raw)
    if not ranked:
        sys.exit(c(C.WARN, "[ERROR] Could not identify any sub groups in results."))

    # -- Season discovery ------------------------------------------------------
    season_info = annotate_results_with_anilist_seasons(raw, anime_name)
    raw_season_map: dict[int, set] = {}
    season_inferred_map: dict[int, bool] = {}
    for r in raw:
        s = get_result_season_number(r)
        e = extract_episode_number(r["title"])
        if e is not None:
            raw_season_map.setdefault(s, set()).add(e)
            season_inferred_map[s] = season_inferred_map.get(s, False) or bool(r.get("_season_inferred"))

    season_map: dict[int, set] = {}
    suppressed_inferred: list[int] = []
    for s, eps in sorted(raw_season_map.items()):
        inferred = season_inferred_map.get(s, False)
        if inferred and not _inferred_season_confident(s, eps, season_info):
            suppressed_inferred.append(s)
            continue
        season_map[s] = eps

    batch_season_map, inspected_batches = enrich_season_map_from_batch_candidates(
        raw, anime_name, season_info
    )
    batch_only_seasons: set[int] = set()
    for s, eps in sorted(batch_season_map.items()):
        if s not in season_map:
            batch_only_seasons.add(s)
        season_map.setdefault(s, set()).update(eps)

    print()
    divider("Available Seasons")
    for s in sorted(season_map):
        inferred_suffix = f" {c(C.DIM, '(AniList alias inference)')}" if season_inferred_map.get(s) else ""
        batch_suffix = f" {c(C.DIM, '(batch file list)')}" if s in batch_only_seasons else ""
        print(f"  {c(C.AMBER_B, f'Season {s}')}"
              f"{c(C.AMBER, '  -  ')}"
              f"{c(C.VALUE2, str(len(season_map[s])))} episode(s)"
              f"{inferred_suffix}{batch_suffix}")
    if suppressed_inferred:
        sup = ", ".join(f"S{s:02d}" for s in suppressed_inferred)
        print(f"\n  {c(C.DIM, '[Info]')} {c(C.DIM, 'Suppressed low-confidence inferred seasons:')} "
              f"{c(C.MUTED, sup)}")
    if inspected_batches:
        print(f"  {c(C.DIM, '[Info]')} {c(C.DIM, 'Season map enriched from batch file list inspection.')}")

    # -- Season selection ------------------------------------------------------
    print()
    available_season_nums = sorted(season_map.keys())
    avail_str = ", ".join(str(s) for s in available_season_nums)

    if args.season is not None:
        seasons_to_run   = [args.season]
        all_seasons_mode = False
        print(f"  {c(C.LABEL, 'Season:')} {c(C.VALUE2, str(args.season))}")
    else:
        print(f"  {c(C.DIM, 'Available: ')} {c(C.AMBER_B, avail_str)}")
        print(f"  {c(C.DIM, 'Syntax   : ')} "
              f"{c(C.WHITE_DIM, '2        -> single season')}")
        print(f"  {c(C.DIM, '           ')} "
              f"{c(C.WHITE_DIM, '2,4      -> seasons 2 and 4 only')}")
        print(f"  {c(C.DIM, '           ')} "
              f"{c(C.WHITE_DIM, '2-4      -> seasons 2 through 4')}")
        print(f"  {c(C.DIM, '           ')} "
              f"{c(C.WHITE_DIM, 'Enter    -> all seasons')}")
        print()
        season_str = prompt("Season(s)", "").strip()

        if not season_str:
            # blank -> all seasons
            seasons_to_run   = available_season_nums
            all_seasons_mode = True

        elif re.match(r"^\d+(,\d+)+$", season_str):
            # comma list: "2,4" or "2,3,4"
            parsed = sorted({int(x) for x in season_str.split(",")})
            missing = [s for s in parsed if s not in season_map]
            if missing:
                print(f"  {yellow('[WARN]')} Season(s) not found in search results: "
                      f"{c(C.AMBER_B, ', '.join(str(s) for s in missing))} - "
                      f"will attempt targeted search per season.")
            seasons_to_run   = parsed
            all_seasons_mode = False

        elif re.match(r"^(\d+)-(\d+)$", season_str):
            # range: "2-4"
            m_range = re.match(r"^(\d+)-(\d+)$", season_str)
            lo, hi  = int(m_range.group(1)), int(m_range.group(2))
            if lo > hi:
                lo, hi = hi, lo
            seasons_to_run   = list(range(lo, hi + 1))
            all_seasons_mode = False
            missing = [s for s in seasons_to_run if s not in season_map]
            if missing:
                print(f"  {yellow('[WARN]')} Season(s) not found in search results: "
                      f"{c(C.AMBER_B, ', '.join(str(s) for s in missing))} - "
                      f"will attempt targeted search per season.")

        elif season_str.isdigit():
            # single number
            seasons_to_run   = [int(season_str)]
            all_seasons_mode = False

        else:
            print(f"  {yellow('[WARN]')} Could not parse {c(C.AMBER_B, repr(season_str))} - "
                  f"defaulting to all seasons.")
            seasons_to_run   = available_season_nums
            all_seasons_mode = True

        desc = ", ".join(f"Season {s}" for s in seasons_to_run)
        print(f"  {c(C.INFO, 'Running:')} {c(C.VALUE2, desc)}")

    # Connect qBittorrent once for all seasons
    print()
    divider("qBittorrent")
    print(f"  {c(C.LABEL, 'Connecting ...')}")
    client = connect_qbit()

    def _on_sigint(sig, frame):
        print(f"\n{c(C.WARN, 'Interrupted.')}")
        sys.exit(0)
    signal.signal(signal.SIGINT, _on_sigint)


    # -- Season loop -----------------------------------------------------------
    any_failure = False
    for season in seasons_to_run:
        ok = run_one_season(
            season, raw, ranked, anime_name, args,
            jellyfin_anime_dir, client, delete_files,
            auto_confirm=all_seasons_mode,
            allowed_seasons=set(seasons_to_run),
            season_info=season_info,
        )
        if not ok:
            any_failure = True

    finish_run(any_failure=any_failure, exit_code=(1 if any_failure else 0))

    # -- Final banner ----------------------------------------------------------
    print()
    print(c(C.AMBER, "=" * 64))
    print_completion_banner(any_failure)
    print(c(C.AMBER, "=" * 64))
    print()
    print_visual_footer(any_failure)
    sys.exit(1 if any_failure else 0)


if __name__ == "__main__":
    main()





