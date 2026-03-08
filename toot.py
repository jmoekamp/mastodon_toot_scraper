#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mastodon Comment Cache Generator
==================================
Recursively scans .markdown files, fetches Mastodon threads,
downloads avatars locally, respects a blocklist, generates
self-contained embeddable HTML fragments, and writes JSON
sidecar metadata.

Compatible with Python 3.7+

Usage:
    python toot.py /path/to/content
    python toot.py /path/to/content --token YOUR_TOKEN
    python toot.py /path/to/content --cleanup-tootcache
    python toot.py /path/to/content --force
    python toot.py /path/to/content --regenerate
    python toot.py /path/to/content --dry-run
    python toot.py --help

Exit codes:
    0 - Success (or dry-run)
    1 - One or more errors occurred (API failures, invalid directory, etc.)
"""

import argparse
import atexit
import configparser
import hashlib
import json
import sys
import os
import signal
import tempfile
import threading
import time
import html as H
import re

if sys.version_info < (3, 7):
    sys.exit("Python 3.7 or later is required.")

from html.parser import HTMLParser
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone, timedelta
try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False


# =========================================================================
# Defaults (all overridable via CLI)
# =========================================================================

DEFAULT_TOOT_CACHE  = "/var/tmp/gencache/mastodontootcache"
DEFAULT_AVATAR_DIR  = "/var/tmp/gencache/mastodonavatarcache"
DEFAULT_AVATAR_URL  = "https://www.blog.example.com/images/mastodonavatarcache"
DEFAULT_MEDIA_DIR   = "/var/tmp/gencache/mastodonmediacache"
DEFAULT_MEDIA_URL   = "https://www.blog.example.com/images/mastodonmediacache"
DEFAULT_MAX_AGE_H   = 3        # hours – toot cache TTL
DEFAULT_AV_AGE_D    = 30       # days  – avatar cache TTL
DEFAULT_MEDIA_AGE_D = 30       # days  – media cache TTL
DEFAULT_RATE_LIMIT  = 5.0      # seconds between API calls
DEFAULT_MAX_DEPTH   = 5        # max nesting depth
DEFAULT_HIGHLIGHT   = 10       # min favourites to highlight a toot
DEFAULT_FOLD_DEPTH  = 3        # fold subtrees at this depth
DEFAULT_FOLD_THRESH = 50       # only fold if total comments exceed this
DEFAULT_MAX_MEDIA   = 3        # max media attachments to embed per toot
DEFAULT_TIMEOUT     = 3540     # seconds (59 minutes) – global run timeout
DEFAULT_SINCE_WEEKS = 4        # only process posts from last N weeks
DEFAULT_RETRIES     = 3        # API retry attempts
DEFAULT_STALE_AFTER = "12m"    # skip threads with no reply for 12 months
DEFAULT_STALE_AGE_D = 7        # days  – cache TTL for stale threads
DEFAULT_PREFIX      = "https://social.example.com/@user/"
SIDECAR_VERSION     = 2

# =========================================================================
# Configurable labels (i18n)
# =========================================================================

DEFAULT_LABELS = {
    # Timestamps
    "edited":               "(edited)",
    "edited_title":         "Edited {date}",
    "posted_aria":          "Posted {date}",
    "generated":            "Generated {date}",

    # Content warnings / sensitive media
    "show_content":         "Show content",
    "sensitive_toggle":     "Sensitive content &ndash; click to show",

    # Filtered / blocked
    "post_hidden":          "This post is not displayed.",

    # Boost
    "boosted":              "boosted",
    "boost_duplicate":      "also boosted &ndash; original shown above",

    # Media placeholder (non-OP)
    "media_link":           "{n} media attachments &ndash; view on original toot",
    "media_link_1":         "{n} media attachment &ndash; view on original toot",
    "media_more":           "{n} more media &ndash; view on original toot",
    "media_more_1":         "{n} more &ndash; view on original toot",

    # Poll
    "poll_aria":            "Poll",
    "poll_final":           "Final results",
    "poll_open":            "Poll open",
    "voters":               "{n} voters",
    "voters_1":             "{n} voter",
    "votes":                "{n} votes",
    "votes_1":              "{n} vote",

    # Fold
    "fold_replies":         "{n} more replies",
    "fold_replies_1":       "{n} more reply",

    # ARIA labels
    "post_by_aria":         "Post by {handle}",
    "engagement_aria":      "Engagement",
    "replies_aria":         "{n} replies",
    "replies_aria_1":       "{n} reply",
    "boosts_aria":          "{n} boosts",
    "boosts_aria_1":        "{n} boost",
    "favs_aria":            "{n} favourites",
    "favs_aria_1":          "{n} favourite",
    "permalink":            "Permalink",
    "comments_aria":        "Mastodon comments",

    # Header / footer
    "mastodon":             "Mastodon",
    "comment_count":        "{n} comments",
    "comment_count_1":      "{n} comment",
    "comments_header":      "Comments",
    "article_label":        "Article",
    "reply_on":             "Reply on",
    "view_on":              "View on",
    "no_comments":          "No comments yet.",
    "be_first":             "Be the first to reply!",
    "comments_separator":   "Comments",

    # Thread stats
    "thread_stats":         "{total} toots from {users} people in this thread",

    # Truncation
    "more_replies":         "&hellip; and {n} more replies &ndash; continue on {instance}",
    "more_replies_1":       "&hellip; and {n} more reply &ndash; continue on {instance}",
}


def _pl(labels, key, n):
    """Pick singular (_1) or plural label for count *n*."""
    if n == 1:
        return labels.get(key + "_1", labels[key])
    return labels[key]


def load_labels(path):
    """
    Load label overrides from a key=value file.
    Lines starting with # or ; are comments.  Blank lines are skipped.
    Returns a full labels dict (defaults + overrides).
    """
    labels = dict(DEFAULT_LABELS)
    if not path:
        return labels
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for lineno, raw in enumerate(fh, 1):
                line = raw.strip()
                if not line or line[0] in ("#", ";"):
                    continue
                if "=" not in line:
                    log_warn("  LABELS  line {}: no '=' found, "
                             "skipping".format(lineno))
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip()
                if key not in DEFAULT_LABELS:
                    log_warn("  LABELS  line {}: unknown key '{}', "
                             "ignoring".format(lineno, key))
                    continue
                labels[key] = val
    except OSError as e:
        log_error("  LABELS  could not read {}: {}".format(path, e))
    return labels


# =========================================================================
# Logging (verbosity levels)
# =========================================================================

LOG_SILENT  = 0   # suppress everything, including errors
LOG_QUIET   = 1   # errors only
LOG_NORMAL  = 2   # default: errors, warnings, status, summary
LOG_VERBOSE = 3   # everything: debug info, cache hits, blocklist matches

_log_level = LOG_NORMAL


def set_log_level(level):
    global _log_level
    _log_level = level


def log_error(msg):
    """Always shown unless --silent."""
    if _log_level >= LOG_QUIET:
        print(msg, file=sys.stderr)


def log_warn(msg):
    """Shown at normal and verbose."""
    if _log_level >= LOG_NORMAL:
        print(msg, file=sys.stderr)


def log_info(msg):
    """Standard operational messages. Shown at normal and verbose."""
    if _log_level >= LOG_NORMAL:
        print(msg)


def log_debug(msg):
    """Detailed messages. Only shown with --verbose."""
    if _log_level >= LOG_VERBOSE:
        print(msg)


# =========================================================================
# Config file (INI format, [toot] section)
# =========================================================================

# Mapping: config key -> (argparse dest, type converter)
# Supports both underscores and hyphens in config keys.
_CONFIG_KEYS = {
    "token":          ("token",          str),
    "theme":          ("theme",          str),
    "toot_cache_dir": ("toot_cache_dir", str),
    "max_age":        ("max_age",        float),
    "avatar_dir":     ("avatar_dir",     str),
    "avatar_url":     ("avatar_url",     str),
    "avatar_max_age": ("avatar_max_age", int),
    "media_dir":      ("media_dir",      str),
    "media_url":      ("media_url",      str),
    "media_max_age":  ("media_max_age",  int),
    "stale_after":    ("stale_after",    str),
    "max_age_stale":  ("max_age_stale",  int),
    "prefix":         ("prefix",         str),
    "blocklist":      ("blocklist",      str),
    "whitelist":      ("whitelist",      str),
    "labels":         ("labels",         str),
    "rate_limit":     ("rate_limit",     float),
    "max_depth":      ("max_depth",      int),
    "retries":        ("retries",        int),
    "max_toots":      ("max_toots",      int),
    "highlight_above":("highlight_above", int),
    "fold_depth":     ("fold_depth",      int),
    "fold_threshold": ("fold_threshold",  int),
    "max_media":      ("max_media",       int),
    "css_extra":      ("css_extra",       str),
    "timeout":        ("timeout",         int),
    "sort":           ("sort",           str),
    "since":          ("since",          int),
}

# Boolean keys (true/false/yes/no/1/0 in config)
_CONFIG_BOOLS = {
    "force":          "force",
    "all":            "all",
    "hide_bots":      "hide_bots",
    "custom_emojis":  "custom_emojis",
    "article":        "article",
    "cleanup_tootcache": "cleanup_tootcache",
    "cleanup_avatars": "cleanup_avatars",
    "cleanup_media":  "cleanup_media",
    "cleanup_all":    "cleanup_all",
    "regenerate":      "regenerate",
    "regenerate_all":  "regenerate_all",
    "verbose":        "verbose",
    "quiet":          "quiet",
    "silent":         "silent",
}


def load_config(path):
    """
    Load configuration from an INI file.
    Expected format:

        [toot]
        token = abc123
        theme = dark
        toot_cache_dir = /var/tmp/gencache/mastodontootcache
        max_age = 3
        avatar_dir = /var/tmp/gencache/mastodonavatarcache
        avatar_url = https://www.blog.example.com/images/mastodonavatarcache
        avatar_max_age = 30
        media_dir = /var/tmp/gencache/mastodonmediacache
        media_url = https://www.blog.example.com/images/mastodonmediacache
        media_max_age = 30
        stale_after = 12m
        max_age_stale = 7
        prefix = https://social.example.com/@user/
        blocklist = /etc/mastodon-blocklist.txt
        whitelist = /etc/mastodon-whitelist.txt
        rate_limit = 5
        max_depth = 5
        since = 4
        verbose = true

    Returns a dict suitable for argparse set_defaults().
    Keys use underscores (matching argparse dest names).
    Hyphens in config keys are accepted and converted.
    """
    if not path or not os.path.isfile(path):
        return {}

    cp = configparser.ConfigParser()
    cp.read(path, encoding="utf-8")

    if not cp.has_section("toot"):
        return {}

    defaults = {}
    for raw_key, raw_val in cp.items("toot"):
        # Normalize: hyphens -> underscores
        key = raw_key.replace("-", "_")

        if key in _CONFIG_KEYS:
            dest, converter = _CONFIG_KEYS[key]
            try:
                defaults[dest] = converter(raw_val)
            except (ValueError, TypeError):
                log_warn("WARN: config: invalid value for '{}': {}".format(
                    raw_key, raw_val))
        elif key in _CONFIG_BOOLS:
            dest = _CONFIG_BOOLS[key]
            defaults[dest] = raw_val.lower() in ("true", "yes", "1")
        else:
            log_warn("WARN: config: unknown key '{}'".format(raw_key))

    return defaults


# =========================================================================
# Blocklist / Whitelist
# =========================================================================

def load_blocklist(path):
    """
    Load blocklist or whitelist file. One URL or URL-prefix per line.
    Lines starting with # or ; are comments. Empty lines ignored.
    """
    if not path or not os.path.isfile(path):
        return []
    entries = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            entries.append(line.rstrip("/"))
    return entries


def _matches_list(toot, entries):
    """
    Check if a toot matches any entry in a list.
      https://server/@user/12345  ->  matches that exact toot
      https://server/@user        ->  matches all toots by that user
      https://server              ->  matches all toots from that server
    """
    if not entries:
        return False

    toot_url = (toot.get("url") or toot.get("uri") or "").rstrip("/")
    acct_url = toot.get("account", {}).get("url", "").rstrip("/")

    parsed = urlparse(toot_url)
    server = ""
    if parsed.scheme and parsed.netloc:
        server = "{}://{}".format(parsed.scheme, parsed.netloc)

    for entry in entries:
        if toot_url and toot_url == entry:
            return True
        if acct_url and acct_url == entry:
            return True
        if toot_url and toot_url.startswith(entry + "/"):
            return True
        if server and server == entry:
            return True
    return False


def is_blocked(toot, blocklist):
    """Check if a toot matches any blocklist entry."""
    if _matches_list(toot, blocklist):
        log_debug("    BLOCK toot {} matched blocklist".format(
            toot.get("id", "?")))
        return True
    return False


def is_filtered(toot, blocklist, whitelist):
    """
    Check if a toot should be hidden.
    If a whitelist is provided, it takes precedence: toots matching
    the whitelist are always shown (even if also on the blocklist).
    Toots NOT on the whitelist are hidden.
    If no whitelist is provided, the blocklist alone decides.
    """
    if whitelist:
        if _matches_list(toot, whitelist):
            return False
        log_debug("    FILTER toot {} not on whitelist".format(
            toot.get("id", "?")))
        return True
    if is_blocked(toot, blocklist):
        return True
    return False


# =========================================================================
# Cache directory registry (cross-cache collision detection)
# =========================================================================

# Maps os.path.abspath(dir) → label.  Populated by register_cache_dirs().
_cache_registry = {}


def register_cache_dirs(**dirs):
    """
    Register cache directories and warn about overlaps.
    Call once from main() with label=path pairs, e.g.
      register_cache_dirs(avatar="/a", media="/m", toot="/t")
    """
    global _cache_registry
    global _hash_seen
    _cache_registry = {}
    _hash_seen = {}
    seen = {}
    for label, path in dirs.items():
        absp = os.path.abspath(path)
        if absp in seen:
            log_warn("WARN  Cache directory overlap: '{}' and '{}' both use "
                     "{}".format(seen[absp], label, absp))
        seen[absp] = label
        _cache_registry[absp] = label


def _check_cross_cache(fname, own_dir):
    """
    Check if *fname* already exists in a registered cache directory
    other than *own_dir*.  Logs a warning if found.
    """
    if not _cache_registry:
        return
    own_abs = os.path.abspath(own_dir)
    for reg_dir, reg_label in _cache_registry.items():
        if reg_dir == own_abs:
            continue
        cross_path = os.path.join(reg_dir, fname)
        if os.path.isfile(cross_path):
            log_warn("    WARN  {} exists in {} cache ({}), "
                     "also being written to {}".format(
                         fname[:20], reg_label, reg_dir, own_abs))


# Maps (cache_dir_abs, filename) → first URL seen.  Detects the
# astronomically unlikely case of two different URLs producing the
# same SHA-256 hash within a single cache directory.
_hash_seen = {}


def _check_hash_collision(fname, url, cache_dir):
    """
    Record that *fname* was produced by *url* in *cache_dir*.
    Warns if a *different* URL previously produced the same filename.
    """
    key = (os.path.abspath(cache_dir), fname)
    prev = _hash_seen.get(key)
    if prev is None:
        _hash_seen[key] = url
    elif prev != url:
        log_warn("    WARN  Hash collision in {}: {} maps to both "
                 "{} and {}".format(
                     cache_dir, fname[:20] + "...", prev[:60], url[:60]))


# =========================================================================
# Avatar: download, SVG placeholder, resolution
# =========================================================================

def avatar_filename(url):
    """SHA256(url) + original file extension."""
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    _, ext = os.path.splitext(urlparse(url).path)
    if not ext:
        ext = ".png"
    return digest + ext


def svg_placeholder(username):
    """
    Generate an inline SVG data-URI placeholder with the first
    letter of the username. Color derived from username hash.
    """
    letter = "?"
    if username:
        for ch in username:
            if ch.isalnum():
                letter = ch.upper()
                break

    # Derive a hue from the username hash (0-360)
    h = int(hashlib.md5(username.encode("utf-8")).hexdigest()[:8], 16) % 360
    bg = "hsl({},55%,45%)".format(h)

    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="80" height="80"'
        ' viewBox="0 0 80 80">'
        '<rect width="80" height="80" rx="12" fill="{}"/>'.format(bg) +
        '<text x="40" y="40" dy=".35em" text-anchor="middle"'
        ' font-family="sans-serif" font-size="38" font-weight="700"'
        ' fill="#fff">{}</text></svg>'.format(H.escape(letter))
    )
    import base64
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return "data:image/svg+xml;base64,{}".format(encoded)


def download_avatar(url, avatar_dir, max_age_s):
    """
    Download avatar to avatar_dir. Skip if fresh.
    Returns local filename or None on failure.
    """
    fname = avatar_filename(url)
    _check_hash_collision(fname, url, avatar_dir)
    local_path = os.path.join(avatar_dir, fname)

    try:
        age = time.time() - os.path.getmtime(local_path)
        if age < max_age_s:
            log_debug("    AVATAR cached: {} (age: {} days)".format(
                fname[:16] + "...", int(age // 86400)))
            return fname
    except OSError:
        pass

    try:
        req = Request(url, headers={
            "User-Agent": "MastodonCommentBot/1.0",
            "Accept": "image/*",
        })
        with urlopen(req, timeout=15) as resp:
            data = resp.read()
        _check_cross_cache(fname, avatar_dir)
        with open(local_path, "wb") as fh:
            fh.write(data)
        log_debug("    AVATAR downloaded: {}".format(fname[:16] + "..."))
        return fname
    except Exception as e:
        log_warn("    WARN  Avatar download failed: {}".format(e))
        return None


def resolve_avatar_src(toot, avatar_dir, avatar_base_url, av_max_age_s):
    """
    Download avatar, return public URL. On failure, return
    inline SVG placeholder with first letter + hash-based color.
    """
    acct = toot.get("account", {})
    remote = acct.get("avatar_static", acct.get("avatar", ""))
    username = acct.get("acct", acct.get("username", "?"))

    if not remote:
        log_debug("    AVATAR placeholder for @{} (no URL)".format(username))
        return svg_placeholder(username)

    fname = download_avatar(remote, avatar_dir, av_max_age_s)
    if fname:
        return "{}/{}".format(avatar_base_url.rstrip("/"), fname)

    log_debug("    AVATAR placeholder for @{} (download failed)".format(username))
    return svg_placeholder(username)


def media_filename(url):
    """SHA256(url) + original file extension."""
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    _, ext = os.path.splitext(urlparse(url).path)
    if not ext:
        ext = ".jpg"
    return digest + ext


def download_media(url, media_dir, max_age_s):
    """
    Download media file to media_dir.  Skip if fresh.
    Returns local filename or None on failure.
    """
    fname = media_filename(url)
    _check_hash_collision(fname, url, media_dir)
    local_path = os.path.join(media_dir, fname)

    try:
        age = time.time() - os.path.getmtime(local_path)
        if age < max_age_s:
            log_debug("    MEDIA  cached: {} (age: {} days)".format(
                fname[:16] + "...", int(age // 86400)))
            return fname
    except OSError:
        pass

    try:
        req = Request(url, headers={
            "User-Agent": "MastodonCommentBot/1.0",
            "Accept": "image/*,video/*,audio/*",
        })
        with urlopen(req, timeout=30) as resp:
            data = resp.read()
        _check_cross_cache(fname, media_dir)
        with open(local_path, "wb") as fh:
            fh.write(data)
        log_debug("    MEDIA  downloaded: {}".format(fname[:16] + "..."))
        return fname
    except Exception as e:
        log_warn("    WARN  Media download failed: {}".format(e))
        return None


def resolve_media_src(url, media_dir, media_base_url, max_age_s):
    """
    Download media file, return public URL.
    On failure, fall back to original URL.
    """
    if not media_dir or not media_base_url:
        return url
    fname = download_media(url, media_dir, max_age_s)
    if fname:
        return "{}/{}".format(media_base_url.rstrip("/"), fname)
    return url


# =========================================================================
# Frontmatter parser (no PyYAML dependency)
# =========================================================================

def parse_frontmatter(filepath):
    """Read YAML frontmatter between --- markers. Returns dict or None."""
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            text = fh.read()
    except Exception:
        return None

    if not text.startswith("---"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None

    fm = {}
    for line in text[3:end].strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, _, val = line.partition(":")
        key = key.strip().lower()
        val = val.strip()
        if len(val) >= 2 and val[0] in ('"', "'") and val[-1] == val[0]:
            val = val[1:-1]
        fm[key] = val
    return fm


def is_enabled(fm):
    return fm.get("mastodoncomment", "").lower() in ("true", "yes", "1")


def get_post_date(fm):
    """
    Parse the 'date' field from frontmatter. Returns datetime or None.
    Tries several common formats.
    """
    raw = fm.get("date", "").strip()
    if not raw:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]

    # Handle Z suffix
    raw_clean = raw.replace("Z", "+00:00")

    for fmt in formats:
        try:
            return datetime.strptime(raw_clean, fmt)
        except ValueError:
            continue

    # Try fromisoformat (3.7+)
    try:
        return datetime.fromisoformat(raw_clean)
    except (ValueError, AttributeError):
        pass

    return None


def resolve_toot_url(raw, prefix=DEFAULT_PREFIX):
    """Returns (base_url, toot_id, full_url)."""
    raw = raw.strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        full_url = raw
    else:
        full_url = prefix.rstrip("/") + "/" + raw

    parsed = urlparse(full_url)
    base = "{}://{}".format(parsed.scheme, parsed.netloc)
    toot_id = None
    for seg in reversed(parsed.path.strip("/").split("/")):
        if seg.isdigit():
            toot_id = seg
            break
    if not toot_id:
        raise ValueError("No toot ID found in '{}'".format(raw))
    return base, toot_id, full_url


# =========================================================================
# Mastodon API
# =========================================================================

_RETRYABLE_CODES = {429, 500, 502, 503, 504}

# Threshold: start slowing down when fewer than this many requests remain.
_RL_LOW_THRESHOLD = 15

# Module-level state: updated after every successful api_get call.
_last_ratelimit = {"remaining": None, "reset": None}

# Graceful shutdown: set by SIGTERM/SIGINT handler, checked in main loop.
_shutdown_requested = False


def _shutdown_handler(signum, frame):
    """Signal handler for SIGTERM/SIGINT: request graceful shutdown."""
    global _shutdown_requested
    _shutdown_requested = True
    sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    log_warn("  {}  received, finishing current thread...".format(sig_name))


def _timeout_expired():
    """Called by threading.Timer when --timeout seconds have elapsed."""
    global _shutdown_requested
    _shutdown_requested = True
    log_warn("  TIMEOUT  time limit reached, finishing current thread...")


def _parse_ratelimit_headers(http_headers):
    """
    Extract X-RateLimit-Remaining and X-RateLimit-Reset from
    Mastodon response headers.  Returns dict with int/float or None.
    """
    remaining = None
    reset_ts = None
    raw_rem = http_headers.get("X-RateLimit-Remaining")
    if raw_rem is not None:
        try:
            remaining = int(raw_rem)
        except (ValueError, TypeError):
            pass
    raw_reset = http_headers.get("X-RateLimit-Reset")
    if raw_reset:
        try:
            # Mastodon sends ISO 8601: 2025-06-15T12:05:00.000Z
            clean = raw_reset.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean)
            reset_ts = dt.timestamp()
        except (ValueError, TypeError):
            pass
    return {"remaining": remaining, "reset": reset_ts}


def _interruptible_sleep(seconds):
    """
    Sleep for *seconds*, but check _shutdown_requested every second.
    Returns early if a shutdown signal was received.
    """
    end = time.time() + seconds
    while time.time() < end:
        remaining = end - time.time()
        if remaining <= 0:
            break
        time.sleep(min(remaining, 1.0))
        if _shutdown_requested:
            break


def _rate_limit_sleep(min_wait=0):
    """
    Sleep for the **greater** of *min_wait* (--rate-limit) and the
    server's rate-limit header-derived delay.

    - Budget exhausted (remaining == 0): max(min_wait, time-to-reset + 1s)
    - Budget low (remaining < threshold): max(min_wait, time-to-reset / remaining)
    - Otherwise: min_wait
    """
    server_delay = 0.0
    rl = _last_ratelimit
    remaining = rl.get("remaining")
    reset_ts = rl.get("reset")

    if remaining is not None and reset_ts is not None:
        secs_to_reset = max(0.0, reset_ts - time.time())
        if remaining <= 0 and secs_to_reset > 0:
            server_delay = secs_to_reset + 1.0
        elif 0 < remaining < _RL_LOW_THRESHOLD and secs_to_reset > 0:
            # Spread remaining budget evenly over time-to-reset
            server_delay = secs_to_reset / remaining

    total = max(min_wait, server_delay)
    if server_delay > min_wait and server_delay > 0.5:
        log_debug("  RATE  remaining={}, sleeping {:.1f}s "
                  "(server {:.1f}s > configured {}s)".format(
                      remaining, total, server_delay, min_wait))
    if total > 0:
        _interruptible_sleep(total)


def api_get(url, token=None, retries=3):
    """
    GET a JSON resource from the Mastodon API.
    Retries on transient errors (429, 5xx, network) with
    exponential backoff: 2s, 4s, 8s, ... (429 respects Retry-After).
    Updates module-level _last_ratelimit on success.
    """
    global _last_ratelimit
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer {}".format(token)

    last_err = None
    for attempt in range(retries):
        req = Request(url, headers=headers)
        try:
            with urlopen(req, timeout=20) as r:
                _last_ratelimit = _parse_ratelimit_headers(r.headers)
                charset = r.headers.get_content_charset("utf-8")
                raw = r.read().decode(charset)
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                raise RuntimeError(
                    "Invalid JSON from {} (got {} bytes)".format(
                        url, len(raw)))
        except HTTPError as e:
            last_err = "HTTP {}: {} - {}".format(e.code, e.reason, url)
            if e.code not in _RETRYABLE_CODES:
                raise RuntimeError(last_err)
            # 429: prefer Retry-After header over blind backoff
            wait = 2 ** (attempt + 1)
            if e.code == 429:
                retry_after = e.headers.get("Retry-After") if e.headers else None
                if retry_after:
                    try:
                        wait = max(wait, int(retry_after))
                    except (ValueError, TypeError):
                        pass
            log_debug("  RETRY {}/{} in {}s: {}".format(
                attempt + 1, retries, wait, last_err))
            _interruptible_sleep(wait)
        except URLError as e:
            last_err = "Network: {} - {}".format(e.reason, url)
            wait = 2 ** (attempt + 1)
            log_debug("  RETRY {}/{} in {}s: {}".format(
                attempt + 1, retries, wait, last_err))
            _interruptible_sleep(wait)

    raise RuntimeError(last_err or "Request failed after {} attempts".format(retries))

# =========================================================================
# HTML sanitizer
# =========================================================================

class SafeHTML(HTMLParser):
    def __init__(self, ugc=False):
        HTMLParser.__init__(self)
        self._o = []
        self._rel = ("nofollow ugc noopener noreferrer" if ugc
                      else "noopener noreferrer")

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "br":
            self._o.append("<br>")
        elif tag == "p" and self._o:
            self._o.append("<br><br>")
        elif tag == "a":
            href = H.escape(a.get("href", ""))
            cls_attr = a.get("class", "")
            rel_attr = a.get("rel", "")
            is_hashtag = ("hashtag" in cls_attr or "tag" in rel_attr)
            is_mention = ("mention" in cls_attr and not is_hashtag)
            if is_hashtag:
                css_cls = ' class="{}__hashtag"'.format(NS)
            elif is_mention:
                css_cls = ' class="{}__mention"'.format(NS)
            else:
                css_cls = ""
            self._o.append(
                '<a href="{}" target="_blank" '
                'rel="{}"{}>'.format(href, self._rel, css_cls))
        elif tag in ("strong", "b"):
            self._o.append("<strong>")
        elif tag in ("em", "i"):
            self._o.append("<em>")
        elif tag == "code":
            self._o.append("<code>")

    def handle_endtag(self, tag):
        m = {"a": "</a>", "strong": "</strong>", "b": "</strong>",
             "em": "</em>", "i": "</em>", "code": "</code>"}
        if tag in m:
            self._o.append(m[tag])

    def handle_data(self, data):
        self._o.append(H.escape(data))

    def result(self):
        return "".join(self._o).strip()


def clean(raw, ugc=False):
    p = SafeHTML(ugc=ugc)
    p.feed(raw)
    return p.result()


# Regex matching characters commonly found in emoji-only messages:
# emoji codepoints, variation selectors, zero-width joiners, skin tones.
_EMOJI_CODEPOINTS = re.compile(
    u"["
    u"\U0000200D"              # zero-width joiner
    u"\U0000FE0E\U0000FE0F"   # variation selectors
    u"\U00002600-\U000027BF"   # misc symbols, dingbats
    u"\U0001F300-\U0001FAFF"   # all emoji blocks
    u"\U00002702-\U000027B0"
    u"\U000024C2-\U0001F251"
    u"\U0001F900-\U0001F9FF"
    u"\U0001FA00-\U0001FAFF"
    u"\U00002764"              # heavy heart
    u"\U0001F1E0-\U0001F1FF"   # flags
    u"\U0000203C\U00002049"    # ‼ ⁉
    u"\U000020E3"              # combining enclosing keycap
    u"\U00000023\U0000002A"    # # *
    u"\U00000030-\U00000039"   # 0-9 (for keycap sequences)
    u"]+")

_TAG_STRIP = re.compile(r"<[^>]+>")


def _is_emoji_only(html_body, max_chars=10):
    """
    Return True if *html_body* (cleaned HTML) contains only 1 to
    *max_chars* emoji codepoints (plus whitespace / variation selectors).
    Used to detect emoji-reaction toots like '👍' or '❤️🔥'.
    """
    text = _TAG_STRIP.sub("", html_body).strip()
    if not text or len(text) > max_chars:
        return False
    stripped = _EMOJI_CODEPOINTS.sub("", text)
    # After removing all emoji chars, only whitespace should remain
    return stripped.strip() == ""


def render_alt_badge(alt_text):
    """Return an ALT badge element if alt_text is non-empty."""
    if not alt_text:
        return ""
    safe = H.escape(alt_text)
    return (
        '<button class="{ns}__alt-badge" type="button" '
        'title="{alt}" aria-label="Image description: {alt}">ALT</button>'
    ).format(ns=NS, alt=safe)


def _render_media_html(attachments, media_dir=None, media_base_url=None,
                       media_max_age_s=0, max_media=0, toot_url="",
                       labels=None):
    """
    Render media attachments (image, video, gifv, audio) to HTML.
    Returns the concatenated HTML string (empty if no attachments).
    Used by both render_article_block and render_toot for OP media.
    If *max_media* > 0 and there are more attachments, only the first
    *max_media* are embedded; the rest are replaced with a link.
    """
    if labels is None:
        labels = DEFAULT_LABELS
    show = attachments
    extra = 0
    if max_media > 0 and len(attachments) > max_media:
        show = attachments[:max_media]
        extra = len(attachments) - max_media

    parts = []
    for m in show:
        raw_u   = m.get("url", "")
        raw_pre = m.get("preview_url", raw_u)
        u   = H.escape(resolve_media_src(
            raw_u, media_dir, media_base_url, media_max_age_s))
        pre = H.escape(resolve_media_src(
            raw_pre, media_dir, media_base_url, media_max_age_s))
        alt = H.escape(m.get("description", "") or "")
        mtype = m.get("type", "")

        if mtype == "image":
            alt_badge = render_alt_badge(m.get("description", "") or "")
            # Responsive: srcset with preview (small) and original (full)
            meta = m.get("meta") or {}
            pre_w = (meta.get("small") or {}).get("width", 400)
            full_w = (meta.get("original") or {}).get("width", 1200)
            if raw_u != raw_pre and full_w > pre_w:
                srcset = ' srcset="{pre} {pw}w, {full} {fw}w"'.format(
                    pre=pre, pw=pre_w, full=u, fw=full_w)
                sizes = ' sizes="(max-width:520px) 100vw, 600px"'
            else:
                srcset = ""
                sizes = ""
            parts.append(
                '<div class="{0}__media"><a href="{1}" target="_blank">'
                '<img src="{2}" alt="{3}" loading="lazy"{4}{5}></a>'
                '{6}</div>'.format(NS, u, pre, alt, srcset, sizes,
                                   alt_badge))
        elif mtype == "video":
            parts.append(
                '<div class="{0}__media"><video controls preload="metadata"'
                ' src="{1}" poster="{2}"></video></div>'.format(NS, u, pre))
        elif mtype == "gifv":
            parts.append(
                '<div class="{0}__media"><video autoplay loop muted'
                ' playsinline preload="metadata"'
                ' src="{1}" poster="{2}"></video></div>'.format(NS, u, pre))
        elif mtype == "audio":
            parts.append(
                '<div class="{0}__media"><audio controls preload="metadata"'
                ' src="{1}"></audio></div>'.format(NS, u))

    if extra > 0 and toot_url:
        esc_url = H.escape(toot_url)
        more_label = _pl(labels, "media_more", extra).format(n=extra)
        parts.append(
            '<div class="{ns}__media-link">'
            '<a href="{url}" target="_blank"'
            ' rel="nofollow ugc noopener">'
            '{svg} {label} &#8599;</a></div>'.format(
                ns=NS, url=esc_url,
                svg=SVG_MEDIA,
                label=more_label))

    return "".join(parts)


def _wrap_sensitive(media_html, sensitive, labels):
    """
    Wrap media HTML in a <details> spoiler if the toot is marked sensitive.
    Returns the (possibly wrapped) HTML.
    """
    if not media_html or not sensitive:
        return media_html
    return (
        '<details class="{ns}__sensitive">\n'
        '  <summary class="{ns}__sensitive-toggle">'
        '{svg} {sensitive_label}</summary>\n'
        '  <div class="{ns}__sensitive-body">{media}</div>\n'
        '</details>'
    ).format(ns=NS, svg=SVG_CW, media=media_html,
             sensitive_label=labels["sensitive_toggle"])


def _render_card_html(card, media_dir=None, media_base_url=None,
                      media_max_age_s=0):
    """
    Render a Mastodon link preview card to HTML.
    Returns empty string if card is None or lacks url/title.
    Thumbnail image is cached via the media cache.
    """
    if not card or not isinstance(card, dict):
        return ""
    url = card.get("url", "")
    title = (card.get("title") or "").strip()
    if not url or not title:
        return ""

    esc_url = H.escape(url)
    esc_title = H.escape(title)
    desc = (card.get("description") or "").strip()
    esc_desc = H.escape(desc[:200] + ("..." if len(desc) > 200 else ""))
    provider = (card.get("provider_name") or "").strip()
    if not provider:
        try:
            provider = urlparse(url).netloc
        except Exception:
            provider = ""
    esc_provider = H.escape(provider)

    # Thumbnail: cache locally via media cache
    thumb_html = ""
    image_url = card.get("image") or ""
    if image_url:
        cached_src = H.escape(resolve_media_src(
            image_url, media_dir, media_base_url, media_max_age_s))
        thumb_html = (
            '<div class="{ns}__card-thumb">'
            '<img src="{src}" alt="" loading="lazy"></div>'
        ).format(ns=NS, src=cached_src)

    return (
        '<a href="{url}" class="{ns}__card-preview" target="_blank"'
        ' rel="nofollow noopener">\n'
        '  {thumb}\n'
        '  <div class="{ns}__card-text">\n'
        '    <div class="{ns}__card-provider">{provider}</div>\n'
        '    <div class="{ns}__card-title">{title}</div>\n'
        '    <div class="{ns}__card-desc">{desc}</div>\n'
        '  </div>\n'
        '</a>'
    ).format(ns=NS, url=esc_url, thumb=thumb_html,
             provider=esc_provider, title=esc_title, desc=esc_desc)


def _render_quote_html(quote, custom_emojis=False, labels=None):
    """
    Render a quoted toot (Mastodon 4.3+ quote posts) as an embedded box.
    Returns empty string if quote is None or not a dict.
    """
    if not quote or not isinstance(quote, dict):
        return ""
    if labels is None:
        labels = DEFAULT_LABELS

    qa = quote.get("account", {})
    q_name = H.escape(qa.get("display_name") or qa.get("username", "?"))
    if custom_emojis:
        q_name = resolve_emojis(q_name, qa.get("emojis", []))
    q_handle = H.escape("@{}".format(qa.get("acct", "?")))
    q_avatar = H.escape(qa.get("avatar_static", qa.get("avatar", "")))
    q_url = H.escape(quote.get("url") or quote.get("uri", "#"))
    q_ts = fmt_rel(quote.get("created_at", ""))
    q_ts_abs = fmt_abs(quote.get("created_at", ""))
    q_ts_iso = fmt_iso(quote.get("created_at", ""))

    q_body = clean(quote.get("content", ""), ugc=True)
    if custom_emojis:
        q_body = resolve_emojis(q_body, quote.get("emojis", []))

    # Truncate very long quoted content
    # (render full body but CSS will clamp to 4 lines)
    q_lang = quote.get("language") or ""
    q_lang_attr = ' lang="{}"'.format(H.escape(q_lang)) if q_lang else ""

    return (
        '<a href="{url}" class="{ns}__quote" target="_blank"'
        ' rel="nofollow noopener">\n'
        '  <div class="{ns}__quote-head">\n'
        '    <img class="{ns}__quote-av" src="{avatar}" alt=""'
        ' loading="lazy">\n'
        '    <span class="{ns}__quote-name">{name}</span>\n'
        '    <span class="{ns}__quote-handle">{handle}</span>\n'
        '    <span class="{ns}__quote-time">'
        '<time datetime="{ts_iso}" title="{ts_abs}">{ts}</time></span>\n'
        '  </div>\n'
        '  <div class="{ns}__quote-body"{lang}>{body}</div>\n'
        '</a>'
    ).format(ns=NS, url=q_url, avatar=q_avatar,
             name=q_name, handle=q_handle,
             ts=q_ts, ts_abs=q_ts_abs, ts_iso=q_ts_iso,
             lang=q_lang_attr, body=q_body)


def resolve_emojis(text, emojis):
    """
    Replace :shortcode: patterns in text with inline <img> tags.
    emojis is the list from the Mastodon API (each with
    'shortcode', 'url', 'static_url').
    """
    if not emojis or not text:
        return text

    lookup = {}
    for e in emojis:
        sc = e.get("shortcode", "")
        url = e.get("static_url") or e.get("url", "")
        if sc and url:
            lookup[sc] = url

    if not lookup:
        return text

    count = [0]

    def _repl(m):
        sc = m.group(1)
        url = lookup.get(sc)
        if url is None:
            return m.group(0)
        count[0] += 1
        return (
            '<img class="{ns}__emoji" src="{url}" alt=":{sc}:" '
            'title=":{sc}:" loading="lazy">'
        ).format(ns=NS, url=H.escape(url), sc=H.escape(sc))

    result = re.sub(r":([a-zA-Z0-9_]+):", _repl, text)
    if count[0]:
        log_debug("    EMOJI resolved {} custom emoji{}".format(
            count[0], "s" if count[0] != 1 else ""))
    return result


# =========================================================================
# Time formatting (English)
# =========================================================================

def fmt_abs(iso):
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y at %H:%M")
    except Exception:
        return iso


def fmt_iso(iso):
    """Normalize ISO timestamp for <time datetime='...'>."""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.isoformat()
    except Exception:
        return iso


def fmt_rel(iso):
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        d = (datetime.now(timezone.utc) - dt).total_seconds()
        if d < 60:
            return "just now"
        if d < 3600:
            return "{} min ago".format(int(d // 60))
        if d < 86400:
            return "{} hr ago".format(int(d // 3600))
        if d < 604800:
            t = int(d // 86400)
            return "{} day{} ago".format(t, "s" if t != 1 else "")
        return fmt_abs(iso)
    except Exception:
        return iso


# =========================================================================
# Reply tree
# =========================================================================

class Node(object):
    __slots__ = ("d", "children")
    def __init__(self, d):
        self.d = d
        self.children = []

    @property
    def id(self):
        return str(self.d["id"])

    @property
    def pid(self):
        v = self.d.get("in_reply_to_id")
        return str(v) if v else None


def build_tree(root_data, desc):
    root = Node(root_data)
    idx = {root.id: root}
    for d in desc:
        idx[str(d["id"])] = Node(d)
    for nid, n in idx.items():
        if nid == root.id:
            continue
        parent = idx.get(n.pid) if n.pid else None
        if parent:
            parent.children.append(n)
        else:
            root.children.append(n)
    return root


def _iter_nodes(node):
    """Yield all descendant nodes (excluding node itself)."""
    for child in node.children:
        yield child
        for n in _iter_nodes(child):
            yield n


def _count_descendants(node):
    """Count all descendants (children, grandchildren, ...) of *node*."""
    c = 0
    for ch in node.children:
        c += 1 + _count_descendants(ch)
    return c


def sort_tree(node, mode="oldest"):
    """
    Recursively sort children of each node.
      oldest  - chronological (by created_at ascending, default)
      newest  - reverse chronological (by created_at descending)
      popular - by favourites_count descending
    """
    if not node.children:
        return

    if mode == "newest":
        node.children.sort(
            key=lambda n: n.d.get("created_at", ""), reverse=True)
    elif mode == "popular":
        node.children.sort(
            key=lambda n: (n.d.get("favourites_count", 0) or 0), reverse=True)
    else:
        # oldest (default) - chronological
        node.children.sort(
            key=lambda n: n.d.get("created_at", ""))

    for child in node.children:
        sort_tree(child, mode)


def extract_article_chain(root_node):
    """
    Extract the OP's self-thread: starting from root, follow only
    children authored by the same account as the root.  Once a
    non-OP toot appears in a branch, the entire branch is cut --
    even if the OP replies further down that branch.

    Returns a flat list of toot dicts (in chronological order),
    excluding the root toot itself.
    """
    op_acct = root_node.d.get("account", {}).get("acct", "")
    if not op_acct:
        op_acct = root_node.d.get("account", {}).get("username", "")

    chain = []

    def _walk(node):
        for child in node.children:
            child_acct = child.d.get("account", {}).get("acct", "")
            if not child_acct:
                child_acct = child.d.get("account", {}).get("username", "")
            if child_acct == op_acct:
                chain.append(child.d)
                _walk(child)
            # non-OP child: skip entire sub-branch

    _walk(root_node)

    # Sort chronologically
    chain.sort(key=lambda t: t.get("created_at", ""))
    return chain


def render_article_block(root_data, chain, avatar_dir=None,
                         avatar_base_url=None, av_max_age_s=0,
                         custom_emojis=False,
                         media_dir=None, media_base_url=None,
                         media_max_age_s=0,
                         labels=None, max_media=0):
    """
    Render the OP's article chain as a sequential block (no nesting).
    Returns HTML string.
    """
    if labels is None:
        labels = DEFAULT_LABELS
    if not chain:
        return ""

    parts = []
    all_toots = [root_data] + chain

    for t in all_toots:
        a = t.get("account", {})
        content = clean(t.get("content", ""))
        if custom_emojis:
            content = resolve_emojis(content, t.get("emojis", []))

        ts_abs = fmt_abs(t.get("created_at", ""))
        ts_rel = fmt_rel(t.get("created_at", ""))
        ts_iso = fmt_iso(t.get("created_at", ""))
        toot_url = H.escape(t.get("url") or t.get("uri", "#"))
        toot_id = H.escape(str(t.get("id", "")))

        edited = t.get("edited_at")
        if edited:
            edited_tag = (' <time class="{ns}__edited" datetime="{ei}"'
                          ' title="{etitle}">'
                          '{elabel}</time>').format(
                ns=NS, ea=fmt_abs(edited), ei=fmt_iso(edited),
                etitle=labels["edited_title"].format(date=fmt_abs(edited)),
                elabel=labels["edited"])
        else:
            edited_tag = ""

        # Media
        raw_url = t.get("url") or t.get("uri", "#")
        media = _render_media_html(
            t.get("media_attachments", []),
            media_dir=media_dir, media_base_url=media_base_url,
            media_max_age_s=media_max_age_s,
            max_media=max_media, toot_url=raw_url, labels=labels)
        media = _wrap_sensitive(media, t.get("sensitive", False), labels)

        # Link preview card (skip if media attachments present)
        card = ""
        if not t.get("media_attachments"):
            card = _render_card_html(
                t.get("card"), media_dir=media_dir,
                media_base_url=media_base_url,
                media_max_age_s=media_max_age_s)

        # Quoted toot
        quote = _render_quote_html(
            t.get("quote"), custom_emojis=custom_emojis, labels=labels)

        lang = t.get("language") or ""
        lang_attr = ' lang="{}"'.format(H.escape(lang)) if lang else ""

        parts.append((
            '<div class="{ns}__article-post" id="toot-{tid}">\n'
            '  <div class="{ns}__article-body"{lang}>{content}</div>\n'
            '  {media}\n'
            '  {card}\n'
            '  {quote}\n'
            '  <div class="{ns}__article-meta">'
            '<a href="{url}" target="_blank" rel="noopener" '
            'class="{ns}__time" title="{ts_abs}">'
            '<time datetime="{ts_iso}">{ts_rel}</time></a>'
            '{edited}'
            '<a href="#toot-{tid}" class="{ns}__permalink" '
            'title="{permalink}" aria-label="{permalink}">{svg_link}</a>'
            '</div>\n'
            '</div>'
        ).format(ns=NS, tid=toot_id, content=content, lang=lang_attr,
                 media=media, card=card, quote=quote,
                 url=toot_url, ts_abs=ts_abs, ts_rel=ts_rel, ts_iso=ts_iso,
                 edited=edited_tag, svg_link=SVG_LINK,
                 permalink=labels["permalink"]))

    # Author header (from root toot)
    ra = root_data.get("account", {})
    name = H.escape(ra.get("display_name") or ra.get("username", "?"))
    if custom_emojis:
        name = resolve_emojis(name, ra.get("emojis", []))
    verified = SVG_VERIFIED if has_verified_link(ra) else ""
    handle = H.escape("@{}".format(ra.get("acct", "?")))
    acct_url = H.escape(ra.get("url", "#"))

    if avatar_dir and avatar_base_url:
        avatar = H.escape(
            resolve_avatar_src(root_data, avatar_dir,
                               avatar_base_url, av_max_age_s))
    else:
        avatar = H.escape(
            ra.get("avatar_static", ra.get("avatar", "")))

    header = (
        '<div class="{ns}__article-author">\n'
        '  <a href="{acct_url}" target="_blank" rel="noopener">'
        '<img class="{ns}__av" src="{avatar}" alt="" loading="lazy"></a>\n'
        '  <div>\n'
        '    <a href="{acct_url}" target="_blank" rel="noopener" '
        'class="{ns}__name">{name}{verified}</a>\n'
        '    <div class="{ns}__handle">{handle}</div>\n'
        '  </div>\n'
        '</div>'
    ).format(ns=NS, acct_url=acct_url, avatar=avatar,
             name=name, verified=verified, handle=handle)

    count = len(all_toots)
    log_debug("  ARTICLE  {} post{} from OP".format(
        count, "s" if count != 1 else ""))

    _op_a = root_data.get("account", {})
    _op_handle = H.escape(_op_a.get("acct", ""))

    return (
        '<div class="{ns}__article" role="article"'
        ' aria-label="Article thread by @{op_handle}">\n'
        '  <div class="{ns}__article-label">Thread by</div>\n'
        '  {header}\n'
        '  <div class="{ns}__article-chain">\n'
        '    {posts}\n'
        '  </div>\n'
        '</div>\n'
    ).format(ns=NS, header=header, posts="\n    ".join(parts),
             op_handle=_op_handle)


# =========================================================================
# Statistics collector
# =========================================================================

def collect_stats(root_data, descendants, blocklist, whitelist=None):
    """Count total toots and unique users (excluding filtered)."""
    all_toots = [root_data] + descendants
    op_acct = (root_data.get("account", {}).get("acct")
               or root_data.get("account", {}).get("username", ""))
    users = set()
    count = 0
    blocked_count = 0
    latest_date = ""

    for t in all_toots:
        # OP toots are never filtered
        t_acct = t.get("account", {}).get("acct") or t.get("account", {}).get("username", "")
        is_op = (t is root_data) or (op_acct and t_acct == op_acct)
        if not is_op and is_filtered(t, blocklist, whitelist):
            blocked_count += 1
            continue
        count += 1
        acct = t.get("account", {}).get("acct", "")
        if acct:
            users.add(acct)
        created = t.get("created_at", "")
        if created > latest_date:
            latest_date = created

    return {
        "total": count,
        "users": len(users),
        "blocked": blocked_count,
        "latest_date": latest_date,
    }


# =========================================================================
# HTML rendering (namespaced .mt-*)
# =========================================================================

NS = "mt"

SVG_REPLY = ('<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21 11.5a8.4 8.4 0 0 1-.9 '
             '3.8 8.5 8.5 0 0 1-7.6 4.7 8.4 8.4 0 0 1-3.8-.9L3 21l1.9-5.7'
             'a8.4 8.4 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.4 8.4 0 0 1 '
             '3.8-.9h.5a8.5 8.5 0 0 1 8 8v.5z"/></svg>')
SVG_BOOST = ('<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M17 1l4 4-4 4"/>'
             '<path d="M3 11V9a4 4 0 0 1 4-4h14"/>'
             '<path d="M7 23l-4-4 4-4"/>'
             '<path d="M21 13v2a4 4 0 0 1-4 4H3"/></svg>')
SVG_STAR  = ('<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2l3.09 6.26L22 9.27l-5'
             ' 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01'
             'z"/></svg>')
SVG_MASTO = ('<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21.3 14.9c-.3 1.5-2.6 '
             '3.2-5.3 3.5-1.4.2-2.8.3-4.2.2-2.4-.1-4.2-.7-4.2-.7v.8c.3 '
             '2.3 2.3 2.4 4.2 2.5 1.9 0 3.6-.5 3.6-.5l.1 1.7s-1.3.7-3.7'
             '.9c-1.3 0-2.9-.1-4.8-.5C3.6 21.8 3 18.3 2.9 14.8v-4c0-3.4 '
             '2.2-4.4 2.2-4.4C6.2 5.8 8 5.5 9.9 5.5h0c1.9 0 3.7.3 4.8.9'
             ' 0 0 2.2 1 2.2 4.4 0 0 0 2.5-.3 4.2zM18 9.5c0-1-.3-1.8-.8-'
             '2.4-.5-.6-1.2-.9-2.1-.9-1 0-1.8.4-2.3 1.2l-.5.8-.5-.8C11.3 '
             '6.6 10.5 6.2 9.5 6.2c-.9 0-1.6.3-2.1.9-.5.6-.8 1.4-.8 2.4v5'
             'h2V9.7c0-1 .4-1.5 1.3-1.5.9 0 1.4.6 1.4 1.8v2.7h2V10c0-1.2'
             '.5-1.8 1.4-1.8.9 0 1.3.5 1.3 1.5v4.8h2z"/></svg>')
SVG_CW    = ('<svg viewBox="0 0 24 24" width="16" height="16" aria-hidden="true">'
             '<path d="M12 9v2m0 4h.01M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z"'
             ' fill="none" stroke="currentColor" stroke-width="2"'
             ' stroke-linecap="round"/></svg>')
SVG_VERIFIED = ('<svg class="{ns}__verified" viewBox="0 0 24 24" aria-hidden="true">'
                '<path d="M9 12l2 2 4-4"/>'
                '<path d="M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0z"/>'
                '</svg>'.format(ns=NS))
SVG_BOT   = ('<svg class="{ns}__bot-icon" viewBox="0 0 24 24" aria-hidden="true">'
             '<rect x="4" y="8" width="16" height="12" rx="2"/>'
             '<circle cx="9" cy="14" r="1.5"/>'
             '<circle cx="15" cy="14" r="1.5"/>'
             '<line x1="12" y1="2" x2="12" y2="8"/>'
             '<circle cx="12" cy="2" r="1.5"/>'
             '</svg>'.format(ns=NS))

SVG_MEDIA = ('<svg class="{ns}__media-icon" viewBox="0 0 24 24" aria-hidden="true">'
             '<rect x="3" y="3" width="18" height="18" rx="2"/>'
             '<circle cx="8.5" cy="8.5" r="1.5"/>'
             '<path d="M21 15l-5-5L5 21"/>'
             '</svg>'.format(ns=NS))

SVG_LINK  = ('<svg class="{ns}__link-icon" viewBox="0 0 24 24" aria-hidden="true">'
             '<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07'
             'l-1.72 1.71"/>'
             '<path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07'
             'l1.71-1.71"/>'
             '</svg>'.format(ns=NS))


def has_verified_link(account):
    """Check if a Mastodon account has at least one verified link."""
    for field in account.get("fields", []):
        if field.get("verified_at"):
            return True
    return False


def render_blocked(depth=0, labels=None):
    if labels is None:
        labels = DEFAULT_LABELS
    return (
        '<div class="{ns}__toot" data-depth="{depth}">\n'
        '  <div class="{ns}__line" aria-hidden="true"></div>\n'
        '  <div class="{ns}__blocked" role="status">\n'
        '    {post_hidden}\n'
        '  </div>\n'
        '</div>'
    ).format(ns=NS, depth=depth, post_hidden=labels["post_hidden"])


class RenderContext:
    """
    Bundles all invariant and shared-mutable state for render_toot,
    so recursive calls pass a single object instead of 16 keyword args.

    Invariant (set once, never changed during tree walk):
        blocklist, whitelist, avatar_dir, avatar_base_url, av_max_age_s,
        max_depth, custom_emojis, article_ids, op_acct, reply_map,
        media_dir, media_base_url, media_max_age_s, labels

    Shared mutable (mutated in-place during tree walk):
        counter          – dict {"remaining": int, "skipped": int} or None
        seen_boost_ids   – set of original toot IDs or None
    """
    __slots__ = (
        "blocklist", "whitelist",
        "avatar_dir", "avatar_base_url", "av_max_age_s",
        "max_depth", "custom_emojis",
        "article_ids", "op_acct", "reply_map",
        "media_dir", "media_base_url", "media_max_age_s",
        "labels", "counter", "seen_boost_ids",
        "highlight_above", "fold_depth", "max_media",
    )

    def __init__(self, blocklist=None, whitelist=None,
                 avatar_dir=None, avatar_base_url=None, av_max_age_s=0,
                 max_depth=5, custom_emojis=False,
                 article_ids=None, op_acct=None, reply_map=None,
                 media_dir=None, media_base_url=None, media_max_age_s=0,
                 labels=None, counter=None, seen_boost_ids=None,
                 highlight_above=0, fold_depth=0, max_media=0):
        self.blocklist = blocklist
        self.whitelist = whitelist
        self.avatar_dir = avatar_dir
        self.avatar_base_url = avatar_base_url
        self.av_max_age_s = av_max_age_s
        self.max_depth = max_depth
        self.custom_emojis = custom_emojis
        self.article_ids = article_ids
        self.op_acct = op_acct
        self.reply_map = reply_map
        self.media_dir = media_dir
        self.media_base_url = media_base_url
        self.media_max_age_s = media_max_age_s
        self.labels = labels if labels is not None else DEFAULT_LABELS
        self.counter = counter
        self.seen_boost_ids = seen_boost_ids
        self.highlight_above = highlight_above
        self.fold_depth = fold_depth
        self.max_media = max_media


def render_toot(node, depth=0, is_root=False, ctx=None):
    """
    Render a single toot node and its children recursively.
    ctx: a RenderContext holding all invariant and shared-mutable state.
    """
    if ctx is None:
        ctx = RenderContext()
    # Unpack frequently used context members into locals
    blocklist       = ctx.blocklist
    whitelist       = ctx.whitelist
    avatar_dir      = ctx.avatar_dir
    avatar_base_url = ctx.avatar_base_url
    av_max_age_s    = ctx.av_max_age_s
    max_depth       = ctx.max_depth
    counter         = ctx.counter
    custom_emojis   = ctx.custom_emojis
    article_ids     = ctx.article_ids
    op_acct         = ctx.op_acct
    reply_map       = ctx.reply_map
    media_dir       = ctx.media_dir
    media_base_url  = ctx.media_base_url
    media_max_age_s = ctx.media_max_age_s
    labels          = ctx.labels
    seen_boost_ids  = ctx.seen_boost_ids

    t = node.d
    tid = str(t.get("id", ""))

    # Check counter: skip if exhausted (root toot is always shown)
    if not is_root and counter is not None and counter["remaining"] <= 0:
        # Count this toot and all descendants as skipped
        def _count_nodes(n):
            c = 1
            for ch in n.children:
                c += _count_nodes(ch)
            return c
        counter["skipped"] += _count_nodes(node)
        return ""

    # Toots already shown in article block
    in_article = (article_ids is not None and tid in article_ids)
    if in_article:
        # Render children first
        kids = ""
        if node.children:
            parts = []
            for c in node.children:
                rendered = render_toot(c, depth + 1, ctx=ctx)
                if rendered:
                    parts.append(rendered)
            if parts:
                kids = '<div class="{0}__replies">{1}</div>'.format(
                    NS, "\n".join(parts))

        # Check if any direct child is NOT in article (= non-OP reply)
        has_outside_reply = any(
            str(c.d.get("id", "")) not in article_ids
            for c in node.children
        ) if node.children else False

        if has_outside_reply:
            # Show as compact card with handle + first 3 words on one line
            raw_text = re.sub(r"<[^>]+>", " ", t.get("content", ""))
            raw_text = H.unescape(raw_text).strip()
            words = raw_text.split()
            preview = " ".join(words[:3]) if words else ""
            ellip = "&hellip;" if len(words) > 3 else ""
            stub_body = (
                '<a href="#toot-{tid}" class="{ns}__article-ref">'
                '{preview}{ellip}</a>'
            ).format(ns=NS, tid=H.escape(tid),
                     preview=H.escape(preview), ellip=ellip)

            a = t.get("account", {})
            handle = H.escape("@{}".format(a.get("acct", "?")))
            acct_url = H.escape(a.get("url", "#"))

            visual_depth = min(depth, max_depth)
            indent_px = visual_depth * 10

            stub = (
                '<div class="{ns}__toot" id="toot-c-{tid}"'
                ' data-depth="{depth}"'
                ' style="padding-left:{indent}px">\n'
                '  <div class="{ns}__line"></div>\n'
                '  <div class="{ns}__card">\n'
                '    <div class="{ns}__article-stub-line">'
                '<a href="{acct_url}" target="_blank"'
                ' rel="noopener" '
                'class="{ns}__handle">{handle}</a> '
                '{stub_body}</div>\n'
                '  </div>\n'
                '  {kids}\n'
                '</div>'
            ).format(ns=NS, tid=H.escape(tid), depth=depth,
                     indent=indent_px, acct_url=acct_url,
                     handle=handle, stub_body=stub_body,
                     kids=kids)
            return stub
        else:
            # No outside replies: skip entirely, pass through children
            return kids

    # OP toots are never filtered (blocklist/whitelist exempt)
    toot_acct_raw = t.get("account", {}).get("acct") or t.get("account", {}).get("username", "")
    _is_op_toot = is_root or (op_acct and toot_acct_raw == op_acct)
    blocked = (not _is_op_toot) and is_filtered(t, blocklist, whitelist)

    # Decrement counter for non-root, non-blocked toots
    if not is_root and counter is not None and not blocked:
        counter["remaining"] -= 1

    # Effective visual depth: cap at max_depth
    visual_depth = min(depth, max_depth)

    # Always render children even if parent is blocked
    kids = ""
    if node.children:
        parts = []
        for c in node.children:
            rendered = render_toot(c, depth + 1, ctx=ctx)
            if rendered:
                parts.append(rendered)
        if parts:
            kids = '<div class="{0}__replies">{1}</div>'.format(
                NS, "\n".join(parts))

        # Fold deep subtrees into <details> when fold is active
        if (kids
                and ctx.fold_depth > 0
                and depth >= ctx.fold_depth):
            n_desc = _count_descendants(node)
            fold_label = _pl(labels, "fold_replies", n_desc).format(n=n_desc)
            kids = (
                '<details class="{ns}__fold">\n'
                '<summary class="{ns}__fold-toggle">'
                '{label}</summary>\n'
                '{kids}\n'
                '</details>'
            ).format(ns=NS, label=fold_label, kids=kids)

    if blocked:
        return render_blocked(visual_depth, labels=labels) + "\n" + kids

    # --- Normal rendering ---

    # Boost detection: if this toot is a reblog, show original content
    # with a "boosted by" banner
    reblog = t.get("reblog")
    boost_banner = ""
    original_id = str(t.get("id", ""))
    original_url = t.get("url") or t.get("uri", "#")
    if reblog and isinstance(reblog, dict):
        reblog_id = str(reblog.get("id", ""))
        booster_acct = t.get("account", {})
        booster_handle = H.escape(
            "@{}".format(booster_acct.get("acct", "?")))
        booster_url = H.escape(booster_acct.get("url", "#"))

        # Duplicate boost: original already rendered elsewhere
        if seen_boost_ids is not None and reblog_id in seen_boost_ids:
            log_debug("    BOOST  duplicate: @{} also boosted toot {}".format(
                booster_acct.get("acct", "?"), reblog_id))
            indent_px = min(depth, max_depth) * 10
            return (
                '<div class="{ns}__toot" id="toot-{tid}"'
                ' data-depth="{depth}"'
                ' style="padding-left:{indent}px">\n'
                '  <div class="{ns}__line" aria-hidden="true"></div>\n'
                '  <div class="{ns}__card">\n'
                '    <div class="{ns}__boost-banner">'
                '{svg} <a href="{url}" target="_blank"'
                ' rel="nofollow ugc noopener">'
                '{handle}</a> {dup_label}</div>\n'
                '  </div>\n'
                '  {kids}\n'
                '</div>'
            ).format(ns=NS, tid=H.escape(tid), depth=depth,
                     indent=indent_px, svg=SVG_BOOST,
                     url=booster_url, handle=booster_handle,
                     dup_label=labels["boost_duplicate"],
                     kids=kids)

        # First time seeing this original toot
        if seen_boost_ids is not None:
            seen_boost_ids.add(reblog_id)

        boost_banner = (
            '<div class="{ns}__boost-banner">'
            '{svg} <a href="{url}" target="_blank"'
            ' rel="nofollow ugc noopener">'
            '{handle}</a> {boosted_label}</div>'
        ).format(ns=NS, svg=SVG_BOOST, url=booster_url,
                 handle=booster_handle, boosted_label=labels["boosted"])
        # Swap to reblog data for rendering
        t = reblog
        log_debug("    BOOST  @{} boosted toot {}".format(
            booster_acct.get("acct", "?"), t.get("id", "?")))

    a = t.get("account", {})

    # Determine OP status early (needed for ugc rel and media gating)
    toot_acct = a.get("acct") or a.get("username", "")
    is_op = (op_acct and toot_acct == op_acct) or is_root

    if avatar_dir and avatar_base_url:
        avatar = H.escape(
            resolve_avatar_src(t, avatar_dir, avatar_base_url, av_max_age_s))
    else:
        avatar = H.escape(a.get("avatar_static", a.get("avatar", "")))

    name     = H.escape(a.get("display_name") or a.get("username", "?"))
    if custom_emojis:
        name = resolve_emojis(name, a.get("emojis", []))
    verified = SVG_VERIFIED if has_verified_link(a) else ""
    if verified:
        log_debug("    VERIFIED  @{}".format(a.get("acct", "?")))
    bot_badge = SVG_BOT if a.get("bot", False) else ""
    if bot_badge:
        log_debug("    BOT  @{}".format(a.get("acct", "?")))
    handle   = H.escape("@{}".format(a.get("acct", "?")))
    acct_url = H.escape(a.get("url", "#"))
    toot_url = H.escape(original_url)
    toot_id  = H.escape(original_id)
    # rel attribute: OP links are trusted, non-OP links get nofollow ugc
    link_rel = "noopener" if is_op else "nofollow ugc noopener"
    ts_abs   = fmt_abs(t.get("created_at", ""))
    ts_rel   = fmt_rel(t.get("created_at", ""))
    ts_iso   = fmt_iso(t.get("created_at", ""))
    edited   = t.get("edited_at")
    if edited:
        edited_abs = fmt_abs(edited)
        edited_iso = fmt_iso(edited)
        edited_tag = (' <time class="{ns}__edited" datetime="{ei}"'
                      ' title="{etitle}">'
                      '{elabel}</time>').format(
            ns=NS, ei=edited_iso,
            etitle=labels["edited_title"].format(date=edited_abs),
            elabel=labels["edited"])
        log_debug("    EDITED  toot {} at {}".format(
            t.get("id", "?"), edited_abs))
    else:
        edited_tag = ""
    body     = clean(t.get("content", ""), ugc=(not is_op))
    if custom_emojis:
        body = resolve_emojis(body, t.get("emojis", []))
    n_reply  = t.get("replies_count", 0)
    n_boost  = t.get("reblogs_count", 0)
    n_fav    = t.get("favourites_count", 0)

    cls = "{0}__toot {0}__root".format(NS) if is_root else "{0}__toot".format(NS)
    if boost_banner:
        cls += " {0}__toot--boost".format(NS)
    if (ctx.highlight_above > 0
            and not is_root
            and (n_fav or 0) >= ctx.highlight_above):
        cls += " {0}__toot--popular".format(NS)

    # Reply-to indicator (only at depth >= 2, where context isn't obvious)
    reply_to_html = ""
    if not is_root and depth >= 2 and reply_map:
        parent_handle = reply_map.get(tid, "")
        if parent_handle:
            reply_to_html = (
                '<div class="{ns}__reply-to">'
                '&#8617; @{handle}</div>'
            ).format(ns=NS, handle=H.escape(parent_handle))

    # Content warning handling
    spoiler = (t.get("spoiler_text") or "").strip()
    if spoiler:
        log_debug("    CW  toot {}: {}".format(t.get("id", "?"), spoiler))
        body_html = (
            '<div class="{ns}__cw-label">{cw_svg} {spoiler}</div>\n'
            '      <details class="{ns}__cw-details">\n'
            '        <summary class="{ns}__cw-toggle">{show_content}</summary>\n'
            '        <div class="{ns}__cw-body">{body}</div>\n'
            '      </details>'
        ).format(ns=NS, cw_svg=SVG_CW,
                 spoiler=resolve_emojis(H.escape(spoiler),
                                        t.get("emojis", []))
                         if custom_emojis
                         else H.escape(spoiler),
                 body=body, show_content=labels["show_content"])
    else:
        body_html = body

    # Wrap in div only if non-empty; suppress empty body blocks
    if body_html.strip():
        lang = t.get("language") or ""
        lang_attr = ' lang="{}"'.format(H.escape(lang)) if lang else ""
        emoji_cls = " {ns}__body--emoji".format(ns=NS) if _is_emoji_only(body_html) else ""
        body_html = '<div class="{ns}__body{ecls}"{lang}>{b}</div>'.format(
            ns=NS, ecls=emoji_cls, lang=lang_attr, b=body_html)
    else:
        body_html = ""

    # Media (only for OP's toots)
    media = ""
    if is_op:
        raw_toot_url = t.get("url") or t.get("uri", "#")
        media = _render_media_html(
            t.get("media_attachments", []),
            media_dir=media_dir, media_base_url=media_base_url,
            media_max_age_s=media_max_age_s,
            max_media=ctx.max_media, toot_url=raw_toot_url,
            labels=labels)
    else:
        n_att = len(t.get("media_attachments", []))
        if n_att:
            toot_link = H.escape(t.get("url") or t.get("uri", "#"))
            media = (
                '<div class="{ns}__media-link">'
                '<a href="{url}" target="_blank"'
                ' rel="nofollow ugc noopener">'
                '{svg} {media_label} &#8599;</a></div>'
            ).format(ns=NS, url=toot_link,
                     svg=SVG_MEDIA,
                     media_label=_pl(labels, "media_link", n_att).format(
                         n=n_att))

    # Sensitive media: wrap in details/summary
    if media and t.get("sensitive", False):
        log_debug("    SENSITIVE  toot {} has sensitive media".format(
            t.get("id", "?")))
        media = _wrap_sensitive(media, True, labels)

    # Poll
    poll_html = ""
    poll_data = t.get("poll")
    if poll_data and poll_data.get("options"):
        total = poll_data.get("votes_count", 0) or 0
        voters = poll_data.get("voters_count")
        expired = poll_data.get("expired", False)
        options_html = ""
        for opt in poll_data["options"]:
            label = H.escape(opt.get("title", ""))
            votes = opt.get("votes_count", 0) or 0
            pct = (votes * 100.0 / total) if total > 0 else 0
            pct_str = "{:.1f}".format(pct)
            options_html += (
                '<div class="{ns}__poll-opt"'
                ' role="meter" aria-valuenow="{pct}"'
                ' aria-valuemin="0" aria-valuemax="100"'
                ' aria-label="{label}: {pct_str}%">'
                '<div class="{ns}__poll-bar" style="width:{pct}%"></div>'
                '<span class="{ns}__poll-label">{label}</span>'
                '<span class="{ns}__poll-pct" aria-hidden="true">'
                '{pct_str}%</span>'
                '</div>\n'
            ).format(ns=NS, label=label, pct=pct_str, pct_str=pct_str)

        if voters is not None:
            footer_txt = _pl(labels, "voters", voters).format(n=voters)
        else:
            footer_txt = _pl(labels, "votes", total).format(n=total)
        if expired:
            footer_txt += " &middot; " + labels["poll_final"]
        else:
            footer_txt += " &middot; " + labels["poll_open"]

        poll_html = (
            '<div class="{ns}__poll" role="group" aria-label="{poll_aria}">\n'
            '{options}'
            '<div class="{ns}__poll-footer">{footer}</div>\n'
            '</div>'
        ).format(ns=NS, options=options_html, footer=footer_txt,
                 poll_aria=labels["poll_aria"])

    # Link preview card (OP only, skip if media attachments present)
    card_html = ""
    if is_op and not t.get("media_attachments"):
        card_html = _render_card_html(
            t.get("card"), media_dir=media_dir,
            media_base_url=media_base_url,
            media_max_age_s=media_max_age_s)

    # Quoted toot (Mastodon 4.3+ quote posts)
    quote_html = _render_quote_html(
        t.get("quote"), custom_emojis=custom_emojis, labels=labels)

    # Indentation style: use padding-left based on visual_depth
    indent_px = visual_depth * 10
    if is_root:
        indent_px = 0

    return (
        '<div class="{cls}" id="toot-{toot_id}" data-depth="{depth}"'
        ' style="padding-left:{indent}px">\n'
        '  <div class="{ns}__line" aria-hidden="true"></div>\n'
        '  <div class="{ns}__card" role="article"'
        ' aria-label="{post_by_aria}">\n'
        '    {boost_banner}\n'
        '    <div class="{ns}__head">\n'
        '      <a href="{acct_url}" target="_blank" rel="{link_rel}"'
        ' aria-hidden="true" tabindex="-1">'
        '<img class="{ns}__av" src="{avatar}" alt="" '
        'loading="lazy"></a>\n'
        '      <div class="{ns}__who">\n'
        '        <a href="{acct_url}" target="_blank" rel="{link_rel}" '
        'class="{ns}__name">{name}{verified}{bot_badge}</a>\n'
        '        <span class="{ns}__handle">{handle}</span>\n'
        '      </div>\n'
        '      <a href="{toot_url}" target="_blank" rel="{link_rel}" '
        'class="{ns}__time" title="{ts_abs}"'
        ' aria-label="{posted_aria}">'
        '<time datetime="{ts_iso}">{ts_rel}</time></a>{edited}\n'
        '    </div>\n'
        '    {reply_to}\n'
        '    {body_html}\n'
        '    {media}\n'
        '    {poll}\n'
        '    {card}\n'
        '    {quote}\n'
        '    <div class="{ns}__stats" role="group"'
        ' aria-label="{engagement_aria}">\n'
        '      <span class="{ns}__st {ns}__st--reply"'
        ' aria-label="{replies_aria}">'
        '{svg_reply}{n_reply}</span>\n'
        '      <span class="{ns}__st {ns}__st--boost"'
        ' aria-label="{boosts_aria}">'
        '{svg_boost}{n_boost}</span>\n'
        '      <span class="{ns}__st {ns}__st--fav"'
        ' aria-label="{favs_aria}">'
        '{svg_star}{n_fav}</span>\n'
        '    </div>\n'
        '  </div>\n'
        '  {kids}\n'
        '</div>'
    ).format(
        cls=cls, toot_id=toot_id, depth=depth, indent=indent_px, ns=NS,
        boost_banner=boost_banner, link_rel=link_rel,
        acct_url=acct_url, avatar=avatar, name=name, verified=verified,
        bot_badge=bot_badge, handle=handle,
        toot_url=toot_url, ts_abs=ts_abs, ts_rel=ts_rel, ts_iso=ts_iso,
        edited=edited_tag,
        reply_to=reply_to_html, body_html=body_html,
        media=media, poll=poll_html, card=card_html, quote=quote_html,
        n_reply=n_reply, n_boost=n_boost, n_fav=n_fav,
        svg_reply=SVG_REPLY, svg_boost=SVG_BOOST, svg_star=SVG_STAR,
        kids=kids,
        post_by_aria=labels["post_by_aria"].format(handle=handle),
        posted_aria=labels["posted_aria"].format(date=ts_abs),
        engagement_aria=labels["engagement_aria"],
        replies_aria=_pl(labels, "replies_aria", n_reply).format(n=n_reply),
        boosts_aria=_pl(labels, "boosts_aria", n_boost).format(n=n_boost),
        favs_aria=_pl(labels, "favs_aria", n_fav).format(n=n_fav),
    )


# =========================================================================
# Scoped CSS (self-contained, no link underlines)
# =========================================================================

SCOPED_CSS = """<style>
.{ns}-wrap{{{vars}
  --mt-av:44px;--mt-r:10px;
  all:initial;display:block;
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,sans-serif;
  font-size:15px;line-height:1.55;color:var(--mt-text);
  background:var(--mt-bg);padding:1.25rem;border-radius:var(--mt-r);max-width:700px;
}}
.{ns}-wrap *{{box-sizing:border-box}}
.{ns}-wrap a,.{ns}-wrap a:link,.{ns}-wrap a:visited,.{ns}-wrap a:hover,.{ns}-wrap a:active{{
  text-decoration:none!important;border-bottom:none!important;
}}
.{ns}__generated{{font-size:.72rem;color:var(--mt-dim);text-align:right;margin-bottom:.5rem}}
.{ns}__header{{display:flex;align-items:center;gap:.6rem;padding-bottom:.9rem;margin-bottom:.3rem;border-bottom:1px solid var(--mt-border);font-size:.82rem;color:var(--mt-dim)}}
.{ns}__header svg{{width:22px;height:22px;fill:var(--mt-accent);flex-shrink:0}}
.{ns}__header strong{{color:var(--mt-accent);font-weight:700}}
.{ns}__toot{{position:relative;margin-top:.15rem}}
.{ns}__root{{padding-left:0!important}}
.{ns}__root>.{ns}__line{{display:none}}
.{ns}__line{{position:absolute;left:-13px;top:0;bottom:0;width:2px;background:var(--mt-line);border-radius:1px}}
.{ns}__card{{background:var(--mt-card);border:1px solid var(--mt-border);border-radius:var(--mt-r);padding:.85rem 1rem;margin-bottom:.5rem;transition:border-color .2s,box-shadow .2s}}
.{ns}__card:hover{{border-color:var(--mt-accent);box-shadow:0 2px 16px var(--mt-shadow)}}
.{ns}__root>.{ns}__card{{background:var(--mt-card2);border-color:var(--mt-accent)}}
.{ns}__toot--popular>.{ns}__card{{border-color:var(--mt-fav);box-shadow:inset 3px 0 0 var(--mt-popular)}}
.{ns}__head{{display:flex;align-items:center;gap:.6rem;margin-bottom:.55rem}}
.{ns}__av{{width:var(--mt-av);height:var(--mt-av);border-radius:9px;object-fit:cover;border:2px solid var(--mt-border);transition:border-color .2s;flex-shrink:0}}
.{ns}__card:hover .{ns}__av{{border-color:var(--mt-accent)}}
.{ns}__who{{flex:1;min-width:0}}
.{ns}__name{{display:block;font-weight:700;font-size:.92rem;color:var(--mt-text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.{ns}__name:hover{{color:var(--mt-accent)}}
.{ns}__verified{{width:16px;height:16px;display:inline-block;vertical-align:middle;margin-left:3px;fill:none;stroke:var(--mt-verified);stroke-width:2;stroke-linecap:round;stroke-linejoin:round;flex-shrink:0}}
.{ns}__bot-icon{{width:15px;height:15px;display:inline-block;vertical-align:middle;margin-left:3px;fill:none;stroke:var(--mt-bot);stroke-width:2;stroke-linecap:round;stroke-linejoin:round;flex-shrink:0}}
.{ns}__handle{{display:block;font-family:monospace;font-size:.75rem;color:var(--mt-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.{ns}__time{{font-size:.75rem;color:var(--mt-dim);white-space:nowrap;flex-shrink:0}}
.{ns}__time:hover{{color:var(--mt-accent)}}
.{ns}__edited{{font-size:.7rem;color:var(--mt-dim);margin-left:.3rem;font-style:italic}}
.{ns}__reply-to{{font-size:.72rem;color:var(--mt-dim);padding:.15rem 0 .1rem;opacity:.7}}
.{ns}__boost-banner{{display:flex;align-items:center;gap:.35rem;font-size:.72rem;color:var(--mt-boost);padding:.25rem 0 .15rem;opacity:.85}}
.{ns}__boost-banner svg{{width:14px;height:14px}}
.{ns}__boost-banner a{{color:var(--mt-boost);font-weight:600}}
.{ns}__body{{font-size:.9rem;line-height:1.62;word-break:break-word;-webkit-hyphens:auto;hyphens:auto}}
.{ns}__body:empty{{display:none}}
.{ns}__body--emoji{{font-size:2rem;line-height:1.4;letter-spacing:.1em}}
.{ns}__body a{{color:var(--mt-accent)}}
.{ns}__body a:hover{{opacity:.8}}
.{ns}__hashtag{{color:var(--mt-hashtag);font-weight:600;padding:.05em .3em;background:var(--mt-hashtag-bg);border-radius:4px;font-size:.88em}}
.{ns}__hashtag:hover{{background:var(--mt-hashtag-hover)}}
.{ns}__mention{{color:var(--mt-mention);font-weight:500}}
.{ns}__mention:hover{{color:var(--mt-mention-hover)}}
.{ns}__emoji{{display:inline;width:1.2em;height:1.2em;vertical-align:middle;margin:0 .05em;object-fit:contain}}
.{ns}__body code{{font-family:monospace;font-size:.84em;padding:.1em .35em;background:var(--mt-border);border-radius:4px}}
.{ns}__cw-label{{font-size:.85rem;font-weight:600;color:var(--mt-cw);margin-bottom:.3rem;display:flex;align-items:center;gap:.3rem}}
.{ns}__cw-label svg{{flex-shrink:0}}
.{ns}__cw-details{{margin-top:.3rem}}
.{ns}__cw-toggle{{cursor:pointer;font-size:.8rem;color:var(--mt-accent);font-weight:500;padding:.2rem .5rem;background:var(--mt-border);border-radius:4px;display:inline-block}}
.{ns}__cw-toggle:hover{{opacity:.8}}
.{ns}__cw-body{{margin-top:.4rem}}
.{ns}__media{{margin-top:.6rem;border-radius:8px;overflow:hidden;border:1px solid var(--mt-border);position:relative}}
.{ns}__media img,.{ns}__media video{{display:block;width:100%;max-height:360px;object-fit:cover}}
.{ns}__alt-badge{{position:absolute;bottom:6px;left:6px;background:rgba(0,0,0,.7);color:#fff;font-size:.6rem;font-weight:700;letter-spacing:.04em;padding:2px 5px;border-radius:4px;border:none;cursor:help;line-height:1.3;max-width:calc(100% - 12px);overflow:hidden;white-space:nowrap;text-overflow:ellipsis;transition:all .2s ease}}
.{ns}__alt-badge:hover,.{ns}__alt-badge:focus{{white-space:normal;max-height:8rem;overflow-y:auto;background:rgba(0,0,0,.85);font-size:.72rem;font-weight:400;padding:6px 8px}}
.{ns}__sensitive{{margin-top:.6rem;border:1px solid var(--mt-border);border-radius:8px;overflow:hidden}}
.{ns}__sensitive-toggle{{display:flex;align-items:center;gap:.3rem;padding:.5rem .75rem;font-size:.82rem;font-weight:600;color:var(--mt-cw);cursor:pointer;list-style:none}}
.{ns}__sensitive-toggle::-webkit-details-marker{{display:none}}
.{ns}__sensitive-toggle svg{{flex-shrink:0}}
.{ns}__sensitive-body{{padding:0}}
.{ns}__sensitive-body .{ns}__media{{margin-top:0;border-radius:0;border:none;border-top:1px solid var(--mt-border)}}
.{ns}__media-link{{margin-top:.5rem;font-size:.82rem;font-style:italic}}
.{ns}__media-link a{{color:var(--mt-dim);display:flex;align-items:center;gap:.3rem}}
.{ns}__media-link a:hover{{color:var(--mt-accent)}}
.{ns}__media-icon{{width:15px;height:15px;stroke:currentColor;stroke-width:1.8;fill:none;stroke-linecap:round;stroke-linejoin:round;flex-shrink:0}}
.{ns}__poll{{margin-top:.6rem;border:1px solid var(--mt-border);border-radius:8px;padding:.6rem .75rem;background:var(--mt-card)}}
.{ns}__poll-opt{{position:relative;margin-bottom:.4rem;padding:.35rem .6rem;border-radius:5px;overflow:hidden;background:var(--mt-bg);display:flex;align-items:center;justify-content:space-between;min-height:1.8rem}}
.{ns}__poll-opt:last-of-type{{margin-bottom:0}}
.{ns}__poll-bar{{position:absolute;left:0;top:0;bottom:0;background:var(--mt-poll-bar);border-radius:5px;transition:width .4s ease}}
.{ns}__poll-label{{position:relative;z-index:1;font-size:.85rem;color:var(--mt-text);font-weight:500}}
.{ns}__poll-pct{{position:relative;z-index:1;font-size:.8rem;color:var(--mt-dim);font-weight:600;white-space:nowrap;margin-left:.5rem}}
.{ns}__poll-footer{{margin-top:.4rem;font-size:.75rem;color:var(--mt-dim)}}
.{ns}__card-preview{{display:flex;margin-top:.6rem;border:1px solid var(--mt-border);border-radius:8px;overflow:hidden;text-decoration:none!important;color:inherit;transition:border-color .15s}}
.{ns}__card-preview:hover{{border-color:var(--mt-accent)}}
.{ns}__card-thumb{{flex:0 0 120px;min-height:80px;background:var(--mt-border)}}
.{ns}__card-thumb img{{width:100%;height:100%;object-fit:cover;display:block}}
.{ns}__card-text{{flex:1;min-width:0;padding:.55rem .7rem;display:flex;flex-direction:column;gap:.15rem}}
.{ns}__card-provider{{font-size:.7rem;color:var(--mt-dim);text-transform:uppercase;letter-spacing:.03em}}
.{ns}__card-title{{font-size:.82rem;font-weight:600;color:var(--mt-text);line-height:1.3;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}}
.{ns}__card-desc{{font-size:.75rem;color:var(--mt-dim);line-height:1.35;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}}
.{ns}__quote{{display:block;margin-top:.6rem;border:1px solid var(--mt-border);border-radius:8px;padding:.6rem .75rem;text-decoration:none!important;color:inherit;transition:border-color .15s}}
.{ns}__quote:hover{{border-color:var(--mt-accent)}}
.{ns}__quote-head{{display:flex;align-items:center;gap:.4rem;margin-bottom:.3rem;font-size:.78rem}}
.{ns}__quote-av{{width:18px;height:18px;border-radius:50%;flex-shrink:0}}
.{ns}__quote-name{{font-weight:600;color:var(--mt-text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:10em}}
.{ns}__quote-handle{{color:var(--mt-dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:10em}}
.{ns}__quote-time{{color:var(--mt-dim);margin-left:auto;white-space:nowrap;font-size:.72rem}}
.{ns}__quote-body{{font-size:.82rem;line-height:1.45;color:var(--mt-text);display:-webkit-box;-webkit-line-clamp:4;-webkit-box-orient:vertical;overflow:hidden;-webkit-hyphens:auto;hyphens:auto}}
.{ns}__quote-body a{{color:var(--mt-accent);pointer-events:none}}
.{ns}__stats{{display:flex;gap:1rem;margin-top:.6rem;padding-top:.5rem;border-top:1px solid var(--mt-border)}}
.{ns}__st{{display:inline-flex;align-items:center;gap:.25rem;font-size:.78rem}}
.{ns}__st svg{{width:14px;height:14px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}}
.{ns}__st--reply{{color:var(--mt-reply)}}
.{ns}__st--boost{{color:var(--mt-boost)}}
.{ns}__st--fav{{color:var(--mt-fav)}}
.{ns}__replies{{padding-left:4px}}
.{ns}__blocked{{background:var(--mt-blocked-bg);border-radius:var(--mt-r);padding:.7rem 1rem;margin-bottom:.5rem;color:var(--mt-blocked-text);font-size:.85rem;font-style:italic;text-align:center}}
.{ns}__article{{margin-bottom:1rem;padding-bottom:.8rem;border-bottom:2px solid var(--mt-border)}}
.{ns}__article-label{{font-size:.72rem;color:var(--mt-dim);text-transform:uppercase;letter-spacing:.05em;margin-bottom:.5rem}}
.{ns}__article-author{{display:flex;align-items:center;gap:.6rem;margin-bottom:.8rem}}
.{ns}__article-chain{{display:flex;flex-direction:column;gap:0}}
.{ns}__article-post{{padding:.6rem 0;border-top:1px solid var(--mt-border)}}
.{ns}__article-post:first-child{{border-top:none;padding-top:0}}
.{ns}__article-body{{line-height:1.55;font-size:.95rem;-webkit-hyphens:auto;hyphens:auto}}
.{ns}__article-meta{{margin-top:.3rem;font-size:.72rem;color:var(--mt-dim);display:flex;align-items:center}}
.{ns}__permalink{{margin-left:auto;color:var(--mt-dim);opacity:.4;transition:opacity .15s}}
.{ns}__permalink:hover{{opacity:1;color:var(--mt-accent)}}
.{ns}__link-icon{{width:14px;height:14px;stroke:currentColor;stroke-width:2;fill:none;stroke-linecap:round;stroke-linejoin:round}}
.{ns}__article-ref{{color:var(--mt-accent);font-style:italic;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}}
.{ns}__article-ref:hover{{opacity:.8}}
.{ns}__article-stub-line{{display:flex;align-items:center;gap:.4rem;padding:.35rem .75rem;font-size:.88rem;min-width:0;overflow:hidden}}
.{ns}__article-stub-line .{ns}__handle{{flex-shrink:1;min-width:0;max-width:16em}}
.{ns}__article-sep{{font-size:.72rem;color:var(--mt-dim);text-transform:uppercase;letter-spacing:.05em;padding:.6rem 0 .3rem;margin-top:.2rem;border-top:2px solid var(--mt-border)}}
.{ns}__threadstats{{margin-top:.6rem;padding-top:.5rem;border-top:1px solid var(--mt-border);font-size:.78rem;color:var(--mt-dim);text-align:center}}
.{ns}__more{{margin-top:.6rem;padding:.7rem 1rem;background:var(--mt-card2);border:1px solid var(--mt-border);border-radius:var(--mt-r);text-align:center;font-size:.85rem}}
.{ns}__more a{{color:var(--mt-accent);font-weight:600}}
.{ns}__more a:hover{{opacity:.8}}
.{ns}__footer{{margin-top:.4rem;padding-top:.7rem;border-top:1px solid var(--mt-border);font-size:.78rem;color:var(--mt-dim);text-align:center}}
.{ns}__footer a{{color:var(--mt-accent)}}
.{ns}__footer a:hover{{opacity:.8}}
.{ns}__nocomments{{text-align:center;padding:1.5rem .5rem;color:var(--mt-dim);font-size:.9rem}}
.{ns}__empty-stats{{display:flex;justify-content:center;gap:1.2rem;padding:.8rem 0;margin:0 auto;border-bottom:1px solid var(--mt-border)}}
.{ns}__empty-stats .{ns}__st{{font-size:.85rem}}
.{ns}__be-first{{display:inline-block;margin-top:.5rem;color:var(--mt-accent);font-weight:600;font-size:.88rem}}
@keyframes mt-fadeUp{{from{{opacity:0;transform:translateY(10px)}}to{{opacity:1;transform:translateY(0)}}}}
.{ns}__toot{{animation:mt-fadeUp .35s ease both}}
.{ns}__fold{{margin-top:.2rem}}
.{ns}__fold-toggle{{cursor:pointer;font-size:.78rem;color:var(--mt-accent);font-weight:500;padding:.3rem .6rem;background:var(--mt-border);border-radius:4px;display:inline-block;margin:.2rem 0 .3rem}}
.{ns}__fold-toggle:hover{{opacity:.8}}
.{ns}__fold[open]>.{ns}__fold-toggle{{margin-bottom:.4rem;opacity:.6}}
@media(max-width:520px){{.{ns}-wrap{{--mt-av:36px;padding:.75rem}}.{ns}__card{{padding:.65rem .75rem}}}}
@media print{{
.{ns}-wrap{{background:#fff!important;color:#111!important;padding:0;box-shadow:none}}
.{ns}__card{{background:#fff!important;border:1px solid #ccc!important;box-shadow:none!important;break-inside:avoid}}
.{ns}__toot--popular>.{ns}__card{{box-shadow:none!important;border-left:3px solid #d97706!important}}
.{ns}__root>.{ns}__card{{background:#f8f8f8!important}}
.{ns}__av{{width:24px!important;height:24px!important}}
.{ns}__body,.{ns}__body a,.{ns}__article-body,.{ns}__article-body a{{color:#111!important}}
.{ns}__name,.{ns}__handle,.{ns}__time,.{ns}__reply-to,.{ns}__st,.{ns}__poll-footer,.{ns}__card-provider,.{ns}__card-desc,.{ns}__quote-handle,.{ns}__quote-time{{color:#555!important}}
.{ns}__line{{display:none}}
.{ns}__fold>summary{{display:none!important}}
.{ns}__fold>:not(summary){{display:block!important}}
.{ns}__sensitive>summary{{display:none!important}}
.{ns}__sensitive>:not(summary){{display:block!important}}
.{ns}__body a::after,.{ns}__article-body a::after{{content:" (" attr(href) ")";font-size:.7em;color:#888;word-break:break-all}}
.{ns}__card-preview::after,.{ns}__quote::after,.{ns}__av+*::after{{content:none!important}}
.{ns}__stats{{font-size:.72rem}}
.{ns}__boost-banner svg,.{ns}__st svg{{width:12px!important;height:12px!important}}
}}
</style>"""

# Ordered list of CSS custom property names (shared by all themes).
_THEME_KEYS = [
    # Layout
    "--mt-bg", "--mt-card", "--mt-card2",
    "--mt-border", "--mt-line", "--mt-shadow",
    # Typography
    "--mt-text", "--mt-dim",
    # Accents
    "--mt-accent", "--mt-reply",
    "--mt-boost", "--mt-fav",
    # Content warnings / sensitive
    "--mt-cw",
    # Verification / bot
    "--mt-verified", "--mt-bot",
    # Blocked / filtered
    "--mt-blocked-bg", "--mt-blocked-text",
    # Hashtags
    "--mt-hashtag", "--mt-hashtag-bg", "--mt-hashtag-hover",
    # Mentions
    "--mt-mention", "--mt-mention-hover",
    # Polls
    "--mt-poll-bar",
    # Popular highlight
    "--mt-popular",
]

THEME_DARK = {
    "--mt-bg":            "#0d1017",
    "--mt-card":          "#151a24",
    "--mt-card2":         "#1a2030",
    "--mt-border":        "#262e3d",
    "--mt-line":          "#262e3d",
    "--mt-shadow":        "rgba(0,0,0,.3)",
    "--mt-text":          "#e0e6f0",
    "--mt-dim":           "#7a8599",
    "--mt-accent":        "#6b8aff",
    "--mt-reply":         "#6b8aff",
    "--mt-boost":         "#34d4b0",
    "--mt-fav":           "#fbbf24",
    "--mt-cw":            "#e5a54b",
    "--mt-verified":      "#34d4b0",
    "--mt-bot":           "#7a8899",
    "--mt-blocked-bg":    "#1c2030",
    "--mt-blocked-text":  "#555e70",
    "--mt-hashtag":       "#8b9cf7",
    "--mt-hashtag-bg":    "rgba(107,138,255,.12)",
    "--mt-hashtag-hover": "rgba(107,138,255,.22)",
    "--mt-mention":       "#b098e6",
    "--mt-mention-hover": "#c9b5f5",
    "--mt-poll-bar":      "rgba(107,138,255,.18)",
    "--mt-popular":       "rgba(251,191,36,.35)",
}

THEME_LIGHT = {
    "--mt-bg":            "#f4f5f7",
    "--mt-card":          "#fff",
    "--mt-card2":         "#f0f2ff",
    "--mt-border":        "#d8dce5",
    "--mt-line":          "#d8dce5",
    "--mt-shadow":        "rgba(0,0,0,.06)",
    "--mt-text":          "#1a1d23",
    "--mt-dim":           "#6b7280",
    "--mt-accent":        "#4f5dd6",
    "--mt-reply":         "#4f5dd6",
    "--mt-boost":         "#0d9488",
    "--mt-fav":           "#d97706",
    "--mt-cw":            "#b45309",
    "--mt-verified":      "#0d9488",
    "--mt-bot":           "#8896a7",
    "--mt-blocked-bg":    "#e8e8ec",
    "--mt-blocked-text":  "#9ca3af",
    "--mt-hashtag":       "#4f5dd6",
    "--mt-hashtag-bg":    "rgba(79,93,214,.1)",
    "--mt-hashtag-hover": "rgba(79,93,214,.18)",
    "--mt-mention":       "#7c5cbf",
    "--mt-mention-hover": "#5a3d9e",
    "--mt-poll-bar":      "rgba(79,93,214,.12)",
    "--mt-popular":       "rgba(217,119,6,.25)",
}


def _vars_to_css(theme_dict):
    """Convert a theme dict to an inline CSS custom-property string."""
    return "".join("{}:{};".format(k, theme_dict[k]) for k in _THEME_KEYS)


def scoped_css(theme="dark"):
    if theme == "auto":
        # Emit both schemes via @media queries; light is the fallback
        vars_block = (
            "{light}\n"
            "}}\n"
            "@media(prefers-color-scheme:dark){{.{ns}-wrap{{\n"
            "  {dark}\n"
            "}}"
        ).format(
            ns=NS,
            light=_vars_to_css(THEME_LIGHT),
            dark=_vars_to_css(THEME_DARK))
    elif theme == "light":
        vars_block = _vars_to_css(THEME_LIGHT)
    else:
        vars_block = _vars_to_css(THEME_DARK)
    return SCOPED_CSS.format(ns=NS, vars=vars_block)


# =========================================================================
# Fragment generation
# =========================================================================

def generate_fragment(root_node, instance, total, toot_url,
                      theme="dark", blocklist=None, whitelist=None,
                      avatar_dir=None, avatar_base_url=None,
                      av_max_age_s=0, max_depth=5,
                      stats=None, max_toots=0, sort="oldest",
                      custom_emojis=False, article_html="",
                      article_ids=None,
                      media_dir=None, media_base_url=None,
                      media_max_age_s=0,
                      article_nocomment=False,
                      labels=None, highlight_above=0,
                      fold_depth=0, fold_threshold=0,
                      max_media=0, css_extra=""):
    if labels is None:
        labels = DEFAULT_LABELS
    css = scoped_css(theme)
    if css_extra:
        css += "\n<style>\n{}\n</style>".format(css_extra)

    # Sort tree before rendering
    if sort != "oldest":
        sort_tree(root_node, sort)
        log_debug("  SORT  tree by '{}'".format(sort))

    # Set up counter for max_toots (0 = unlimited)
    counter = None
    if max_toots > 0:
        counter = {"remaining": max_toots, "skipped": 0}

    # Identify OP account for media gating
    _ra = root_node.d.get("account", {})
    _op_acct = _ra.get("acct") or _ra.get("username", "")

    # Build reply-to map: toot_id -> parent account handle
    _reply_map = {}
    def _build_reply_map(node):
        parent_acct = node.d.get("account", {}).get("acct", "")
        for child in node.children:
            _reply_map[str(child.d.get("id", ""))] = parent_acct
            _build_reply_map(child)
    _build_reply_map(root_node)

    ctx = RenderContext(
        blocklist=blocklist, whitelist=whitelist,
        avatar_dir=avatar_dir, avatar_base_url=avatar_base_url,
        av_max_age_s=av_max_age_s, max_depth=max_depth,
        counter=counter, custom_emojis=custom_emojis,
        article_ids=article_ids, op_acct=_op_acct,
        reply_map=_reply_map,
        media_dir=media_dir, media_base_url=media_base_url,
        media_max_age_s=media_max_age_s, labels=labels,
        seen_boost_ids=set(), highlight_above=highlight_above,
        fold_depth=fold_depth if (fold_depth > 0
                                  and (fold_threshold <= 0
                                       or total - 1 >= fold_threshold)) else 0,
        max_media=max_media)
    tree_html = render_toot(root_node, depth=0, is_root=True, ctx=ctx)
    esc_inst  = H.escape(instance)
    esc_url   = H.escape(toot_url)
    n         = total - 1
    # In article mode, subtract toots shown in article block
    if article_ids:
        # article_ids includes root, which is already excluded by -1
        n = n - (len(article_ids) - 1)
        if n < 0:
            n = 0
    count_txt = _pl(labels, "comment_count", n).format(n=n)
    now_utc   = datetime.now(timezone.utc).strftime("%b %d, %Y at %H:%M UTC")

    # Stats line
    stats_html = ""
    if stats and stats["total"] > 1:
        st_total = stats["total"]
        st_users = stats["users"]

        # In article mode, recalculate for comment section only
        if article_ids:
            # Subtract article toots from total
            st_total = st_total - len(article_ids)
            if st_total < 0:
                st_total = 0

            # Recalculate unique users from non-article toots only
            comment_users = set()
            all_toots = [root_node.d] + [
                n.d for n in _iter_nodes(root_node)]
            for t in all_toots:
                tid = str(t.get("id", ""))
                if tid in article_ids:
                    continue
                acct = t.get("account", {}).get("acct", "")
                if acct:
                    comment_users.add(acct)
            st_users = len(comment_users)

        if st_total > 0:
            stats_html = (
                '<div class="{ns}__threadstats">'
                '{stats_text}'
                '</div>\n'
            ).format(ns=NS, stats_text=labels["thread_stats"].format(
                total=st_total, users=st_users))

    # Truncation notice
    more_html = ""
    if counter is not None and counter["skipped"] > 0:
        skipped = counter["skipped"]
        more_html = (
            '<div class="{ns}__more">'
            '<a href="{url}" target="_blank" rel="noopener">'
            '{more_label} &#8599;</a></div>\n'
        ).format(ns=NS, url=esc_url,
                 more_label=_pl(labels, "more_replies", skipped).format(
                     n=skipped, instance=esc_inst))
        log_debug("  TRUNCATE  showing {}, skipped {}".format(
            max_toots, skipped))

    # Separator between article and comments
    article_sep = ""
    if article_html and not article_nocomment:
        article_sep = (
            '<div class="{ns}__article-sep">{sep}</div>'
        ).format(ns=NS, sep=labels["comments_separator"])

    # In nocomment mode, suppress comment tree, stats, truncation notice
    if article_nocomment and article_html:
        tree_html = ""
        more_html = ""
        stats_html = ""
        count_txt = labels["article_label"]

    footer_label = (labels["view_on"]
                    if (article_nocomment and article_html)
                    else labels["reply_on"])

    return (
        "{css}\n"
        '<div class="{ns}-wrap" role="region"'
        ' aria-label="{comments_aria}">\n'
        '  <div class="{ns}__generated" aria-hidden="true">'
        '{generated}</div>\n'
        '  <div class="{ns}__header">\n'
        "    {svg}\n"
        "    <span><strong>{mastodon}</strong>"
        " &middot; {count}</span>\n"
        "  </div>\n"
        "  {article}\n"
        "  {article_sep}\n"
        "  {tree}\n"
        "  {more}\n"
        "  {stats}\n"
        '  <div class="{ns}__footer">\n'
        '    <a href="{url}" target="_blank" rel="noopener">'
        "{footer_label} {inst} &#8599;</a>\n"
        "  </div>\n"
        "</div>\n"
    ).format(css=css, ns=NS, svg=SVG_MASTO,
             generated=labels["generated"].format(date=now_utc),
             mastodon=labels["mastodon"],
             comments_aria=labels["comments_aria"],
             count=count_txt, inst=esc_inst, article=article_html,
             article_sep=article_sep,
             tree=tree_html,
             more=more_html, stats=stats_html, url=esc_url,
             footer_label=footer_label)


def generate_empty(toot_url, instance, theme="dark", labels=None,
                   stammtoot=None, css_extra=""):
    if labels is None:
        labels = DEFAULT_LABELS
    css      = scoped_css(theme)
    if css_extra:
        css += "\n<style>\n{}\n</style>".format(css_extra)
    esc_inst = H.escape(instance)
    esc_url  = H.escape(toot_url)
    now_utc  = datetime.now(timezone.utc).strftime("%b %d, %Y at %H:%M UTC")

    # Root toot engagement stats
    stats_html = ""
    if stammtoot and isinstance(stammtoot, dict):
        n_reply = stammtoot.get("replies_count", 0) or 0
        n_boost = stammtoot.get("reblogs_count", 0) or 0
        n_fav   = stammtoot.get("favourites_count", 0) or 0
        if n_reply or n_boost or n_fav:
            stats_html = (
                '  <div class="{ns}__empty-stats" role="group"'
                ' aria-label="{engagement_aria}">\n'
                '    <span class="{ns}__st {ns}__st--reply"'
                ' aria-label="{replies_aria}">'
                '{svg_reply}{n_reply}</span>\n'
                '    <span class="{ns}__st {ns}__st--boost"'
                ' aria-label="{boosts_aria}">'
                '{svg_boost}{n_boost}</span>\n'
                '    <span class="{ns}__st {ns}__st--fav"'
                ' aria-label="{favs_aria}">'
                '{svg_star}{n_fav}</span>\n'
                '  </div>\n'
            ).format(ns=NS,
                     svg_reply=SVG_REPLY, svg_boost=SVG_BOOST,
                     svg_star=SVG_STAR,
                     n_reply=n_reply, n_boost=n_boost, n_fav=n_fav,
                     engagement_aria=labels["engagement_aria"],
                     replies_aria=_pl(labels, "replies_aria",
                                      n_reply).format(n=n_reply),
                     boosts_aria=_pl(labels, "boosts_aria",
                                     n_boost).format(n=n_boost),
                     favs_aria=_pl(labels, "favs_aria",
                                   n_fav).format(n=n_fav))

    return (
        "{css}\n"
        '<div class="{ns}-wrap" role="region"'
        ' aria-label="{comments_aria}">\n'
        '  <div class="{ns}__generated" aria-hidden="true">'
        '{generated}</div>\n'
        '  <div class="{ns}__header">\n'
        "    {svg}\n"
        "    <span><strong>{mastodon}</strong>"
        " &middot; {comments_header}</span>\n"
        "  </div>\n"
        "{stats}"
        '  <div class="{ns}__nocomments">\n'
        "    {no_comments}<br>\n"
        '    <a href="{url}" target="_blank" rel="noopener"'
        ' class="{ns}__be-first">'
        "{be_first} &#8599;</a>\n"
        "  </div>\n"
        '  <div class="{ns}__footer">\n'
        '    <a href="{url}" target="_blank" rel="noopener">'
        "{reply_on} {inst} &#8599;</a>\n"
        "  </div>\n"
        "</div>\n"
    ).format(css=css, ns=NS, svg=SVG_MASTO,
             generated=labels["generated"].format(date=now_utc),
             mastodon=labels["mastodon"],
             comments_aria=labels["comments_aria"],
             comments_header=labels["comments_header"],
             no_comments=labels["no_comments"],
             be_first=labels["be_first"],
             reply_on=labels["reply_on"],
             inst=esc_inst, url=esc_url,
             stats=stats_html)


# =========================================================================
# Atomic file writing
# =========================================================================


def _content_hash(text):
    """
    SHA-256 hex digest of a fragment, ignoring the "Generated …" timestamp
    so that regeneration with identical settings produces the same hash.
    """
    stable = re.sub(
        r'<div class="[^"]*__generated"[^>]*>.*?</div>', '', text)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def _atomic_write(path, content):
    """
    Write *content* to *path* atomically.
    Writes to a temporary file in the same directory, then renames.
    On failure the original file is left intact.
    """
    dirn = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=dirn, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(content)
        os.replace(tmp, path)
    except BaseException:
        # Clean up temp file on any failure
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


LOCK_FILENAME = ".toot.lock"


def _acquire_lock(lock_dir):
    """
    Acquire an exclusive, non-blocking lock via flock().
    Returns the open file object (keep it alive!) or None if locking
    is unavailable (e.g. Windows).  Raises RuntimeError if another
    instance holds the lock.
    """
    if not _HAS_FCNTL:
        return None
    lock_path = os.path.join(lock_dir, LOCK_FILENAME)
    # Open (or create) the lock file; we keep the fd open for the
    # lifetime of the process – flock is released when fd is closed.
    fh = open(lock_path, "w")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        raise RuntimeError(
            "Another toot.py instance is already running "
            "(lock: {})".format(lock_path))
    # Write PID for diagnostics (not used for locking)
    fh.write(str(os.getpid()))
    fh.flush()
    return fh


def _release_lock(fh):
    """Release and close the lock file."""
    if fh is None:
        return
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        fh.close()
    except OSError:
        pass


# =========================================================================
# JSON sidecar
# =========================================================================

def write_sidecar(out_dir, toot_id, stammtoot, descendants, blocklist, stats,
                  whitelist=None, fetched_at=None, content_hash=None):
    """Write a .json sidecar file alongside the .html.include."""
    op_acct = (stammtoot.get("account", {}).get("acct")
               or stammtoot.get("account", {}).get("username", ""))
    filtered_urls = []
    for t in descendants:
        t_acct = t.get("account", {}).get("acct") or t.get("account", {}).get("username", "")
        if not (op_acct and t_acct == op_acct) and is_filtered(t, blocklist, whitelist):
            filtered_urls.append(t.get("url") or t.get("uri") or str(t.get("id")))

    now_iso = datetime.now(timezone.utc).isoformat()
    data = {
        "sidecar_version": SIDECAR_VERSION,
        "toot_id": toot_id,
        "reply_count": len(descendants),
        "fetched_at": fetched_at or now_iso,
        "regenerated_at": now_iso if fetched_at else None,
        "filtered_toots": filtered_urls,
        "latest_toot_date": stats.get("latest_date", ""),
        "total_visible": stats.get("total", 0),
        "unique_users": stats.get("users", 0),
        "content_hash": content_hash or "",
        "api_data": {
            "stammtoot": stammtoot,
            "descendants": descendants,
        },
    }

    path = os.path.join(out_dir, "{}.json".format(toot_id))
    _atomic_write(path, json.dumps(data, indent=2, ensure_ascii=False))


def read_sidecar(out_dir, toot_id):
    """
    Read a .json sidecar and return
    (stammtoot, descendants, fetched_at, content_hash) or None.
    Returns None if the sidecar is missing, corrupt, or lacks api_data.
    """
    path = os.path.join(out_dir, "{}.json".format(toot_id))
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        ver = data.get("sidecar_version", 1)
        if ver > SIDECAR_VERSION:
            log_warn("    SIDECAR {} has version {} (expected <= {}), "
                     "skipping".format(toot_id, ver, SIDECAR_VERSION))
            return None
        if ver < SIDECAR_VERSION:
            log_debug("    SIDECAR {} version {} (current {}), "
                      "will be upgraded on write".format(
                          toot_id, ver, SIDECAR_VERSION))
        api = data.get("api_data")
        if not api or not isinstance(api, dict):
            return None
        stammtoot = api.get("stammtoot")
        descendants = api.get("descendants")
        if not isinstance(stammtoot, dict) or not isinstance(descendants, list):
            return None
        fetched_at = data.get("fetched_at", "")
        content_hash = data.get("content_hash", "")
        return stammtoot, descendants, fetched_at, content_hash
    except (OSError, json.JSONDecodeError, KeyError) as e:
        log_debug("    SIDECAR read failed for {}: {}".format(toot_id, e))
        return None


_SIDECAR_REQUIRED = ("sidecar_version", "toot_id", "api_data")


def validate_sidecars(toot_cache_dir, active_toot_ids=None):
    """
    Validate all .json sidecars in *toot_cache_dir*.
    Returns a dict with summary counts and per-file issues.

    Checks per sidecar:
      - valid JSON
      - required top-level keys (sidecar_version, toot_id, api_data)
      - sidecar_version <= SIDECAR_VERSION (not from the future)
      - sidecar_version == SIDECAR_VERSION (up to date)
      - api_data.stammtoot is a dict with "id" and "account"
      - api_data.descendants is a list
      - fetched_at is present and non-empty
      - matching .html.include exists

    Cross-checks (if active_toot_ids provided):
      - orphaned sidecars (no matching .markdown source)
      - missing sidecars (.markdown source but no .json)
    """
    issues = []       # list of (filename, severity, message)
    n_ok = 0
    n_warn = 0
    n_error = 0
    n_checked = 0

    sidecar_tids = set()

    if not os.path.isdir(toot_cache_dir):
        issues.append(("(dir)", "error",
                       "Toot cache directory does not exist: {}".format(
                           toot_cache_dir)))
        return {"ok": 0, "warn": 0, "error": 1, "checked": 0,
                "issues": issues}

    for fname in sorted(os.listdir(toot_cache_dir)):
        if not fname.endswith(".json"):
            continue
        if fname.endswith(".deprecated"):
            continue
        tid = fname[:-len(".json")]
        sidecar_tids.add(tid)
        fpath = os.path.join(toot_cache_dir, fname)
        n_checked += 1
        file_ok = True

        # 1. Valid JSON
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError as e:
            issues.append((fname, "error",
                           "Invalid JSON: {}".format(e)))
            n_error += 1
            continue
        except OSError as e:
            issues.append((fname, "error",
                           "Cannot read: {}".format(e)))
            n_error += 1
            continue

        if not isinstance(data, dict):
            issues.append((fname, "error",
                           "Top level is not a JSON object"))
            n_error += 1
            continue

        # 2. Required top-level keys
        for key in _SIDECAR_REQUIRED:
            if key not in data:
                issues.append((fname, "error",
                               "Missing required key: {}".format(key)))
                n_error += 1
                file_ok = False

        if not file_ok:
            continue

        # 3. Version checks
        ver = data.get("sidecar_version", 1)
        if not isinstance(ver, int):
            issues.append((fname, "error",
                           "sidecar_version is not an integer: {!r}".format(ver)))
            n_error += 1
            continue
        if ver > SIDECAR_VERSION:
            issues.append((fname, "error",
                           "Version {} is newer than supported ({})"
                           .format(ver, SIDECAR_VERSION)))
            n_error += 1
            file_ok = False
        elif ver < SIDECAR_VERSION:
            issues.append((fname, "warn",
                           "Version {} is outdated (current: {}), "
                           "will be upgraded on next write"
                           .format(ver, SIDECAR_VERSION)))
            n_warn += 1

        # 4. api_data structure
        api = data.get("api_data")
        if not isinstance(api, dict):
            issues.append((fname, "error",
                           "api_data is not a dict"))
            n_error += 1
            file_ok = False
        else:
            st = api.get("stammtoot")
            if not isinstance(st, dict):
                issues.append((fname, "error",
                               "api_data.stammtoot is not a dict"))
                n_error += 1
                file_ok = False
            else:
                if "id" not in st:
                    issues.append((fname, "warn",
                                   "stammtoot missing 'id' field"))
                    n_warn += 1
                if "account" not in st:
                    issues.append((fname, "warn",
                                   "stammtoot missing 'account' field"))
                    n_warn += 1
            desc = api.get("descendants")
            if not isinstance(desc, list):
                issues.append((fname, "error",
                               "api_data.descendants is not a list"))
                n_error += 1
                file_ok = False

        # 5. fetched_at
        fa = data.get("fetched_at")
        if not fa:
            issues.append((fname, "warn",
                           "Missing or empty fetched_at"))
            n_warn += 1

        # 6. toot_id consistency
        stored_tid = str(data.get("toot_id", ""))
        if stored_tid and stored_tid != tid:
            issues.append((fname, "warn",
                           "toot_id inside JSON ({}) does not match "
                           "filename ({})".format(stored_tid, tid)))
            n_warn += 1

        # 7. Matching .html.include
        include_path = os.path.join(
            toot_cache_dir, "{}.html.include".format(tid))
        if not os.path.isfile(include_path):
            issues.append((fname, "warn",
                           "No matching .html.include file"))
            n_warn += 1

        if file_ok and not any(
                s == "error" and f == fname for f, s, _ in issues):
            n_ok += 1

    # Cross-checks with .markdown sources
    if active_toot_ids is not None:
        orphaned = sidecar_tids - active_toot_ids
        missing = active_toot_ids - sidecar_tids
        for tid in sorted(orphaned):
            issues.append(("{}.json".format(tid), "warn",
                           "Orphaned sidecar (no matching .markdown source)"))
            n_warn += 1
        for tid in sorted(missing):
            issues.append(("(missing)", "warn",
                           "No sidecar for active toot {}".format(tid)))
            n_warn += 1

    return {
        "ok": n_ok,
        "warn": n_warn,
        "error": n_error,
        "checked": n_checked,
        "issues": issues,
    }


# All top-level keys a current (v2) sidecar should have, with defaults
# for fields that might be missing in older versions.
_SIDECAR_DEFAULTS = {
    "sidecar_version": SIDECAR_VERSION,
    "toot_id":         "",
    "reply_count":     0,
    "fetched_at":      "",
    "regenerated_at":  None,
    "filtered_toots":  [],
    "latest_toot_date":"",
    "total_visible":   0,
    "unique_users":    0,
    "content_hash":    "",
    "api_data":        None,   # required, never auto-filled
}


def migrate_sidecars(toot_cache_dir):
    """
    Upgrade all .json sidecars in *toot_cache_dir* to current
    SIDECAR_VERSION.  Adds missing fields with sensible defaults,
    recomputes reply_count from api_data if available.
    No API calls.

    Returns dict: {migrated, skipped, error, checked}
    """
    n_migrated = 0
    n_skipped = 0
    n_error = 0
    n_checked = 0

    if not os.path.isdir(toot_cache_dir):
        log_error("ERROR: Toot cache directory does not exist: {}".format(
            toot_cache_dir))
        return {"migrated": 0, "skipped": 0, "error": 1, "checked": 0}

    for fname in sorted(os.listdir(toot_cache_dir)):
        if not fname.endswith(".json"):
            continue
        if fname.endswith(".deprecated"):
            continue
        tid = fname[:-len(".json")]
        fpath = os.path.join(toot_cache_dir, fname)
        n_checked += 1

        # Read
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError) as e:
            log_warn("  ERROR  {}: cannot read ({})".format(fname, e))
            n_error += 1
            continue

        if not isinstance(data, dict):
            log_warn("  ERROR  {}: top level is not a dict".format(fname))
            n_error += 1
            continue

        ver = data.get("sidecar_version", 1)
        if ver > SIDECAR_VERSION:
            log_warn("  SKIP   {}: version {} is newer than supported ({})".format(
                fname, ver, SIDECAR_VERSION))
            n_skipped += 1
            continue

        # Check if migration is needed
        needs_update = ver < SIDECAR_VERSION
        for key in _SIDECAR_DEFAULTS:
            if key == "api_data":
                continue  # never auto-fill
            if key not in data:
                needs_update = True
                break

        if not needs_update:
            log_debug("  OK     {}: already current (v{})".format(fname, ver))
            n_skipped += 1
            continue

        # Apply defaults for missing fields
        changed = []
        for key, default in _SIDECAR_DEFAULTS.items():
            if key == "api_data":
                continue
            if key not in data:
                data[key] = default
                changed.append(key)

        # Recompute reply_count from api_data if available
        api = data.get("api_data")
        if isinstance(api, dict):
            desc = api.get("descendants")
            if isinstance(desc, list):
                data["reply_count"] = len(desc)

        # Ensure toot_id matches filename
        if not data.get("toot_id"):
            data["toot_id"] = tid

        # Bump version
        old_ver = data.get("sidecar_version", 1)
        data["sidecar_version"] = SIDECAR_VERSION

        # Write back atomically
        _atomic_write(fpath, json.dumps(data, indent=2, ensure_ascii=False))
        n_migrated += 1

        parts = []
        if old_ver < SIDECAR_VERSION:
            parts.append("v{} -> v{}".format(old_ver, SIDECAR_VERSION))
        if changed:
            parts.append("added: {}".format(", ".join(changed)))
        log_info("  MIGRATE  {}: {}".format(fname, "; ".join(parts)))

    return {
        "migrated": n_migrated,
        "skipped": n_skipped,
        "error": n_error,
        "checked": n_checked,
    }


# =========================================================================
# Cache freshness
# =========================================================================

def cache_is_fresh(path, max_age_seconds):
    try:
        return (time.time() - os.path.getmtime(path)) < max_age_seconds
    except OSError:
        return False


def parse_stale_after(val):
    """
    Parse a stale-after value into seconds.
    Accepts:  '12m' (months), '365d' (days), '365' (days),
              '0' or '' to disable.
    Returns seconds or 0 (disabled).
    """
    val = str(val).strip().lower()
    if not val or val == "0":
        return 0
    if val.endswith("m"):
        months = int(val[:-1])
        return months * 30 * 86400      # approximate: 30 days/month
    if val.endswith("d"):
        return int(val[:-1]) * 86400
    return int(val) * 86400             # bare number = days


def thread_is_stale(toot_cache_dir, toot_id, stale_after_s):
    """
    Check if a thread's last activity is older than stale_after_s.
    Requires both the .html.include AND the .json sidecar to exist.
    Returns True if the thread should be skipped (stale).
    Returns False if the thread should be processed normally.
    """
    if stale_after_s <= 0:
        return False

    include_path = os.path.join(
        toot_cache_dir, "{}.html.include".format(toot_id))
    sidecar_path = os.path.join(
        toot_cache_dir, "{}.json".format(toot_id))

    if not os.path.isfile(include_path) or not os.path.isfile(sidecar_path):
        return False

    try:
        with open(sidecar_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        latest = data.get("latest_toot_date", "")
        if not latest:
            return False
        # Parse ISO date (e.g. "2024-01-15T10:30:00.000Z")
        latest_clean = latest.replace("Z", "+00:00")
        dt = datetime.fromisoformat(latest_clean)
        age_s = (datetime.now(timezone.utc) - dt).total_seconds()
        return age_s > stale_after_s
    except (json.JSONDecodeError, ValueError, KeyError, OSError) as e:
        log_debug("    STALE check failed for {}: {}".format(toot_id, e))
        return False


# =========================================================================
# Cleanup: remove orphaned cache files
# =========================================================================

def do_cleanup(toot_cache_dir, active_toot_ids):
    """
    Remove .html.include files with no matching .markdown source.
    Rename .json files to .json.deprecated instead of deleting.
    """
    removed = 0
    deprecated = 0

    if not os.path.isdir(toot_cache_dir):
        return removed, deprecated

    for fname in os.listdir(toot_cache_dir):
        fpath = os.path.join(toot_cache_dir, fname)
        if not os.path.isfile(fpath):
            continue

        # Extract toot_id from filename
        if fname.endswith(".html.include"):
            tid = fname[:-len(".html.include")]
        elif fname.endswith(".json") and not fname.endswith(".deprecated"):
            tid = fname[:-len(".json")]
        else:
            continue

        if tid not in active_toot_ids:
            if fname.endswith(".html.include"):
                os.remove(fpath)
                removed += 1
                log_info("  REMOVE  {}".format(fname))
            elif fname.endswith(".json"):
                new_path = fpath + ".deprecated"
                os.rename(fpath, new_path)
                deprecated += 1
                log_info("  DEPREC  {} -> {}.deprecated".format(fname, fname))

    return removed, deprecated


def do_cleanup_avatars(avatar_dir, toot_cache_dir):
    """
    Remove avatar files that are not referenced by any active
    .html.include file.  Scans HTML fragments for SHA256-based
    avatar filenames.
    """
    if not os.path.isdir(avatar_dir):
        log_warn("  Avatar dir does not exist: {}".format(avatar_dir))
        return 0

    # Collect all avatar filenames present on disk
    on_disk = set()
    for fname in os.listdir(avatar_dir):
        fpath = os.path.join(avatar_dir, fname)
        if os.path.isfile(fpath):
            on_disk.add(fname)

    if not on_disk:
        log_info("  No avatars on disk.")
        return 0

    # Scan all active .html.include files for referenced avatar filenames
    referenced = set()
    # Pattern: 64-char hex hash + extension (avatar filenames)
    pat = re.compile(r"([0-9a-f]{64}\.[a-zA-Z]{2,5})")

    if os.path.isdir(toot_cache_dir):
        for fname in os.listdir(toot_cache_dir):
            if not fname.endswith(".html.include"):
                continue
            fpath = os.path.join(toot_cache_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as fh:
                    content = fh.read()
                for m in pat.finditer(content):
                    referenced.add(m.group(1))
            except Exception:
                continue

    orphaned = on_disk - referenced
    removed = 0
    for fname in sorted(orphaned):
        fpath = os.path.join(avatar_dir, fname)
        try:
            os.remove(fpath)
            removed += 1
            log_info("  REMOVE  avatar {}".format(fname))
        except OSError as e:
            log_warn("  WARN  could not remove {}: {}".format(fname, e))

    kept = len(on_disk) - removed
    log_info("  Avatars: {} removed, {} kept".format(removed, kept))
    return removed


def do_cleanup_media(media_dir, toot_cache_dir):
    """
    Remove media files that are not referenced by any active
    .html.include file.  Scans HTML fragments for SHA256-based
    media filenames.
    """
    if not os.path.isdir(media_dir):
        log_warn("  Media dir does not exist: {}".format(media_dir))
        return 0

    # Collect all media filenames present on disk
    on_disk = set()
    for fname in os.listdir(media_dir):
        fpath = os.path.join(media_dir, fname)
        if os.path.isfile(fpath):
            on_disk.add(fname)

    if not on_disk:
        log_info("  No media files on disk.")
        return 0

    # Scan all active .html.include files for referenced media filenames
    referenced = set()
    # Pattern: 64-char hex hash + extension (media filenames)
    pat = re.compile(r"([0-9a-f]{64}\.[a-zA-Z0-9]{2,5})")

    if os.path.isdir(toot_cache_dir):
        for fname in os.listdir(toot_cache_dir):
            if not fname.endswith(".html.include"):
                continue
            fpath = os.path.join(toot_cache_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as fh:
                    content = fh.read()
                for m in pat.finditer(content):
                    referenced.add(m.group(1))
            except Exception:
                continue

    orphaned = on_disk - referenced
    removed = 0
    for fname in sorted(orphaned):
        fpath = os.path.join(media_dir, fname)
        try:
            os.remove(fpath)
            removed += 1
            log_info("  REMOVE  media {}".format(fname))
        except OSError as e:
            log_warn("  WARN  could not remove {}: {}".format(fname, e))

    kept = len(on_disk) - removed
    log_info("  Media: {} removed, {} kept".format(removed, kept))
    return removed


# =========================================================================
# Process one .markdown file
# =========================================================================

def _fetch_thread(base_url, toot_id, token, retries, rate_limit):
    """
    Fetch stammtoot and context from API.
    Returns (stammtoot, descendants) or raises RuntimeError.
    Logs a warning if the server appears to have truncated the response.
    """
    stammtoot = api_get(
        "{}/api/v1/statuses/{}".format(base_url, toot_id), token,
        retries=retries)
    _rate_limit_sleep(rate_limit)
    ctx = api_get(
        "{}/api/v1/statuses/{}/context".format(base_url, toot_id), token,
        retries=retries)
    desc = ctx.get("descendants", [])

    # Pagination warning: Mastodon's /context endpoint silently caps at
    # ~4000 descendants.  Compare with the OP's replies_count (which is
    # a recursive count maintained by the server).
    reported = stammtoot.get("replies_count", 0) or 0
    got = len(desc)
    if reported > 0 and got > 0 and got < reported * 0.9:
        log_warn("  WARN  Possible truncation for toot {}: "
                 "server reports {} replies but only {} descendants "
                 "returned".format(toot_id, reported, got))

    return stammtoot, desc


def _filter_bots(desc, hide_bots):
    """Remove bot replies from descendants if requested."""
    if not hide_bots or not desc:
        return desc
    before = len(desc)
    desc = [d for d in desc if not d.get("account", {}).get("bot", False)]
    removed = before - len(desc)
    if removed:
        log_debug("  BOTS  filtered {} bot repl{}".format(
            removed, "ies" if removed != 1 else "y"))
    return desc


def _render_thread(stammtoot, desc, fm, instance, full_url,
                   theme="dark", blocklist=None, whitelist=None,
                   avatar_dir=None, avatar_base_url=None,
                   av_max_age_s=0, max_depth=5,
                   max_toots=0, sort="oldest",
                   custom_emojis=False, article=False,
                   media_dir=None, media_base_url=None,
                   media_max_age_s=0, labels=None,
                   highlight_above=0,
                   fold_depth=0, fold_threshold=0,
                   max_media=0, css_extra=""):
    """
    Build tree, handle article mode, and return the HTML fragment string.
    Shared by process_file and regenerate_file.
    """
    if labels is None:
        labels = DEFAULT_LABELS
    stats = collect_stats(stammtoot, desc, blocklist, whitelist)

    if not desc:
        return generate_empty(full_url, instance, theme, labels=labels,
                              stammtoot=stammtoot, css_extra=css_extra), stats

    root  = build_tree(stammtoot, desc)
    total = 1 + len(desc)

    # Article mode: extract OP's self-thread if enabled
    art_html = ""
    art_ids = None
    is_article = (article
                  and fm.get("mastodonarticle", "").lower()
                  in ("true", "yes", "1"))
    is_nocomment = (is_article
                    and fm.get("mastodonarticle_nocomment", "").lower()
                    in ("true", "yes", "1"))
    if is_article:
        chain = extract_article_chain(root)
        if chain:
            log_debug("  ARTICLE  {} continuation post{} from OP".format(
                len(chain), "s" if len(chain) != 1 else ""))
            art_html = render_article_block(
                stammtoot, chain,
                avatar_dir=avatar_dir,
                avatar_base_url=avatar_base_url,
                av_max_age_s=av_max_age_s,
                custom_emojis=custom_emojis,
                media_dir=media_dir,
                media_base_url=media_base_url,
                media_max_age_s=media_max_age_s,
                labels=labels,
                max_media=max_media)
            art_ids = set(str(t.get("id", "")) for t in chain)
            art_ids.add(str(stammtoot.get("id", "")))

    if is_nocomment:
        log_debug("  ARTICLE  nocomment mode – suppressing comment tree")

    fragment = generate_fragment(
        root, instance, total, full_url,
        theme=theme, blocklist=blocklist, whitelist=whitelist,
        avatar_dir=avatar_dir, avatar_base_url=avatar_base_url,
        av_max_age_s=av_max_age_s, max_depth=max_depth,
        stats=stats, max_toots=max_toots, sort=sort,
        custom_emojis=custom_emojis, article_html=art_html,
        article_ids=art_ids if art_html else None,
        media_dir=media_dir, media_base_url=media_base_url,
        media_max_age_s=media_max_age_s,
        article_nocomment=is_nocomment,
        labels=labels, highlight_above=highlight_above,
        fold_depth=fold_depth, fold_threshold=fold_threshold,
        max_media=max_media, css_extra=css_extra)
    return fragment, stats


def _parse_and_resolve(filepath, prefix):
    """
    Parse frontmatter and resolve toot URL.
    Returns (fm, base_url, toot_id, full_url) or a (status, toot_id) tuple
    on early exit.
    """
    fm = parse_frontmatter(filepath)
    if fm is None or not is_enabled(fm):
        return "skipped", None

    raw_toot = fm.get("commenttoot", "").strip()
    if not raw_toot:
        log_warn("  WARN  mastodoncomment=true but commenttoot is missing")
        return "skipped", None

    try:
        base_url, toot_id, full_url = resolve_toot_url(raw_toot, prefix)
    except ValueError as e:
        log_warn("  WARN  {}".format(e))
        return "error", None

    return fm, base_url, toot_id, full_url


def process_file(filepath, toot_cache_dir, max_age_s, token=None,
                 theme="dark", force=False, blocklist=None,
                 whitelist=None,
                 avatar_dir=None, avatar_base_url=None,
                 av_max_age_s=0, rate_limit=5.0, max_depth=5,
                 retries=3, hide_bots=False, max_toots=0,
                 sort="oldest", custom_emojis=False,
                 article=False,
                 media_dir=None, media_base_url=None,
                 media_max_age_s=0,
                 stale_after_s=0,
                 max_age_stale_s=0,
                 prefix=DEFAULT_PREFIX,
                 labels=None,
                 highlight_above=0,
                 fold_depth=0, fold_threshold=0,
                 max_media=0, css_extra=""):
    """
    Returns: ('written'|'cached'|'skipped'|'stale'|'frozen'|'error',
              toot_id_or_None)
    On API error, the old cache file is preserved.
    """
    parsed = _parse_and_resolve(filepath, prefix)
    if isinstance(parsed[0], str):
        return parsed  # early exit
    fm, base_url, toot_id, full_url = parsed

    out_path = os.path.join(toot_cache_dir, "{}.html.include".format(toot_id))

    # Freeze check: skip if frontmatter says so and include exists
    is_frozen = fm.get("mastodonfreeze", "").lower() in ("true", "yes", "1")
    if is_frozen and os.path.isfile(out_path):
        log_debug("  FROZEN {}  (mastodonfreeze=true)".format(
            os.path.basename(out_path)))
        return "frozen", toot_id

    # Determine effective cache TTL: stale threads use the longer interval
    is_stale = (not force
                and thread_is_stale(toot_cache_dir, toot_id, stale_after_s))
    effective_max_age = max_age_stale_s if is_stale else max_age_s

    # Cache freshness check (with effective TTL)
    if not force and cache_is_fresh(out_path, effective_max_age):
        age_min = int((time.time() - os.path.getmtime(out_path)) / 60)
        if is_stale:
            log_debug("  STALE {}  (age: {} min, TTL: {} day(s))".format(
                os.path.basename(out_path), age_min,
                int(max_age_stale_s / 86400)))
            return "stale", toot_id
        else:
            log_debug("  CACHE {}  (age: {} min)".format(
                os.path.basename(out_path), age_min))
            return "cached", toot_id

    # Fetch from API
    log_info("  FETCH toot {} from {} ...".format(toot_id, base_url))
    try:
        stammtoot, desc = _fetch_thread(
            base_url, toot_id, token, retries, rate_limit)
    except RuntimeError as e:
        log_error("  ERROR {}".format(e))
        if os.path.isfile(out_path):
            log_warn("  KEEP  Preserving old cache file")
        return "error", toot_id

    desc = _filter_bots(desc, hide_bots)
    instance = urlparse(base_url).netloc

    fragment, stats = _render_thread(
        stammtoot, desc, fm, instance, full_url,
        theme=theme, blocklist=blocklist, whitelist=whitelist,
        avatar_dir=avatar_dir, avatar_base_url=avatar_base_url,
        av_max_age_s=av_max_age_s, max_depth=max_depth,
        max_toots=max_toots, sort=sort,
        custom_emojis=custom_emojis, article=article,
        media_dir=media_dir, media_base_url=media_base_url,
        media_max_age_s=media_max_age_s, labels=labels,
        highlight_above=highlight_above,
        fold_depth=fold_depth, fold_threshold=fold_threshold,
        max_media=max_media, css_extra=css_extra)

    _atomic_write(out_path, fragment)

    write_sidecar(toot_cache_dir, toot_id, stammtoot, desc, blocklist, stats,
                  whitelist=whitelist, content_hash=_content_hash(fragment))

    log_info("  OK    {} repl{} -> {}".format(
        len(desc), "ies" if len(desc) != 1 else "y",
        os.path.basename(out_path)))
    return "written", toot_id


def regenerate_file(filepath, toot_cache_dir,
                    theme="dark", blocklist=None, whitelist=None,
                    avatar_dir=None, avatar_base_url=None,
                    av_max_age_s=0, max_depth=5,
                    hide_bots=False, max_toots=0,
                    sort="oldest", custom_emojis=False,
                    article=False,
                    media_dir=None, media_base_url=None,
                    media_max_age_s=0,
                    prefix=DEFAULT_PREFIX,
                    ignore_frozen=False,
                    labels=None,
                    highlight_above=0,
                    fold_depth=0, fold_threshold=0,
                    max_media=0, css_extra=""):
    """
    Regenerate the .html.include fragment from cached JSON sidecar data.
    No API calls are made.
    Returns: ('written'|'skipped'|'frozen'|'error', toot_id_or_None)
    """
    parsed = _parse_and_resolve(filepath, prefix)
    if isinstance(parsed[0], str):
        return parsed
    fm, base_url, toot_id, full_url = parsed

    # Frozen check (unless ignore_frozen is set)
    if not ignore_frozen:
        is_frozen = fm.get("mastodonfreeze", "").lower() in ("true", "yes", "1")
        if is_frozen:
            log_debug("  FROZEN  (mastodonfreeze=true, skipping regeneration)")
            return "frozen", None

    # Read cached API data from sidecar
    cached = read_sidecar(toot_cache_dir, toot_id)
    if cached is None:
        log_warn("  SKIP  {} – no sidecar or missing api_data".format(toot_id))
        return "skipped", toot_id

    stammtoot, desc, orig_fetched_at, old_hash = cached
    desc = _filter_bots(desc, hide_bots)
    instance = urlparse(base_url).netloc
    out_path = os.path.join(toot_cache_dir, "{}.html.include".format(toot_id))

    fragment, stats = _render_thread(
        stammtoot, desc, fm, instance, full_url,
        theme=theme, blocklist=blocklist, whitelist=whitelist,
        avatar_dir=avatar_dir, avatar_base_url=avatar_base_url,
        av_max_age_s=av_max_age_s, max_depth=max_depth,
        max_toots=max_toots, sort=sort,
        custom_emojis=custom_emojis, article=article,
        media_dir=media_dir, media_base_url=media_base_url,
        media_max_age_s=media_max_age_s, labels=labels,
        highlight_above=highlight_above,
        fold_depth=fold_depth, fold_threshold=fold_threshold,
        max_media=max_media, css_extra=css_extra)

    new_hash = _content_hash(fragment)
    if old_hash and new_hash == old_hash:
        log_debug("  UNCHANGED  {} (content hash match)".format(toot_id))
        return "unchanged", toot_id

    _atomic_write(out_path, fragment)

    write_sidecar(toot_cache_dir, toot_id, stammtoot, desc, blocklist, stats,
                  whitelist=whitelist, fetched_at=orig_fetched_at,
                  content_hash=new_hash)

    log_info("  REGEN {} repl{} -> {}".format(
        len(desc), "ies" if len(desc) != 1 else "y",
        os.path.basename(out_path)))
    return "written", toot_id


# =========================================================================
# File search with --since filter
# =========================================================================

def find_markdown_files(root_dir, since_weeks=None):
    """
    Find all .markdown files. If since_weeks is set, only return
    files whose frontmatter 'date' field is within the last N weeks.
    """
    cutoff = None
    if since_weeks is not None and since_weeks > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(weeks=since_weeks)

    result = []
    for dirpath, _dirs, fnames in os.walk(root_dir):
        for fn in fnames:
            if not fn.endswith(".markdown"):
                continue
            fpath = os.path.join(dirpath, fn)

            if cutoff is not None:
                fm = parse_frontmatter(fpath)
                if fm:
                    post_date = get_post_date(fm)
                    if post_date is not None:
                        # Make timezone-aware for comparison
                        if post_date.tzinfo is None:
                            post_date = post_date.replace(tzinfo=timezone.utc)
                        if post_date < cutoff:
                            log_debug("  SKIP  {} (date: {})".format(
                                fn, post_date.strftime("%Y-%m-%d")))
                            continue

            result.append(fpath)

    result.sort()
    return result


def collect_sidecar_stats(root_dir, toot_cache_dir, prefix=DEFAULT_PREFIX,
                          stale_after_s=0):
    """
    Read all sidecars for active .markdown files and return a list of
    per-thread stat dicts.  No API calls, no writes.

    Each dict contains:
      toot_id, source, reply_count, unique_users, latest_date,
      fetched_at, age_hours, stale, frozen
    """
    results = []
    now = time.time()
    for dirpath, _dirs, fnames in os.walk(root_dir):
        for fn in sorted(fnames):
            if not fn.endswith(".markdown"):
                continue
            fpath = os.path.join(dirpath, fn)
            fm = parse_frontmatter(fpath)
            if fm is None or not is_enabled(fm):
                continue
            raw = fm.get("commenttoot", "").strip()
            if not raw:
                continue
            try:
                _, tid, _ = resolve_toot_url(raw, prefix)
            except ValueError:
                continue

            is_frozen = fm.get("mastodonfreeze", "").lower() in (
                "true", "yes", "1")

            # Read sidecar metadata (without full api_data parse)
            sc_path = os.path.join(toot_cache_dir, "{}.json".format(tid))
            try:
                with open(sc_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except (OSError, json.JSONDecodeError):
                results.append({
                    "toot_id": tid,
                    "source": os.path.relpath(fpath, root_dir),
                    "reply_count": None,
                    "unique_users": None,
                    "latest_date": None,
                    "fetched_at": None,
                    "age_hours": None,
                    "stale": None,
                    "frozen": is_frozen,
                    "sidecar": False,
                })
                continue

            reply_count = data.get("reply_count", 0)
            unique_users = data.get("unique_users", 0)
            latest_date = data.get("latest_toot_date", "")
            fetched_at = data.get("fetched_at", "")

            # Compute age since fetch
            age_hours = None
            if fetched_at:
                try:
                    clean_ts = fetched_at.replace("Z", "+00:00")
                    ft = datetime.fromisoformat(clean_ts).timestamp()
                    age_hours = round((now - ft) / 3600.0, 1)
                except (ValueError, TypeError):
                    pass

            # Compute stale status
            is_stale = False
            if stale_after_s > 0 and latest_date:
                try:
                    clean_ld = latest_date.replace("Z", "+00:00")
                    ld = datetime.fromisoformat(clean_ld).timestamp()
                    is_stale = (now - ld) > stale_after_s
                except (ValueError, TypeError):
                    pass

            results.append({
                "toot_id": tid,
                "source": os.path.relpath(fpath, root_dir),
                "reply_count": reply_count,
                "unique_users": unique_users,
                "latest_date": latest_date,
                "fetched_at": fetched_at,
                "age_hours": age_hours,
                "stale": is_stale,
                "frozen": is_frozen,
                "sidecar": True,
            })
    return results


def collect_all_toot_ids(root_dir, prefix=DEFAULT_PREFIX):
    """
    Scan all .markdown files and collect all active toot IDs
    (for cleanup purposes).
    """
    ids = set()
    for dirpath, _dirs, fnames in os.walk(root_dir):
        for fn in fnames:
            if not fn.endswith(".markdown"):
                continue
            fpath = os.path.join(dirpath, fn)
            fm = parse_frontmatter(fpath)
            if fm is None or not is_enabled(fm):
                continue
            raw = fm.get("commenttoot", "").strip()
            if not raw:
                continue
            try:
                _, tid, _ = resolve_toot_url(raw, prefix)
                ids.add(tid)
            except ValueError:
                continue
    return ids


# =========================================================================
# Main
# =========================================================================

def main():
    # --- Pre-parse: extract --config before full argument parsing ---
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--config", default=None)
    pre_args, _ = pre.parse_known_args()

    config_defaults = {}
    if pre_args.config:
        config_defaults = load_config(pre_args.config)
        if not config_defaults and pre_args.config:
            # File specified but empty or missing [toot] section
            pass  # not an error, just no overrides

    # --- Main parser ---
    ap = argparse.ArgumentParser(
        description="Generate Mastodon comment includes "
                    "from .markdown files.")
    ap.add_argument(
        "directory",
        help="Root directory containing .markdown files")
    ap.add_argument(
        "--config", default=None,
        help="Path to config file (INI format with [toot] section). "
             "CLI options override config values.")
    ap.add_argument(
        "-t", "--token", default=None,
        help="Mastodon access token for non-public toots")
    ap.add_argument(
        "--prefix", default=DEFAULT_PREFIX,
        help="URL prefix for bare numeric toot IDs "
             "(default: {})".format(DEFAULT_PREFIX))
    ap.add_argument(
        "--theme", choices=["dark", "light", "auto"], default="dark",
        help="Color scheme: dark, light, or auto "
             "(uses prefers-color-scheme media query). Default: dark")
    ap.add_argument(
        "--toot-cache-dir", default=DEFAULT_TOOT_CACHE,
        help="Directory for .html.include files "
             "(default: {})".format(DEFAULT_TOOT_CACHE))
    ap.add_argument(
        "--max-age", type=float, default=DEFAULT_MAX_AGE_H,
        help="Max toot cache age in hours (default: {})".format(
            DEFAULT_MAX_AGE_H))
    ap.add_argument(
        "--avatar-dir", default=DEFAULT_AVATAR_DIR,
        help="Directory for downloaded avatars "
             "(default: {})".format(DEFAULT_AVATAR_DIR))
    ap.add_argument(
        "--avatar-url", default=DEFAULT_AVATAR_URL,
        help="Public base URL for avatars "
             "(default: {})".format(DEFAULT_AVATAR_URL))
    ap.add_argument(
        "--avatar-max-age", type=int, default=DEFAULT_AV_AGE_D,
        help="Max avatar age in days (default: {})".format(
            DEFAULT_AV_AGE_D))
    ap.add_argument(
        "--media-dir", default=DEFAULT_MEDIA_DIR,
        help="Directory for cached OP media files "
             "(default: {})".format(DEFAULT_MEDIA_DIR))
    ap.add_argument(
        "--media-url", default=DEFAULT_MEDIA_URL,
        help="Public base URL for cached media "
             "(default: {})".format(DEFAULT_MEDIA_URL))
    ap.add_argument(
        "--media-max-age", type=int, default=DEFAULT_MEDIA_AGE_D,
        help="Max media cache age in days (default: {})".format(
            DEFAULT_MEDIA_AGE_D))
    ap.add_argument(
        "--stale-after", default=DEFAULT_STALE_AFTER,
        help="Skip threads with no reply for this duration. "
             "Accepts days (e.g. '365' or '365d') or months ('12m'). "
             "Set to '0' to disable. "
             "(default: {})".format(DEFAULT_STALE_AFTER))
    ap.add_argument(
        "--max-age-stale", type=int, default=DEFAULT_STALE_AGE_D,
        help="Cache TTL in days for stale threads "
             "(default: {} day(s))".format(DEFAULT_STALE_AGE_D))
    ap.add_argument(
        "--blocklist", default=None,
        help="Path to blocklist file "
             "(one URL/prefix per line, # and ; for comments)")
    ap.add_argument(
        "--whitelist", default=None,
        help="Path to whitelist file. If provided, only toots matching "
             "the whitelist are shown "
             "(same format as blocklist)")
    ap.add_argument(
        "--labels", default=None,
        help="Path to labels file for i18n "
             "(key = value, one per line)")
    ap.add_argument(
        "--rate-limit", type=float, default=DEFAULT_RATE_LIMIT,
        help="Seconds between API calls, minimum 1 "
             "(default: {})".format(DEFAULT_RATE_LIMIT))
    ap.add_argument(
        "--max-depth", type=int, default=DEFAULT_MAX_DEPTH,
        help="Max nesting depth, flat after "
             "(default: {})".format(DEFAULT_MAX_DEPTH))
    ap.add_argument(
        "--retries", type=int, default=DEFAULT_RETRIES,
        help="Number of API retry attempts on transient errors "
             "(429, 5xx, network). Uses exponential backoff. "
             "(default: {})".format(DEFAULT_RETRIES))
    ap.add_argument(
        "--max-toots", type=int, default=0,
        help="Max number of replies to display. "
             "Remaining replies link to the thread on Mastodon. "
             "0 = show all (default: 0)")
    ap.add_argument(
        "--highlight-above", type=int, default=DEFAULT_HIGHLIGHT,
        help="Highlight replies with at least this many favourites "
             "(0 = off). Default: {}".format(DEFAULT_HIGHLIGHT))
    ap.add_argument(
        "--fold-depth", type=int, default=DEFAULT_FOLD_DEPTH,
        help="Fold subtrees at this nesting depth into a "
             "collapsible <details> element "
             "(0 = off). Default: {}".format(DEFAULT_FOLD_DEPTH))
    ap.add_argument(
        "--fold-threshold", type=int, default=DEFAULT_FOLD_THRESH,
        help="Only fold when total comment count exceeds this number "
             "(0 = always fold if fold-depth > 0). "
             "Default: {}".format(DEFAULT_FOLD_THRESH))
    ap.add_argument(
        "--max-media", type=int, default=DEFAULT_MAX_MEDIA,
        help="Max media attachments to embed per toot. "
             "Excess attachments show a link to the original toot. "
             "0 = unlimited. Default: {}".format(DEFAULT_MAX_MEDIA))
    ap.add_argument(
        "--css-extra", default=None,
        help="Path to a CSS file whose contents are appended "
             "after the built-in scoped CSS. Use to override "
             "colors, spacing, or add custom rules.")
    ap.add_argument(
        "--sort", choices=["oldest", "newest", "popular"],
        default="oldest",
        help="Sort order for replies: oldest (chronological, default), "
             "newest (reverse chronological), popular (by favourites)")
    ap.add_argument(
        "--since", type=int, default=DEFAULT_SINCE_WEEKS,
        help="Only process posts from last N weeks, "
             "based on frontmatter date (default: {}). "
             "Use 0 to process all.".format(DEFAULT_SINCE_WEEKS))
    ap.add_argument(
        "--all", action="store_true",
        help="Process all .markdown files regardless of age "
             "(overrides --since)")
    ap.add_argument(
        "--cleanup-tootcache", action="store_true",
        help="Remove orphaned toot cache files and exit")
    ap.add_argument(
        "--cleanup-avatars", action="store_true",
        help="Remove avatars not referenced by any active thread")
    ap.add_argument(
        "--cleanup-media", action="store_true",
        help="Remove cached media not referenced by any active thread")
    ap.add_argument(
        "--cleanup-all", action="store_true",
        help="Run all cleanup modes (tootcache, avatars, media)")
    ap.add_argument(
        "--hide-bots", action="store_true",
        help="Hide replies from bot accounts")
    ap.add_argument(
        "--custom-emojis", action="store_true",
        help="Resolve custom Mastodon emojis (:shortcode:) to "
             "inline images (default: off)")
    ap.add_argument(
        "--article", action="store_true",
        help="Enable article mode: if mastodonarticle=true in "
             "frontmatter, show the OP's self-thread as a "
             "sequential article block before comments (default: off)")
    ap.add_argument(
        "--force", action="store_true",
        help="Ignore cache, refetch everything")
    ap.add_argument(
        "--regenerate", action="store_true",
        help="Regenerate HTML fragments from cached JSON sidecars "
             "(no API calls, skips frozen threads)")
    ap.add_argument(
        "--regenerate-all", action="store_true",
        help="Like --regenerate but also processes frozen threads")
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be done, no API calls")
    ap.add_argument(
        "--validate", action="store_true",
        help="Validate all JSON sidecars for integrity "
             "(no API calls, no writes, exit 1 on errors)")
    ap.add_argument(
        "--stats-only", action="store_true",
        help="Read cached sidecars and print per-thread statistics "
             "as JSON lines to stdout. No API calls, no writes.")
    ap.add_argument(
        "--migrate-sidecars", action="store_true",
        help="Upgrade all JSON sidecars to the current version, "
             "adding missing fields. No API calls.")
    ap.add_argument(
        "--no-lock", action="store_true",
        help="Skip lockfile check (allow parallel instances)")
    ap.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT,
        help="Global timeout in seconds for the entire run. "
             "After this time, the current thread finishes and the "
             "process exits with a summary. 0 = no timeout "
             "(default: {})".format(DEFAULT_TIMEOUT))
    vgroup = ap.add_mutually_exclusive_group()
    vgroup.add_argument(
        "--verbose", action="store_true",
        help="Show detailed output including cache hits, "
             "avatar downloads, blocklist matches")
    vgroup.add_argument(
        "--quiet", action="store_true",
        help="Show only errors and warnings")
    vgroup.add_argument(
        "--silent", action="store_true",
        help="Suppress all output, including errors")

    # Apply config file defaults (CLI still overrides)
    if config_defaults:
        ap.set_defaults(**config_defaults)

    args = ap.parse_args()

    # Log which config was loaded
    if pre_args.config:
        # Defer this message until after log level is set below
        _config_msg = "CONFIG      {}".format(os.path.abspath(pre_args.config))
    else:
        _config_msg = None

    # Set log level
    if args.silent:
        set_log_level(LOG_SILENT)
    elif args.quiet:
        set_log_level(LOG_QUIET)
    elif args.verbose:
        set_log_level(LOG_VERBOSE)
    else:
        set_log_level(LOG_NORMAL)

    # Validate rate limit
    if args.rate_limit < 1.0:
        log_warn("WARN: rate-limit raised to minimum of 1 second")
        args.rate_limit = 1.0

    # Read extra CSS file
    css_extra = ""
    if args.css_extra:
        try:
            with open(args.css_extra, "r", encoding="utf-8") as fh:
                css_extra = fh.read().strip()
            log_debug("CSS EXTRA   {} ({} bytes)".format(
                args.css_extra, len(css_extra)))
        except OSError as e:
            log_error("ERROR: Cannot read --css-extra: {}".format(e))
            sys.exit(1)

    root_dir = os.path.abspath(args.directory)
    if not os.path.isdir(root_dir):
        log_error("ERROR: Not a directory: {}".format(root_dir))
        sys.exit(1)

    toot_cache_dir = os.path.abspath(args.toot_cache_dir)
    avatar_dir     = os.path.abspath(args.avatar_dir)
    media_dir      = os.path.abspath(args.media_dir)
    os.makedirs(toot_cache_dir, exist_ok=True)
    os.makedirs(avatar_dir, exist_ok=True)
    os.makedirs(media_dir, exist_ok=True)

    # Register caches for cross-cache collision detection
    register_cache_dirs(
        toot=toot_cache_dir, avatar=avatar_dir, media=media_dir)

    # --- Lockfile: prevent concurrent runs ---
    lock_fh = None
    if not args.no_lock:
        try:
            lock_fh = _acquire_lock(toot_cache_dir)
            if lock_fh:
                atexit.register(_release_lock, lock_fh)
                log_debug("LOCK    acquired {}".format(
                    os.path.join(toot_cache_dir, LOCK_FILENAME)))
        except RuntimeError as e:
            log_error("ERROR: {}".format(e))
            sys.exit(1)

    # --- Install signal handlers for graceful shutdown ---
    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    # --- Global timeout ---
    _timeout_timer = None
    if args.timeout > 0:
        _timeout_timer = threading.Timer(args.timeout, _timeout_expired)
        _timeout_timer.daemon = True
        _timeout_timer.start()
        log_debug("TIMEOUT     {}s".format(args.timeout))

    # --- Cleanup mode ---
    if args.cleanup_all:
        args.cleanup_tootcache = True
        args.cleanup_avatars = True
        args.cleanup_media = True

    if args.cleanup_tootcache or args.cleanup_avatars or args.cleanup_media:
        if _config_msg:
            log_info(_config_msg)

        if args.cleanup_tootcache:
            log_info("CLEANUP  Scanning for orphaned cache files...")
            active_ids = collect_all_toot_ids(root_dir, prefix=args.prefix)
            log_info("         {} active toot ID(s) found in .markdown files".format(
                len(active_ids)))
            removed, deprecated = do_cleanup(toot_cache_dir, active_ids)
            log_info("")
            log_info("=" * 50)
            log_info("  Removed:    {} .html.include file(s)".format(removed))
            log_info("  Deprecated: {} .json file(s)".format(deprecated))
            log_info("=" * 50)

        if args.cleanup_avatars:
            log_info("CLEANUP  Scanning for orphaned avatars...")
            av_removed = do_cleanup_avatars(avatar_dir, toot_cache_dir)
            log_info("")
            log_info("=" * 50)
            log_info("  Avatars removed: {}".format(av_removed))
            log_info("=" * 50)

        if args.cleanup_media:
            log_info("CLEANUP  Scanning for orphaned media...")
            med_removed = do_cleanup_media(media_dir, toot_cache_dir)
            log_info("")
            log_info("=" * 50)
            log_info("  Media removed: {}".format(med_removed))
            log_info("=" * 50)

        return

    # --- Validate mode ---
    if args.validate:
        if _config_msg:
            log_info(_config_msg)
        log_info("VALIDATE  Checking sidecars in {}".format(toot_cache_dir))

        active_ids = collect_all_toot_ids(root_dir, prefix=args.prefix)
        log_info("          {} active toot ID(s) in .markdown files".format(
            len(active_ids)))

        result = validate_sidecars(toot_cache_dir, active_toot_ids=active_ids)

        # Print issues grouped by severity
        for severity in ("error", "warn"):
            for fname, sev, msg in result["issues"]:
                if sev != severity:
                    continue
                tag = "ERROR" if sev == "error" else "WARN "
                log_info("  {}  {}: {}".format(tag, fname, msg))

        log_info("")
        log_info("=" * 50)
        log_info("  Checked: {} sidecar(s)".format(result["checked"]))
        log_info("  OK:      {}".format(result["ok"]))
        log_info("  Warn:    {}".format(result["warn"]))
        log_info("  Errors:  {}".format(result["error"]))
        log_info("=" * 50)

        if result["error"]:
            sys.exit(1)
        return

    # --- Stats-only mode ---
    if args.stats_only:
        stale_after_s = parse_stale_after(args.stale_after)
        stats_list = collect_sidecar_stats(
            root_dir, toot_cache_dir,
            prefix=args.prefix, stale_after_s=stale_after_s)

        # Output as JSON array to stdout (bypasses log system)
        sys.stdout.write(json.dumps(stats_list, indent=2,
                                    ensure_ascii=False))
        sys.stdout.write("\n")
        return

    # --- Migrate sidecars mode ---
    if args.migrate_sidecars:
        if _config_msg:
            log_info(_config_msg)
        log_info("MIGRATE  Upgrading sidecars in {}".format(toot_cache_dir))
        log_info("         Target version: {}".format(SIDECAR_VERSION))

        result = migrate_sidecars(toot_cache_dir)

        log_info("")
        log_info("=" * 50)
        log_info("  Checked:  {} sidecar(s)".format(result["checked"]))
        log_info("  Migrated: {}".format(result["migrated"]))
        log_info("  Skipped:  {} (already current or future)".format(
            result["skipped"]))
        if result["error"]:
            log_info("  Errors:   {}".format(result["error"]))
        log_info("=" * 50)

        if result["error"]:
            sys.exit(1)
        return

    # --- Regenerate mode ---
    if args.regenerate or args.regenerate_all:
        ignore_frozen   = args.regenerate_all
        blocklist       = load_blocklist(args.blocklist)
        whitelist       = load_blocklist(args.whitelist)
        labels          = load_labels(args.labels)
        av_max_age_s    = args.avatar_max_age * 86400.0
        media_max_age_s = args.media_max_age * 86400.0
        since_weeks  = None if args.all else (args.since if args.since > 0 else None)
        files = find_markdown_files(root_dir, since_weeks=since_weeks)

        if _config_msg:
            log_info(_config_msg)
        mode_label = "REGENERATE-ALL" if ignore_frozen else "REGENERATE"
        log_info("{}  Rebuilding HTML from cached JSON sidecars".format(
            mode_label))
        log_info("DIR         {} .markdown file(s) in {}".format(
            len(files), root_dir))
        log_info("TOOT CACHE  {}".format(toot_cache_dir))
        log_info("PREFIX      {}".format(args.prefix))
        if blocklist:
            log_info("BLOCKLIST   {} entr{} from {}".format(
                len(blocklist),
                "ies" if len(blocklist) != 1 else "y",
                args.blocklist))
        if whitelist:
            log_info("WHITELIST   {} entr{} from {}".format(
                len(whitelist),
                "ies" if len(whitelist) != 1 else "y",
                args.whitelist))
        if args.labels:
            log_info("LABELS      {}".format(args.labels))
        log_info("")

        stats = {"written": 0, "unchanged": 0, "skipped": 0, "frozen": 0, "error": 0}
        for filepath in files:
            if _shutdown_requested:
                log_info("  SHUTDOWN  stopping after signal")
                break
            rel = os.path.relpath(filepath, root_dir)
            fm = parse_frontmatter(filepath)
            if fm is None or not is_enabled(fm):
                continue
            raw = fm.get("commenttoot", "").strip()
            if not raw:
                continue

            log_info("FILE  {}".format(rel))

            result, tid = regenerate_file(
                filepath, toot_cache_dir,
                theme=args.theme, blocklist=blocklist,
                whitelist=whitelist,
                avatar_dir=avatar_dir, avatar_base_url=args.avatar_url,
                av_max_age_s=av_max_age_s, max_depth=args.max_depth,
                hide_bots=args.hide_bots, max_toots=args.max_toots,
                sort=args.sort, custom_emojis=args.custom_emojis,
                article=args.article,
                media_dir=media_dir, media_base_url=args.media_url,
                media_max_age_s=media_max_age_s,
                prefix=args.prefix,
                ignore_frozen=ignore_frozen,
                labels=labels,
                highlight_above=args.highlight_above,
                fold_depth=args.fold_depth,
                fold_threshold=args.fold_threshold,
                max_media=args.max_media,
                css_extra=css_extra)
            stats[result] = stats.get(result, 0) + 1

        log_info("")
        log_info("=" * 50)
        if _shutdown_requested:
            log_info("  (Interrupted – partial run)")
        log_info("  Regenerated: {}".format(stats["written"]))
        if stats["unchanged"]:
            log_info("  Unchanged:   {}".format(stats["unchanged"]))
        if stats["frozen"]:
            log_info("  Frozen:      {}".format(stats["frozen"]))
        log_info("  Skipped:     {} (no sidecar or disabled)".format(
            stats["skipped"]))
        if stats["error"]:
            log_info("  Errors:      {}".format(stats["error"]))
        log_info("=" * 50)
        if stats["error"]:
            sys.exit(1)
        return

    # --- Normal mode ---
    max_age_s       = args.max_age * 3600.0
    av_max_age_s    = args.avatar_max_age * 86400.0
    media_max_age_s = args.media_max_age * 86400.0
    stale_after_s   = parse_stale_after(args.stale_after)
    max_age_stale_s = args.max_age_stale * 86400.0
    blocklist       = load_blocklist(args.blocklist)
    whitelist       = load_blocklist(args.whitelist)
    labels          = load_labels(args.labels)
    since_weeks  = None if args.all else (args.since if args.since > 0 else None)

    files = find_markdown_files(root_dir, since_weeks=since_weeks)

    if _config_msg:
        log_info(_config_msg)
    log_info("DIR         {} .markdown file(s) in {}".format(
        len(files), root_dir))
    log_info("TOOT CACHE  {}".format(toot_cache_dir))
    log_info("PREFIX      {}".format(args.prefix))
    log_info("AVATARS     {} -> {}".format(avatar_dir, args.avatar_url))
    log_info("MEDIA       {} -> {}".format(media_dir, args.media_url))
    log_info("TOOT TTL    {} hour(s)".format(args.max_age))
    log_info("AVATAR TTL  {} day(s)".format(args.avatar_max_age))
    log_info("MEDIA TTL   {} day(s)".format(args.media_max_age))
    if stale_after_s > 0:
        log_info("STALE AFTER {} ({:.0f} day(s)), TTL {} day(s)".format(
            args.stale_after, stale_after_s / 86400, args.max_age_stale))
    log_info("RATE LIMIT  {} second(s)".format(args.rate_limit))
    log_info("MAX DEPTH   {}".format(args.max_depth))
    log_info("RETRIES     {}".format(args.retries))
    if args.max_toots > 0:
        log_info("MAX TOOTS   {}".format(args.max_toots))
    if args.sort != "oldest":
        log_info("SORT        {}".format(args.sort))
    if args.custom_emojis:
        log_info("EMOJIS      custom emoji resolution enabled")
    if args.article:
        log_info("ARTICLE     article mode enabled")
    if since_weeks:
        log_info("SINCE       last {} week(s)".format(since_weeks))
    else:
        log_info("SINCE       all posts" + (" (--all)" if args.all else ""))
    if blocklist:
        log_info("BLOCKLIST   {} entr{} from {}".format(
            len(blocklist),
            "ies" if len(blocklist) != 1 else "y",
            args.blocklist))
    if whitelist:
        log_info("WHITELIST   {} entr{} from {}".format(
            len(whitelist),
            "ies" if len(whitelist) != 1 else "y",
            args.whitelist))
    if args.labels:
        log_info("LABELS      {}".format(args.labels))
    log_info("")

    stats = {"written": 0, "cached": 0, "skipped": 0, "stale": 0, "frozen": 0, "error": 0}
    active_ids = set()

    for filepath in files:
        if _shutdown_requested:
            log_info("  SHUTDOWN  stopping after signal")
            break
        rel = os.path.relpath(filepath, root_dir)

        fm = parse_frontmatter(filepath)
        if fm is None or not is_enabled(fm):
            continue

        raw = fm.get("commenttoot", "").strip()
        if not raw:
            continue

        log_info("FILE  {}".format(rel))

        if args.dry_run:
            try:
                _, tid, url = resolve_toot_url(raw, prefix=args.prefix)
                log_info("      -> would generate {}.html.include".format(tid))
            except ValueError as e:
                log_warn("      WARN {}".format(e))
            continue

        result, tid = process_file(
            filepath, toot_cache_dir, max_age_s,
            token=args.token, theme=args.theme, force=args.force,
            blocklist=blocklist,
            whitelist=whitelist,
            avatar_dir=avatar_dir, avatar_base_url=args.avatar_url,
            av_max_age_s=av_max_age_s, rate_limit=args.rate_limit,
            max_depth=args.max_depth, retries=args.retries,
            hide_bots=args.hide_bots, max_toots=args.max_toots,
            sort=args.sort, custom_emojis=args.custom_emojis,
            article=args.article,
            media_dir=media_dir, media_base_url=args.media_url,
            media_max_age_s=media_max_age_s,
            stale_after_s=stale_after_s,
            max_age_stale_s=max_age_stale_s,
            prefix=args.prefix,
            labels=labels,
            highlight_above=args.highlight_above,
            fold_depth=args.fold_depth,
            fold_threshold=args.fold_threshold,
            max_media=args.max_media,
            css_extra=css_extra)
        stats[result] = stats.get(result, 0) + 1
        if tid:
            active_ids.add(tid)

        if result == "written":
            _rate_limit_sleep(args.rate_limit)

    log_info("")
    log_info("=" * 50)
    if _shutdown_requested:
        log_info("  (Interrupted – partial run)")
    if args.dry_run:
        log_info("  (Dry run - nothing was written)")
    else:
        log_info("  Written:  {}".format(stats["written"]))
        log_info("  Cached:   {}".format(stats["cached"]))
        log_info("  Stale:    {}".format(stats["stale"]))
        log_info("  Frozen:   {}".format(stats["frozen"]))
        log_info("  Skipped:  {}".format(stats["skipped"]))
        log_info("  Errors:   {}".format(stats["error"]))
    log_info("=" * 50)
    if not args.dry_run and stats["error"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
