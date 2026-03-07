# toot.py – Mastodon Comment Embedding for Static Sites

A static-site generator plugin that fetches Mastodon threads and renders them as self-contained HTML fragments for embedding in blog posts. It scans `.markdown` files for frontmatter fields, fetches the corresponding Mastodon thread via the public API, and writes `.html.include` files ready for server-side inclusion.

## Table of Contents

- [Quick Start](#quick-start)
- [Requirements](#requirements)
- [Frontmatter Fields](#frontmatter-fields)
- [Command-Line Options](#command-line-options)
- [Config File](#config-file)
- [Caching](#caching)
- [Blocklist / Whitelist](#blocklist--whitelist)
- [Labels / i18n](#labels--i18n)
- [Article Mode](#article-mode)
- [Themes](#themes)
- [Features Reference](#features-reference)
- [Examples](#examples)
- [Jekyll Integration](#jekyll-integration)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

1. Add frontmatter to your blog post:

```yaml
---
title: My Blog Post
date: 2025-06-15T10:00:00+02:00
mastodoncomment: true
commenttoot: "114123456789012345"
---
```

2. Run the generator:

```bash
python3 toot.py /path/to/content/
```

3. Include the generated HTML in your template:

```html
<!-- Hugo example -->
{{ readFile "/var/tmp/gencache/mastodontootcache/114123456789012345.html.include" | safeHTML }}
```

```html
<!-- Jekyll example (see Jekyll Integration) -->
{% if page.mastodoncomment and page.commenttoot %}
  {% capture toot_file %}toots/{{ page.commenttoot }}.html.include{% endcapture %}
  {% include {{ toot_file }} %}
{% endif %}
```

See [Jekyll Integration](#jekyll-integration) for a complete setup guide.

---

## Requirements

Python 3.7 or later. No external dependencies – the script uses only the standard library.

---

## Frontmatter Fields

These fields are read from the YAML frontmatter block at the top of each `.markdown` file.

| Field | Required | Values | Description |
|-------|----------|--------|-------------|
| `mastodoncomment` | yes | `true` / `false` | Enables comment generation for this post |
| `commenttoot` | yes | toot ID or full URL | The root toot of the comment thread |
| `date` | no | ISO 8601 datetime | Used by `--since` to skip old posts |
| `mastodonarticle` | no | `true` / `false` | Marks this thread for article mode (requires `--article` flag) |
| `mastodonarticle_nocomment` | no | `true` / `false` | When set together with `mastodonarticle`, suppresses the comment section below the article. Only the article block and footer link are shown |
| `mastodonfreeze` | no | `true` / `false` | Freezes this thread: no API calls, no regeneration. The existing include file is kept as-is. Not overridden by `--force` |

The `commenttoot` field accepts either a bare numeric ID (resolved against the `--prefix` URL, default `https://social.example.com/@user/`) or a full Mastodon URL like `https://mastodon.social/@user/114123456789012345`.

---

## Command-Line Options

```
python3 toot.py [options] directory
```

`directory` is the root directory to scan recursively for `.markdown` files.

### Authentication

| Option | Default | Description |
|--------|---------|-------------|
| `--token TOKEN` | none | Mastodon access token for non-public toots |
| `--prefix URL` | `https://social.example.com/@user/` | URL prefix prepended to bare numeric toot IDs in `commenttoot` |

### Appearance

| Option | Default | Description |
|--------|---------|-------------|
| `--theme {dark,light,auto}` | `dark` | Color scheme for the generated HTML. `auto` adapts to the visitor's OS/browser preference via `prefers-color-scheme` |
| `--max-depth N` | `5` | Maximum nesting depth for replies. Deeper replies are rendered at the max level |
| `--sort {oldest,newest,popular}` | `oldest` | Sort order for replies. `popular` sorts by favourite count |
| `--max-toots N` | `0` (all) | Maximum number of replies to display. Excess replies are replaced with a link to the thread on Mastodon |
| `--highlight-above N` | `10` | Highlight replies with at least N favourites (golden accent border). Set to 0 to disable |
| `--fold-depth N` | `3` | Fold subtrees at this nesting depth into a collapsible `<details>` element. Set to 0 to disable |
| `--fold-threshold N` | `50` | Only fold when total comment count exceeds this number. Set to 0 to always fold (when fold-depth > 0) |
| `--max-media N` | `3` | Max media attachments to embed per toot. Excess shows a "N more" link. Set to 0 for unlimited |
| `--css-extra FILE` | off | Path to a CSS file appended after the built-in scoped CSS. Use to override colors, spacing, or add custom rules without modifying the script |

### Caching

| Option | Default | Description |
|--------|---------|-------------|
| `--toot-cache-dir DIR` | `/var/tmp/gencache/mastodontootcache` | Directory for `.html.include` and `.json` sidecar files |
| `--max-age HOURS` | `3` | Maximum age of cached HTML before re-fetching |
| `--avatar-dir DIR` | `/var/tmp/gencache/mastodonavatarcache` | Directory for downloaded avatar images |
| `--avatar-url URL` | `https://www.blog.example.com/images/mastodonavatarcache` | Public base URL to serve avatars from |
| `--avatar-max-age DAYS` | `30` | Maximum age of cached avatars before re-downloading |
| `--media-dir DIR` | `/var/tmp/gencache/mastodonmediacache` | Directory for cached OP media files (images, videos) and link preview thumbnails |
| `--media-url URL` | `https://www.blog.example.com/images/mastodonmediacache` | Public base URL to serve cached media from |
| `--media-max-age DAYS` | `30` | Maximum age of cached media before re-downloading |

### Filtering

| Option | Default | Description |
|--------|---------|-------------|
| `--since N` | `4` | Only process posts from the last N weeks (based on frontmatter `date`). Use `0` for all posts |
| `--all` | off | Process all `.markdown` files regardless of age (overrides `--since`) |
| `--stale-after VALUE` | `12m` | Skip threads where the last reply is older than this duration. Accepts days (`365` or `365d`) or months (`12m`). Requires an existing include file and JSON sidecar. Set to `0` to disable |
| `--max-age-stale DAYS` | `7` | Cache TTL in days for stale threads. Stale threads are re-checked at this longer interval instead of `--max-age` |
| `--blocklist FILE` | none | Path to a blocklist file for filtering out users, servers, or specific toots |
| `--whitelist FILE` | none | Path to a whitelist file. If provided, only toots matching the whitelist are shown (same format as blocklist) |
| `--labels FILE` | none | Path to a labels file for i18n (key = value, one per line). Undefined keys fall back to English |
| `--hide-bots` | off | Hide replies from bot accounts |

### Network

| Option | Default | Description |
|--------|---------|-------------|
| `--rate-limit SECONDS` | `5.0` | Minimum seconds between API calls. If the server's rate-limit headers indicate a longer delay is needed, the longer value is used instead |
| `--retries N` | `3` | Number of retry attempts on transient errors (HTTP 429, 5xx, network errors). Uses exponential backoff (2s, 4s, 8s, …) |

### Special Features

| Option | Default | Description |
|--------|---------|-------------|
| `--custom-emojis` | off | Resolve custom Mastodon emojis (`:shortcode:`) to inline images |
| `--article` | off | Enable article mode (see [Article Mode](#article-mode)) |

### Maintenance

| Option | Default | Description |
|--------|---------|-------------|
| `--cleanup-tootcache` | off | Remove orphaned toot cache files (no matching `.markdown` source) and exit |
| `--cleanup-avatars` | off | Remove avatar files not referenced by any active thread |
| `--cleanup-media` | off | Remove cached media files not referenced by any active thread |
| `--cleanup-all` | off | Run all cleanup modes (tootcache, avatars, media) |
| `--force` | off | Ignore cache, re-fetch everything |
| `--regenerate` | off | Regenerate HTML fragments from cached JSON sidecars (no API calls, skips frozen threads) |
| `--regenerate-all` | off | Like `--regenerate` but also processes frozen threads |
| `--dry-run` | off | Show what would be done without making API calls or writing files |
| `--no-lock` | off | Skip lockfile check, allowing parallel instances |
| `--validate` | off | Validate all JSON sidecars for integrity (no API calls, no writes). Exit 1 on errors, exit 0 on warnings only |
| `--stats-only` | off | Read cached sidecars and print per-thread statistics as a JSON array to stdout. No API calls, no writes. Use with `--quiet` for clean JSON output |
| `--migrate-sidecars` | off | Upgrade all JSON sidecars to the current version, adding missing fields. No API calls. Idempotent — safe to run multiple times |
| `--timeout SECONDS` | `3540` (59 min) | Global timeout for the entire run. After this many seconds, the current thread finishes and the process exits with a summary. Set to 0 to disable |

### Logging

| Option | Description |
|--------|-------------|
| `--verbose` | Detailed output: cache hits, avatar downloads, blocklist/whitelist matches, emoji resolution |
| `--quiet` | Only errors and warnings |
| `--silent` | Suppress all output |

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success — all threads processed without errors (also used for dry-run) |
| `1` | One or more errors occurred (API failures, invalid URLs, corrupt data, invalid directory) |

This enables use with `set -e` in shell scripts and error detection in CI/CD pipelines or cron jobs.

### Config File

| Option | Description |
|--------|-------------|
| `--config FILE` | Path to an INI config file. CLI options always override config values |

---

## Config File

All command-line options can also be set in an INI file with a `[toot]` section. Use underscores or hyphens interchangeably in key names.

```ini
[toot]
token = abc123def456
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
labels = /etc/mastodon-labels-de.txt
rate_limit = 5
max_depth = 5
retries = 3
max_toots = 0
highlight_above = 10
fold_depth = 3
fold_threshold = 50
max_media = 3
css_extra = /path/to/custom.css
sort = oldest
since = 4
hide_bots = false
custom_emojis = false
article = false
verbose = false
quiet = false
```

Boolean values accept `true`, `yes`, `1` (and their negatives). Pass the config file with:

```bash
python3 toot.py --config /path/to/toot.ini /path/to/content/
```

**Precedence:** CLI flags → config file → built-in defaults.

---

## Caching

toot.py uses a two-layer cache to minimize API calls.

### Toot Cache

Each thread is stored as two files in the toot cache directory:

- `{toot_id}.html.include` – the rendered HTML fragment, ready for inclusion
- `{toot_id}.json` – the raw API response (sidecar), used for incremental updates

The HTML is regenerated when `--max-age` hours have passed since the last fetch. Use `--force` to bypass the cache entirely.

### Avatar Cache

User avatars are downloaded once and served from a local directory. The filename is a SHA-256 hash of the avatar URL, preserving the original file extension. Avatars are re-downloaded when `--avatar-max-age` days have passed.

### Media Cache

Images and videos from the OP's toots are downloaded and cached locally in `--media-dir`. Like avatars, each file is stored with a SHA-256 hash of its original URL as the filename, preserving the file extension. The cached files are served from the `--media-url` base URL instead of the original Mastodon CDN. Media is re-downloaded when `--media-max-age` days have passed. This ensures media remains available even if the original instance goes down or purges old media. Link preview card thumbnails (OpenGraph images) are also cached through the same mechanism. Media from non-OP toots is not cached (non-OP media is not embedded – only linked).

### Stale Thread Skipping

Threads that have not received a new reply in a long time are checked less frequently to save API calls. Two options control this behavior:

- **`--stale-after`** (default: `12m`) — inactivity threshold. A thread is considered stale when its last reply is older than this duration.
- **`--max-age-stale`** (default: `7` days) — cache TTL for stale threads. Instead of the normal `--max-age` (hours), stale threads use this longer interval.

When a thread is checked, toot.py reads the `latest_toot_date` from the JSON sidecar. If that date is older than `--stale-after` **and** the cached `.html.include` is younger than `--max-age-stale`, the thread is reported as "Stale" and no API call is made. Once the stale TTL expires, the thread is re-fetched normally. If new replies are found, the thread becomes active again and returns to the normal `--max-age` cycle.

This requires that the thread has been fetched at least once before (so the sidecar exists). New threads without a sidecar or include file are always processed normally.

```bash
# Stale after 6 months, re-check every 14 days
python3 toot.py --stale-after 6m --max-age-stale 14 /path/to/content/

# Stale after 1 year, re-check every 7 days (default)
python3 toot.py --stale-after 12m /path/to/content/

# Disable stale detection entirely
python3 toot.py --stale-after 0 /path/to/content/
```

Stale threads are reported separately in the summary as "Stale". Use `--force` to override and reprocess all threads regardless of staleness.

### Frozen Threads

Individual threads can be frozen by setting `mastodonfreeze: true` in the post's frontmatter:

```yaml
---
mastodoncomment: true
commenttoot: "114123456789012345"
mastodonfreeze: true
---
```

A frozen thread is never re-fetched: no API calls are made and the existing include file is preserved permanently. Unlike stale detection (which still re-checks periodically), freezing is an explicit per-post decision. This is useful for threads that have been moderated or curated to a final state and should not change.

Frozen threads are reported as "Frozen" in the summary. Unlike stale threads, `--force` does **not** override a freeze — to reprocess a frozen thread, remove `mastodonfreeze` from the frontmatter first.

### Lockfile

toot.py uses an exclusive lockfile (`.toot.lock` in the toot cache directory) to prevent concurrent instances from running against the same cache. If a second instance detects an active lock, it exits immediately with exit code 1 and an error message.

The lock is acquired using `flock()` (POSIX advisory locking) and released automatically when the process exits, even on crashes or `SIGKILL`. The lock file contains the PID of the holding process for diagnostic purposes.

Use `--no-lock` to disable locking, for example when running separate instances against different content directories with different cache directories:

```bash
# These use different cache dirs, so no conflict
python3 toot.py /site-a/content/ --toot-cache-dir /cache/a/ --no-lock &
python3 toot.py /site-b/content/ --toot-cache-dir /cache/b/ --no-lock &
```

On systems without `flock()` support (e.g. Windows), locking is silently skipped.

### Validation

`--validate` checks all JSON sidecars for integrity without making API calls or writing any files:

```bash
python3 toot.py /path/to/content/ --validate
```

Each sidecar is checked for valid JSON, required fields (`sidecar_version`, `toot_id`, `api_data`), version compatibility, `api_data` structure (stammtoot must be a dict with `id` and `account`, descendants must be a list), presence of `fetched_at`, consistency between the toot ID in the filename and the stored `toot_id`, and existence of a matching `.html.include` file.

Cross-checks against `.markdown` sources report orphaned sidecars (no matching source) and missing sidecars (source exists but no cached JSON).

Structural problems (corrupt JSON, missing required keys, future version, bad api_data) are errors. Outdated versions, missing metadata, and orphaned/missing sidecars are warnings. Exit code is 1 if any errors are found, 0 otherwise (warnings alone do not cause a non-zero exit).

### Statistics

`--stats-only` reads cached sidecars and outputs per-thread statistics as a JSON array to stdout. No API calls are made and nothing is written. Use `--quiet` for clean JSON output without log messages.

```bash
# All threads as JSON
python3 toot.py /path/to/content/ --stats-only --quiet --all

# Top 5 threads by reply count
python3 toot.py /path/to/content/ --stats-only --quiet --all | jq 'sort_by(-.reply_count) | .[0:5]'

# Stale threads only
python3 toot.py /path/to/content/ --stats-only --quiet --all | jq '[.[] | select(.stale==true)]'

# Threads missing sidecars
python3 toot.py /path/to/content/ --stats-only --quiet --all | jq '[.[] | select(.sidecar==false)]'
```

Each entry contains: `toot_id`, `source` (relative path to .markdown file), `reply_count`, `unique_users`, `latest_date`, `fetched_at`, `age_hours` (hours since last fetch), `stale` (boolean, based on `--stale-after`), `frozen` (boolean), and `sidecar` (boolean, false if no cached data exists).

### Custom CSS

`--css-extra /path/to/custom.css` appends additional CSS rules after the built-in scoped styles. The file contents are injected as a second `<style>` block in every generated HTML fragment. This is the recommended way to customize the appearance without editing the script.

Example `custom.css` — override accent color and card background:

```css
/* Use site brand color for links and accents */
.mt-wrap { --mt-accent: #e63946; }

/* Wider cards on desktop */
@media (min-width: 768px) {
  .mt__card { max-width: 720px; }
}

/* Hide engagement stats completely */
.mt__stats { display: none; }
```

All built-in CSS variables (`--mt-bg`, `--mt-card`, `--mt-accent`, etc.) can be overridden this way. Changing `--css-extra` triggers a content hash change, so `--regenerate` will rewrite affected fragments.

### Sidecar Migration

`--migrate-sidecars` upgrades all JSON sidecars to the current format without making API calls. Use this after upgrading toot.py to a version with a new sidecar format, or after `--validate` reports outdated or incomplete sidecars.

```bash
# Check what needs migration
python3 toot.py /path/to/content/ --validate

# Migrate all sidecars
python3 toot.py /path/to/content/ --migrate-sidecars
```

The migration adds missing fields with sensible defaults, recomputes `reply_count` from stored API data, fills in `toot_id` from the filename if empty, and bumps `sidecar_version` to the current version. Sidecars that are already current are skipped. Future-version sidecars (from a newer toot.py) are left untouched. The operation is idempotent — running it multiple times is safe. Corrupt sidecars are reported as errors but do not prevent other sidecars from being migrated.

### Cleanup

Over time, cache files accumulate for deleted or changed posts. Three cleanup modes are available:

```bash
# Remove toot cache files with no matching .markdown source
python3 toot.py --cleanup-tootcache /path/to/content/

# Remove avatars not referenced by any active thread
python3 toot.py --cleanup-avatars /path/to/content/

# Remove cached media not referenced by any active thread
python3 toot.py --cleanup-media /path/to/content/

# All at once
python3 toot.py --cleanup-all /path/to/content/
```

---

### Regeneration

The `--regenerate` flag rebuilds all HTML include fragments from the cached JSON sidecars without making any API calls. This is useful when you change settings that affect rendering but don't need fresh data from Mastodon:

```bash
# Regenerate after changing theme
python3 toot.py --regenerate --theme light /path/to/content/

# Regenerate after updating blocklist/whitelist
python3 toot.py --regenerate --whitelist /etc/whitelist.txt /path/to/content/

# Regenerate with different sort order
python3 toot.py --regenerate --sort popular /path/to/content/
```

Regeneration requires existing JSON sidecars with `api_data` (written automatically during normal runs). Threads without a sidecar are skipped. The original `fetched_at` timestamp is preserved; a `regenerated_at` field is added to the sidecar.

By default, `--regenerate` skips threads marked with `mastodonfreeze: true`. Use `--regenerate-all` to also regenerate frozen threads:

```bash
# Regenerate everything, including frozen threads
python3 toot.py --regenerate-all --theme light /path/to/content/
```

Settings that take effect on regeneration include: `--theme`, `--css-extra`, `--blocklist`, `--whitelist`, `--max-depth`, `--max-toots`, `--max-media`, `--highlight-above`, `--fold-depth`, `--fold-threshold`, `--sort`, `--hide-bots`, `--custom-emojis`, `--article`, avatar/media URL mappings.

Regeneration is incremental: a SHA-256 hash of the generated HTML (excluding the "Generated …" timestamp) is stored in the sidecar. On the next `--regenerate`, the new output is compared against the stored hash — if identical, the file is not rewritten. This preserves file modification times and avoids unnecessary I/O, which is relevant for build systems that trigger on mtime changes. If settings change (e.g. `--theme`), the hash will differ and the file is rewritten.

---

## Blocklist / Whitelist

Both blocklist and whitelist use the same file format: one URL or URL prefix per line. Lines starting with `#` or `;` are comments.

### Blocklist

Toots matching the blocklist are hidden. Toots by the thread OP are exempt and always shown:

```text
# Block a specific toot
https://mastodon.social/@spammer/114999999999999999

# Block an entire user
https://mastodon.social/@spammer

# Block an entire instance
https://spam-instance.example.com
```

### Whitelist

If a whitelist is provided, **only** toots matching the whitelist are shown. All other toots are hidden. Toots by the thread OP are always shown regardless of the whitelist.

```text
# Only show replies from these users
https://mastodon.social/@trusted-user
https://social.example.com/@friend

# Allow an entire instance
https://trusted-instance.example.com
```

### Matching Logic

The matching logic is prefix-based and applies identically to both lists:

| Entry | Effect |
|-------|--------|
| `https://server/@user/12345` | Matches that exact toot |
| `https://server/@user` | Matches all toots by that user |
| `https://server` | Matches all toots from that server |

### Combining Both

When both blocklist and whitelist are provided, the **whitelist takes precedence**: a toot that matches the whitelist is shown even if it also matches the blocklist. Toots not on the whitelist are hidden regardless of the blocklist. If only a blocklist is provided (no whitelist), it works as usual.

**Important:** Toots by the thread OP are always shown, regardless of blocklist or whitelist. This ensures the original thread and any OP self-replies remain intact.

Filtered toots are replaced with a "This reply was hidden" placeholder to preserve thread structure.

---

## Labels / i18n

All user-facing strings (labels, ARIA descriptions, button text) are configurable via a labels file. The file uses a simple key=value format, one per line. Lines starting with `#` or `;` are comments. Any key not defined in the file falls back to its English default.

```bash
python3 toot.py --labels /etc/mastodon-labels-de.txt /path/to/content/
```

### Example: German labels file

```
# Timestamps
edited = (bearbeitet)
edited_title = Bearbeitet am {date}
posted_aria = Veröffentlicht am {date}
generated = Erstellt am {date}

# Content
show_content = Inhalt anzeigen
sensitive_toggle = Sensible Inhalte &ndash; klicken zum Anzeigen
post_hidden = Dieser Beitrag wird nicht angezeigt.
boosted = hat geteilt
boost_duplicate = ebenfalls geteilt &ndash; Original oben angezeigt

# Media
media_link = {n} Medienanhänge &ndash; im Original ansehen
media_link_1 = {n} Medienanhang &ndash; im Original ansehen
media_more = {n} weitere Medien &ndash; im Original ansehen
media_more_1 = {n} weiteres &ndash; im Original ansehen

# Poll
poll_aria = Umfrage
poll_final = Endergebnis
poll_open = Umfrage offen
voters = {n} Abstimmende
voters_1 = {n} Abstimmende/r
votes = {n} Stimmen
votes_1 = {n} Stimme
fold_replies = {n} weitere Antworten
fold_replies_1 = {n} weitere Antwort

# ARIA
post_by_aria = Beitrag von {handle}
engagement_aria = Interaktionen
replies_aria = {n} Antworten
replies_aria_1 = {n} Antwort
boosts_aria = {n} Boosts
boosts_aria_1 = {n} Boost
favs_aria = {n} Favoriten
favs_aria_1 = {n} Favorit
permalink = Permalink
comments_aria = Mastodon-Kommentare

# Header / Footer
mastodon = Mastodon
comment_count = {n} Kommentare
comment_count_1 = {n} Kommentar
comments_header = Kommentare
article_label = Artikel
reply_on = Antworten auf
view_on = Ansehen auf
no_comments = Noch keine Kommentare. Sei der Erste!
comments_separator = Kommentare

# Thread stats
thread_stats = {total} Beiträge von {users} Personen in diesem Thread

# Truncation
more_replies = &hellip; und {n} weitere Antworten &ndash; weiter auf {instance}
more_replies_1 = &hellip; und {n} weitere Antwort &ndash; weiter auf {instance}
```

### Placeholder variables

Labels can contain placeholders in `{...}` that are filled in at render time:

| Placeholder | Used in | Meaning |
|---|---|---|
| `{date}` | `edited_title`, `posted_aria`, `generated` | Formatted date string |
| `{handle}` | `post_by_aria` | User's @handle |
| `{n}` | count labels (`comment_count`, `replies_aria`, etc.) | Numeric count |
| `{total}`, `{users}` | `thread_stats` | Thread statistics |
| `{instance}` | `more_replies` | Mastodon instance hostname |

### Singular / plural

Keys ending in `_1` are used when the count is exactly 1. If a `_1` variant is not defined, the base (plural) form is used for all counts.

---

## Article Mode

Article mode is designed for Mastodon threads where the original poster (OP) writes a long-form piece as a series of self-replies – essentially a blog post published as a thread.

### How It Works

When activated, toot.py extracts the OP's self-reply chain and presents it as a continuous article block above the comment section. The remaining replies from other users are shown below as comments.

### Activation

Article mode requires **two** conditions:

1. The `--article` flag (or `article = true` in config)
2. The post's frontmatter includes `mastodonarticle: true`

```yaml
---
mastodoncomment: true
commenttoot: "114123456789012345"
mastodonarticle: true
---
```

### Article Chain Extraction

Starting from the root toot, the algorithm follows only direct children authored by the same account as the root. Once a non-OP toot appears in a branch, the entire branch is cut – even if the OP replies further down that branch.

Example thread:

```
Root (OP)
├── Toot 2 (OP)          ← in article
│   └── Toot 3 (OP)      ← in article
├── Toot 4 (OP)          ← in article
│   └── Toot 5 (UserB)   ← NOT in article (chain broken)
│       └── Toot 6 (OP)  ← NOT in article (parent broke chain)
└── Toot 7 (UserB)       ← NOT in article (not OP)
```

Result: Article contains toots 2, 3, and 4.

### Article Layout

The generated HTML has this structure:

```
┌─────────────────────────────────────────┐
│ Thread by                               │
│ 🖼 AuthorName  @author@instance         │
│                                         │
│ Root post content...                    │
│                         Jan 15  🔗      │
│ ─────────────────────────────────────── │
│ Second post content...                  │
│                         Jan 15  🔗      │
│ ─────────────────────────────────────── │
│ Third post content...                   │
│                         Jan 15  🔗      │
└─────────────────────────────────────────┘
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMMENTS

@author@instance  Root post content…
  └── [UserB] Great thread!

@author@instance  Fourth post content…
  └── [UserB] Nice point!
```

Key elements:

- **Article block** at the top with all OP posts displayed sequentially
- **Permalink anchors** (🔗) on each article post for deep-linking
- **"Comments" separator** between article and comment section
- **Compact stubs** in the comment tree for article toots that received replies – showing only the handle and first three words, linking back up to the full post in the article block
- **Comment count and thread stats** exclude article toots, showing only actual comments

### Article-Only Mode (No Comments)

If a post should display only the article thread without any comment section, add `mastodonarticle_nocomment: true` to the frontmatter:

```yaml
---
mastodoncomment: true
commenttoot: "114123456789012345"
mastodonarticle: true
mastodonarticle_nocomment: true
---
```

In this mode the generated HTML contains only the article block and a "View on [instance]" footer link. The comment tree, "Comments" separator, thread statistics, and truncation notice are all suppressed. The header displays "Article" instead of a comment count.

This is useful for threads that function purely as long-form articles where reader replies are not relevant to the blog post.

---

## Themes

Three color schemes are available via `--theme`:

- **dark** (default) – dark background, light text
- **light** – light background, dark text
- **auto** – adapts to the visitor's OS/browser preference via `@media (prefers-color-scheme: ...)`. Light is the fallback for browsers that don't support the media query.

```bash
python3 toot.py --theme auto /path/to/content/
```

All CSS is scoped under the `.mt-wrap` namespace and uses CSS custom properties, so it won't conflict with your site's styles. The 23 custom properties (`--mt-bg`, `--mt-card`, `--mt-accent`, etc.) are defined as structured dicts in the source code, making it straightforward to add custom themes.

---

## Features Reference

### Rendering Features

| Feature | Description |
|---------|-------------|
| **Threaded replies** | Replies are nested up to `--max-depth` levels with indentation and thread lines |
| **Reply-to indicator** | Replies at depth ≥ 2 show "↩ @username" to clarify who they're responding to |
| **Content warnings** | Spoiler text is shown as a collapsible "Show content" block |
| **Sensitive media** | Images/videos marked sensitive are wrapped in a click-to-reveal element |
| **Empty body suppression** | Toots with no text content (e.g. media-only posts, empty boost wrappers) omit the body element entirely, avoiding blank gaps in the layout |
| **Alt-text badge** | Images with alt text show an "ALT" badge in the bottom-left corner. Hover or click to read the full description |
| **Media gating** | Only the OP's media is embedded inline. Non-OP media shows a link to the original toot ("1 media attachment – view on original toot ↗") |
| **Responsive images** | OP images use `srcset` with the preview (~400px) and original resolution, both cached locally. The browser picks the smallest sufficient source based on viewport width, saving bandwidth on mobile |
| **Media limit** | `--max-media` (default 3) caps embedded media per toot. Excess attachments show a "N more – view on original toot" link. Set to 0 for unlimited |
| **Emoji-only toots** | Replies consisting only of 1–10 emoji characters (e.g. 👍, ❤️🔥) are rendered in larger font (2rem), matching Mastodon's native display |
| **Link preview cards** | When an OP toot contains a link preview (OpenGraph card), it is rendered as a compact box with thumbnail, title, description, and provider. Thumbnails are cached locally via the media cache. Cards are suppressed when the toot has media attachments (matching Mastodon's behavior) |
| **Quoted toots** | Mastodon 4.3+ quote posts are rendered as an embedded box showing the quoted author's avatar, handle, timestamp, and body text (clamped to 4 lines). Clicking the box opens the original quoted toot. Works for all toots (OP and non-OP), coexists with media and cards |
| **Polls** | Rendered as bar charts with vote counts and percentages |
| **Custom emojis** | `:shortcode:` patterns replaced with inline images (opt-in via `--custom-emojis`) |
| **Verified badges** | Users with verified profile links get a ✓ badge |
| **Bot badges** | Bot accounts are marked with a 🤖 icon |
| **Edited indicator** | Edited toots show "(edited)" with the edit timestamp |
| **Boost detection** | Reblogs/boosts are marked with a "🔁 @user boosted" banner, showing the original content and author. When multiple users boost the same toot, only the first is rendered in full — duplicates show a compact "@user also boosted – original shown above" stub |
| **Hashtag highlighting** | Hashtags in toot content are highlighted in the accent color |
| **Mention highlighting** | @mentions are highlighted in a distinct color |
| **ARIA labels** | Full accessibility markup: `role="article"` on toot cards, `role="region"` on wrapper, `aria-label` on stats and interactive elements, `aria-hidden` on decorative SVGs and thread lines |
| **SEO-safe links** | Non-OP links (profile, toot, body, media) get `rel="nofollow ugc noopener"`. OP links remain trusted with only `rel="noopener"` |
| **Semantic `<time>` elements** | All timestamps (posted, edited) use `<time datetime="...">` for machine-readable dates |
| **Language tags** | Toot bodies carry a `lang` attribute from the Mastodon API (e.g. `lang="de"`), enabling correct screenreader pronunciation and CSS auto-hyphenation |
| **Sidecar versioning** | JSON sidecars include a `sidecar_version` field. Newer toot.py versions can read older sidecars; sidecars from future versions are skipped with a warning |
| **Regenerate skip** | `--regenerate` compares a SHA-256 content hash before writing. Unchanged fragments are skipped, preserving file mtime and avoiding unnecessary I/O |
| **Atomic writes** | All file writes (`.html.include` and `.json`) use temp-file-then-rename, so a crash never leaves corrupt files. Corrupt sidecars are skipped gracefully |
| **Cross-cache collision detection** | At startup, warns if cache directories overlap (e.g. `--avatar-dir` == `--media-dir`). At runtime, warns before writing a file that already exists in a different cache. Also detects the (astronomically unlikely) case of two different URLs producing the same SHA-256 hash within a single cache |
| **Exit codes** | Exit 0 on success, exit 1 on errors. Enables `set -e` in build scripts and CI/CD error detection |
| **Graceful SIGTERM/SIGINT/timeout** | On `SIGTERM`, `SIGINT` (Ctrl+C), or `--timeout` expiry, the current thread finishes processing (atomic write completes), then the loop stops and prints a summary with "(Interrupted – partial run)". All sleeps (rate limiting, retries) are interruptible in 1-second steps, so the process responds promptly even during long waits |
| **Auto theme** | `--theme auto` emits both dark and light schemes via `@media (prefers-color-scheme)`. No JavaScript needed |
| **Print stylesheet** | `@media print` rules override colors to white background, force-open all folds and sensitive content, shrink avatars, hide decorative thread lines, print link URLs inline, and add `break-inside:avoid` on toot cards. Works with all themes |
| **Custom CSS** | `--css-extra file.css` appends custom CSS rules after the built-in scoped styles. Override colors, adjust spacing, or add new rules without modifying the script. Affects content hash for regeneration skip |
| **Lockfile** | Exclusive `flock()`-based lockfile prevents concurrent instances from corrupting shared cache. Disable with `--no-lock` |
| **Sidecar validation** | `--validate` checks all JSON sidecars for structural integrity, version compatibility, and cross-references with `.markdown` sources. No API calls, no writes |
| **Statistics export** | `--stats-only` outputs per-thread statistics (reply count, unique users, latest activity, stale/frozen status, cache age) as a JSON array to stdout. No API calls, no writes. Pipe through `jq` for filtering and reporting |
| **Sidecar migration** | `--migrate-sidecars` upgrades all JSON sidecars to the current version, adding missing fields (e.g. `content_hash`) and recomputing `reply_count` from stored API data. No API calls. Idempotent — safe to run repeatedly |

### Operational Features

| Feature | Description |
|---------|-------------|
| **Incremental processing** | Only re-fetches threads when cache has expired |
| **Rate limiting** | Configurable minimum delay between API calls (default: 5s). Mastodon's `X-RateLimit-Remaining` and `X-RateLimit-Reset` headers are evaluated after each response; when the server requires a longer wait, that value overrides the configured minimum. 429 responses respect `Retry-After` |
| **Pagination warning** | Logs a warning when the server's `/context` response appears truncated (fewer descendants returned than the root toot's `replies_count` suggests). Mastodon silently caps at ~4000 descendants |
| **Charset fallback** | API responses are decoded using the charset from the server's `Content-Type` header, falling back to UTF-8. Handles edge cases with reverse proxies or non-standard server configurations |
| **Retry with backoff** | Transient errors (429, 5xx, network) trigger automatic retries with exponential backoff |
| **Stale thread detection** | Threads with no new reply beyond a configurable threshold (default: 12 months) use a longer cache TTL (`--max-age-stale`, default: 7 days) instead of the normal `--max-age`. Stale threads are still re-checked periodically, just less frequently |
| **Frozen threads** | Per-post `mastodonfreeze: true` frontmatter to permanently skip regeneration, preserving the existing include file |
| **JSON sidecars** | Raw API responses are saved alongside HTML for debugging and reprocessing |
| **Avatar hashing** | Avatars are stored with SHA-256 filenames, avoiding conflicts and enabling CDN caching |
| **Media caching** | OP media (images, videos) are downloaded and served from a local cache, ensuring long-term availability |
| **Thread statistics** | Footer shows toot count, unique user count |
| **Thread truncation** | `--max-toots` limits displayed replies with a "N more replies" link |
| **Popular highlighting** | Replies with ≥ N favourites (default 10) get a golden accent border, making them easy to spot in long threads. Disable with `--highlight-above 0` |
| **Foldable deep threads** | Subtrees at or beyond `--fold-depth` (default 3) are collapsed into a clickable `<details>` element ("N more replies"). Only activates when total comments exceed `--fold-threshold` (default 50). Pure HTML/CSS, no JavaScript |

---

## Examples

### Basic Usage

Process all recent posts in the content directory:

```bash
python3 toot.py /srv/blog/content/
```

### With Config File and Verbose Output

```bash
python3 toot.py --config ~/toot.ini --verbose /srv/blog/content/
```

### Process All Posts, Light Theme

```bash
python3 toot.py --all --theme light /srv/blog/content/
```

### Article Mode with Custom Emojis

```bash
python3 toot.py --article --custom-emojis /srv/blog/content/
```

### Dry Run (Preview)

See what would be processed without making any API calls:

```bash
python3 toot.py --dry-run --verbose /srv/blog/content/
```

### Full Production Setup

```bash
python3 toot.py \
  --config /etc/toot.ini \
  --token "$MASTODON_TOKEN" \
  --blocklist /etc/mastodon-blocklist.txt \
  --article \
  --custom-emojis \
  --hide-bots \
  --sort oldest \
  --max-depth 4 \
  --max-toots 50 \
  --quiet \
  /srv/blog/content/
```

### Periodic Cleanup (Cron)

```bash
# Weekly: remove orphaned cache files
0 3 * * 0 python3 /opt/toot.py --cleanup-all --quiet /srv/blog/content/

# Hourly: refresh comments
0 * * * * python3 /opt/toot.py --config /etc/toot.ini --quiet /srv/blog/content/
```

When running under systemd timers or in containers, toot.py handles `SIGTERM` gracefully: the current thread finishes writing (atomic write completes), the loop stops, and a summary is printed. `SIGINT` (Ctrl+C) is handled identically. Use `--timeout 3600` to ensure the process exits within a fixed time budget, for example in cron jobs that must not overlap:

```bash
# Hourly: refresh comments, hard limit 50 minutes
0 * * * * python3 /opt/toot.py --config /etc/toot.ini --quiet --timeout 3000 /srv/blog/content/
```

---

## Jekyll Integration

Jekyll does not have a built-in mechanism to include files from arbitrary paths outside the project. The recommended approach is to copy the generated include files into Jekyll's `_includes` directory as a pre-build step.

### Setup

Add a pre-build step that copies the relevant include files into the Jekyll project:

```bash
#!/bin/bash
# pre-build.sh — run before `jekyll build`

CACHE="/var/tmp/gencache/mastodontootcache"
DEST="/srv/blog/_includes/toots"

mkdir -p "$DEST"

# Generate comments
python3 /opt/toot.py --config /etc/toot.ini --quiet /srv/blog/

# Copy only include files (not JSON sidecars)
for f in "$CACHE"/*.html.include; do
  [ -f "$f" ] && cp "$f" "$DEST/"
done

jekyll build
```

Then include in your layout:

```liquid
{% if page.mastodoncomment and page.commenttoot %}
  {% capture toot_file %}toots/{{ page.commenttoot }}.html.include{% endcapture %}
  {% include {{ toot_file }} %}
{% endif %}
```

### Jekyll Frontmatter

Jekyll frontmatter works identically to the standard format. All toot.py variables are available as `page.*` in Liquid templates:

```yaml
---
layout: post
title: My Blog Post
date: 2025-06-15 10:00:00 +0200
mastodoncomment: true
commenttoot: "114123456789012345"
mastodonarticle: true
---
```

Note: Jekyll parses YAML natively, so quoting the numeric `commenttoot` value is recommended to ensure it is treated as a string.

### Complete Jekyll Build Pipeline

A typical cron job for a Jekyll blog with Mastodon comments:

```bash
#!/bin/bash
# /opt/build-blog.sh

set -e

# 1. Refresh Mastodon comments
python3 /opt/toot.py --config /etc/toot.ini --quiet /srv/blog/

# 2. Sync include files into Jekyll
rsync -a --delete \
  /var/tmp/gencache/mastodontootcache/*.html.include \
  /srv/blog/_includes/toots/

# 3. Build Jekyll
cd /srv/blog
JEKYLL_ENV=production bundle exec jekyll build --destination /var/www/blog/

# 4. Periodic cleanup (optional, weekly)
if [ "$(date +%u)" = "7" ]; then
  python3 /opt/toot.py --cleanup-all --quiet /srv/blog/
fi
```

```cron
# Hourly build
0 * * * * /opt/build-blog.sh >> /var/log/blog-build.log 2>&1
```

---

## Troubleshooting

### "mastodoncomment=true but commenttoot is missing"

The `.markdown` file has `mastodoncomment: true` but no `commenttoot` field. Add the toot ID or URL to the frontmatter.

### "No toot ID found in '...'"

The `commenttoot` value could not be parsed. Ensure it's either a numeric ID or a valid Mastodon URL ending in a numeric toot ID.

### "Another toot.py instance is already running"

Another process holds the lockfile. Check for running cron jobs or background processes (`ps aux | grep toot.py`). If a previous run crashed without releasing the lock (e.g. `SIGKILL`), the lock is released automatically by the OS when the process dies — this error means a process is genuinely still running. If you're certain no other instance is active, the lock file (`.toot.lock` in the toot cache directory) can be deleted manually. Use `--no-lock` to disable locking entirely.

### Rate limiting (HTTP 429)

The Mastodon instance is throttling requests. The script reads `Retry-After` headers and waits accordingly. It also monitors `X-RateLimit-Remaining` and automatically slows down when the budget gets low. If you still see 429 errors, increase `--rate-limit` to add more time between calls.

### "Possible truncation" warning

Mastodon's `/context` API endpoint silently caps the number of returned descendants (typically around 4000). The script detects this by comparing the number of descendants received against the root toot's `replies_count`. This is a server-side limitation that cannot be worked around — the warning is informational only. Very large threads will show fewer replies than actually exist.

### Stale comments

Cached HTML is served until `--max-age` expires. Use `--force` to re-fetch immediately, or reduce `--max-age` for more frequent updates.

### Missing avatars in output

Ensure `--avatar-dir` points to a directory your web server can read, and `--avatar-url` matches the public URL that directory is served from.

### Missing or broken OP images

If OP media shows broken images, check that `--media-dir` is writable and `--media-url` matches the public URL that directory is served from. If the original media download failed, toot.py falls back to the original Mastodon CDN URL.

### "Cache directory overlap" warning

This appears when two or more of `--toot-cache-dir`, `--avatar-dir`, and `--media-dir` point to the same directory. While this won't crash, it means avatar and media files share the same directory — `--cleanup-avatars` or `--cleanup-media` may incorrectly delete each other's files. Use separate directories for each cache.

### "Hash collision" warning

This means two different URLs produced the same SHA-256 filename within a single cache directory. With SHA-256 this is astronomically unlikely (1 in 2^128 for a targeted collision), so this almost certainly indicates a bug rather than a genuine hash collision. If you see this warning, please report it — the second URL's content will overwrite the first.

### Empty thread / "0 comments"

The toot may be on a private or restricted account. Use `--token` with an access token that has read permissions for that account.

### Corrupt or truncated JSON sidecar

If a JSON sidecar becomes corrupt (e.g. from a crash during write), toot.py handles this gracefully: the thread is skipped during `--regenerate` and re-fetched normally on the next regular run. All file writes (both `.html.include` and `.json`) use atomic writes (temp file + rename), so a crash or kill signal will never leave a half-written file — either the old file is preserved intact or the new one is fully written.

---

## Output Structure

For each processed thread, toot.py generates:

```
{toot-cache-dir}/
├── 114123456789012345.html.include   ← HTML fragment (include in your template)
└── 114123456789012345.json           ← Raw API data (debugging/reprocessing)

{avatar-dir}/
├── a1b2c3d4...f0.png                ← SHA-256 hashed avatar filenames
└── e5f6a7b8...c9.jpg

{media-dir}/
├── cf80ba4a...d3.jpg                ← SHA-256 hashed OP media filenames
└── 9a1e2b3c...f7.mp4
```

The `.html.include` file is a self-contained fragment with inline `<style>` and all CSS scoped under the `.mt-wrap` class. It can be included directly in any HTML page without additional stylesheets.
