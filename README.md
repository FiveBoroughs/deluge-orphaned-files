# Deluge Orphaned Files

A Python utility to find orphaned files in your Deluge torrent client's download directory.

## Overview

This tool helps identify files that exist on disk but are not tracked by any active torrents in Deluge. It's particularly useful if you:
- Use hardlinks between download and media directories
- Follow the [TRaSH guides](https://trash-guides.info/) for media organization
- Need to clean up your download directory
- Want to verify media file consistency

## Directory Structure Example

```

<!-- Environment Variable Explanations -->
   Key environment variables to configure in your `.env` file (after copying from `.env.example`):
   - `DELUGE_HOST`, `DELUGE_PORT`, `DELUGE_USERNAME`, `DELUGE_PASSWORD`: Connection details for your Deluge daemon.
   - `DELUGE_TORRENT_BASE_REMOTE_FOLDER`: The absolute base path where Deluge stores torrent data (as seen by the Deluge daemon itself). For example, if Deluge saves to `/srv/downloads/torrents`, this should be `/srv/downloads/torrents`.
   - `LOCAL_TORRENT_BASE_LOCAL_FOLDER`: The absolute base path where this script can access the same torrent data locally. This might be a direct path if running on the same machine as Deluge, or a mounted path (e.g., `/mnt/nas/downloads/torrents`) if accessing remotely. Ensure this path corresponds to the same directory level as `DELUGE_TORRENT_BASE_REMOTE_FOLDER` for accurate path comparisons.
   - `LOCAL_MEDIA_BASE_LOCAL_FOLDER`: The absolute base path for your media library, if you are using the media comparison features.
   - `EXTENSIONS_BLACKLIST`: A comma-separated list of file extensions (e.g., `.nfo`, `.txt`) or full filenames to ignore during local file scans. This is crucial for excluding metadata files, temporary files (like those ending in `.partial` or `.fastresume` if not handled by Deluge itself), or other unwanted file types (e.g., `.torrent.invalid`) from being processed or reported. Ensure a leading dot for extensions (e.g., `.jpg`, `.invalid`).
   - `LOCAL_SUBFOLDERS_BLACKLIST`: A comma-separated list of top-level subfolder names within your `LOCAL_TORRENT_BASE_LOCAL_FOLDER` and `LOCAL_MEDIA_BASE_LOCAL_FOLDER` that should be entirely skipped during local scans (e.g., `music,ebooks,samples`).

   Key environment variables to configure in your `.env` file include:
   - `DELUGE_HOST`, `DELUGE_PORT`, `DELUGE_USERNAME`, `DELUGE_PASSWORD`: Connection details for your Deluge daemon.
   - `DELUGE_TORRENT_BASE_REMOTE_FOLDER`: The absolute base path where Deluge stores torrent data (as seen by the Deluge daemon itself). For example, if Deluge saves to `/srv/downloads/torrents`, this should be `/srv/downloads/torrents`.
   - `LOCAL_TORRENT_BASE_LOCAL_FOLDER`: The absolute base path where this script can access the same torrent data locally. This might be a direct path if running on the same machine as Deluge, or a mounted path (e.g., `/mnt/nas/downloads/torrents`) if accessing remotely. Ensure this path corresponds to the same directory level as `DELUGE_TORRENT_BASE_REMOTE_FOLDER` for accurate path comparisons.
   - `LOCAL_MEDIA_BASE_LOCAL_FOLDER`: The absolute base path for your media library, if you are using the media comparison features.
   - `EXTENSIONS_BLACKLIST`: A comma-separated list of file extensions (e.g., `.nfo`, `.txt`) or full filenames to ignore during local file scans. This is crucial for excluding metadata files, temporary files (like those ending in `.partial` or `.fastresume` if not handled by Deluge itself), or other unwanted file types (e.g., `.torrent.invalid`) from being processed or reported. Ensure a leading dot for extensions (e.g., `.jpg`, `.invalid`).
   - `LOCAL_SUBFOLDERS_BLACKLIST`: A comma-separated list of top-level subfolder names within your `LOCAL_TORRENT_BASE_LOCAL_FOLDER` and `LOCAL_MEDIA_BASE_LOCAL_FOLDER` that should be entirely skipped during local scans (e.g., `music,ebooks,samples`).
data/
├── torrents/
│   ├── movies/
│   ├── tv/
│   └── music/
└── media/
    ├── movies/
    ├── tv/
    └── music/
```

The output is a JSON file that can be used to manually delete the orphaned files.
No changes are made to Deluge's configuration, not files are deleted.


## Features

- Scans Deluge state and download directories to find:
  - Orphaned files (in download folder but not in Deluge)
  - Missing media files (in torrents but not media folder)
  - Missing torrent files (in media but not torrents folder)
- Fast file comparison using MD5 hashing
- Hash caching to speed up subsequent scans
- Progress bars for long operations
- Configurable file/folder exclusions
- Detailed logging options
- **Consistent path normalization**: Employs robust path normalization for both Deluge-reported paths and locally scanned file paths. This ensures reliable comparisons, prevents duplicate entries in the database, and improves overall data integrity for tracking files across different sources.

## Prerequisites

- Python 3.10 or higher
- Deluge torrent client
- Read access to Deluge state and download directories
- Read access to media directory

## Installation

### Docker (Recommended)
```bash
docker run -v /mnt/tank/data/orphaned_files.json:/app/orphaned_files.json \
           -v /mnt/tank/data/torrents:/data/torrents \
           -v /mnt/tank/data/media:/data/media \
           -e DELUGE_HOST=localhost \
           -e DELUGE_PORT=58846 \
           -e DELUGE_USERNAME=admin \
           -e DELUGE_PASSWORD=password \
           -e DELUGE_TORRENT_BASE_REMOTE_FOLDER=/data/torrents \
           -e LOCAL_TORRENT_BASE_LOCAL_FOLDER=/data/torrents \
           -e LOCAL_MEDIA_BASE_LOCAL_FOLDER=/data/media \
           -e OUTPUT_FILE=orphaned_files.json \
           -e EXTENSIONS_BLACKLIST=.nfo,.srt,.jpg \
           -e LOCAL_SUBFOLDERS_BLACKLIST=music,ebooks,courses \
           -e CACHE_SAVE_INTERVAL=25 \
           -e TZ=Europe/Paris \
           fiveboroughs/deluge-orphaned-files
```

### Manual Installation
```bash
git clone https://github.com/fiveboroughs/deluge-orphaned-files.git
cd deluge-orphaned-files
pip install -r requirements.txt
```

## Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

<!-- Environment Variable Explanations -->
   Key environment variables to configure in your `.env` file (after copying from `.env.example`):
   - `DELUGE_HOST`, `DELUGE_PORT`, `DELUGE_USERNAME`, `DELUGE_PASSWORD`: Connection details for your Deluge daemon.
   - `DELUGE_TORRENT_BASE_REMOTE_FOLDER`: The absolute base path where Deluge stores torrent data (as seen by the Deluge daemon itself). For example, if Deluge saves to `/srv/downloads/torrents`, this should be `/srv/downloads/torrents`.
   - `LOCAL_TORRENT_BASE_LOCAL_FOLDER`: The absolute base path where this script can access the same torrent data locally. This might be a direct path if running on the same machine as Deluge, or a mounted path (e.g., `/mnt/nas/downloads/torrents`) if accessing remotely. Ensure this path corresponds to the same directory level as `DELUGE_TORRENT_BASE_REMOTE_FOLDER` for accurate path comparisons.
   - `LOCAL_MEDIA_BASE_LOCAL_FOLDER`: The absolute base path for your media library, if you are using the media comparison features.
   - `EXTENSIONS_BLACKLIST`: A comma-separated list of file extensions (e.g., `.nfo`, `.txt`) or full filenames to ignore during local file scans. This is crucial for excluding metadata files, temporary files (like those ending in `.partial` or `.fastresume` if not handled by Deluge itself), or other unwanted file types (e.g., `.torrent.invalid`) from being processed or reported. Ensure a leading dot for extensions (e.g., `.jpg`, `.invalid`).
   - `LOCAL_SUBFOLDERS_BLACKLIST`: A comma-separated list of top-level subfolder names within your `LOCAL_TORRENT_BASE_LOCAL_FOLDER` and `LOCAL_MEDIA_BASE_LOCAL_FOLDER` that should be entirely skipped during local scans (e.g., `music,ebooks,samples`).


<!-- Environment Variable Explanations -->
   Key environment variables to configure in your `.env` file (after copying from `.env.example`):
   - `DELUGE_HOST`, `DELUGE_PORT`, `DELUGE_USERNAME`, `DELUGE_PASSWORD`: Connection details for your Deluge daemon.
   - `DELUGE_TORRENT_BASE_REMOTE_FOLDER`: The absolute base path where Deluge stores torrent data (as seen by the Deluge daemon itself). For example, if Deluge saves to `/srv/downloads/torrents`, this should be `/srv/downloads/torrents`.
   - `LOCAL_TORRENT_BASE_LOCAL_FOLDER`: The absolute base path where this script can access the same torrent data locally. This might be a direct path if running on the same machine as Deluge, or a mounted path (e.g., `/mnt/nas/downloads/torrents`) if accessing remotely. Ensure this path corresponds to the same directory level as `DELUGE_TORRENT_BASE_REMOTE_FOLDER` for accurate path comparisons.
   - `LOCAL_MEDIA_BASE_LOCAL_FOLDER`: The absolute base path for your media library, if you are using the media comparison features.
   - `EXTENSIONS_BLACKLIST`: A comma-separated list of file extensions (e.g., `.nfo`, `.txt`) or full filenames to ignore during local file scans. This is crucial for excluding metadata files, temporary files (like those ending in `.partial` or `.fastresume` if not handled by Deluge itself), or other unwanted file types (e.g., `.torrent.invalid`) from being processed or reported. Ensure a leading dot for extensions (e.g., `.jpg`, `.invalid`).
   - `LOCAL_SUBFOLDERS_BLACKLIST`: A comma-separated list of top-level subfolder names within your `LOCAL_TORRENT_BASE_LOCAL_FOLDER` and `LOCAL_MEDIA_BASE_LOCAL_FOLDER` that should be entirely skipped during local scans (e.g., `music,ebooks,samples`).

   After configuring these, you can run the script using:
```

<!-- Environment Variable Explanations -->
   Key environment variables to configure in your `.env` file (after copying from `.env.example`):
   - `DELUGE_HOST`, `DELUGE_PORT`, `DELUGE_USERNAME`, `DELUGE_PASSWORD`: Connection details for your Deluge daemon.
   - `DELUGE_TORRENT_BASE_REMOTE_FOLDER`: The absolute base path where Deluge stores torrent data (as seen by the Deluge daemon itself). For example, if Deluge saves to `/srv/downloads/torrents`, this should be `/srv/downloads/torrents`.
   - `LOCAL_TORRENT_BASE_LOCAL_FOLDER`: The absolute base path where this script can access the same torrent data locally. This might be a direct path if running on the same machine as Deluge, or a mounted path (e.g., `/mnt/nas/downloads/torrents`) if accessing remotely. Ensure this path corresponds to the same directory level as `DELUGE_TORRENT_BASE_REMOTE_FOLDER` for accurate path comparisons.
   - `LOCAL_MEDIA_BASE_LOCAL_FOLDER`: The absolute base path for your media library, if you are using the media comparison features.
   - `EXTENSIONS_BLACKLIST`: A comma-separated list of file extensions (e.g., `.nfo`, `.txt`) or full filenames to ignore during local file scans. This is crucial for excluding metadata files, temporary files (like those ending in `.partial` or `.fastresume` if not handled by Deluge itself), or other unwanted file types (e.g., `.torrent.invalid`) from being processed or reported. Ensure a leading dot for extensions (e.g., `.jpg`, `.invalid`).
   - `LOCAL_SUBFOLDERS_BLACKLIST`: A comma-separated list of top-level subfolder names within your `LOCAL_TORRENT_BASE_LOCAL_FOLDER` and `LOCAL_MEDIA_BASE_LOCAL_FOLDER` that should be entirely skipped during local scans (e.g., `music,ebooks,samples`).

2. Edit `.env` with your settings:
```bash
# Required settings
DELUGE_HOST=localhost
DELUGE_PORT=58846
DELUGE_USERNAME=admin
DELUGE_PASSWORD=password

# Paths
DELUGE_TORRENT_BASE_REMOTE_FOLDER=/data/torrents
LOCAL_TORRENT_BASE_LOCAL_FOLDER=/data/torrents
LOCAL_MEDIA_BASE_LOCAL_FOLDER=/data/media

# Optional settings
OUTPUT_FILE=orphaned_files.json
EXTENSIONS_BLACKLIST=.nfo,.srt,.jpg
LOCAL_SUBFOLDERS_BLACKLIST=music,ebooks,courses
CACHE_SAVE_INTERVAL=25
```

## Usage

### Basic Usage
```bash
python deluge_orphaned_files.py
```

### Command Line Arguments
```bash
python deluge_orphaned_files.py [options]
```

Available options:
- `--clean-cache`: Remove stale entries from hash cache
- `--debug`: Enable detailed debug logging
- `--skip-media-check`: Skip media folder comparison

### Scheduled mode (internal cron)

The Docker entrypoint is a small scheduler (`deluge_orphaned_files/scheduler.py`) that dispatches to the CLI:

- **`CRON_SCHEDULE` unset** — one-shot: the CLI runs once with the container arguments and the container exits, exactly as before.
- **`CRON_SCHEDULE` set** — daemon: the container stays running and executes the CLI once per cron fire. Arguments are still supplied as the container command.

| Variable | Meaning |
|---|---|
| `CRON_SCHEDULE` | 5-field cron expression (`minute hour day month weekday`), evaluated in container local time (`TZ`). Empty/unset disables daemon mode. |
| `RUN_ON_STARTUP` | `true` (default) also runs once immediately at container start; `false` waits for the first scheduled fire. |
| `NOTIFY_ON_STARTUP` | `false` (default) suppresses the e-mail/Telegram report for that startup run (scheduler appends `--no-notify`); `true` reports startup runs too. |

```bash
docker run -d --restart unless-stopped \
  -e CRON_SCHEDULE="0 2 * * *" \
  -e TZ=Europe/Paris \
  ... (Deluge/path env as above) ... \
  ghcr.io/fiveboroughs/deluge-orphaned-files:latest \
  --sqlite --no-progress --use-inodes --apply-autoremove-labels --execute-pending-actions --force
```

Runs never overlap; a run that overruns its slot skips the missed fire rather than queueing a catch-up. A failed run is logged and the schedule continues. `docker stop` is handled cleanly (SIGTERM is forwarded to a running scan). A heartbeat file backs the image HEALTHCHECK while idling.


#### Exit codes and container health

The CLI exits with a meaningful code (also in `--help`):

| Code | Meaning |
|---|---|
| 0 | Completed |
| 1 | Failed (e.g. scan error, Deluge unreachable for stuck-torrent repair or tracker cleanup, test notification could not be sent) |
| 2 | Usage/configuration error (the request cannot be served as asked, e.g. `--show-last` without `--sqlite`, test notification without its configuration) |
| 3 | Partial: scan saved, but a secondary step (autoremove labeling, pending actions, e-mail/Telegram report) failed |

After every run the scheduler records the outcome (exit code, finish time, consecutive-failure streak) in a status file (`STATUS_PATH`, default `/tmp/delugeof-last-run.json`). The image HEALTHCHECK fails when the heartbeat is stale (>180s, scheduler stuck) **or** two consecutive runs have failed — so a nightly run that keeps erroring shows up as an unhealthy container instead of silently doing nothing. One transient failure does not trip it.

> **Log rotation:** the container logs every run to stdout (JSON-file driver by default). Cap it, e.g. Docker Compose `logging: {driver: json-file, options: {max-size: "10m", max-file: "3"}}` or the equivalent in your orchestrator, so years of nightly runs don't grow unbounded.

**Migrating from an external cron/start-on-schedule setup** (e.g. TrueNAS cron job starting the container): set `CRON_SCHEDULE`, change the app restart policy to *unless-stopped*, and remove the external cron job.

### Retiring Deluge registrations with entirely missing payloads

When a retention tool deletes media, Deluge can continue to report the affected torrents as seeding. `--reconcile-missing-payloads` adds a separate, opt-in inventory pass to the normal scan. It checks every Deluge infohash (including torrents sharing the same files), and never unlinks data.

Set `DELUGE_MEDIA_BASE_REMOTE_FOLDER` to the absolute path Deluge uses for the media root, corresponding to `LOCAL_MEDIA_BASE_LOCAL_FOLDER`. The existing `DELUGE_TORRENT_BASE_REMOTE_FOLDER` and `LOCAL_TORRENT_BASE_LOCAL_FOLDER` settings map the torrent root. The two remote roots must not overlap. The pass checks locations in Deluge's file inventory, not similarly named files elsewhere. Use roots on the actual mounted storage, not parents of separate, nested mounts.

Place a regular file named `.delugeof-root` at the top of **each** local root, on the intended storage while it is mounted (an empty file is enough). Do not put the marker in the underlying mountpoint; otherwise it cannot establish whether the storage is present. Both roots and markers must be accessible and are checked again before removal. An unreadable path, malformed or unmapped inventory, or failed RPC does not count as evidence of absence.

```bash
# Preview only: no candidate state changes and no registrations removed
python -m deluge_orphaned_files.cli --sqlite --reconcile-missing-payloads --execute-pending-actions

# Record observations over multiple runs without removing any registrations
python -m deluge_orphaned_files.cli --sqlite --reconcile-missing-payloads

# Retire due registrations. WARNING: --force also lets legacy pending actions delete orphan files.
python -m deluge_orphaned_files.cli --sqlite --reconcile-missing-payloads --execute-pending-actions --force
```

An observation counts only if Deluge reports a complete seed (`Seeding` or a completed `Paused` seed) and every listed payload path is absent. A file that reappears or a move/re-add breaks the streak. Both `DELETION_CONSECUTIVE_SCANS_THRESHOLD` and `DELETION_DAYS_THRESHOLD` apply (default 7 observations and 7 days); even a threshold below two requires two separate successful observations. Before each removal the pass rechecks the individual torrent and all of its paths, then asks Deluge to remove that one registration with `remove_data=False`. These flags do not change how the legacy orphan-file workflow behaves; `--execute-pending-actions --force` still also executes legacy actions.

### Queueing tracker-rejected registrations for removal

Tracker cleanup is opt-in and runs on its own, without an orphan scan or any file deletion:

```bash
python -m deluge_orphaned_files.cli --tracker-rejected-torrents         # preview only
python -m deluge_orphaned_files.cli --apply-tracker-rejected-removals   # queue and process due actions
```

The command uses `APP_SQLITE_CACHE_PATH` (no `--sqlite` flag needed) and the existing queue grace period, `RELABEL_ACTION_DELAY_DAYS` (default 7 days). It queues complete private torrents in Deluge's `Seeding` state only when `tracker_status` is exactly `Unregistered torrent`, `Torrent removed by its uploader`, or `Torrent has been deleted.` after stripping Deluge's optional `Error:` prefix, surrounding whitespace and case differences. Completion uses Deluge's `is_seed` flag, not just 100% of wanted files. One matching reply is enough even if multiple announce URLs are configured; cooldowns, warnings, authentication and network errors, and unknown replies are not terminal.

Re-running the command does not restart the grace period: the queue keeps one pending action per infohash for the configured Deluge host and port. Once due, the command reads fresh status from Deluge. `Announce OK` or known ineligibility cancels the action; temporary, unknown, or unverifiable status leaves it pending. A still-eligible torrent with an exact terminal reply is removed **from Deluge only** (`remove_data=False`), without applying `othercat` or touching its local data. Logs show the matched reply, due time and outcome, but never the announce URL or credentials.

The preview shows new and existing due times and what would happen to due actions, without changing the queue or Deluge. Schedule the apply command as a **separate invocation** from the normal orphan scan (for example, a separate scheduled container execution). It does not require `--force`, and normal `--execute-pending-actions` does not process tracker removals.
Use one apply schedule per daemon and queue. Do not start a manual apply while
that scheduled run is active.

### Repairing stuck cross-seed torrents

cross-seed injects partial matches paused and only resumes them when little is missing, so some torrents sit paused-incomplete forever even though every missing file already exists on disk, verified complete in another torrent. Two standalone flags (no scan) find and fix those:

```bash
python -m deluge_orphaned_files.cli --repair-stuck-torrents   # dry-run: log planned hardlinks
python -m deluge_orphaned_files.cli --apply-stuck-repairs     # link, guarded recheck, resume what verifies
```

Eligible torrents are paused, incomplete, labelled `cross-seed` or `*.cross-seed`, and added more than `REPAIR_MIN_AGE_HOURS` (24) ago. Each missing wanted file is matched to a verified donor by name+size — or by size alone when at least `REPAIR_SIZE_ONLY_MIN_MB` (50 MB) and unambiguous — then hardlinked into place. Repair uses regular files under the configured torrent base. A destination also used by another torrent, even through a directory alias, is skipped. Directories are never created, and existing partials are kept as `*.delugeof-repair-bak` without overwriting an existing backup.

The verification recheck runs with all file priorities at 0, a positive `REPAIR_MAX_DOWNLOAD_SPEED_KIB` cap and auto-management off. The repair aborts if more than `REPAIR_DOWNLOAD_ABORT_MB` (1 MB) downloads, if the payload counter is lost or reset, or if Deluge no longer reports the guard settings. Torrents that verify 100% are resumed, then their backups are deleted; a cleanup error leaves the verified links and reports the remaining backup. Failed repairs restore the original files and recheck them once Deluge confirms the torrent is paused. If a pause cannot be confirmed, the links, backups and guard settings are left in place for manual recovery.

Still-stuck torrents are reported only (attempts tracked in `stuck-repair-state.json`); no torrents are removed. A run is skipped below `REPAIR_MIN_FREE_GB` (10 GB) free space, and a lock prevents overlapping runs. In scheduled (daemon) mode, add the flags to the container command as a separate scheduled invocation or run them manually with `docker exec`.

#### Planning sidecar recovery (issue #3)

When local hardlinks alone can't repair a variant, run the standalone v1 planner:

```bash
python -m deluge_orphaned_files.sidecar_plan \
  --manifest /input/variants.json --metainfo-dir /input/metainfo
# Optional: --torrent FULL_40_HEX_INFOHASH
```

Provide trusted `.torrent` files named `<lowercase-infohash>.torrent` for the
targets and for complete reference torrents that contain the same media inodes.
The JSON manifest maps each exact **target** infohash to an object such as:

```json
{
  "1111111111111111111111111111111111111111": {
    "sidecars": ["Mapped Release/subtitles.srt", "Mapped Release/sample.nfo"],
    "media_sources": ["2222222222222222222222222222222222222222"]
  }
}
```

Declare only genuinely optional files, using **current Deluge paths relative to
that torrent's save directory**, and choose references that are known to have
the same media bytes. The command uses `DELUGE_HOST`, `DELUGE_PORT`, `DELUGE_USERNAME`,
`DELUGE_PASSWORD`, `DELUGE_TORRENT_BASE_REMOTE_FOLDER` and
`LOCAL_TORRENT_BASE_LOCAL_FOLDER` (environment or `.env`). Use a trusted Deluge
2.2.x endpoint and transport, admin credentials for full inventory visibility,
the Label plugin, and a stable local read-only view of the same payload tree.
Targets must be individually paused, incomplete, labeled `cross-seed` or
`*.cross-seed`; all files must be wanted (nonzero priority).
No scan, cache, workspace, torrent changes, or file writes are needed. JSON is
written to stdout; blocked variants include the reason and make the exit
status nonzero.

The planner requires the daemon's missing piece indices to exactly match the
spans of absent declared sidecars. It hashes each readable target piece against
the target metainfo and hashes the complete references to authenticate the
shared media inodes, including bytes in pieces that cross absent sidecars.
It rejects incomplete piece maps, unsafe paths, shared destinations, partial
files, and unsupported metainfo (v2/hybrid, pad or symlink files), rather than
estimating from file progress. For same-name/size sidecar donors, it checks
every affected target piece with the original media before counting the donor.
It reports absent sidecar bytes and affected piece bytes separately, including
the short final piece, then ranks the remaining work after proven local donors.
`remaining_piece_bytes` is the amount of unresolved piece data, not a transfer
ceiling. Bytes expected by the target in an unhashable boundary piece can only
be confirmed once the missing bytes are recovered. A plan is an observation,
not a reservation, and does not authorize any torrent operation.
The destination check requires an alias-free Deluge inventory: different save
paths must not resolve to the same directory through symlinks or bind mounts.

**Automated bounded download and apply are unavailable.** Stock Deluge 2.2
[`move_storage`](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/deluge/core/torrent.py#L1279-L1316)
uses libtorrent's
[`dont_replace`](https://github.com/arvidn/libtorrent/blob/v2.0.11/include/libtorrent/torrent_handle.hpp#L1314-L1326)
mode, which removes source paths even when pre-staged destination files exist
and can move/copy data; `download_location` changes an option, not the live
storage. There is no safe stock RPC to switch both directions without risking
the original paths. Deluge's
[`force_recheck`](https://github.com/deluge-torrent/deluge/blob/deluge-2.2.0/deluge/core/torrent.py#L1411-L1426)
also calls `resume()` internally. A rate cap and CLI counter polling cannot
enforce a torrent-wide payload budget if the client dies, disconnects, stalls,
or the daemon restarts. Finally, [cross-seed builds donor paths from the
Deluge inventory](https://github.com/cross-seed/cross-seed/blob/4f86d0a8cf5d3e8022ae3ac1717ba5a5eef9a032/cross-seed/src/clients/Deluge.ts#L891-L992),
not just from configured data directories. A private directory or this app's
lock alone cannot keep an injecting job from discovering or linking its inodes.

Execution requires a daemon-owned admission/watchdog and transfer guard with a
bounded allowance for in-flight and redundant data, a non-destructive
save-path switch and completion acknowledgement, and operational exclusion
of external cross-seed inventory/injection for the entire workspace lifetime
(including interruption). It must force reflinks without a copy fallback,
verify private inodes and the isolated missing-piece map without changing file
priorities, preserve interrupted work, verify recovered sidecars against target
and sibling piece hashes on original media, and explicitly recheck after
returning. Resume only at 100% with zero remaining bytes; remove the workspace
only after verifying that no torrent retains its paths or inodes. The planner
does none of those mutations. The existing download-free repair above is
unchanged.

## Output Format

The script generates a JSON file with the following structure:
```json
{
  "host": "admin@localhost:58846",
  "base_path": "/downloads",
  "scan_start": "2024-03-20T10:00:00",
  "scan_end": "2024-03-20T10:05:00",
  "in_local_torrent_folder_but_not_deluge": [...],
  "files_only_in_torrents": [...],
  "files_only_in_media": [...]
}
```

## Database and File Tracking (SQLite Backend)

The script now utilizes an SQLite database (`orphaned_files.sqlite` by default, in the same directory as the script) to store scan results and track file history. This provides more robust data management and enables features like tracking files over multiple scans.

Key tables and concepts:

*   **`scan_results`**: Stores metadata about each scan run (host, paths, start/end times).
*   **`orphaned_files`**: This is the central table tracking individual files identified in various categories.
    *   `path`: The relative path of the file.
    *   `source`: Indicates the context in which the file was found:
        *   `'local_torrent_folder'`: Files present in the local torrent download directory but not registered in Deluge (true orphans).
        *   `'torrents'`: Files registered in Deluge and found in the local torrent download directory.
        *   `'media'`: Files found in the local media directory.
    *   `status`: Tracks the lifecycle of a file entry.
        *   `'active'`: The file is currently detected in the specified `source` category.
        *   `'marked_for_deletion'`: User has marked this file for cleanup (future feature).
        *   `'deleted'`: The file has been confirmed as deleted (future feature).
        *   **Behavior**: When a file is detected during a scan, if it already exists in the database for that `source`, its `status` is updated to `'active'`, its `last_seen_at` timestamp is refreshed. New file entries automatically default to `'active' `.
    *   `consecutive_scans`: The number of distinct calendar days on which back-to-back scans have identified this file in the given `source` and `path` (multiple scans on the same day count once, so deployment bursts cannot inflate it).
        *   **Behavior**: If a file is found on a new day, its `consecutive_scans` is 1 (for new entries, or after a reset) or incremented by 1; additional same-day scans refresh `last_seen_at` without incrementing. If it's *not* found in a subsequent scan (e.g., an orphaned file is deleted or added to Deluge), this counter resets for that record, and a new record might be created if it appears in a different `source` category, or it might simply not be listed in that scan's results for that original `source`.
    *   `first_seen_at` / `last_seen_at`: Timestamps for when the file was first and most recently detected under that specific `path` and `source`.
    *   `include_in_report`: A boolean indicating if the file should be part of standard reporting (primarily for orphaned files).
*   **`file_scan_history`**: Links files from `orphaned_files` to specific scans in `scan_results`, recording the `source` context for that file in that particular scan.
*   **`file_hashes`**: Caches MD5 hashes of files to speed up subsequent scans.

This SQLite backend replaces the previous single JSON output file as the primary data store, though a JSON report can still be generated. The database allows for more detailed historical analysis and is foundational for planned features like managing the deletion lifecycle of orphaned files.

## Performance Considerations

- First run will be slower due to hash calculation
- Subsequent runs use cache for unchanged files
- Large libraries may take significant time
- Use `--skip-media-check` for quick orphan scans

## Troubleshooting

Common issues and solutions:
1. **Permission denied**: Ensure read access to all directories
2. **Connection refused**: Check Deluge daemon status
3. **Slow performance**: Verify disk health and network connection
4. **High memory usage**: Reduce batch size via `CACHE_SAVE_INTERVAL`

## CI dependency updates

GitHub Actions versions and their resolved commits are tracked in `.github/workflows/actions.lock`. Run `gh actions-lock` after editing workflow dependencies. Dependabot checks weekly and updates the lockfile in its PRs.

CI runs the Python tests and builds and smoke-tests the Docker image on pull requests and pushes to `main`. On `main`, the publish job pushes the tested OCI image to GHCR under both `latest` and the commit SHA.