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
    *   `consecutive_scans`: An integer count of how many back-to-back scans have identified this file in the given `source` and `path`.
        *   **Behavior**: If a file is found in one scan, its `consecutive_scans` is 1 (for new entries) or incremented (for existing entries). If it's *not* found in a subsequent scan (e.g., an orphaned file is deleted or added to Deluge), this counter would implicitly stop incrementing for that specific record, and a new record might be created if it appears in a different `source` category, or it might simply not be listed in that scan's results for that original `source`.
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