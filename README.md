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

## Prerequisites

- Python 3.10 or higher
- Deluge torrent client
- Read access to Deluge state and download directories
- Read access to media directory

## Installation

### Docker (Recommended)
```bash
docker run -v /path/to/config:/config \
           -v /path/to/downloads:/downloads \
           -v /path/to/media:/media \
           fiveboroughs/deluge-orphaned-files
```

### Manual Installation
```bash
git clone https://github.com/yourusername/deluge-orphaned-files.git
cd deluge-orphaned-files
pip install -r requirements.txt
```

## Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your settings:
```bash
# Required settings
DELUGE_HOST=localhost
DELUGE_PORT=58846
DELUGE_USERNAME=admin
DELUGE_PASSWORD=password

# Paths
DELUGE_TORRENT_BASE_REMOTE_FOLDER=/downloads
LOCAL_TORRENT_BASE_LOCAL_FOLDER=/downloads
LOCAL_MEDIA_BASE_LOCAL_FOLDER=/media

# Optional settings
OUTPUT_FILE=scan_results.json
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