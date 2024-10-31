# Deluge Orphaned Files

A Python utility to find orphaned files in your Deluge torrent client's download directory. This tool helps identify files that exist on disk but are not tracked by any active torrents in Deluge.

The output is a JSON file that can be used to manually delete the orphaned files.
No changes are made to Deluge's configuration or any data.

## Features

- Scans Deluge state and download directories to find:
  - Orphaned files (exist on disk but not in Deluge)
  - Missing files (in Deluge but not on disk)
  - Mismatched files (same name but different content)
- Fast file comparison using SHA256 hashing
- Hash caching to speed up subsequent scans
- Progress bars for long operations
- Configurable via environment variables or command line arguments

## Prerequisites

- Python 3.10 or higher
- Access to Deluge state directory
- Access to Deluge download directory
- Access to Plex media directory

## Installation

Docker (recommended):
https://hub.docker.com/r/fiveboroughs/deluge-orphaned-files


Or cli
```bash
git clone https://github.com/FiveBoroughs/deluge-orphaned-files.git
cd deluge-orphaned-files
pip install -r requirements.txt
python deluge_orphaned_files.py
```
