"""Compute orphaned / mismatched files between Deluge, torrent folder and media folder.

The heavy comparison work is extracted here so it can be unit-tested and reused
without pulling the whole *cli.py* module (which brings in the huge CLI
surface and side-effects).

Returns *lists* ready for DB persistence or e-mail reporting – the caller
(`cli.find_orphaned_files` or future services) decides what to do with them.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from loguru import logger

from ..deluge.client import get_deluge_files as deluge_get_files
from ..scanning.file_scanner import get_local_files as scan_get_local_files

__all__: list[str] = ["compute_orphans"]


def _size_human(num_bytes: int) -> str:
    """Convert byte size to human readable format.
    
    Args:
        num_bytes: Size in bytes to convert.
        
    Returns:
        String representation with unit (GB or MB) and 2 decimal places.
    """
    if num_bytes >= 1024**3:
        return f"{num_bytes / (1024**3):.2f} GB"
    return f"{num_bytes / (1024**2):.2f} MB"


def compute_orphans(*, config, skip_media_check: bool = False, use_sqlite: bool = False, no_progress: bool = False) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Calculate orphaned files across torrent and media folders.
    
    Compares files in the Deluge client, local torrent folders, and media folders
    to identify orphaned or mismatched files in three categories.
    
    Args:
        config: The validated AppConfig instance with all required settings.
        skip_media_check: If True, only check torrent-folder orphans and skip
            the media folder scan completely.
        use_sqlite: Whether to use SQLite for hash caching instead of JSON files.
        no_progress: Whether to disable progress bars for file scanning.
        
    Returns:
        A tuple containing three lists:
            1. Orphaned torrent files (in torrent folder but not in Deluge)
            2. Files only in torrents (in torrent folder but not in media folder)
            3. Files only in media (in media folder but not in torrent folder)
            
        Each list contains dictionaries with file details (path, size, etc.).
    """

    logger.info("Connecting to Deluge and getting file list…")
    deluge_file_paths, file_labels, file_torrent_ids = deluge_get_files(config)
    logger.info("Retrieved {} files from Deluge", len(deluge_file_paths))

    # Scan torrent folder
    logger.info("Scanning local torrent folder…")
    local_torrent_files = scan_get_local_files(
        folder=config.local_torrent_base_local_folder,
        config=config,
        use_sqlite=use_sqlite,
        no_progress=no_progress,
    )
    logger.info("Found {} files in local torrent folder", len(local_torrent_files))

    orphaned_torrent_files: List[Dict[str, Any]] = [
        {
            "path": path,
            "size": info["size"],
            "size_human": _size_human(info["size"]),
        }
        for path, info in local_torrent_files.items()
        if path not in deluge_file_paths
    ]
    orphaned_torrent_files.sort(key=lambda x: x["size"], reverse=True)
    logger.info("Torrent-folder orphans: {}", len(orphaned_torrent_files))

    if skip_media_check:
        return orphaned_torrent_files, [], []

    # Scan media folder
    logger.info("Scanning local media folder…")
    local_media_files = scan_get_local_files(
        folder=config.local_media_base_local_folder,
        config=config,
        use_sqlite=use_sqlite,
        no_progress=no_progress,
    )
    logger.info("Found {} files in local media folder", len(local_media_files))

    # Hash dictionaries with blacklist filtering
    torrent_hashes: Dict[str, Tuple[str, int, str, str]] = {
        info["hash"]: (
            name,
            info["size"],
            file_labels.get(name, "none"),
            file_torrent_ids.get(name, None),
        )
        for name, info in local_torrent_files.items()
        if not any(name.startswith(sub + "/") for sub in config.local_subfolders_blacklist)
    }
    media_hashes: Dict[str, Tuple[str, int]] = {
        info["hash"]: (name, info["size"]) for name, info in local_media_files.items() if not any(name.startswith(sub + "/") for sub in config.local_subfolders_blacklist)
    }

    torrent_set = frozenset(torrent_hashes.keys())
    media_set = frozenset(media_hashes.keys())

    only_in_torrents: List[Dict[str, Any]] = [
        {
            "path": torrent_hashes[h][0],
            "label": torrent_hashes[h][2],
            "size": torrent_hashes[h][1],
            "size_human": _size_human(torrent_hashes[h][1]),
            "torrent_id": torrent_hashes[h][3],
        }
        for h in torrent_set - media_set
    ]
    only_in_torrents.sort(
        key=lambda x: (
            "a" if x["label"].startswith("other") else x["label"],
            x["size"],
        ),
        reverse=True,
    )

    only_in_media: List[Dict[str, Any]] = [
        {
            "path": media_hashes[h][0],
            "size": media_hashes[h][1],
            "size_human": _size_human(media_hashes[h][1]),
        }
        for h in media_set - torrent_set
    ]
    only_in_media.sort(key=lambda x: x["size"], reverse=True)

    logger.info("Files only in torrents: {}, only in media: {}", len(only_in_torrents), len(only_in_media))

    return orphaned_torrent_files, only_in_torrents, only_in_media
