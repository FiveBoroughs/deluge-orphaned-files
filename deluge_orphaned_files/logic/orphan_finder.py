"""Compute orphaned / mismatched files between Deluge, torrent folder and media folder.

The heavy comparison work is extracted here so it can be unit-tested and reused
without pulling the whole *cli.py* module (which brings in the huge CLI
surface and side-effects).

Returns *lists* ready for DB persistence or e-mail reporting – the caller
(`cli.find_orphaned_files` or future services) decides what to do with them.
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from loguru import logger

from ..deluge.client import get_deluge_files as deluge_get_files
from ..scanning.file_scanner import get_local_files as scan_get_local_files

__all__: list[str] = ["compute_orphans"]


def _clock(ts: float) -> str:
    """Format a POSIX timestamp as a local wall-clock time for log lines."""
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def _duration_human(seconds: float) -> str:
    """Format an elapsed number of seconds as e.g. '7h06m' or '12m34s'."""
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _blacklisted_subfolder(path: str, blacklist: List[str]) -> str | None:
    """Return the blacklisted top-level subfolder owning *path*, or None.

    Matches both the subfolder itself (``cg/…``) and unpackerr's temp directory for it
    (``cg_unpackerred/…``). The plain ``sub + "/"`` test used for the hash comparisons
    misses the latter, which is how 59 hand-managed ``cg_unpackerred/`` files became
    deletion candidates.
    """
    for sub in blacklist:
        if path.startswith(sub + "/") or path.startswith(sub + "_unpackerred/"):
            return sub
    return None


def _is_partial_file(path: str) -> bool:
    """True for Deluge in-progress allocations and hidden files.

    Deleting a ``.parts`` file out from under an active download corrupts it, so these are
    never orphan candidates — matched by pattern rather than via ``EXTENSIONS_BLACKLIST``
    so that no configuration change can turn them back into candidates.
    """
    name = Path(path).name
    return name.startswith(".") or name.endswith(".parts")


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

    The Deluge file list is a snapshot taken before the (potentially multi-hour)
    filesystem scan. Files whose mtime is newer than that snapshot are *not* reported
    as orphans — they may have been added to Deluge while the scan was running — and
    are deferred to the next run instead. Files under a blacklisted subfolder (or its
    ``_unpackerred`` temp directory) and Deluge partial allocations are excluded outright.

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
    # The snapshot is compared against a filesystem scan that can run for hours; record
    # when it was taken so files created *after* it are not mistaken for orphans.
    snapshot_ts = time.time()
    deluge_file_paths, file_labels, file_torrent_ids = deluge_get_files(config)
    logger.info("Retrieved {} files from Deluge (snapshot taken {})", len(deluge_file_paths), _clock(snapshot_ts))

    # Scan torrent folder
    logger.info("Scanning local torrent folder…")
    local_torrent_files = scan_get_local_files(
        folder=config.local_torrent_base_local_folder,
        config=config,
        use_sqlite=use_sqlite,
        no_progress=no_progress,
    )
    scan_done_ts = time.time()
    logger.info("Found {} files in local torrent folder", len(local_torrent_files))

    orphaned_torrent_files: List[Dict[str, Any]] = []
    skipped_newer_than_snapshot = 0
    skipped_blacklisted = 0
    skipped_partial = 0
    for path, info in local_torrent_files.items():
        if path in deluge_file_paths:
            continue

        blacklisted_subfolder = _blacklisted_subfolder(path, config.local_subfolders_blacklist)
        if blacklisted_subfolder is not None:
            skipped_blacklisted += 1
            logger.info("skipped-blacklisted: {} — subfolder '{}' excluded", path, blacklisted_subfolder)
            continue

        if _is_partial_file(path):
            skipped_partial += 1
            logger.info("skipped-partial: {} — Deluge partial/hidden file", path)
            continue

        mtime = info.get("mtime")
        if mtime is not None and mtime >= snapshot_ts:
            # Created or modified after we asked Deluge what it knew about; its status is
            # *unknown*, not orphaned. Defer to the next run, which will see it in the snapshot.
            skipped_newer_than_snapshot += 1
            logger.info(
                "skipped-too-new: {} — modified {} after deluge snapshot {} (size {})",
                path,
                _clock(mtime),
                _clock(snapshot_ts),
                _size_human(info["size"]),
            )
            continue

        logger.info(
            "orphan: {} — not-in-deluge (snapshot {}, file mtime {}, size {})",
            path,
            _clock(snapshot_ts),
            _clock(mtime) if mtime is not None else "unknown",
            _size_human(info["size"]),
        )
        orphaned_torrent_files.append(
            {
                "path": path,
                "size": info["size"],
                "size_human": _size_human(info["size"]),
            }
        )

    orphaned_torrent_files.sort(key=lambda x: x["size"], reverse=True)
    logger.info(
        "Deluge snapshot taken {}, torrent scan completed {} (age {}), {} files newer than snapshot excluded",
        _clock(snapshot_ts),
        _clock(scan_done_ts),
        _duration_human(scan_done_ts - snapshot_ts),
        skipped_newer_than_snapshot,
    )
    logger.info(
        "Orphan candidates dropped: skipped-too-new={}, skipped-blacklisted={}, skipped-partial={}",
        skipped_newer_than_snapshot,
        skipped_blacklisted,
        skipped_partial,
    )
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
