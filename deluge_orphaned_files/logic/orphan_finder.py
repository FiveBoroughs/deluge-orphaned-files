"""Compute orphaned / mismatched files between Deluge, torrent folder and media folder.

The heavy comparison work is extracted here so it can be unit-tested and reused
without pulling the whole *cli.py* module (which brings in the huge CLI
surface and side-effects).

Returns *lists* ready for DB persistence or e-mail reporting – the caller
(`cli.find_orphaned_files` or future services) decides what to do with them.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from loguru import logger

from ..deluge.client import get_deluge_files as deluge_get_files
from ..scanning.file_scanner import get_local_files as scan_get_local_files
from ..scanning.file_scanner import get_local_files_inodes as scan_get_local_files_inodes

__all__: list[str] = ["compute_orphans", "InodePreflightError"]


class InodePreflightError(ValueError):
    """--use-inodes preflight failure; the message carries user-facing remediation."""


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


def _assert_same_filesystem(dir1: Path, dir2: Path) -> int:
    """Verify dir1 and dir2 are on the same filesystem and return its st_dev.

    Inode comparison identifies files by (st_dev, st_ino), which is only meaningful
    when both trees share one filesystem. Checked via stat rather than by creating a
    probe hardlink: link(2) fails with EXDEV across two bind mounts even when both
    expose the same filesystem (exactly the Docker deployment, where /data/torrents
    and /data/media are sibling bind mounts of one dataset) — and stat needs no write
    access to either tree.
    """
    try:
        st1 = os.stat(dir1)
    except OSError as exc:
        raise InodePreflightError(f"--use-inodes: cannot stat {dir1} ({exc}). Is the torrent directory mounted?") from exc
    try:
        st2 = os.stat(dir2)
    except OSError as exc:
        raise InodePreflightError(f"--use-inodes: cannot stat {dir2} ({exc}). Is the media directory mounted?") from exc
    if st1.st_dev != st2.st_dev:
        raise InodePreflightError(
            f"--use-inodes: {dir1} (device {st1.st_dev}) and {dir2} (device {st2.st_dev}) are on different filesystems; "
            "files can never share an inode across them. Use hash mode (default), or mount both from the same filesystem."
        )
    return st1.st_dev


def _torrent_sort_key(entry: Dict[str, Any]) -> Tuple[str, int]:
    """Sort key for only-in-torrents entries: 'other*' labels trail real labels, then by size."""
    return ("a" if entry["label"].startswith("other") else entry["label"], entry["size"])


def _torrent_entry(name: str, size: int, file_labels: Dict[str, str], file_torrent_ids: Dict[str, str]) -> Dict[str, Any]:
    return {
        "path": name,
        "label": file_labels.get(name, "none"),
        "size": size,
        "size_human": _size_human(size),
        "torrent_id": file_torrent_ids.get(name, None),
    }


def _media_entry(name: str, size: int) -> Dict[str, Any]:
    return {"path": name, "size": size, "size_human": _size_human(size)}


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


def compute_orphans(
    *, config, skip_media_check: bool = False, use_sqlite: bool = False, no_progress: bool = False, use_inodes: bool = False
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
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
        use_inodes: If True, compare by (st_dev, st_ino) instead of content hash.
            Much faster but requires both directories to be on the same filesystem.

    Returns:
        A tuple containing three lists:
            1. Orphaned torrent files (in torrent folder but not in Deluge)
            2. Files only in torrents (in torrent folder but not in media folder)
            3. Files only in media (in media folder but not in torrent folder)

        Each list contains dictionaries with file details (path, size, etc.).
    """

    if use_inodes and not skip_media_check:
        # Fail fast, before the potentially multi-hour scans, if the mount layout
        # cannot support inode comparison between the two folders.
        shared_dev = _assert_same_filesystem(
            Path(config.local_torrent_base_local_folder),
            Path(config.local_media_base_local_folder),
        )

    logger.info("Connecting to Deluge and getting file list…")
    # The snapshot is compared against a filesystem scan that can run for hours; record
    # when it was taken so files created *after* it are not mistaken for orphans.
    snapshot_ts = time.time()
    deluge_file_paths, file_labels, file_torrent_ids = deluge_get_files(config)
    logger.info("Retrieved {} files from Deluge (snapshot taken {})", len(deluge_file_paths), _clock(snapshot_ts))

    # Scan torrent folder
    logger.info("Scanning local torrent folder…")
    if use_inodes:
        local_torrent_files = scan_get_local_files_inodes(
            folder=config.local_torrent_base_local_folder,
            config=config,
            no_progress=no_progress,
        )
    else:
        local_torrent_files = scan_get_local_files(
            folder=config.local_torrent_base_local_folder,
            config=config,
            use_sqlite=use_sqlite,
            no_progress=no_progress,
        )
    scan_done_ts = time.time()
    logger.info("Found {} files in local torrent folder", len(local_torrent_files))

    # Refresh Deluge after the filesystem walk. cross-seed can create a hardlink and
    # inject its torrent while a scan is running; hardlink creation preserves the
    # underlying file's old mtime, so the timestamp guard below cannot identify that
    # new directory entry. Union both snapshots: additions during the scan become
    # known, while removals during the scan are conservatively deferred until next time.
    logger.info("Refreshing Deluge file list after torrent-folder scan…")
    refreshed_paths, refreshed_labels, refreshed_torrent_ids = deluge_get_files(config)
    paths_added_during_scan = refreshed_paths - deluge_file_paths
    deluge_file_paths.update(refreshed_paths)
    file_labels.update(refreshed_labels)
    file_torrent_ids.update(refreshed_torrent_ids)
    logger.info(
        "Post-scan Deluge refresh found {} files ({} added since initial snapshot); comparing against {} unique owned paths",
        len(refreshed_paths),
        len(paths_added_during_scan),
        len(deluge_file_paths),
    )

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

    blacklist = config.local_subfolders_blacklist

    if use_inodes:
        # Scan media folder
        logger.info("Scanning local media folder (inode mode)…")
        local_media_files = scan_get_local_files_inodes(
            folder=config.local_media_base_local_folder,
            config=config,
            no_progress=no_progress,
        )
        logger.info("Found {} files in local media folder", len(local_media_files))

        # Group by inode — several names can hardlink to the same physical file. Like
        # hash mode, each physical file is reported once: under its lexicographically
        # smallest name, so the representative (and the DB row accruing
        # consecutive_scans) stays stable across scans.
        torrent_inode_map: Dict[tuple, List[tuple]] = {}
        for name, info in local_torrent_files.items():
            if _blacklisted_subfolder(name, blacklist) is None:
                torrent_inode_map.setdefault(info["inode"], []).append((name, info["size"]))

        media_inode_map: Dict[tuple, List[tuple]] = {}
        for name, info in local_media_files.items():
            if _blacklisted_subfolder(name, blacklist) is None:
                media_inode_map.setdefault(info["inode"], []).append((name, info["size"]))

        # A file on a nested mount inside either tree has a different st_dev than the
        # roots the preflight validated, so it can never match by inode — surface that
        # instead of silently reporting the whole nested subtree as orphaned.
        foreign = sum(1 for inode in torrent_inode_map if inode[0] != shared_dev) + sum(1 for inode in media_inode_map if inode[0] != shared_dev)
        if foreign:
            logger.warning("{} files sit on a different filesystem than the scan roots (nested mount?) and can never match by inode", foreign)

        only_in_torrents: List[Dict[str, Any]] = [_torrent_entry(*min(entries), file_labels, file_torrent_ids) for inode, entries in torrent_inode_map.items() if inode not in media_inode_map]
        only_in_media: List[Dict[str, Any]] = [_media_entry(*min(entries)) for inode, entries in media_inode_map.items() if inode not in torrent_inode_map]

    else:
        # Scan media folder
        logger.info("Scanning local media folder…")
        local_media_files = scan_get_local_files(
            folder=config.local_media_base_local_folder,
            config=config,
            use_sqlite=use_sqlite,
            no_progress=no_progress,
        )
        logger.info("Found {} files in local media folder", len(local_media_files))

        # Keyed by hash: duplicate content collapses to a single representative path.
        torrent_hashes: Dict[str, Tuple[str, int]] = {info["hash"]: (name, info["size"]) for name, info in local_torrent_files.items() if _blacklisted_subfolder(name, blacklist) is None}
        media_hashes: Dict[str, Tuple[str, int]] = {info["hash"]: (name, info["size"]) for name, info in local_media_files.items() if _blacklisted_subfolder(name, blacklist) is None}

        only_in_torrents = [_torrent_entry(name, size, file_labels, file_torrent_ids) for name, size in (torrent_hashes[h] for h in torrent_hashes.keys() - media_hashes.keys())]
        only_in_media = [_media_entry(name, size) for name, size in (media_hashes[h] for h in media_hashes.keys() - torrent_hashes.keys())]

    only_in_torrents.sort(key=_torrent_sort_key, reverse=True)
    only_in_media.sort(key=lambda x: x["size"], reverse=True)

    logger.info("Files only in torrents: {}, only in media: {}", len(only_in_torrents), len(only_in_media))

    return orphaned_torrent_files, only_in_torrents, only_in_media
