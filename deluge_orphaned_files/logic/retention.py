"""Retention and deletion scheduling logic.

Handles:
* Determining which orphaned files should be marked for deletion
  based on `consecutive_scans` and days between first/last seen.
* Actually deleting files (or dry-run marking) according to user choice.

The heavy DB / filesystem operations live here so that the CLI simply calls
these helpers.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger

__all__ = [
    "get_files_to_mark_for_deletion",
    "get_files_to_actually_delete",
    "process_deletions",
]


# ---------------------------------------------------------------------------
# Helper queries
# ---------------------------------------------------------------------------


def get_files_to_mark_for_deletion(db_path: Path) -> List[Dict[str, Any]]:
    """Get files eligible for deletion based on configured criteria.
    
    Queries the database view view_files_eligible_for_deletion which filters files based on:
    - source = 'local_torrent_folder'
    - status = 'active'
    - consecutive_scans ≥ threshold (configured when view was created)
    - days between first_seen / last_seen ≥ threshold
    
    Args:
        db_path: Path to the SQLite database file.
        
    Returns:
        List of dictionaries containing eligible file information including id, path,
        size, days seen difference, and consecutive scans count.
        
    Raises:
        sqlite3.Error: If there's an error querying the database.
    """
    files: List[Dict[str, Any]] = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT file_id, file_path, file_size, days_seen_difference, consecutive_scans
                FROM view_files_eligible_for_deletion;
                """,
            )
            for row in cursor.fetchall():
                files.append(
                    {
                        "id": row[0],
                        "path": row[1],
                        "size_human": row[2],
                        "days_seen_difference": row[3],
                        "consecutive_scans": row[4],
                    }
                )
        logger.debug("Found {} files to mark for deletion", len(files))
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_files_to_mark_for_deletion: {}", exc)
    return files


def get_files_to_actually_delete(db_path: Path) -> List[Dict[str, Any]]:
    """Get files that have already been marked for deletion.
    
    Args:
        db_path: Path to the SQLite database file.
        
    Returns:
        List of dictionaries containing file IDs and paths for files marked for deletion.
        
    Raises:
        sqlite3.Error: If there's an error querying the database.
    """
    files: List[Dict[str, Any]] = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, path FROM orphaned_files WHERE status = 'marked_for_deletion';",
            )
            for row in cursor.fetchall():
                files.append({"id": row[0], "path": row[1]})
        logger.debug("Found {} files marked for actual deletion", len(files))
    except sqlite3.Error as exc:
        logger.error("SQLite error in get_files_to_actually_delete: {}", exc)
    return files


# ---------------------------------------------------------------------------
# Main retention orchestration
# ---------------------------------------------------------------------------


def process_deletions(*, force_delete: bool, db_path: Path, torrent_base_folder: Path) -> None:
    """Process file deletions based on retention policy.
    
    Either marks eligible files for deletion or performs actual deletion of
    previously marked files, depending on the force_delete parameter.
    
    Args:
        force_delete: If True, delete files previously marked. Otherwise, do a dry-run
            and mark eligible files for future deletion.
        db_path: Path to the SQLite database.
        torrent_base_folder: The base path where torrent-side files are stored,
            used for constructing absolute file paths for deletion.
    """
    if not db_path.exists():
        logger.warning("Deletion processing skipped: database not found at {}", db_path)
        return

    if force_delete:
        _delete_marked_files(db_path=db_path, torrent_base_folder=torrent_base_folder)
    else:
        _mark_new_eligible_files(db_path)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _delete_marked_files(*, db_path: Path, torrent_base_folder: Path) -> None:
    """Delete files that are currently active torrent orphans.
    
    Fetches orphaned files from the database and physically deletes them from disk,
    then updates their status in the database to 'deleted'.
    
    Args:
        db_path: Path to the SQLite database.
        torrent_base_folder: Base directory where torrent files are stored.
        
    Raises:
        sqlite3.Error: If there's an error accessing the database.
    """
    logger.info("Force delete enabled – attempting to delete active torrent orphans.")

    # Fetch active orphaned torrent files
    files_to_remove: List[Dict[str, Any]] = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, path FROM orphaned_files WHERE source = 'local_torrent_folder' AND status = 'active';",
            )
            for row in cursor.fetchall():
                files_to_remove.append({"id": row[0], "path": row[1]})
    except sqlite3.Error as exc:
        logger.error("SQLite error fetching active torrent orphans for force deletion: {}", exc)
        return

    if not files_to_remove:
        logger.info("No 'active' orphaned files found to delete.")
        return

    if not torrent_base_folder.exists() or not torrent_base_folder.is_dir():
        logger.error("torrent_base_folder '{}' not found or is not a directory – cannot delete.", torrent_base_folder)
        return

    deleted = 0
    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        for entry in files_to_remove:
            file_id = entry["id"]
            relative_path = Path(entry["path"]).relative_to("/")
            absolute_path = (torrent_base_folder / relative_path).resolve()
            if torrent_base_folder not in absolute_path.parents:
                logger.error("Refusing to delete outside base folder: {}", absolute_path)
                continue
            try:
                if absolute_path.exists():
                    os.remove(absolute_path)
                    deleted += 1
                    logger.success("Deleted {}", absolute_path)
                else:
                    logger.warning("File not found while deleting: {}", absolute_path)
                cursor.execute(
                    "UPDATE orphaned_files SET status = 'deleted', deletion_date = CURRENT_TIMESTAMP WHERE id = ?;",
                    (file_id,),
                )
                conn.commit()
            except (PermissionError, OSError) as exc:
                logger.error("Error deleting {}: {}", absolute_path, exc)
            except sqlite3.Error as exc:
                logger.error("SQLite update error for file id {}: {}", file_id, exc)
                conn.rollback()
    logger.info("Force deletion completed. Deleted {} / {} files.", deleted, len(files_to_remove))


def _mark_new_eligible_files(db_path: Path) -> None:
    """Mark eligible files for future deletion.
    
    Identifies files that meet deletion criteria and updates their status in the
    database to 'marked_for_deletion' without actually removing them.
    
    Args:
        db_path: Path to the SQLite database.
        
    Raises:
        sqlite3.Error: If there's an error updating the database.
    """
    logger.info("Dry-run deletion pass – marking eligible files …")
    to_mark = get_files_to_mark_for_deletion(db_path)
    if not to_mark:
        logger.info("No files newly eligible for deletion.")
        return

    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        marked = 0
        for entry in to_mark:
            try:
                cursor.execute(
                    "UPDATE orphaned_files SET status = 'marked_for_deletion' WHERE id = ? AND status = 'active';",
                    (entry["id"],),
                )
                conn.commit()
                marked += 1
                logger.info(
                    "Marked for deletion: {} (size {}, seen ~{:.0f}d over {} scans)",
                    entry["path"],
                    entry["size_human"],
                    entry["days_seen_difference"],
                    entry["consecutive_scans"],
                )
            except sqlite3.Error as exc:
                logger.error("SQLite error marking id {}: {}", entry["id"], exc)
                conn.rollback()
    logger.info("Marked {} files for deletion. Run with --force to actually delete them.", marked)
