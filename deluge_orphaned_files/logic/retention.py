"""Retention and deletion scheduling logic.

Handles:
* Determining which orphaned files should be marked for deletion
  based on `consecutive_scans` and days between first/last seen.
* Actually deleting files (or dry-run marking) according to user choice.

This is the single Implementation of the deletion lifecycle; the CLI calls
these helpers. (A duplicate copy previously lived in cli.py and — because the
scheduler runs cli.py as __main__ before its rebind block executed — was what
actually ran in production. The two copies were merged here: the cli copy's
behaviour is the base, plus a containment check refusing deletions outside
the torrent base folder.)
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger

__all__ = [
    "get_files_to_mark_for_deletion",
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
    - consecutive_scans ≥ threshold (distinct days seen; same-day rescans count once)
    - days between first_seen / last_seen ≥ threshold

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        List of dictionaries containing eligible file information including id, path,
        size, days seen difference, and consecutive scans count.
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


# ---------------------------------------------------------------------------
# Main retention orchestration
# ---------------------------------------------------------------------------


def process_deletions(*, force_delete: bool, db_path: Path, torrent_base_folder: Path, scans_threshold: int) -> None:
    """Process file deletions based on retention policy.

    Either marks eligible files for deletion or performs actual deletion of
    previously marked files, depending on the force_delete parameter.

    Args:
        force_delete: If True, delete files previously marked. Otherwise, do a dry-run
            and mark eligible files for future deletion.
        db_path: Path to the SQLite database.
        torrent_base_folder: The base path where torrent-side files are stored,
            used for constructing absolute file paths for deletion.
        scans_threshold: Minimum consecutive scans a marked file must still
            satisfy at deletion time.
    """
    if not db_path.exists():
        logger.warning("Deletion processing skipped: database not found at {}", db_path)
        return

    if force_delete:
        _delete_marked_files(db_path=db_path, torrent_base_folder=torrent_base_folder, scans_threshold=scans_threshold)
    else:
        _mark_new_eligible_files(db_path)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _delete_marked_files(*, db_path: Path, torrent_base_folder: Path, scans_threshold: int) -> None:
    """Delete torrent orphans that a previous dry run marked for deletion.

    Fetches files whose status is 'marked_for_deletion' and which still satisfy the
    consecutive-scan threshold, physically deletes them from disk, then updates their
    status to 'deleted'. Files already gone from disk become 'vanished' instead, so the
    database keeps the distinction between what this process removed and what did not.

    A stored path whose resolved parent directory falls outside the resolved base
    folder is refused outright — a bad row can never delete outside the torrent tree.
    The check looks at the parent (not the file itself) so a symlink *file* inside the
    tree deletes the link, never its target.

    Args:
        db_path: Path to the SQLite database.
        torrent_base_folder: Base directory where torrent files are stored.
        scans_threshold: Minimum consecutive scans required at deletion time.
    """
    logger.info("Force delete enabled. Deleting files a previous dry run marked for deletion.")

    # Only act on what the dry run marked, and re-check the consecutive-scan
    # threshold at deletion time, so a single mis-classification cannot delete
    # a file the very first time it is seen.
    files_to_remove: List[Dict[str, Any]] = []
    marked_total = 0
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id, path, consecutive_scans FROM orphaned_files WHERE source = 'local_torrent_folder' AND status = 'marked_for_deletion';",
            )
            for row in cursor.fetchall():
                marked_total += 1
                if row[2] >= scans_threshold:
                    files_to_remove.append({"id": row[0], "path": row[1]})
    except sqlite3.Error as exc:
        logger.error("SQLite error fetching torrent orphans marked for deletion: {}", exc)
        return  # Can't proceed if we can't fetch

    skipped_below_threshold = marked_total - len(files_to_remove)
    logger.info(
        "deletion gate: status=marked_for_deletion, consecutive_scans>={} → {} of {} rows matched ({} skipped-below-threshold)",
        scans_threshold,
        len(files_to_remove),
        marked_total,
        skipped_below_threshold,
    )

    if not files_to_remove:
        logger.info("No files marked for deletion from 'local_torrent_folder' source passed the deletion gate.")
        return

    if not torrent_base_folder.exists() or not torrent_base_folder.is_dir():
        logger.error("Provided torrent_base_folder '{}' does not exist or is not a directory. Cannot proceed with deletions.", torrent_base_folder)
        return

    base_resolved = torrent_base_folder.resolve()
    deleted_count = 0
    vanished_count = 0

    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        for file_info in files_to_remove:
            file_id = file_info["id"]
            absolute_file_path = torrent_base_folder / file_info["path"]

            parent_resolved = absolute_file_path.parent.resolve()
            if parent_resolved != base_resolved and base_resolved not in parent_resolved.parents:
                logger.error("Refusing to delete outside base folder: {}", absolute_file_path)
                continue

            logger.info("Attempting to force delete file: {}", absolute_file_path)
            try:
                if absolute_file_path.exists():
                    os.remove(absolute_file_path)
                    logger.success("Successfully force-deleted file: {}", absolute_file_path)
                    cursor.execute(
                        "UPDATE orphaned_files SET status = 'deleted', deletion_date = CURRENT_TIMESTAMP WHERE id = ?;",
                        (file_id,),
                    )
                    deleted_count += 1
                else:
                    logger.warning("File not found during force delete: {}. Updating status to 'vanished' — this process did not delete it.", absolute_file_path)
                    # 'vanished', not 'deleted': something else removed it. Keeping the two
                    # apart is what lets the DB answer "who deleted this file".
                    cursor.execute(
                        "UPDATE orphaned_files SET status = 'vanished', deletion_date = CURRENT_TIMESTAMP WHERE id = ? AND status NOT IN ('deleted', 'vanished');",
                        (file_id,),
                    )
                    vanished_count += 1
                conn.commit()
            except FileNotFoundError:  # Should be caught by .exists(), but as a fallback
                logger.warning("File not found (caught by except FileNotFoundError) during force delete: {}. Updating status to 'vanished'.", absolute_file_path)
                cursor.execute(
                    "UPDATE orphaned_files SET status = 'vanished', deletion_date = CURRENT_TIMESTAMP WHERE id = ? AND status NOT IN ('deleted', 'vanished');",
                    (file_id,),
                )
                vanished_count += 1
                conn.commit()
            except PermissionError:
                logger.error("Permission denied. Cannot force delete file: {}", absolute_file_path)
            except OSError as exc:
                logger.error("OS error during force delete of file {}: {}", absolute_file_path, exc)
            except sqlite3.Error as exc:
                logger.error("SQLite error updating status for file ID {} ({}) after force delete attempt: {}", file_id, absolute_file_path, exc)
                conn.rollback()

    logger.info(
        "Force deletion process completed. Attempted {} files → deleted={}, vanished={}, skipped-below-threshold={}, failed={}.",
        len(files_to_remove),
        deleted_count,
        vanished_count,
        skipped_below_threshold,
        len(files_to_remove) - deleted_count - vanished_count,
    )


def _mark_new_eligible_files(db_path: Path) -> None:
    """Mark eligible files for future deletion.

    Identifies files that meet deletion criteria and updates their status in the
    database to 'marked_for_deletion' without actually removing them.

    Args:
        db_path: Path to the SQLite database.
    """
    logger.info("Dry run for deletions. Identifying files eligible for deletion and marking them...")
    files_to_mark = get_files_to_mark_for_deletion(db_path)
    if not files_to_mark:
        logger.info("No new files eligible to be marked for deletion based on current criteria.")
        return

    with sqlite3.connect(str(db_path)) as conn:
        cursor = conn.cursor()
        marked = 0
        for file_info in files_to_mark:
            try:
                cursor.execute(
                    "UPDATE orphaned_files SET status = 'marked_for_deletion' WHERE id = ? AND status = 'active';",
                    (file_info["id"],),
                )
                conn.commit()
                marked += 1
                logger.info(
                    "File eligible for deletion: {} (Size: {}, Seen for ~{:.0f} days over {} scans). Marking for deletion. Run with --force to remove.",
                    file_info["path"],
                    file_info["size_human"],
                    file_info["days_seen_difference"],
                    file_info["consecutive_scans"],
                )
            except sqlite3.Error as exc:
                logger.error("SQLite error marking file ID {} ({}) for deletion: {}", file_info["id"], file_info["path"], exc)
                conn.rollback()
    logger.info("Marked {} files for deletion. Run with --force to actually delete them.", marked)
