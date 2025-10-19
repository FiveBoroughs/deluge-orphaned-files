"""Pending actions management for deferred operations in the orphan finder.

This module contains the logic for managing pending actions such as file deletion,
auto-labeling, and manual review requirements. It provides a unified interface for:

1. Creating the necessary database schema
2. Registering new actions with appropriate due dates
3. Executing actions when they become due
4. Tracking action status (DETECTED, PENDING, READY, COMPLETED, CANCELLED)

The module implements a unified waiting period for all actions (configurable via
settings) and provides a clean separation from the main CLI interface.
"""

from __future__ import annotations

import datetime
import json
import sqlite3
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger


class ActionStatus(Enum):
    """Status of a pending action in its lifecycle."""

    DETECTED = auto()  # Initially detected
    PENDING = auto()  # Waiting for action_due_at date
    READY = auto()  # Ready to be executed
    COMPLETED = auto()  # Action has been completed
    CANCELLED = auto()  # Action has been cancelled manually


class ActionType(Enum):
    """Types of actions that can be performed."""

    DELETE = auto()  # Delete file
    RELABEL = auto()  # Apply othercat label
    MANUAL_REVIEW = auto()  # Require manual review
    UNKNOWN = auto()  # Unknown / unsupported action type


def init_pending_actions_schema(db_path: str | Path) -> None:
    """Create and initialize the pending_actions table if it doesn't exist.

    Args:
        db_path: Path to the SQLite database file.

    Raises:
        sqlite3.Error: If there's an error creating/updating the schema.
    """
    db_path = str(db_path)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.trace("Ensuring 'pending_actions' table exists.")

            # Check if the table exists first
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pending_actions';")
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                # Create the table with the existing schema structure
                cursor.execute(
                    """
                CREATE TABLE IF NOT EXISTS pending_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    orphaned_file_id INTEGER,
                    torrent_id TEXT,
                    file_path TEXT NOT NULL,
                    current_label TEXT,
                    proposed_action TEXT NOT NULL,
                    action_details TEXT,
                    size_human TEXT NOT NULL,
                    source TEXT,
                    file_size INTEGER,
                    scan_id_identified INTEGER NOT NULL,
                    identified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    action_due_at TIMESTAMP NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    scan_id_processed INTEGER,
                    processed_at TIMESTAMP,
                    processing_notes TEXT
                )
                """
                )
            else:
                # The table exists - ensure we have our new columns added for forward compatibility
                # These might be needed in future versions but we'll maintain backward compatibility
                try:
                    # We'll use a transaction to ensure all these operations happen or none do
                    cursor.execute("BEGIN TRANSACTION;")

                    # Add columns we might need in the future if they don't exist
                    # SQLite doesn't have an easy way to check if a column exists, so we'll just try to add it
                    # and catch the error if it already exists
                    alter_statements = [
                        "ALTER TABLE pending_actions ADD COLUMN source TEXT;",
                        "ALTER TABLE pending_actions ADD COLUMN file_size INTEGER;",
                    ]

                    for stmt in alter_statements:
                        try:
                            cursor.execute(stmt)
                        except sqlite3.OperationalError as e:
                            if "duplicate column name" in str(e).lower():
                                logger.debug(f"Column already exists: {stmt}")
                            else:
                                raise

                    cursor.execute("COMMIT;")
                except Exception as e:
                    cursor.execute("ROLLBACK;")
                    logger.error(f"Error modifying pending_actions table: {e}")
                    raise

            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_actions_status ON pending_actions (status);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_actions_action_due_at ON pending_actions (action_due_at);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_actions_orphaned_file_id ON pending_actions (orphaned_file_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_actions_scan_id_identified ON pending_actions (scan_id_identified);")

            conn.commit()
            logger.debug("Pending actions schema initialized successfully")

    except sqlite3.Error as e:
        logger.error("SQLite error initializing pending actions schema: {}", e)
        raise
    except Exception as e:
        logger.error("Unexpected error initializing pending actions schema: {}", e)
        raise


def register_pending_action(
    db_path: str | Path,
    file_path: str,
    action_type: ActionType,
    waiting_period_days: int,
    action_params: Optional[str] = None,
    scan_id: Optional[str] = None,
    orphaned_file_id: Optional[int] = None,
    torrent_hash: Optional[str] = None,
    file_size: Optional[int] = None,
    source: Optional[str] = None,
    current_label: Optional[str] = None,
    size_human: Optional[str] = None,
) -> int:
    """Register a new pending action in the database.

    Args:
        db_path: Path to the SQLite database file.
        file_path: Path to the file for which the action is registered.
        action_type: Type of action to register (DELETE, RELABEL, etc).
        waiting_period_days: Number of days to wait before the action becomes due.
        action_params: Optional JSON-encoded parameters specific to the action.
        scan_id: ID of the scan that identified this action.
        orphaned_file_id: ID of the orphaned file in the database.
        torrent_hash: Hash of the torrent the file belongs to (called torrent_id in the DB).
        file_size: Size of the file in bytes.
        source: Source of the file (e.g., "local_torrent_folder", "torrents").
        current_label: Current label of the torrent if applicable.
        size_human: Human-readable size (e.g., "1.2 GB").

    Returns:
        The ID of the newly created pending action.

    Raises:
        sqlite3.Error: If there's an error inserting into the database.
    """
    db_path = str(db_path)
    action_due_at = datetime.datetime.now() + datetime.timedelta(days=waiting_period_days)
    action_due_at_str = action_due_at.strftime("%Y-%m-%d %H:%M:%S")  # Format expected by report_formatter

    # Map ActionType enum to proposed_action string values expected by existing code
    proposed_action_map = {
        ActionType.DELETE: "delete",
        ActionType.RELABEL: "relabel",
        ActionType.MANUAL_REVIEW: "manual_review",
    }
    proposed_action = proposed_action_map.get(action_type, "unknown")

    # Extract action details from the JSON params if provided
    action_details = None
    if action_params:
        try:
            params = json.loads(action_params)
            if action_type == ActionType.RELABEL and "label" in params:
                action_details = params["label"]
        except (json.JSONDecodeError, KeyError):
            pass

    # If size_human wasn't provided but we have file_size, generate a human-readable size
    if not size_human and file_size:
        size_human = _format_size(file_size)

    # Ensure we have a valid size_human for the database constraint
    if not size_human:
        size_human = "Unknown"

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # -------------------------------------------------------------
            # Prevent duplicate pending actions for the same file & action.
            # If one already exists in 'pending' state, update any missing
            # information (size_human / current_label) instead of inserting
            # a new record. This eliminates the DB bloat the user observed.
            # -------------------------------------------------------------
            cursor.execute(
                """
                SELECT id, size_human, current_label, torrent_id
                FROM pending_actions
                WHERE file_path = ? AND proposed_action = ? AND status = 'pending'
                """,
                (file_path, proposed_action),
            )
            duplicate_rows = cursor.fetchall()

            existing_id: int | None = None
            existing_size_human: str | None = None
            existing_current_label: str | None = None
            existing_torrent_id: str | None = None

            for (
                row_id,
                row_size_human,
                row_current_label,
                row_torrent_id,
            ) in duplicate_rows:
                if torrent_hash:
                    if row_torrent_id == torrent_hash:
                        existing_id = row_id
                        existing_size_human = row_size_human
                        existing_current_label = row_current_label
                        existing_torrent_id = row_torrent_id
                        break
                else:
                    if not row_torrent_id:
                        existing_id = row_id
                        existing_size_human = row_size_human
                        existing_current_label = row_current_label
                        existing_torrent_id = row_torrent_id
                        break

            # Backward compatibility: if we didn't find a hash-specific match but have legacy rows, reuse the first one
            if existing_id is None and duplicate_rows and not torrent_hash:
                (
                    existing_id,
                    existing_size_human,
                    existing_current_label,
                    existing_torrent_id,
                ) = duplicate_rows[0]

            if existing_id is not None:
                update_fields: list[str] = []
                params: list[Any] = []

                # Patch size_human if we now have a better value
                if size_human not in (None, "", "Unknown") and existing_size_human in (
                    None,
                    "",
                    "Unknown",
                ):
                    update_fields.append("size_human = ?")
                    params.append(size_human)

                # Patch current_label if known / changed
                if current_label and current_label != existing_current_label:
                    update_fields.append("current_label = ?")
                    params.append(current_label)

                # Store torrent hash if we just matched a legacy row without it
                if torrent_hash and not existing_torrent_id:
                    update_fields.append("torrent_id = ?")
                    params.append(torrent_hash)

                if update_fields:
                    params.append(existing_id)
                    cursor.execute(
                        f"UPDATE pending_actions SET {', '.join(update_fields)} WHERE id = ?",
                        params,
                    )
                    conn.commit()
                # Return existing action ID so caller knows it's reused
                return existing_id

            # Use the existing table schema
            cursor.execute(
                """
            INSERT INTO pending_actions
                (orphaned_file_id, torrent_id, file_path, current_label, proposed_action,
                 action_details, size_human, scan_id_identified, identified_at, action_due_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?)
            """,
                (
                    orphaned_file_id,
                    torrent_hash,
                    file_path,
                    current_label,
                    proposed_action,
                    action_details,
                    size_human,
                    scan_id,
                    action_due_at_str,
                    "pending",
                ),  # torrent_id in the database
            )

            action_id = cursor.lastrowid
            conn.commit()

            # Also update the optional source and file_size columns if they were provided
            if source or file_size:
                updates = []
                params = []

                if source:
                    updates.append("source = ?")
                    params.append(source)

                if file_size:
                    updates.append("file_size = ?")
                    params.append(file_size)

                if updates:
                    params.append(action_id)
                    cursor.execute(
                        f"UPDATE pending_actions SET {', '.join(updates)} WHERE id = ?",
                        params,
                    )
                    conn.commit()

            logger.info(f"Registered pending {proposed_action} action for {file_path}, due at {action_due_at_str}")
            return action_id

    except sqlite3.Error as e:
        logger.error(f"SQLite error registering pending action for {file_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error registering pending action for {file_path}: {e}")
        raise


def _format_size(size_bytes: int) -> str:
    """Format size in bytes to a human-readable string.

    Args:
        size_bytes: Size in bytes

    Returns:
        Human-readable size string (e.g., "1.23 GB")
    """
    if not size_bytes:
        return "Unknown"

    # Define units and their thresholds
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(size_bytes)
    unit_index = 0

    # Find the appropriate unit
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    # Format with 2 decimal places if not bytes
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.2f} {units[unit_index]}"


def get_actions_due_for_execution(db_path: str | Path) -> List[Dict[str, Any]]:
    """Get all pending actions that are due for execution.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        List of dictionaries containing action details.

    Raises:
        sqlite3.Error: If there's an error querying the database.
    """
    db_path = str(db_path)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pending_actions = []

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Query based on the existing table schema
            cursor.execute(
                """
                SELECT id, orphaned_file_id, torrent_id, file_path, current_label,
                       proposed_action, action_details, size_human, scan_id_identified,
                       identified_at, action_due_at, status, scan_id_processed,
                       processed_at, processing_notes, source, file_size
                FROM pending_actions
                WHERE status = ? AND action_due_at <= ?
                ORDER BY action_due_at
            """,
                ("pending", now_str),
            )

            for row in cursor.fetchall():
                # Map from the table's columns to our expected dictionary format
                # Map proposed_action to our ActionType enum
                proposed_action_raw = row[5]  # proposed_action column may hold various historical values
                normalized_action = str(proposed_action_raw).lower().strip() if proposed_action_raw else ""

                # Map historical/legacy strings to canonical actions
                if normalized_action in {"delete", "remove", "purge"}:
                    action_type = ActionType.DELETE
                    proposed_action = "delete"
                elif normalized_action.startswith("label") or normalized_action.startswith("relabel"):
                    # Handles strings like "label as othercat" or "relabel"
                    action_type = ActionType.RELABEL
                    proposed_action = "relabel"
                elif normalized_action in {"manual_review", "manualreview", "review"}:
                    action_type = ActionType.MANUAL_REVIEW
                    proposed_action = "manual_review"
                else:
                    action_type = ActionType.UNKNOWN
                    proposed_action = "unknown"

                # Build parameters for callbacks
                action_params = None
                if action_type == ActionType.RELABEL:
                    param_dict: dict[str, str] = {}
                    if row[2]:  # torrent_id (hash) present in the row
                        param_dict["torrent_id"] = row[2]
                    if row[6]:  # action_details (label)
                        param_dict["label"] = row[6]
                    if param_dict:
                        action_params = json.dumps(param_dict)

                pending_actions.append(
                    {
                        "id": row[0],
                        "orphaned_file_id": row[1],
                        "torrent_hash": row[2],  # torrent_id in the database
                        "file_path": row[3],
                        "current_label": row[4],
                        "action_type": action_type,
                        "proposed_action": proposed_action,
                        "action_params": action_params,
                        "action_details": row[6],
                        "size_human": row[7],
                        "scan_id_identified": row[8],
                        "identified_at": row[9],
                        "action_due_at": row[10],
                        "status": row[11],
                        "scan_id_processed": row[12],
                        "processed_at": row[13],
                        "processing_notes": row[14],
                        "source": row[15],
                        "file_size": row[16],
                    }
                )

        return pending_actions

    except sqlite3.Error as e:
        logger.error(f"SQLite error retrieving pending actions: {e}")
        return []


def update_action_status(
    db_path: str | Path,
    action_id: int,
    new_status: str,
    processing_notes: Optional[str] = None,
    scan_id_processed: Optional[int] = None,
) -> bool:
    """Update the status of a pending action.

    Args:
        db_path: Path to the SQLite database file.
        action_id: ID of the action to update.
        new_status: New status to set ('pending', 'completed', 'cancelled', 'failed')
        processing_notes: Optional notes about the processing (success or error messages)
        scan_id_processed: Optional scan ID when the action was processed.

    Returns:
        True if successful, False otherwise.
    """
    db_path = str(db_path)
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Normalize new_status to a lowercase string so comparisons and DB storage are
    # consistent regardless of whether the caller passes an ActionStatus enum
    # instance or a plain string.
    if isinstance(new_status, ActionStatus):
        new_status = new_status.name.lower()
    else:
        new_status = str(new_status).lower()

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            if new_status in ["completed", "cancelled", "failed"]:
                # For terminal states, set the processed_at timestamp
                if scan_id_processed is None:
                    # Get the latest scan ID if not provided
                    try:
                        cursor.execute("SELECT MAX(id) FROM scans")
                        scan_id_processed = cursor.fetchone()[0]
                    except sqlite3.Error as fetch_err:
                        logger.debug(f"Error fetching latest scan id: {fetch_err}")

                cursor.execute(
                    "UPDATE pending_actions SET status = ?, processed_at = ?, processing_notes = ?, scan_id_processed = ? WHERE id = ?",
                    (
                        new_status,
                        now_str,
                        processing_notes,
                        scan_id_processed,
                        action_id,
                    ),
                )
            else:
                # For non-terminal states, just update the status
                cursor.execute(
                    "UPDATE pending_actions SET status = ? WHERE id = ?",
                    (new_status, action_id),
                )

            conn.commit()
            return cursor.rowcount > 0

    except sqlite3.Error as e:
        logger.error(f"SQLite error updating action status for action {action_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating action status for action {action_id}: {e}")
        return False


def execute_pending_action(
    db_path: str | Path,
    action: Dict[str, Any],
    relabel_callback: Optional[Callable[[str, str, str], bool]] = None,
    delete_callback: Optional[Callable[[str], bool]] = None,
    dry_run: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Execute a pending action.

    Args:
        db_path: Path to the SQLite database file.
        action: Action dictionary containing the action details.
        relabel_callback: Callback function to execute a RELABEL action.
            Function signature: (torrent_id, file_path, label) -> success
        delete_callback: Callback function to execute a DELETE action.
            Function signature: (file_path) -> success
        dry_run: If True, only simulate the action execution.

    Returns:
        Tuple of (success, error_message).
    """
    action_id = action.get("id")
    action_type = action.get("action_type")  # This is our enum
    proposed_action = action.get("proposed_action")  # This is the string value from DB
    file_path = action.get("file_path")
    torrent_hash = action.get("torrent_hash")
    action_details = action.get("action_details")

    if not action_id or not file_path:
        return False, "Missing required action information"

    if dry_run:
        logger.info(f"[DRY RUN] Would execute {proposed_action} action on {file_path}")
        return True, None

    success = False
    error_msg = None

    try:
        # Execute the appropriate action based on the type
        if proposed_action == "delete" or (action_type and action_type == ActionType.DELETE):
            if delete_callback:
                success = delete_callback(file_path)
                if success:
                    logger.info(f"Successfully deleted {file_path}")
                else:
                    error_msg = f"Failed to delete {file_path}"
            else:
                error_msg = "No delete callback provided"

        elif proposed_action == "relabel" or (action_type and action_type == ActionType.RELABEL):
            label = action_details  # The label is stored directly in action_details
            if relabel_callback and torrent_hash and label:
                success = relabel_callback(torrent_hash, file_path, label)
                if success:
                    logger.info(f"Successfully applied label {label} to {torrent_hash} ({file_path})")
                else:
                    error_msg = f"Failed to apply label {label} to {torrent_hash} ({file_path})"
            else:
                error_msg = f"Missing relabel parameters or callback: torrent_hash={torrent_hash}, label={label}, callback={relabel_callback is not None}"

        elif proposed_action == "manual_review" or (action_type and action_type == ActionType.MANUAL_REVIEW):
            # Manual review actions are just marked as completed
            logger.info(f"Marking manual review action {action_id} for {file_path} as completed")
            success = True

        else:
            error_msg = f"Unknown action type: {proposed_action}"

        # Update the action status based on the result
        new_status = "completed" if success else "failed"
        update_action_status(db_path, action_id, new_status, error_msg)

        return success, error_msg

    except Exception as e:
        error_msg = f"Error executing action {action_id}: {str(e)}"
        logger.error(error_msg)
        update_action_status(db_path, action_id, "failed", error_msg)
        return False, error_msg


def execute_pending_actions(
    db_path: str | Path,
    apply_relabel_callback: callable,
    delete_file_callback: callable,
    dry_run: bool = False,
) -> Tuple[int, int, int]:
    """Execute all pending actions that are due.

    This function will:
    1. Get all actions that are due
    2. Execute them based on their type
    3. Update their status accordingly

    Args:
        db_path: Path to the SQLite database file.
        apply_relabel_callback: Callback function for relabeling, takes (file_path, action_params)
        delete_file_callback: Callback function for deletion, takes (file_path)
        dry_run: If True, only simulate action execution

    Returns:
        Tuple of (successful_count, failed_count, skipped_count)

    Raises:
        sqlite3.Error: If there's an error accessing the database.
    """
    db_path = str(db_path)

    # Get due actions
    pending_actions = get_actions_due_for_execution(db_path)

    if not pending_actions:
        logger.info("No pending actions are due for execution.")
        return (0, 0, 0)

    logger.info(f"Found {len(pending_actions)} pending actions that are due for processing.")

    successful_count = 0
    failed_count = 0
    skipped_count = 0

    # Execute each action
    for action in pending_actions:
        action_id = action["id"]
        file_path = action["file_path"]
        action_type = action["action_type"]
        proposed_action = action.get("proposed_action")

        # If action_type could not be resolved earlier, try to infer it from proposed_action
        if action_type == ActionType.UNKNOWN and isinstance(proposed_action, str):
            pa = proposed_action.lower().strip()
            if pa == "delete":
                action_type = ActionType.DELETE
            elif pa == "relabel":
                action_type = ActionType.RELABEL
            elif pa == "manual_review":
                action_type = ActionType.MANUAL_REVIEW
        # Normalize action_type to an enum instance for consistent comparison
        if isinstance(action_type, str):
            try:
                action_type = ActionType[action_type.upper()]
            except KeyError:
                action_type = ActionType.UNKNOWN
        action_params = action["action_params"]

        try:
            # First mark the action as ready
            if not dry_run:
                update_action_status(db_path, action_id, ActionStatus.READY)

            # Execute based on action type
            if action_type == ActionType.DELETE:
                if dry_run:
                    logger.info(f"[DRY RUN] Would delete file: {file_path}")
                else:
                    logger.info(f"Deleting file: {file_path}")
                    success = delete_file_callback(file_path)

                    if success:
                        update_action_status(db_path, action_id, ActionStatus.COMPLETED)
                        successful_count += 1
                    else:
                        update_action_status(
                            db_path,
                            action_id,
                            ActionStatus.PENDING,
                            f"Failed to delete file: {file_path}",
                        )
                        failed_count += 1

            elif action_type == ActionType.RELABEL:
                if dry_run:
                    logger.info(f"[DRY RUN] Would apply label to torrent: {file_path}")
                else:
                    logger.info(f"Applying label to torrent: {file_path}")
                    result = apply_relabel_callback(file_path, action_params)
                    # Callback may return bool or tuple[bool, str|None]
                    if isinstance(result, tuple):
                        success_flag, reason = result
                    else:
                        success_flag, reason = bool(result), None

                    if success_flag:
                        update_action_status(db_path, action_id, ActionStatus.COMPLETED)
                        successful_count += 1
                    else:
                        if reason == "not_found":
                            # Torrent no longer exists; cancel further retries
                            update_action_status(
                                db_path,
                                action_id,
                                ActionStatus.CANCELLED,
                                "Torrent no longer exists in Deluge",
                            )
                        else:
                            # Keep as pending for other failures so it can be retried
                            update_action_status(
                                db_path,
                                action_id,
                                ActionStatus.PENDING,
                                f"Failed to apply label to torrent: {file_path}",
                            )
                        failed_count += 1

            elif action_type == ActionType.MANUAL_REVIEW:
                # Manual review actions just get logged, they can't be automated
                logger.info(f"File requires manual review: {file_path}")
                skipped_count += 1

            else:
                # Unknown action type
                logger.warning(f"Unknown action type {action_type} for file {file_path}")
                if not dry_run:
                    update_action_status(
                        db_path,
                        action_id,
                        ActionStatus.CANCELLED,
                        f"Unknown action type: {action_type}",
                    )
                skipped_count += 1

        except Exception as e:
            logger.error(f"Error executing action {action_id} for {file_path}: {e}")
            if not dry_run:
                update_action_status(db_path, action_id, ActionStatus.PENDING, f"Error: {str(e)}")
            failed_count += 1

    # Log summary
    if dry_run:
        logger.info(f"[DRY RUN] Would have processed {len(pending_actions)} pending actions.")
    else:
        logger.info(f"Successfully processed {len(pending_actions)} pending actions.")
        logger.info(f"Successful: {successful_count}, Failed: {failed_count}, Skipped: {skipped_count}")

    return (successful_count, failed_count, skipped_count)
