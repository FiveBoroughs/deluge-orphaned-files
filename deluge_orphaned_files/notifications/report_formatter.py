"""Generate human-readable reports from scan data stored in SQLite."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from prettytable import PrettyTable

from .. import __version__

__all__ = ["format_scan_results"]


def format_scan_results(db_path: Path, *, scan_id: Optional[int] = None, limit: int = 1) -> str:  # noqa: C901 – long but straightforward
    """Return a formatted text table of the latest or specific scan.

    Args:
        db_path: Path to the SQLite database.
        scan_id: If given, show that specific scan; otherwise shows the most recent scan.
        limit: How many recent scans to look back if scan_id is None (currently only the first is used).

    Returns:
        str: Formatted text report containing scan information and result tables.
    """
    if not db_path.exists():
        return f"SQLite database not found at {db_path}"

    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()

            if scan_id is None:
                cursor.execute(
                    "SELECT id FROM scan_results ORDER BY created_at DESC LIMIT ?;",
                    (limit,),
                )
                rows = cursor.fetchall()
                if not rows:
                    return "No scan results found in database."
                scan_id = rows[0][0]

            cursor.execute(
                "SELECT host, base_path, scan_start, scan_end FROM scan_results WHERE id = ?;",
                (scan_id,),
            )
            scan_info = cursor.fetchone()
            if not scan_info:
                return f"No scan found with ID {scan_id}."

            host, base_path, scan_start, scan_end = scan_info

            cursor.execute(
                """
                SELECT path, label, size_human, source
                FROM orphaned_files
                WHERE id IN (
                    SELECT file_id FROM file_scan_history WHERE scan_id = ?
                )
                AND include_in_report = 1
                ORDER BY
                    CASE WHEN source = 'local_torrent_folder' THEN 1
                         WHEN source = 'torrents' THEN 2
                         WHEN source = 'media' THEN 3
                         ELSE 4 END,
                    CASE WHEN label LIKE 'other%' THEN 'z' || label ELSE label END,
                    size DESC;
                """,
                (scan_id,),
            )
            files = cursor.fetchall()

    except sqlite3.Error as exc:
        logger.error("SQLite error while formatting scan results: {}", exc)
        return f"Error retrieving scan results: {exc}"

    if not files:
        return f"No files to report for scan ID {scan_id}."

    table = PrettyTable(field_names=["Path", "Label", "Size", "Source"])
    table.align = "l"
    # Optionally limit path column width for readability
    try:
        table.max_width["Path"] = 80  # adjust as needed
    except Exception:  # noqa: BLE001 – ignore if prettytable version lacks max_width
        pass

    for path, label, size_human, source in files:
        table.add_row([path, label or "", size_human, source])

    header_lines = [
        f"Scan ID: {scan_id}",
        f"Version: {__version__}",
        f"Host: {host}",
        f"Base Path: {base_path}",
        f"Scan Start: {scan_start}",
        f"Scan End: {scan_end}",
        "",
    ]

    result = "\n".join(header_lines) + table.get_string()

    # Add pending actions section
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()

            # Check if the pending_actions table exists
            cursor.execute(
                """SELECT name FROM sqlite_master
                   WHERE type='table' AND name='pending_actions';"""
            )
            if not cursor.fetchone():
                return result  # Table doesn't exist yet, just return the main report

            # Query pending actions
            cursor.execute(
                """
                SELECT file_path, current_label, size_human, proposed_action,
                       action_details, action_due_at
                FROM pending_actions
                WHERE status = 'pending'
                ORDER BY action_due_at ASC;
                """
            )
            pending_actions = cursor.fetchall()

            if pending_actions:
                # Format pending actions table
                pending_table = PrettyTable(field_names=["Path", "Current Label", "Size", "Source", "Resolution", "When"])
                pending_table.align = "l"
                try:
                    pending_table.max_width["Path"] = 80
                except Exception:  # noqa: BLE001
                    pass

                now = datetime.now()

                # Track paths that have already been added to avoid duplicate rows in the report
                seen_paths: set[str] = set()

                for path, current_label, size_human, proposed_action, action_details, action_due_at in pending_actions:
                    # Skip duplicate entries by absolute path
                    if path in seen_paths:
                        continue
                    seen_paths.add(path)
                    # Format the due date as a relative time
                    try:
                        # Try to parse using multiple formats to handle different date formats
                        due_date = None

                        # Try different date formats
                        date_formats = [
                            "%Y-%m-%d %H:%M:%S",  # Standard format: 2025-06-20 22:20:21
                            "%Y-%m-%dT%H:%M:%S",  # ISO without microseconds: 2025-06-20T13:35:57
                            "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds: 2025-06-20T13:35:57.086643
                            "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO with timezone: 2025-06-20T13:35:57.086643+0000
                        ]

                        # Remove timezone info if present (simplest approach)
                        clean_date_str = action_due_at
                        if "+" in clean_date_str:
                            clean_date_str = clean_date_str.split("+")[0]

                        # Try each format until one works
                        for date_format in date_formats:
                            try:
                                due_date = datetime.strptime(clean_date_str, date_format)
                                break  # Exit the loop if parsing succeeds
                            except ValueError:
                                continue  # Try next format

                        # If no format worked, raise ValueError
                        if due_date is None:
                            raise ValueError(f"No format matched for date string: {action_due_at}")

                        days_until = (due_date - now).days
                        if days_until < 0:
                            when = "Overdue"
                        elif days_until == 0:
                            when = "Today"
                        elif days_until == 1:
                            when = "Tomorrow"
                        else:
                            when = f"{days_until} days"
                    except ValueError as e:
                        logger.warning(f"Failed to parse due date '{action_due_at}': {e}")
                        when = action_due_at  # Fallback if date parsing fails

                    # Determine the resolution based on action type
                    resolution = "Unknown"
                    # Determine human-friendly description of the proposed action
                    pa_lower = (proposed_action or "").lower()
                    if pa_lower.startswith("relabel"):
                        label_name = action_details or "othercat"
                        resolution = f"Label as {label_name}"
                    elif pa_lower.startswith("delete"):
                        resolution = "Deletion"

                    # Add row to pending table
                    pending_table.add_row([path, current_label or "", size_human, "torrents", resolution, when])

                # Add pending actions section to report
                result += "\n\nPending Re-Labeling Actions:\n" + pending_table.get_string()
    except sqlite3.Error as exc:
        logger.error("SQLite error while formatting pending actions: {}", exc)
        # Continue with the main report even if there's an error with pending actions

    return result
