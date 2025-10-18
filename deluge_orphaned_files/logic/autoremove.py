"""Auto-remove labeling logic with cross-seed coordination.

This module provides functionality to automatically apply labels to torrents
based on scan results, with intelligent handling of cross-seed relationships
to prevent re-downloading scenarios.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, List, Dict, Any
from loguru import logger

if TYPE_CHECKING:  # pragma: no cover
    from deluge_client import DelugeRPCClient

from ..settings import config
from .pending_actions import register_pending_action, ActionType

__all__ = ["process_autoremove_labeling"]


def process_autoremove_labeling(client: "DelugeRPCClient", scan_id: int, db_path: str | Path, dry_run: bool = True, target_label_prefix: str = None) -> Dict[str, Any]:
    """Process auto-remove labeling with cross-seed coordination.

    This function identifies torrents that should be labeled with the auto-remove
    label based on scan results, and applies the labels to entire content groups
    to prevent cross-seed re-downloading scenarios.

    Args:
        client: Connected Deluge RPC client.
        scan_id: ID of the scan to process.
        db_path: Path to the SQLite database.
        dry_run: If True, only log what would be done without making changes.
        target_label_prefix: Label to apply (defaults to config.deluge_autoremove_label).

    Returns:
        Dictionary containing statistics about the labeling operation.

    Raises:
        sqlite3.Error: If there's an error accessing the database.
        Exception: If there's an error communicating with Deluge.
    """
    db_path = Path(db_path)
    if target_label_prefix is None:
        target_label_prefix = config.deluge_autoremove_label

    stats = {"torrents_processed": 0, "labels_applied": 0, "content_groups_found": 0, "cross_seed_groups_coordinated": 0, "errors": 0, "dry_run": dry_run}

    if not client or not client.connected:
        logger.error("Deluge client is not connected")
        stats["errors"] += 1
        return stats

    try:
        # Step 1: Get candidate files for labeling from the database
        candidate_files = _get_autoremove_candidates(db_path, scan_id, target_label_prefix)

        if not candidate_files:
            logger.info("No torrents eligible for auto-remove labeling")
            return stats

        # Step 2: Get all active torrents from Deluge for cross-seed detection
        all_deluge_torrents = _get_all_deluge_torrents(client)

        # Step 3: Group candidates by content paths (cross-seed detection)
        content_groups = _group_torrents_by_content(candidate_files, all_deluge_torrents)
        stats["content_groups_found"] = len(content_groups)

        logger.info(
            "Found {} content groups containing {} total torrents eligible for '{}' labeling",
            len(content_groups),
            sum(len(group["torrents"]) for group in content_groups.values()),
            target_label_prefix,
        )

        # Step 4: Process each content group (apply labels to entire groups)
        for content_path, group_info in content_groups.items():
            group_torrents = group_info["torrents"]
            has_cross_seeds = group_info["has_cross_seeds"]

            if has_cross_seeds:
                stats["cross_seed_groups_coordinated"] += 1
                logger.info("Processing cross-seed group for content: {} ({} torrents)", content_path, len(group_torrents))

            # Process all torrents in this content group
            for torrent in group_torrents:
                success = _process_single_torrent_for_labeling(
                    db_path=db_path, torrent=torrent, target_label_prefix=target_label_prefix, scan_id=scan_id, dry_run=dry_run, is_cross_seed_group=has_cross_seeds
                )

                stats["torrents_processed"] += 1
                if success:
                    stats["labels_applied"] += 1
                else:
                    stats["errors"] += 1

    except Exception as exc:
        logger.error("Error during auto-remove labeling process: {}", exc)
        stats["errors"] += 1

    # Log summary
    if dry_run:
        logger.info(
            "Auto-remove labeling dry-run completed: {} torrents processed, {} content groups, {} cross-seed groups",
            stats["torrents_processed"],
            stats["content_groups_found"],
            stats["cross_seed_groups_coordinated"],
        )
    else:
        logger.info(
            "Auto-remove labeling completed: {}/{} labels applied across {} content groups ({} cross-seed groups)",
            stats["labels_applied"],
            stats["torrents_processed"],
            stats["content_groups_found"],
            stats["cross_seed_groups_coordinated"],
        )

    return stats


def _get_autoremove_candidates(db_path: Path, scan_id: int, target_label_prefix: str) -> List[Dict[str, Any]]:
    """Get candidate files for auto-remove labeling from the database.

    Args:
        db_path: Path to the SQLite database.
        scan_id: ID of the scan to process.
        target_label_prefix: Label prefix to exclude (files already labeled).

    Returns:
        List of candidate files with their metadata.
    """
    candidates = []

    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()

            # Query for files that are candidates for labeling
            # Only include files from torrents source that don't already have the target label
            cursor.execute(
                """
                SELECT DISTINCT
                    of.id as file_id,
                    of.path,
                    of.torrent_id,
                    of.size,
                    of.size_human,
                    of.label as current_label
                FROM orphaned_files of
                JOIN file_scan_history fsh ON of.id = fsh.file_id
                WHERE fsh.scan_id = ?
                    AND of.source = 'torrents'
                    AND of.status = 'active'
                    AND of.torrent_id IS NOT NULL
                    AND (
                        of.label IS NULL
                        OR NOT INSTR(LOWER(of.label), LOWER(?)) > 0
                    )
                ORDER BY of.path
                """,
                (scan_id, target_label_prefix),
            )

            for row in cursor.fetchall():
                candidates.append({"file_id": row[0], "file_path": row[1], "torrent_id": row[2], "file_size": row[3], "size_human": row[4], "current_label": row[5]})

        logger.debug("Found {} candidate files for auto-remove labeling", len(candidates))

    except sqlite3.Error as exc:
        logger.error("Database error getting auto-remove candidates: {}", exc)

    return candidates


def _get_all_deluge_torrents(client: "DelugeRPCClient") -> Dict[str, Dict[str, Any]]:
    """Get all active torrents from Deluge with their file information.

    Args:
        client: Connected Deluge RPC client.

    Returns:
        Dictionary mapping torrent_id -> torrent info (including files and labels).
    """
    all_torrents = {}

    try:
        # Get list of all torrents
        torrent_ids = client.core.get_torrents_status({}, [])

        for torrent_id in torrent_ids:
            try:
                # Get detailed info for each torrent
                torrent_info = client.core.get_torrent_status(torrent_id, ["name", "files", "label", "state"])

                if torrent_info and torrent_info.get("files"):
                    all_torrents[torrent_id] = {"name": torrent_info["name"], "label": torrent_info.get("label", ""), "state": torrent_info.get("state", ""), "files": torrent_info["files"]}

            except Exception as exc:
                logger.debug("Error getting info for torrent {}: {}", torrent_id, exc)
                continue

        logger.debug("Retrieved information for {} torrents from Deluge", len(all_torrents))

    except Exception as exc:
        logger.error("Error getting torrents from Deluge: {}", exc)

    return all_torrents


def _group_torrents_by_content(candidate_files: List[Dict[str, Any]], all_deluge_torrents: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group torrents by their content paths to detect cross-seed relationships.

    Args:
        candidate_files: List of candidate files from database.
        all_deluge_torrents: All torrents from Deluge with file info.

    Returns:
        Dictionary mapping content_path -> group info with torrents and cross-seed flag.
    """
    # Create mapping of file paths to torrents that contain them
    content_to_torrents = defaultdict(list)

    # First, add all candidate files to the grouping
    for candidate in candidate_files:
        file_path = candidate["file_path"]
        torrent_id = candidate["torrent_id"]

        # Normalize the file path (remove leading directories that might differ)
        normalized_path = _normalize_content_path(file_path)

        content_to_torrents[normalized_path].append(
            {
                "torrent_id": torrent_id,
                "file_path": file_path,
                "file_id": candidate["file_id"],
                "current_label": candidate["current_label"],
                "size_human": candidate["size_human"],
                "file_size": candidate["file_size"],
                "is_candidate": True,
            }
        )

    # Then, check all other Deluge torrents for matching content
    candidate_torrent_ids = {c["torrent_id"] for c in candidate_files}

    for torrent_id, torrent_info in all_deluge_torrents.items():
        if torrent_id in candidate_torrent_ids:
            continue  # Already processed as candidate

        # Check if this torrent has files matching any of our content groups
        for torrent_file in torrent_info.get("files", []):
            file_path = torrent_file.get("path", "")
            if not file_path:
                continue

            normalized_path = _normalize_content_path(file_path)

            # If this file matches a content group, add this torrent to the group
            if normalized_path in content_to_torrents:
                content_to_torrents[normalized_path].append(
                    {
                        "torrent_id": torrent_id,
                        "file_path": file_path,
                        "file_id": None,  # Not in our candidate database
                        "current_label": torrent_info.get("label", ""),
                        "size_human": "unknown",
                        "file_size": None,
                        "is_candidate": False,
                    }
                )
                break  # Only add torrent once per content group

    # Build final content groups with cross-seed detection
    content_groups = {}

    for content_path, torrents in content_to_torrents.items():
        # Remove duplicates (same torrent_id)
        unique_torrents = {}
        for torrent in torrents:
            torrent_id = torrent["torrent_id"]
            if torrent_id not in unique_torrents:
                unique_torrents[torrent_id] = torrent

        torrents_list = list(unique_torrents.values())
        has_cross_seeds = len(torrents_list) > 1

        # Detect cross-seed patterns in labels
        labels = [t["current_label"] for t in torrents_list if t["current_label"]]
        has_cross_seed_labels = any(".cross-seed" in label for label in labels)

        content_groups[content_path] = {"torrents": torrents_list, "has_cross_seeds": has_cross_seeds or has_cross_seed_labels}

    return content_groups


def _normalize_content_path(file_path: str) -> str:
    """Normalize a file path for content matching.

    This removes variable parts like quality indicators, release groups, etc.
    to help identify the same content across different releases.

    Args:
        file_path: Original file path.

    Returns:
        Normalized path for content matching.
    """
    # For now, use the base filename without extension as the key
    # This could be enhanced with more sophisticated normalization
    path = Path(file_path)

    # Get the directory structure + base filename (without extension)
    if path.parent != Path("."):
        # Include parent directory for better matching
        return f"{path.parent.name}/{path.stem}"
    else:
        return path.stem


def _process_single_torrent_for_labeling(db_path: Path, torrent: Dict[str, Any], target_label_prefix: str, scan_id: int, dry_run: bool, is_cross_seed_group: bool) -> bool:
    """Process a single torrent for auto-remove labeling.

    Args:
        db_path: Path to the SQLite database.
        torrent: Torrent information dictionary.
        target_label_prefix: Label to apply.
        scan_id: ID of the originating scan.
        dry_run: If True, only log what would be done.
        is_cross_seed_group: True if this torrent is part of a cross-seed group.

    Returns:
        True if successful, False otherwise.
    """
    torrent_id = torrent["torrent_id"]
    file_path = torrent["file_path"]
    current_label = torrent["current_label"]
    file_id = torrent["file_id"]

    # Skip if torrent already has the target label
    if current_label and current_label.lower().startswith(target_label_prefix.lower()):
        logger.debug(
            "Torrent {} already has label '{}' starting with '{}'. Skipping.",
            torrent_id[:8],
            current_label,
            target_label_prefix,
        )
        return True

    try:
        if dry_run:
            if is_cross_seed_group:
                logger.info("DRY RUN: Would label torrent {} with '{}' (cross-seed group) (file: {})", torrent_id[:8], target_label_prefix, file_path)
            else:
                logger.info("DRY RUN: Would label torrent {} with '{}' (file: {})", torrent_id[:8], target_label_prefix, file_path)
            return True

        # Only record pending actions for candidates (files in our database)
        if torrent["is_candidate"] and file_id:
            # Prepare action params as JSON
            action_params = json.dumps({"torrent_id": torrent_id, "label": target_label_prefix, "current_label": current_label})

            # Register the pending action
            register_pending_action(
                db_path=db_path,
                file_path=file_path,
                action_type=ActionType.RELABEL,
                waiting_period_days=config.relabel_action_delay_days,
                action_params=action_params,
                scan_id=str(scan_id),
                orphaned_file_id=file_id,
                torrent_hash=torrent_id,
                file_size=torrent.get("file_size"),
                source="torrents",
                size_human=torrent.get("size_human"),
            )

            if is_cross_seed_group:
                logger.info(
                    "Recorded pending action to apply label '{}' to torrent {} (cross-seed group) (file: {}). Previous label: '{}'",
                    target_label_prefix,
                    torrent_id[:8],
                    file_path,
                    current_label or "none",
                )
            else:
                logger.info("Recorded pending action to apply label '{}' to torrent {} (file: {}). Previous label: '{}'", target_label_prefix, torrent_id[:8], file_path, current_label or "none")
        else:
            # For non-candidates (related torrents), log what we would do
            # but don't record pending actions since they're not in our orphan database
            if is_cross_seed_group:
                logger.info("Would coordinate cross-seed labeling for torrent {} with '{}' (file: {}). Previous label: '{}'", torrent_id[:8], target_label_prefix, file_path, current_label or "none")

        return True

    except Exception as exc:
        logger.error("Error processing torrent {} (file: {}): {}", torrent_id[:8] if torrent_id else "unknown", file_path, exc)
        return False
