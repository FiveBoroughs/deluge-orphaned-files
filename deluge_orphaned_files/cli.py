from __future__ import annotations

import os
import sys
import json
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from deluge_client import DelugeRPCClient

import argparse
import sqlite3
from loguru import logger

from typing import List, Dict, Any, Tuple

from .scanning.file_scanner import (
    load_hash_cache,
    save_hash_cache,
)
from .logic.orphan_finder import InodePreflightError, compute_orphans
from .logic.autoremove import process_autoremove_labeling
from .logic.stuck_repair import run_stuck_repair
from .logic.pending_actions import (
    init_pending_actions_schema,
    execute_pending_actions as execute_all_pending_actions,
)
from .logic.retention import process_deletions
from .notifications.report_formatter import (
    format_scan_results as _format_scan_results,
)
from .notifications.emailer import send_scan_report
from .notifications.telegram_notifier import send_scan_report as send_telegram_report
from . import __version__

# ---------------------------------------------------------------------------
# Configure Loguru – log to console and to config/logs/deluge_orphaned_files.log
# ---------------------------------------------------------------------------
# Early --list-env handling (dump AppConfig values)
# ---------------------------------------------------------------------------
if "--list-env" in sys.argv:
    try:
        from .settings import config as _cfg  # Local import to build AppConfig

        masked: dict[str, str] = {}
        for key, value in _cfg.model_dump(mode="python", exclude_defaults=False).items():
            if value is None:
                display = "NOT SET"
            elif any(s in key.lower() for s in ("password", "token", "secret")):
                display = "***"
            else:
                display = str(value)
            masked[key] = display
        print("\nLoaded configuration (masked):")
        print(json.dumps(masked, indent=2))
    except Exception as exc:  # noqa: BLE001
        print("Failed to load AppConfig – validation errors below:\n")
        print(exc)
    sys.exit(0)

# ---------------------------------------------------------------------------
# Misc helper
# ---------------------------------------------------------------------------
# Allow override via environment variable; fallback to ./config/logs inside the
# project (works both in Docker and local checkout)
log_dir_env = os.getenv("APP_LOG_DIR")
if log_dir_env:
    log_dir = Path(log_dir_env)
else:
    # project_root = Path(__file__).resolve().parents[1]
    log_dir = Path(__file__).resolve().parents[1] / "config" / "logs"

# Ensure directory exists
log_dir.mkdir(parents=True, exist_ok=True)

log_file_path = log_dir / "deluge_orphaned_files.log"

logger.remove()  # Remove default stderr logger (Loguru default)
logger.add(
    sys.stdout,
    level="INFO",
    format=("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | " "<level>{level: <8}</level> | " "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - " "<level>{message}</level>"),
)
logger.add(
    log_file_path,
    rotation="1 day",
    retention="30 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
)

logger.info("Logging initialized. Log file: {}", log_file_path)


# ---------------------------------------------------------------------------
# Misc helper
# ---------------------------------------------------------------------------

# Ensure users see path of log file at startup
# (Already logged above; removed duplicate)


def print_version_info() -> None:
    """Log current application version to the standard logger.

    Outputs version information to the logger at INFO level.
    """
    logger.info("Deluge Orphaned Files v{}", __version__)


def print_loaded_env_vars(cfg: "AppConfig") -> None:  # noqa: F821 – forward ref for type checkers
    """Pretty-print the environment variables that populated *cfg*.

    Sensitive values like passwords or tokens are masked. Fields that are ``None``
    are reported as *NOT SET* so users can quickly see what is missing.
    """
    masked: dict[str, str] = {}
    for key, value in cfg.model_dump(mode="python", exclude_defaults=False).items():  # type: ignore[arg-type]
        if value is None:
            display = "NOT SET"
        elif any(s in key.lower() for s in ("password", "token", "secret")):
            display = "***"
        else:
            display = str(value)
        masked[key] = display

    print("\nLoaded configuration (masked):")
    print(json.dumps(masked, indent=2))


# ---------------------------------------------------------------------------
# Global configuration (shared across all modules)
# ---------------------------------------------------------------------------

# NOTE: The canonical AppConfig and the *singleton* "config" object now live
# in :pymod:`deluge_orphaned_files.settings`.  Importing it here ensures we
# share the exact same instance everywhere (including sub-modules executed
# earlier via their own imports).

from .settings import AppConfig, config  # noqa: E402  – import after logger setup for readable init logs


def init_sqlite_cache(db_path: Path) -> None:
    """Initialize the SQLite cache database.

    Creates the database file if it doesn't exist and creates the necessary tables if they don't exist.

    Args:
        db_path: Path to the SQLite database file to initialize.

    Raises:
        sqlite3.Error: If there's an error during database operations.
        Exception: For any other unexpected errors.

    Tables created:
        - file_hashes: For caching file hashes to improve performance
        - scan_results: For storing metadata about each scan
        - orphaned_files: For tracking files that are orphaned
        - file_scan_history: For tracking file presence in each scan
        - Views: Various views for facilitating reports and file management
    """
    db_exists = os.path.exists(db_path)
    if not db_exists:
        logger.trace(f"SQLite database will be created at {db_path}")

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            logger.trace("Ensuring 'file_hashes' table exists in the SQLite cache.")
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS file_hashes (
                file_hash TEXT NOT NULL,
                folder_path TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                mtime REAL NOT NULL,
                file_size INTEGER NOT NULL,
                PRIMARY KEY (folder_path, relative_path)
            );
            """
            )

            cursor.execute(
                """
            CREATE INDEX IF NOT EXISTS idx_file_hashes_folder_path ON file_hashes (folder_path);
            """
            )

            logger.trace("Ensuring 'scan_results' table exists.")
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host TEXT NOT NULL,
                base_path TEXT NOT NULL,
                scan_start TEXT NOT NULL,
                scan_end TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            )

            logger.trace("Ensuring 'orphaned_files' table exists.")
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS orphaned_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_hash TEXT NOT NULL,
                path TEXT NOT NULL,
                source TEXT NOT NULL,  -- 'local_torrent_folder', 'torrents', or 'media'
                torrent_id TEXT,       -- Deluge torrent ID, only for source 'torrents'
                label TEXT,            -- NULL for files not from torrents
                size INTEGER NOT NULL,
                size_human TEXT NOT NULL,
                first_seen_at TIMESTAMP NOT NULL,
                last_seen_at TIMESTAMP NOT NULL,
                consecutive_scans INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'marked_for_deletion', 'deleted' (removed by us), 'vanished' (gone by other means)
                deletion_date TIMESTAMP,
                include_in_report BOOLEAN NOT NULL DEFAULT 1
            );
            """
            )

            logger.trace("Ensuring 'file_scan_history' table exists.")
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS file_scan_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL,
                file_id INTEGER NOT NULL,
                source TEXT NOT NULL,  -- 'local_torrent_folder', 'torrents', or 'media'
                FOREIGN KEY (scan_id) REFERENCES scan_results(id),
                FOREIGN KEY (file_id) REFERENCES orphaned_files(id)
            );
            """
            )

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_fsh_scan_id ON file_scan_history (scan_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_fsh_file_id ON file_scan_history (file_id);")

            # Initialize pending actions schema using the dedicated module
            init_pending_actions_schema(db_path)

            logger.trace("Ensuring 'vw_latest_scan_report' view exists.")
            cursor.execute(
                """
            DROP VIEW IF EXISTS vw_latest_scan_report;
            -- Drop view if it exists to ensure it's updated
            """
            )
            cursor.execute(
                """
            CREATE VIEW vw_latest_scan_report AS
            SELECT
                sr.id AS scan_id,
                sr.host AS scan_host,
                sr.base_path AS scan_base_path,
                sr.scan_start,
                sr.scan_end,
                sr.created_at AS scan_created_at,
                of.id AS file_id,
                of.path AS file_path,
                of.label AS file_label,
                of.size AS file_size,
                of.size_human AS file_size_human,
                fsh.source AS scan_context_file_source,
                -- Source specific to this file in this scan context
                of.status AS file_status,
                of.consecutive_scans AS file_consecutive_scans, -- Added this line
                of.file_hash
            FROM scan_results sr
            JOIN file_scan_history fsh ON sr.id = fsh.scan_id
            JOIN orphaned_files of ON fsh.file_id = of.id
            WHERE sr.id = (SELECT id FROM scan_results ORDER BY created_at DESC LIMIT 1);
            """
            )

        logger.info("SQLite cache schema (tables and main view) initialized/verified successfully.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error during schema initialization (tables/main view) in {db_path}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during schema initialization (tables/main view) in {db_path}: {e}")
        raise

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.debug("Ensuring 'view_files_eligible_for_deletion' view exists.")

            cursor.execute(
                """
            DROP VIEW IF EXISTS view_files_eligible_for_deletion;
            """
            )

            # SQLite doesn't allow parameters in view definitions, so we need to use string formatting
            # This is safe because we're using configuration values, not user input
            create_view_sql = f"""
            CREATE VIEW view_files_eligible_for_deletion AS
            SELECT
                of.id AS file_id,
                of.path AS file_path,
                of.size_human AS file_size,
                of.first_seen_at,
                of.last_seen_at,
                of.consecutive_scans,
                julianday(of.last_seen_at) - julianday(of.first_seen_at) AS days_seen_difference
            FROM orphaned_files of
            WHERE
                of.source = 'local_torrent_folder'
                AND of.status = 'active'
                AND of.consecutive_scans >= {config.deletion_consecutive_scans_threshold}
                AND (julianday(of.last_seen_at) - julianday(of.first_seen_at)) >= {config.deletion_days_threshold};
            """
            cursor.execute(create_view_sql)
        logger.trace("'view_files_eligible_for_deletion' view created/verified.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error creating 'view_files_eligible_for_deletion' in {db_path}: {e}")

    except Exception as e:
        logger.error(f"Unexpected error creating 'view_files_eligible_for_deletion' in {db_path}: {e}")

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.debug("Ensuring 'vw_autoremove_candidates_latest_scan' view exists.")
            logger.debug(
                "Auto-remove candidates require >= %s consecutive scans before labeling.",
                config.autoremove_min_consecutive_scans,
            )
            cursor.execute("DROP VIEW IF EXISTS vw_autoremove_candidates_latest_scan;")

            # SQLite doesn't allow parameters in view definitions, so we need to use string formatting
            # This is safe because we're using configuration values, not user input
            target_label_prefix_for_sql = config.deluge_autoremove_label.lower()
            create_autoremove_view_sql = f"""
            CREATE VIEW vw_autoremove_candidates_latest_scan AS
            SELECT
                of.id AS file_id,
                of.path AS file_path,
                of.label AS current_label,
                of.torrent_id,
                of.size_human
            FROM orphaned_files of
            JOIN file_scan_history fsh ON of.id = fsh.file_id
            JOIN scan_results sr ON fsh.scan_id = sr.id
            WHERE
                sr.id = (SELECT id FROM scan_results ORDER BY created_at DESC LIMIT 1)
                AND of.source = 'torrents'
                AND of.status = 'active'
                AND of.consecutive_scans >= {config.autoremove_min_consecutive_scans}
                AND of.torrent_id IS NOT NULL
                AND (
                    of.label IS NULL
                    OR NOT INSTR(LOWER(of.label), '{target_label_prefix_for_sql}') > 0
                );
            """
            cursor.execute(create_autoremove_view_sql)
        logger.trace("'vw_autoremove_candidates_latest_scan' view created/verified.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error creating 'vw_autoremove_candidates_latest_scan' in {db_path}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error creating 'vw_autoremove_candidates_latest_scan' in {db_path}: {e}")

    _convert_legacy_consecutive_scans(db_path)

    return True


def _convert_legacy_consecutive_scans(db_path: Path) -> None:
    """One-time conversion of ``consecutive_scans`` from scan counts to sighting days.

    Before the distinct-day counting change, every scan incremented the counter,
    so burst days (scheduler startup runs on container recreates) inflated stored
    values. Recompute each live row's counter from recorded sightings in
    ``file_scan_history``: distinct scan dates in the trailing run of scans (for
    that row's source) containing the file, stopping at the first absence. The
    result is capped at the stored value, so conversion can only lower a counter;
    rows without recorded sightings drop to 1. Gated by ``PRAGMA user_version``
    so it runs exactly once per database (fresh databases are stamped immediately).
    """
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            if cursor.execute("PRAGMA user_version").fetchone()[0] >= 1:
                return

            scans = cursor.execute("SELECT id, scan_start FROM scan_results ORDER BY scan_start, id").fetchall()
            scan_date = {scan_id: scan_start[:10] for scan_id, scan_start in scans}
            sightings: Dict[int, set] = {}
            scans_by_source: Dict[str, set] = {}
            for scan_id, file_id, source in cursor.execute("SELECT scan_id, file_id, source FROM file_scan_history"):
                sightings.setdefault(file_id, set()).add(scan_id)
                scans_by_source.setdefault(source, set()).add(scan_id)
            # Source scans in global chronological order, so "absence" is only
            # judged against scans that actually recorded that source — except
            # 'local_torrent_folder', which every scan records (even with
            # --skip-media-check) and whose counters the old code reset on any
            # absence, including entirely empty orphan scans that leave no
            # history rows at all. For that source every scan counts.
            ordered_source_scans = {source: [scan_id for scan_id, _ in scans if scan_id in ids] for source, ids in scans_by_source.items()}
            ordered_source_scans["local_torrent_folder"] = [scan_id for scan_id, _ in scans]

            converted = 0
            rows = cursor.execute("SELECT id, source, consecutive_scans FROM orphaned_files WHERE status IN ('active', 'marked_for_deletion') AND consecutive_scans > 0").fetchall()
            for file_id, source, stored in rows:
                seen_scans = sightings.get(file_id, set())
                days: set = set()
                run_started = False
                for scan_id in reversed(ordered_source_scans.get(source, [])):
                    if scan_id in seen_scans:
                        days.add(scan_date[scan_id])
                        run_started = True
                    elif run_started:
                        break
                new_value = min(stored, len(days)) if days else min(stored, 1)
                if new_value != stored:
                    cursor.execute("UPDATE orphaned_files SET consecutive_scans = ? WHERE id = ?", (new_value, file_id))
                    converted += 1

            cursor.execute("PRAGMA user_version = 1")
            conn.commit()
            if converted:
                logger.info("Converted legacy consecutive_scans to sighting-day counts for {} file rows in {}", converted, db_path)
    except sqlite3.Error as e:
        logger.error(f"SQLite error converting legacy consecutive_scans in {db_path}: {e}")


def find_orphaned_files(skip_media_check: bool = False, use_sqlite: bool = False, no_progress: bool = False, use_inodes: bool = False) -> int:
    """Find orphaned files by comparing Deluge files with local torrent and media folders.

    Scans local folders and Deluge client to identify orphaned files in three categories:
    - Files in local torrent folder but not in Deluge (orphaned torrents)
    - Files only in Deluge but not in the media folder
    - Files only in the media folder but not in Deluge

    Args:
        skip_media_check: Whether to skip checking the media folder.
        use_sqlite: Whether to use SQLite for caching and save results only to the database.
        no_progress: Whether to disable progress bars for this scan.

    Returns:
        The scan ID if saved to database, otherwise 0.

    Raises:
        RuntimeError: If there's an error connecting to Deluge.
        OSError: If there's an error accessing local files.
    """
    scan_start_time = datetime.now()
    scan_id = 0

    # Modularised orphan calculation
    orphaned_torrent_files, only_in_torrents, only_in_media = compute_orphans(
        config=config,
        skip_media_check=skip_media_check,
        use_sqlite=use_sqlite,
        no_progress=no_progress,
        use_inodes=use_inodes,
    )

    logger.info(
        "Orphan detection complete → {} torrent-folder orphans, {} only-in-torrent, {} only-in-media",
        len(orphaned_torrent_files),
        len(only_in_torrents),
        len(only_in_media),
    )

    # Save results regardless of whether orphans were found
    logger.info(
        "\nScan complete. Found {} orphans, {} files only in torrents, {} files only in media",
        len(orphaned_torrent_files),
        len(only_in_torrents),
        len(only_in_media),
    )

    if use_sqlite:
        scan_id_from_db, _ = save_scan_results_to_db(orphaned_torrent_files, only_in_torrents, only_in_media, scan_start_time)
        return scan_id_from_db
    else:
        save_scan_results(orphaned_torrent_files, only_in_torrents, only_in_media, scan_start_time)
        # scan_id remains 0 if not use_sqlite (as initialized at the start of the function)
        return scan_id


def save_scan_results_to_db(
    orphaned_torrent_files: List[Dict[str, Any]],
    only_in_torrents: List[Dict[str, Any]],
    only_in_media: List[Dict[str, Any]],
    scan_start_time: datetime,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Save scan results to the SQLite database.

    Args:
        orphaned_torrent_files: List of files in local torrent folder but not in Deluge
        only_in_torrents: List of files only in torrents (not in media)
        only_in_media: List of files only in media (not in torrents)
        scan_start_time: When the scan started

    Returns:
        Tuple[int, List[Dict[str, Any]]]: A tuple containing:
            - The ID of the scan record
            - List of files only in torrents with updated database IDs
    """
    scan_end_time = datetime.now()
    # One sighting timestamp for the whole scan: first_seen/last_seen and the
    # distinct-day consecutive_scans gate all key off scan start, so a scan
    # straddling midnight still records a single sighting day per file.
    now_iso = scan_start_time.isoformat()

    try:
        with sqlite3.connect(str(config.sqlite_cache_path)) as conn:
            cursor = conn.cursor()

            # Insert scan metadata
            cursor.execute(
                """
            INSERT INTO scan_results (host, base_path, scan_start, scan_end)
            VALUES (?, ?, ?, ?)
            """,
                (
                    f"{config.deluge_username}@{config.deluge_host}:{config.deluge_port}",
                    str(config.deluge_torrent_base_remote_folder),
                    scan_start_time.isoformat(),
                    scan_end_time.isoformat(),
                ),
            )

            scan_id = cursor.lastrowid
            logger.info(f"Created scan record with ID {scan_id}")

        # Mark tracked 'local_torrent_folder' files not found in this scan as inactive.
        # Include rows already marked for deletion: if Deluge has reacquired the path (or
        # it otherwise stopped being an orphan), the mark must be cleared before the
        # force-deletion gate runs later in this invocation.
        current_source_for_orphans = "local_torrent_folder"
        cursor.execute(
            """
            SELECT id, path FROM orphaned_files
            WHERE source = ? AND status IN ('active', 'marked_for_deletion')
            """,
            (current_source_for_orphans,),
        )

        tracked_db_orphan_files = {row[1]: row[0] for row in cursor.fetchall()}  # path: id

        current_disk_orphan_files_paths = set()
        for file_info_ot_pre_check in orphaned_torrent_files:
            path_ot_pre_check = file_info_ot_pre_check["path"] if isinstance(file_info_ot_pre_check, dict) else file_info_ot_pre_check
            current_disk_orphan_files_paths.add(path_ot_pre_check)

        orphans_no_longer_seen_ids = []
        for db_path, db_file_id in tracked_db_orphan_files.items():
            if db_path not in current_disk_orphan_files_paths:
                orphans_no_longer_seen_ids.append(db_file_id)

        if orphans_no_longer_seen_ids:
            placeholders = ",".join("?" for _ in orphans_no_longer_seen_ids)
            cursor.execute(
                f"""
                UPDATE orphaned_files
                SET status = 'inactive',
                    consecutive_scans = 0,
                    include_in_report = 0
                WHERE id IN ({placeholders}) AND source = ?
            """,
                tuple(orphans_no_longer_seen_ids) + (current_source_for_orphans,),
            )
            logger.info(
                f"Marked {len(orphans_no_longer_seen_ids)} previously tracked '{current_source_for_orphans}' files "
                f"as 'inactive' (consecutive scans reset) because they were not found in scan ID {scan_id}."
            )

        # Process orphaned torrent files
        for file_info in orphaned_torrent_files:
            path = file_info["path"] if isinstance(file_info, dict) else file_info

            # For string entries (old format), we need to get the file info
            if not isinstance(file_info, dict):
                full_path = os.path.join(str(config.local_torrent_base_local_folder), path)
                if os.path.exists(full_path):
                    size = os.path.getsize(full_path)
                    size_human = f"{size / (1024**3):.2f} GB" if size >= 1024**3 else f"{size / (1024**2):.2f} MB"
                else:
                    size = 0
                    size_human = "0 MB"
            else:
                path = file_info["path"]
                size = file_info["size"]
                size_human = file_info["size_human"]

            file_hash = ""
            full_path = os.path.join(str(config.local_torrent_base_local_folder), path)
            if os.path.exists(full_path):
                try:
                    # Try to get hash from cache first
                    cursor.execute(
                        """
                        SELECT file_hash FROM file_hashes
                        WHERE folder_path = ? AND relative_path = ?
                        """,
                        (str(config.local_torrent_base_local_folder), path),
                    )

                    result = cursor.fetchone()
                    if result:
                        file_hash = result[0]
                except sqlite3.Error as e:
                    logger.error(f"Error getting file hash from cache: {e}")

            # Determine if this file should be included in the report
            # All orphaned torrent files are included
            include_in_report = True

            current_source = "local_torrent_folder"

            # Check if this orphaned file (by path and source) already exists
            cursor.execute(
                """
            SELECT id, consecutive_scans, status, last_seen_at, first_seen_at FROM orphaned_files
            WHERE path = ? AND source = ?
            """,
                (path, current_source),
            )
            existing_record = cursor.fetchone()

            if existing_record:
                file_id = existing_record[0]
                existing_consecutive_scans = existing_record[1]
                existing_status = existing_record[2]
                existing_last_seen_at = existing_record[3]
                existing_first_seen_at = existing_record[4]

                # File exists, update it
                # Also update file_hash, size, size_human if they might have changed for the same path.
                # An empty resolved hash (e.g. an inode-mode scan, which does no hashing) must not
                # overwrite a hash stored by an earlier hash-mode run.
                # Preserve a previous deletion mark while the path remains a current orphan. The
                # force-deletion pass relies on that mark; all other rediscovered states reactivate.
                # consecutive_scans counts distinct *days* seen, not scans: run_on_startup makes
                # scan count a function of container recreates, which must not fast-forward the
                # deletion/autoremove thresholds. A counter reset to 0 (absence) restarts at 1.
                # The MIN clamp to the first_seen..now calendar span also heals counters inflated
                # by the old per-scan counting: distinct sighting days can never exceed that span,
                # so a legacy row converges on its next sighting without a separate migration.
                cursor.execute(
                    """
                UPDATE orphaned_files
                SET last_seen_at = ?,
                    consecutive_scans = MIN(
                        CASE
                            WHEN consecutive_scans = 0 THEN 1
                            WHEN date(last_seen_at) < date(?) THEN consecutive_scans + 1
                            ELSE consecutive_scans
                        END,
                        MAX(1, CAST(julianday(date(?)) - julianday(date(first_seen_at)) AS INTEGER) + 1)
                    ),
                    file_hash = COALESCE(NULLIF(?, ''), file_hash),
                    size = ?,
                    size_human = ?,
                    include_in_report = ?,
                    status = CASE
                        WHEN status = 'marked_for_deletion' THEN status
                        ELSE 'active'
                    END,
                    torrent_id = COALESCE(torrent_id, ?) -- Populate torrent_id if NULL
                WHERE id = ?
                """,
                    (
                        now_iso,
                        now_iso,
                        now_iso,
                        file_hash,
                        size,
                        size_human,
                        include_in_report,
                        file_info.get("torrent_id"),
                        file_id,
                    ),
                )
                if existing_consecutive_scans == 0:
                    updated_consecutive_scans = 1
                elif existing_last_seen_at and existing_last_seen_at[:10] < now_iso[:10]:
                    updated_consecutive_scans = existing_consecutive_scans + 1
                else:
                    updated_consecutive_scans = existing_consecutive_scans
                updated_status = "marked_for_deletion" if existing_status == "marked_for_deletion" else "active"
                if existing_first_seen_at:
                    span_days = (datetime.fromisoformat(now_iso[:10]).date() - datetime.fromisoformat(existing_first_seen_at[:10]).date()).days + 1
                    updated_consecutive_scans = min(updated_consecutive_scans, max(1, span_days))
                logger.debug(
                    (
                        f"Updated existing orphaned file: ID {file_id}, Path {path}, Source {current_source}. "
                        f"Consecutive_scans: {existing_consecutive_scans} -> {updated_consecutive_scans}. Status set to '{updated_status}'."
                    )
                )
            else:
                # File does not exist, insert it
                cursor.execute(
                    (
                        "INSERT INTO orphaned_files "
                        "(file_hash, path, source, label, size, size_human, "
                        "first_seen_at, last_seen_at, consecutive_scans, include_in_report) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)"
                    ),
                    (
                        file_hash,
                        path,
                        current_source,
                        None,  # No label for orphaned files from 'local_torrent_folder'
                        size,
                        size_human,
                        now_iso,  # first_seen_at
                        now_iso,  # last_seen_at
                        include_in_report,  # include_in_report is True for these
                    ),
                )
                file_id = cursor.lastrowid
                logger.debug(f"Inserted new orphaned file: ID {file_id}, Path {path}, Source {current_source}. " f"Consecutive_scans: 1. Status 'active'.")

            # Insert into file_scan_history
            cursor.execute(
                """
            INSERT INTO file_scan_history (scan_id, file_id, source)
            VALUES (?, ?, ?)
            """,
                (scan_id, file_id, current_source),
            )

        # Process files only in torrents
        for file_info in only_in_torrents:
            path = str(file_info["path"])  # Ensure path is string
            label = file_info.get("label", "none")
            size = file_info["size"]
            size_human = file_info["size_human"]
            torrent_id = file_info.get("torrent_id")

            file_hash = ""
            full_path = os.path.join(str(config.local_torrent_base_local_folder), path)
            if os.path.exists(full_path):
                try:
                    # Get hash from cache
                    cursor.execute(
                        "SELECT file_hash FROM file_hashes WHERE folder_path = ? AND relative_path = ?",
                        (str(config.local_torrent_base_local_folder), path),
                    )
                    result = cursor.fetchone()
                    if result:
                        file_hash = result[0]
                except sqlite3.Error as e:
                    logger.error(f"Error getting file hash from cache for 'torrents' source: {e}")

            include_in_report = size > 100000000 and not label.startswith("other") and not label.startswith("soft")
            current_source = "torrents"

            cursor.execute(
                "SELECT id FROM orphaned_files WHERE path = ? AND source = ?",
                (path, current_source),
            )
            existing_record = cursor.fetchone()

            if existing_record:
                file_id = existing_record[0]
                cursor.execute(
                    """
                UPDATE orphaned_files
                SET last_seen_at = ?,
                    consecutive_scans = MIN(
                        CASE
                            WHEN consecutive_scans = 0 THEN 1
                            WHEN date(last_seen_at) < date(?) THEN consecutive_scans + 1
                            ELSE consecutive_scans
                        END,
                        MAX(1, CAST(julianday(date(?)) - julianday(date(first_seen_at)) AS INTEGER) + 1)
                    ),
                    file_hash = COALESCE(NULLIF(?, ''), file_hash),
                    label = ?,
                    size = ?,
                    size_human = ?,
                    include_in_report = ?,
                    status = 'active',
                    torrent_id = ?
                WHERE id = ?
                """,
                    (
                        now_iso,
                        now_iso,
                        now_iso,
                        file_hash,
                        label,
                        size,
                        size_human,
                        include_in_report,
                        torrent_id,
                        file_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO orphaned_files
                    (file_hash, path, source, label, size, size_human,
                    first_seen_at, last_seen_at, consecutive_scans, include_in_report, torrent_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        file_hash,
                        path,
                        current_source,
                        label,
                        size,
                        size_human,
                        now_iso,
                        now_iso,
                        include_in_report,
                        torrent_id,
                    ),
                )
                file_id = cursor.lastrowid

            cursor.execute(
                """
            INSERT INTO file_scan_history (scan_id, file_id, source)
            VALUES (?, ?, ?)
            """,
                (scan_id, file_id, current_source),
            )

        # Process files only in media
        for file_info in only_in_media:
            path = str(file_info["path"])  # Ensure path is string
            size = file_info["size"]
            size_human = file_info["size_human"]

            file_hash = ""
            full_path = os.path.join(str(config.local_media_base_local_folder), path)
            if os.path.exists(full_path):
                try:
                    # Get hash from cache
                    cursor.execute(
                        "SELECT file_hash FROM file_hashes WHERE folder_path = ? AND relative_path = ?",
                        (str(config.local_media_base_local_folder), path),
                    )
                    result = cursor.fetchone()
                    if result:
                        file_hash = result[0]
                except sqlite3.Error as e:
                    logger.error(f"Error getting file hash from cache for 'media' source: {e}")

            media_label = None
            include_in_report = True
            current_source = "media"

            cursor.execute(
                "SELECT id FROM orphaned_files WHERE path = ? AND source = ?",
                (path, current_source),
            )
            existing_record = cursor.fetchone()

            if existing_record:
                file_id = existing_record[0]
                cursor.execute(
                    """
                UPDATE orphaned_files
                SET last_seen_at = ?,
                    consecutive_scans = MIN(
                        CASE
                            WHEN consecutive_scans = 0 THEN 1
                            WHEN date(last_seen_at) < date(?) THEN consecutive_scans + 1
                            ELSE consecutive_scans
                        END,
                        MAX(1, CAST(julianday(date(?)) - julianday(date(first_seen_at)) AS INTEGER) + 1)
                    ),
                    file_hash = COALESCE(NULLIF(?, ''), file_hash),
                    label = ?,
                    size = ?,
                    size_human = ?,
                    include_in_report = ?,
                    status = 'active'
                WHERE id = ?
                """,
                    (
                        now_iso,
                        now_iso,
                        now_iso,
                        file_hash,
                        media_label,
                        size,
                        size_human,
                        include_in_report,
                        file_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO orphaned_files
                    (file_hash, path, source, label, size, size_human,
                    first_seen_at, last_seen_at, consecutive_scans, include_in_report)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        file_hash,
                        path,
                        current_source,
                        media_label,
                        size,
                        size_human,
                        now_iso,
                        now_iso,
                        include_in_report,
                    ),
                )
                file_id = cursor.lastrowid

            cursor.execute(
                """
            INSERT INTO file_scan_history (scan_id, file_id, source)
            VALUES (?, ?, ?)
            """,
                (scan_id, file_id, current_source),
            )

        conn.commit()

    except sqlite3.Error as e:
        logger.error(f"Database error in save_scan_results_to_db: {e}")
        if conn:
            conn.rollback()
        scan_id = 0  # Reset scan_id on error
    finally:
        if conn:
            conn.close()

    return scan_id, only_in_torrents


def get_formatted_scan_results(scan_id=None, limit=1):
    """
    Get formatted scan results from the database.

    Args:
        scan_id (int, optional): The ID of the scan to retrieve. If None, gets the most recent scan.
        limit (int, optional): Number of scans to retrieve if scan_id is None.

    Returns:
        str: Formatted scan results
    """
    try:
        with sqlite3.connect(str(config.sqlite_cache_path)) as conn:
            cursor = conn.cursor()

            # Get the scan ID if not provided
            if scan_id is None:
                cursor.execute(
                    """
                SELECT id FROM scan_results
                ORDER BY created_at DESC
                LIMIT ?
                """,
                    (limit,),
                )

                results = cursor.fetchall()
                if not results:
                    return "No scan results found in database."

            # For backward compatibility, use the first result as the primary scan_id
            # but now we can handle multiple results if needed in the future
            scan_id = results[0][0]

        # Get scan metadata
        cursor.execute(
            """
        SELECT host, base_path, scan_start, scan_end
        FROM scan_results
        WHERE id = ?
        """,
            (scan_id,),
        )

        scan_info = cursor.fetchone()
        if not scan_info:
            return f"No scan found with ID {scan_id}."

        host, base_path, scan_start, scan_end = scan_info

        # Get filtered files
        cursor.execute(
            """
        SELECT path, label, size_human, source
        FROM orphaned_files
        WHERE id IN (
            SELECT file_id
            FROM file_scan_history
            WHERE scan_id = ?
        )
        AND include_in_report = 1
        ORDER BY
            CASE
                WHEN source = 'local_torrent_folder' THEN 1
                WHEN source = 'torrents' THEN 2
                WHEN source = 'media' THEN 3
                ELSE 4
            END,
            CASE WHEN label LIKE 'other%' THEN 'z' || label ELSE label END,
            size DESC
        """,
            (scan_id,),
        )

        files = cursor.fetchall()
        conn.close()

        # Format the results
        if not files:
            return f"No files to report for scan ID {scan_id}."

        result = [
            f"Scan ID: {scan_id}",
            f"Host: {host}",
            f"Base Path: {base_path}",
            f"Scan Start: {scan_start}",
            f"Scan End: {scan_end}",
            "",
            "Path | Label | Size | Source",
        ]

        for path, label, size_human, source in files:
            label = label or ""
            result.append(f"{path} | {label} | {size_human} | {source}")

        return "\n".join(result)

    except Exception as e:
        logger.error(f"Error getting formatted scan results: {str(e)}")
        return f"Error retrieving scan results: {str(e)}"


def save_scan_results(
    orphaned_torrent_files: List[Dict[str, Any]],
    only_in_torrents: List[Dict[str, Any]],
    only_in_media: List[Dict[str, Any]],
    scan_start_time: datetime = None,
) -> None:
    """
    Save scan results to JSON file and optionally to the database.

    Formats the scan results into a structured JSON format and saves to the
    configured output file. Includes timestamp, file counts, and detailed
    information about each file category.

    Args:
        orphaned_torrent_files: List of files in local torrent folder but not in Deluge
        only_in_torrents: List of files only in torrents (not in media)
        only_in_media: List of files only in media (not in torrents)
        scan_start_time: When the scan started (optional)
    """
    if scan_start_time is None:
        scan_start_time = datetime.now()

    output_data = {
        "host": f"{config.deluge_username}@{config.deluge_host}:{config.deluge_port}",
        "base_path": str(config.deluge_torrent_base_remote_folder),
        "scan_start": scan_start_time.isoformat(),
        "scan_end": datetime.now().isoformat(),
        "in_local_torrent_folder_but_not_deluge": orphaned_torrent_files,
        "files_only_in_torrents": only_in_torrents,
        "files_only_in_media": only_in_media,
    }

    try:
        with open(config.output_file, "w") as f:
            json.dump(output_data, f, indent=2)
        logger.info(f"Scan results saved to {config.output_file}")
    except IOError as e:
        logger.error(f"Failed to save scan results to {config.output_file}: {e}")


def clean_hash_cache(folder: Path, use_sqlite=False) -> None:
    """
    Clean stale entries from the cache for files that no longer exist.

    Args:
        folder (Path): The folder whose cache to clean
        use_sqlite (bool): Whether to clean SQLite cache instead of JSON
    """
    # Get the current list of files in the folder
    current_files = set()
    for root, dirs, files in os.walk(folder):
        current_path = Path(root)
        relative_root_path = current_path.relative_to(folder)

        if relative_root_path.parts and relative_root_path.parts[0] in config.local_subfolders_blacklist:
            logger.trace(f"Skipping blacklisted directory: {Path(folder) / relative_root_path.parts[0]} and all its subdirectories {dirs}.")
            dirs[:] = []  # Don't descend into blacklisted directories
            continue

        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, folder)
            current_files.add(relative_path)

    if use_sqlite:
        if not os.path.exists(str(config.sqlite_cache_path)):
            logger.warning(f"SQLite cache file not found at {str(config.sqlite_cache_path)}")
            return

        try:
            with sqlite3.connect(str(config.sqlite_cache_path)) as conn:
                cursor = conn.cursor()

            # Get all entries for this folder
            cursor.execute(
                """
                SELECT relative_path FROM file_hashes WHERE folder_path = ?;
                """,
                (str(folder),),
            )
            db_files = {row[0] for row in cursor.fetchall()}
            stale_files = db_files - current_files

            if stale_files:
                # Delete stale entries
                for stale_file in stale_files:
                    cursor.execute(
                        """
                    DELETE FROM file_hashes
                    WHERE folder_path = ? AND relative_path = ?
                    """,
                        (str(folder), stale_file),
                    )

                conn.commit()
                logger.info(f"Removed {len(stale_files)} stale entries from SQLite cache for {folder}")
            else:
                logger.info(f"No stale entries found in SQLite cache for {folder}")

            conn.close()

        except Exception as e:
            logger.error(f"Error cleaning SQLite cache: {str(e)}")
    else:
        # Clean JSON cache
        cache_file = folder / ".hash_cache.json"
        hash_cache = load_hash_cache(cache_file)

        # Remove entries for files that no longer exist
        updated_cache = {k: v for k, v in hash_cache.items() if k in current_files}

        # Save cleaned cache
        save_hash_cache(cache_file, updated_cache)

        removed = len(hash_cache) - len(updated_cache)
        if removed > 0:
            logger.info(f"Removed {removed} stale entries from JSON hash cache for {folder}")
        else:
            logger.info(f"No stale entries found in JSON hash cache for {folder}")


def migrate_json_to_sqlite(no_progress: bool = False):
    """
    Migrate data from JSON hash caches to the SQLite database.
    Reads the JSON hash caches from both the torrent and media folders
    and inserts the data into the SQLite database.
    """
    logger.info("Starting migration of JSON hash caches to SQLite database...")

    # Initialize the SQLite database
    init_sqlite_cache(str(config.sqlite_cache_path))  # Initialize full database schema

    folders_to_migrate = [
        (config.local_torrent_base_local_folder, "torrent folder"),
        (config.local_media_base_local_folder, "media folder"),
    ]

    total_migrated = 0

    for folder_path, folder_desc in folders_to_migrate:
        cache_file = folder_path / ".hash_cache.json"

        if not cache_file.exists():
            logger.warning(f"JSON cache file not found for {folder_desc}: {cache_file}")
            continue

        try:
            # Load the JSON cache
            hash_cache = load_hash_cache(cache_file)

            if not hash_cache:
                logger.warning(f"No entries found in JSON cache for {folder_desc}")
                continue

            logger.info(f"Migrating {len(hash_cache)} entries from {folder_desc} JSON cache to SQLite")

            migrated_count = 0

            # Connect to the SQLite database using with statement for proper resource management
            with sqlite3.connect(str(config.sqlite_cache_path)) as conn:
                cursor = conn.cursor()
                conn.execute("BEGIN TRANSACTION")

                with tqdm(
                    total=len(hash_cache),
                    desc=f"Migrating {folder_desc}",
                    disable=no_progress,
                ) as pbar:
                    for relative_path, data in hash_cache.items():
                        file_hash = data["hash"]
                        mtime = data["mtime"]

                        full_path = folder_path / relative_path
                        file_size = full_path.stat().st_size if full_path.exists() else 0

                        # Insert or replace the entry in the SQLite database
                        cursor.execute(
                            """
                        INSERT OR REPLACE INTO file_hashes
                        (file_hash, folder_path, relative_path, mtime, file_size)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                file_hash,
                                str(folder_path),
                                relative_path,
                                mtime,
                                file_size,
                            ),
                        )

                        migrated_count += 1
                        pbar.update(1)

                # Transaction is automatically committed when the with block exits

            logger.info(f"Successfully migrated {migrated_count} entries from {folder_desc} JSON cache to SQLite")
            total_migrated += migrated_count

        except Exception as e:
            logger.error(f"Error migrating JSON cache for {folder_desc}: {str(e)}")

    logger.info(f"Migration complete. Total entries migrated: {total_migrated}")


def list_scan_history(limit=10):
    """
    List the scan history from the database.

    Args:
        limit (int): Maximum number of scans to list

    Returns:
        str: Formatted scan history
    """
    try:
        with sqlite3.connect(str(config.sqlite_cache_path)) as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
            SELECT id, host, scan_start, scan_end, created_at,
                   (SELECT COUNT(*) FROM file_scan_history WHERE scan_id = scan_results.id) as file_count
            FROM scan_results
            ORDER BY created_at DESC
            LIMIT ?
            """,
                (limit,),
            )

            scans = cursor.fetchall()

        if not scans:
            return "No scan history found in database."

        result = ["Scan History:", ""]
        result.append("ID | Host | Scan Start | Scan End | Files | Created At")

        for scan_id, host, scan_start, scan_end, created_at, file_count in scans:
            # Format dates for better readability
            scan_start_dt = datetime.fromisoformat(scan_start)
            scan_end_dt = datetime.fromisoformat(scan_end)
            created_at_dt = datetime.fromisoformat(created_at)

            scan_start_str = scan_start_dt.strftime("%Y-%m-%d %H:%M:%S")
            scan_end_str = scan_end_dt.strftime("%Y-%m-%d %H:%M:%S")
            created_at_str = created_at_dt.strftime("%Y-%m-%d %H:%M:%S")

            result.append(f"{scan_id} | {host} | {scan_start_str} | {scan_end_str} | {file_count} | {created_at_str}")

        return "\n".join(result)

    except Exception as e:
        logger.error(f"Error listing scan history: {str(e)}")
        return f"Error retrieving scan history: {str(e)}"


def _process_deletion_cycle(*, force_delete: bool, db_path: Path, torrent_base_folder: Path, scans_threshold: int) -> None:
    """Run deletion in two phases so an always-force schedule keeps advancing.

    A force run first deletes rows marked by a previous scan. It then performs the
    non-destructive marking pass so files eligible in the current scan are staged for
    revalidation and possible deletion on the next run.
    """
    process_deletions(
        force_delete=force_delete,
        db_path=db_path,
        torrent_base_folder=torrent_base_folder,
        scans_threshold=scans_threshold,
    )
    if force_delete:
        logger.info("Preparing currently eligible orphan files for validation on the next force run...")
        process_deletions(
            force_delete=False,
            db_path=db_path,
            torrent_base_folder=torrent_base_folder,
            scans_threshold=scans_threshold,
        )


# Old process_autoremove_labeling function removed - now using modular version from logic/autoremove.py


def execute_pending_actions(
    db_path: Path,
    client: "DelugeRPCClient",
    dry_run: bool = False,
) -> None:
    """
    Process pending actions that are due for execution.

    This is a wrapper around the execute_pending_actions function in the pending_actions module.
    It provides callbacks for the specific actions needed in this application context.

    Args:
        db_path (Path): Path to the SQLite database.
        client (DelugeRPCClient): An active Deluge RPC client instance.
        dry_run (bool): If True, only simulate the execution without actually making changes.
    """

    # Define callbacks for actions
    def apply_relabel_callback(file_path: str, action_params: str) -> tuple[bool, str | None]:
        """
        Apply a label to a torrent.

        Args:
            file_path: Path to the file (used for logging)
            action_params: JSON string containing action parameters
                           Expected format: {"torrent_id": "...", "label": "..."}

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            params = json.loads(action_params) if action_params else {}
            torrent_id = params.get("torrent_id")
            new_label = params.get("label")

            if not torrent_id or not new_label:
                logger.error(f"Missing required parameters (torrent_id or label) for relabeling: {action_params}")
                return (False, "missing_params")

            # Check if the torrent still exists in Deluge
            try:
                status = client.core.get_torrent_status(torrent_id, ["name"])
                if not status:  # Empty dict if torrent doesn't exist
                    logger.warning(f"Cannot apply label '{new_label}' to torrent ID {torrent_id} as it no longer exists in Deluge.")
                    return (False, "not_found")
            except Exception as e:
                logger.warning(f"Error checking if torrent ID {torrent_id} exists: {e}")
                return False

            # Apply the label
            client.label.set_torrent(torrent_id, new_label)
            logger.info(f"Applied label '{new_label}' to torrent ID {torrent_id} (file: {file_path})")
            return (True, None)

        except Exception as e:
            logger.error(f"Error applying label to torrent (file: {file_path}): {e}")
            return (False, "error")

    def delete_file_callback(file_path: str) -> bool:
        """
        Delete a file from the filesystem.

        Args:
            file_path: Path to the file to delete

        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted file: {file_path}")
                return True
            else:
                logger.warning(f"File does not exist, cannot delete: {file_path}")
                return False
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {e}")
            return False

    # Execute all pending actions using the dedicated module
    execute_all_pending_actions(
        db_path=db_path,
        apply_relabel_callback=apply_relabel_callback,
        delete_file_callback=delete_file_callback,
        dry_run=dry_run,
    )


def _dispatch_notifications(*, scan_id: int, no_notify: bool) -> None:
    """Send the scan report via e-mail and/or Telegram, unless suppressed.

    ``--no-notify`` (appended by the scheduler for startup runs) silences both
    channels for this run; the report is not formatted at all in that case.
    """
    if no_notify:
        logger.info("Notifications suppressed for this run (--no-notify); skipping e-mail/Telegram report.")
        return

    # Send e-mail notification if SMTP is configured and we have a valid scan_id
    if scan_id > 0 and config.smtp_host and config.smtp_username and config.smtp_password and config.smtp_from_addr and config.smtp_to_addrs:
        logger.info("Preparing e-mail report …")
        report_body = _format_scan_results(config.sqlite_cache_path, scan_id=scan_id)
        send_scan_report(
            smtp_host=config.smtp_host,
            smtp_port=config.smtp_port,
            username=config.smtp_username,
            password=config.smtp_password,
            from_addr=config.smtp_from_addr,
            to_addrs=config.smtp_to_addrs,
            report_body=report_body,
            use_ssl=config.smtp_use_ssl,
        )
    else:
        logger.debug("SMTP configuration incomplete or e-mail disabled; skipping notification.")

    # Telegram notification
    if scan_id > 0 and config.telegram_bot_token and config.telegram_chat_id:
        report_body = _format_scan_results(config.sqlite_cache_path, scan_id=scan_id)
        send_telegram_report(
            bot_token=config.telegram_bot_token,
            chat_id=config.telegram_chat_id,
            report_body=report_body,
        )
    else:
        logger.debug("Telegram configuration incomplete or notification disabled; skipping notification.")


def main() -> None:
    """
    Main entry point for the Deluge Orphaned Files application.

    Parses command line arguments and executes the appropriate functionality based on those arguments.
    Main operations include:
    - Finding orphaned files by comparing Deluge client data with local files
    - Managing the file hash cache (cleaning, migrating)
    - Processing file deletions for orphaned files
    - Applying auto-remove labels to torrents
    - Displaying scan results and history
    """
    print_version_info()

    # Create parser with description and version info
    parser = argparse.ArgumentParser(
        description="Deluge Orphaned Files - Find and manage orphaned torrent files",
        epilog="For more information, visit https://github.com/FiveBoroughs/deluge-orphaned-files",
    )
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {__version__}")

    # Organize arguments into logical groups
    scan_group = parser.add_argument_group("Scanning Options")
    scan_group.add_argument(
        "--skip-media-check",
        action="store_true",
        help="Only check Deluge vs local torrent files (skip media folder comparison)",
    )
    scan_group.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars during scan operations",
    )
    scan_group.add_argument(
        "--use-inodes",
        action="store_true",
        default=False,
        help=(
            "Compare files by inode (st_dev, st_ino) instead of content hash. "
            "Much faster — no hashing needed. Requires the torrent and media "
            "directories to be on the same filesystem. "
            "Hardlinked names are collapsed to one entry per physical file."
        ),
    )

    # Storage options group
    storage_group = parser.add_argument_group("Storage Options")
    storage_group.add_argument(
        "--sqlite",
        action="store_true",
        help="Use SQLite for caching and save results to database instead of JSON",
    )
    storage_group.add_argument(
        "--migrate-to-sqlite",
        dest="migrate_to_sqlite",
        action="store_true",
        help="Migrate data from JSON hash caches to SQLite database",
    )
    storage_group.add_argument(
        "--clean-cache",
        action="store_true",
        help="Clean the hash cache by removing entries for files that no longer exist",
    )

    # Results and reporting group
    results_group = parser.add_argument_group("Results & Reporting")
    results_group.add_argument(
        "--list-scans",
        action="store_true",
        help="List all previous scans with IDs and timestamps",
    )
    results_group.add_argument("--scan-id", type=int, help="Show detailed results for a specific scan ID")
    results_group.add_argument("--show-last", action="store_true", help="Show results for the most recent scan")
    results_group.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit the number of scans to show when listing (default: 10)",
    )

    # File management group
    file_group = parser.add_argument_group("File Management")
    file_group.add_argument(
        "--force",
        dest="force",
        action="store_true",
        help="Actually delete files marked for deletion (DANGEROUS: use with caution)",
    )
    file_group.add_argument(
        "--apply-autoremove-labels",
        action="store_true",
        help=f"Apply '{config.deluge_autoremove_label}' label in Deluge to torrents found only on torrent disk " "(not in media) and not already labeled as such. Performs a dry run if not specified.",
    )
    file_group.add_argument(
        "--execute-pending-actions",
        action="store_true",
        help="Execute pending actions that are due (e.g., apply deferred labels). Use with --force to actually apply changes.",
    )

    # Stuck cross-seed repair group (standalone; issue #1)
    repair_group = parser.add_argument_group("Stuck Torrent Repair")
    repair_group.add_argument(
        "--repair-stuck-torrents",
        action="store_true",
        help="Dry-run: find paused incomplete cross-seed torrents whose missing files already exist verified in other torrents, and log the hardlinks that would repair them. Standalone, no scan.",
    )
    repair_group.add_argument(
        "--apply-stuck-repairs",
        action="store_true",
        help="Actually hardlink donor files into stuck cross-seed torrents and force-recheck them under a download guard (see --repair-stuck-torrents). Runs on its own, without a scan.",
    )

    # Notification testing group
    notification_group = parser.add_argument_group("Notification Testing")
    notification_group.add_argument(
        "--test-email",
        action="store_true",
        help="Send a test e-mail using configured SMTP settings and exit",
    )
    notification_group.add_argument(
        "--test-telegram",
        action="store_true",
        help="Send a test Telegram message using configured bot token / chat ID and exit",
    )
    notification_group.add_argument(
        "--no-notify",
        action="store_true",
        help="Skip the e-mail/Telegram scan report for this run (the scheduler appends this for startup runs unless NOTIFY_ON_STARTUP=true)",
    )

    # Logging options
    log_group = parser.add_argument_group("Logging Options")
    log_group.add_argument("--debug", action="store_true", help="Enable debug logging")
    log_group.add_argument("--trace", action="store_true", help="Enable trace logging (most verbose)")
    log_group.add_argument(
        "--list-env",
        dest="list_env",
        action="store_true",
        help="List loaded environment variables (masked) and exit",
    )

    args = parser.parse_args()

    # Early-exit helper flags -------------------------------------------------
    if args.list_env:
        print_loaded_env_vars(config)
        return

    if args.repair_stuck_torrents or args.apply_stuck_repairs:
        run_stuck_repair(config, apply=args.apply_stuck_repairs)
        return

    if args.debug:
        logger.remove()
        logger.add(
            sys.stdout,
            level="DEBUG",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        )
        logger.add(
            log_file_path,
            rotation="1 day",
            retention="30 days",
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        )
        logger.debug("Debug logging enabled.")
    if args.trace:
        logger.remove()
        logger.add(
            sys.stdout,
            level="TRACE",
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        )
        logger.add(
            log_file_path,
            rotation="1 day",
            retention="30 days",
            level="TRACE",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        )
        logger.trace("Trace logging enabled.")

    # Record the flags and thresholds in effect so a log file read months later is
    # self-describing and does not require guessing how the run was invoked.
    logger.info("Flags in effect: {}", " ".join(f"--{name.replace('_', '-')}={value}" for name, value in sorted(vars(args).items())))
    logger.info(
        "Deletion thresholds in effect: DELETION_CONSECUTIVE_SCANS_THRESHOLD={}, DELETION_DAYS_THRESHOLD={}",
        config.deletion_consecutive_scans_threshold,
        config.deletion_days_threshold,
    )

    if args.sqlite:
        logger.debug(f"Initializing SQLite database schema at {config.sqlite_cache_path}")
        init_sqlite_cache(str(config.sqlite_cache_path))  # Initialize full database schema

    # Email test
    if args.test_email:
        if config.smtp_host and config.smtp_username and config.smtp_password and config.smtp_from_addr and config.smtp_to_addrs:
            test_body = (
                "This is a test e-mail from Deluge Orphaned Files.\n\n" f"Timestamp: {datetime.now().isoformat(timespec='seconds')}\n" "If you received this, SMTP settings are working correctly."
            )
            send_scan_report(
                smtp_host=config.smtp_host,
                smtp_port=config.smtp_port,
                username=config.smtp_username,
                password=config.smtp_password,
                from_addr=config.smtp_from_addr,
                to_addrs=config.smtp_to_addrs,
                report_body=test_body,
                use_ssl=config.smtp_use_ssl,
            )
        else:
            logger.error("Cannot send test e-mail: incomplete SMTP configuration (check SMTP_* env vars).")
        return

    # Telegram test
    if args.test_telegram:
        if config.telegram_bot_token and config.telegram_chat_id:
            test_body = (
                "*Deluge Orphaned Files* – Telegram test message\n\n" f"Timestamp: {datetime.now().isoformat(timespec='seconds')}\n" "If you received this, Telegram settings are working correctly."
            )
            send_telegram_report(
                bot_token=config.telegram_bot_token,
                chat_id=config.telegram_chat_id,
                report_body=test_body,
            )
        else:
            logger.error("Cannot send test Telegram message: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not configured.")
        return

    if args.show_last or args.scan_id is not None:
        if not args.sqlite:
            logger.error("Cannot show scan results without --sqlite flag, as data is in the database.")
            return
        if not os.path.exists(str(config.sqlite_cache_path)):
            logger.error(f"SQLite database not found at {str(config.sqlite_cache_path)}")
            return
        results = get_formatted_scan_results(args.scan_id if args.scan_id is not None else None)
        print(results)
        return

    if args.list_scans:
        if not args.sqlite:
            logger.error("Cannot list scans without --sqlite flag, as data is in the database.")
            return
        if not os.path.exists(str(config.sqlite_cache_path)):
            logger.error(f"SQLite database not found at {str(config.sqlite_cache_path)}")
            return
        history = list_scan_history(args.limit)
        print(history)
        return

    if args.migrate_to_sqlite:
        migrate_json_to_sqlite(no_progress=args.no_progress)
        return

    if args.clean_cache:
        if not args.sqlite:
            logger.info("Cleaning JSON hash cache...")
            clean_hash_cache(config.local_torrent_base_local_folder, use_sqlite=False)
            clean_hash_cache(config.local_media_base_local_folder, use_sqlite=False)
        else:
            logger.info("Cleaning SQLite hash cache...")
            if not os.path.exists(str(config.sqlite_cache_path)):
                logger.error(f"SQLite database not found at {str(config.sqlite_cache_path)} for cache cleaning.")
                return
            clean_hash_cache(config.local_torrent_base_local_folder, use_sqlite=True)
            clean_hash_cache(config.local_media_base_local_folder, use_sqlite=True)
        return

    # Run the scan using the storage mode chosen by the user (--sqlite flag)
    try:
        scan_id = find_orphaned_files(
            skip_media_check=args.skip_media_check,
            use_sqlite=args.sqlite,
            no_progress=args.no_progress,
            use_inodes=args.use_inodes,
        )
    except InodePreflightError as exc:
        # The --use-inodes same-filesystem preflight carries its own remediation message;
        # exit cleanly instead of dumping a traceback.
        logger.error("{}", exc)
        sys.exit(1)

    # Handle auto-remove labels or pending actions if requested
    if args.apply_autoremove_labels:
        logger.info(f"Attempting to apply '{config.deluge_autoremove_label}' labels to eligible torrents based on the latest scan...")
    else:
        logger.info(f"Performing dry-run for '{config.deluge_autoremove_label}' labeling. Use --apply-autoremove-labels to apply changes.")

    if args.execute_pending_actions:
        logger.info("Processing pending actions that are due for execution...")
    else:
        logger.info("Skipping processing of pending actions. Use --execute-pending-actions to check and execute pending actions.")

    if not args.sqlite:
        logger.warning(
            "Skipping '{}' labeling because --sqlite is not enabled; database-backed scan results are required.",
            config.deluge_autoremove_label,
        )
    else:
        deluge_client = None
        try:
            from deluge_client import DelugeRPCClient

            deluge_client = DelugeRPCClient(
                host=config.deluge_host,
                port=config.deluge_port,
                username=config.deluge_username,
                password=config.deluge_password,
            )
            deluge_client.connect()
            if deluge_client.connected:
                logger.debug("Successfully connected to Deluge for labeling.")
                process_autoremove_labeling(
                    client=deluge_client,
                    scan_id=scan_id,
                    db_path=config.sqlite_cache_path,
                    dry_run=not args.apply_autoremove_labels,
                    target_label_prefix=config.deluge_autoremove_label,
                )

                # Execute pending actions if requested
                if args.execute_pending_actions:
                    execute_pending_actions(
                        db_path=config.sqlite_cache_path,
                        client=deluge_client,
                        dry_run=not args.force,
                    )
                    if not args.force:
                        logger.info("Pending actions were only processed in dry-run mode. Use --force to actually apply changes.")

            else:
                logger.error(f"Failed to connect to Deluge for labeling. Skipping '{config.deluge_autoremove_label}' labeling.")
        except ConnectionRefusedError:
            logger.error(f"Deluge connection refused at {config.deluge_host}:{config.deluge_port}. Skipping '{config.deluge_autoremove_label}' labeling. Ensure Deluge is running and accessible.")
        except Exception as e:
            logger.error(f"An error occurred during '{config.deluge_autoremove_label}' labeling: {e}. Skipping.")
        finally:
            if deluge_client and deluge_client.connected:
                deluge_client.disconnect()
                logger.debug("Disconnected from Deluge after labeling.")

    if args.sqlite:
        if scan_id > 0:
            logger.info(f"Scan saved to database with ID: {scan_id}")
            logger.info(f"To view these results later, use: --scan-id {scan_id} --sqlite")

        logger.info(f"Processing potential deletions (Force mode: {args.force})...")
        _process_deletion_cycle(
            force_delete=args.force,
            db_path=config.sqlite_cache_path,
            torrent_base_folder=config.local_torrent_base_local_folder,
            scans_threshold=config.deletion_consecutive_scans_threshold,
        )
    elif not args.sqlite:
        logger.info(f"Scan results saved to {config.output_file}")
        logger.info("Deletion processing (--force) is only available with --sqlite mode.")

    _dispatch_notifications(scan_id=scan_id, no_notify=args.no_notify)

    # Only show the formatted table from the database when using SQLite mode
    if scan_id > 0:
        try:
            summary_table = _format_scan_results(config.sqlite_cache_path, scan_id=scan_id)
            logger.info("\n{}", summary_table)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to generate console summary table: {}", exc)


if __name__ == "__main__":
    main()
