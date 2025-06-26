from __future__ import annotations

import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from .deluge.client import get_deluge_files as deluge_get_files
from tqdm import tqdm
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from deluge_client import DelugeRPCClient

import argparse
import sqlite3
from loguru import logger
import sys  # For stdout logging
from typing import List, Dict, Any, Optional, Tuple, Set
from pydantic import (
    Field,
    field_validator,
    model_validator,
    ValidationInfo,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from .scanning.hasher import get_file_hash_with_algorithm, infer_algorithm_from_hash  # noqa: F401
from .scanning.file_scanner import (
    should_process_file,
    get_local_files as _scan_get_local_files,
    load_hash_cache as scan_load_hash_cache,
    save_hash_cache as scan_save_hash_cache,
    load_hashes_from_sqlite as scan_load_hashes_from_sqlite,
)
from .database.hash_cache import (
    init_sqlite_cache as db_init_sqlite_cache,
    load_hashes_from_sqlite as db_load_hashes_from_sqlite,
    upsert_hash_to_sqlite as db_upsert_hash_to_sqlite,
)
from .logic.orphan_finder import compute_orphans
from .logic.pending_actions import (
    init_pending_actions_schema,
    register_pending_action,
    execute_pending_actions as execute_all_pending_actions,
    ActionType,
)
from .logic.retention import (
    get_files_to_mark_for_deletion as retention_get_files_to_mark,
    get_files_to_actually_delete as retention_get_files_to_delete,
    process_deletions as retention_process_deletions,
)
from .notifications.report_formatter import (
    format_scan_results as _format_scan_results,
)
from .notifications.emailer import send_scan_report
from .notifications.telegram_notifier import send_scan_report as send_telegram_report
from . import __version__

# ---------------------------------------------------------------------------
# Configure Loguru – log to console and to config/logs/deluge_orphaned_files.log
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


# ---------------------------------------------------------------------------
# Global configuration (shared across all modules)
# ---------------------------------------------------------------------------

# NOTE: The canonical AppConfig and the *singleton* "config" object now live
# in :pymod:`deluge_orphaned_files.settings`.  Importing it here ensures we
# share the exact same instance everywhere (including sub-modules executed
# earlier via their own imports).

from .settings import config  # noqa: E402  – import after logger setup for readable init logs

# Legacy per-file AppConfig instantiation removed – we now rely on
# settings.config which has already validated all environment variables.


class AppConfig(BaseSettings):
    # Required fields (Pydantic raises error if alias not found in environment)
    deluge_host: str = Field(alias="DELUGE_HOST")
    deluge_port: int = Field(alias="DELUGE_PORT")
    deluge_username: str = Field(alias="DELUGE_USERNAME")
    deluge_password: str = Field(alias="DELUGE_PASSWORD")
    deluge_torrent_base_remote_folder: str = Field(alias="DELUGE_TORRENT_BASE_REMOTE_FOLDER")
    local_torrent_base_local_folder: Path = Field(alias="LOCAL_TORRENT_BASE_LOCAL_FOLDER")
    local_media_base_local_folder: Path = Field(alias="LOCAL_MEDIA_BASE_LOCAL_FOLDER")
    output_file: Path = Field(alias="OUTPUT_FILE")

    # Optional fields with defaults
    # For lists from env vars, we read as string and parse in root_validator
    # Pydantic will use the default value if the alias is not found in os.environ
    cache_save_interval: int = Field(default=25, alias="CACHE_SAVE_INTERVAL")
    min_file_size_mb: int = Field(default=10, alias="MIN_FILE_SIZE_MB")  # Minimum file size in MB to process

    deletion_consecutive_scans_threshold: int = Field(default=7, alias="DELETION_CONSECUTIVE_SCANS_THRESHOLD")
    deletion_days_threshold: int = Field(default=7, alias="DELETION_DAYS_THRESHOLD")

    # Shadow fields to capture raw environment variable strings
    raw_extensions_blacklist_str: Optional[str] = Field(default=None, alias="EXTENSIONS_BLACKLIST")
    raw_local_subfolders_blacklist_str: Optional[str] = Field(default=None, alias="LOCAL_SUBFOLDERS_BLACKLIST")

    # Final list fields, populated by model_validator
    extensions_blacklist: List[str] = Field(default_factory=list, alias="_DO_NOT_LOAD_EXTENSIONS_BLACKLIST_FROM_ENV_")
    local_subfolders_blacklist: List[str] = Field(default_factory=list, alias="_DO_NOT_LOAD_LOCAL_SUBFOLDERS_BLACKLIST_FROM_ENV_")

    # SMTP / e-mail settings
    smtp_host: Optional[str] = Field(default=None, alias="SMTP_HOST")
    smtp_port: int = Field(default=465, alias="SMTP_PORT")
    smtp_username: Optional[str] = Field(default=None, alias="SMTP_USERNAME")
    smtp_password: Optional[str] = Field(default=None, alias="SMTP_PASSWORD")
    smtp_from_addr: Optional[str] = Field(default=None, alias="SMTP_FROM_ADDR")
    raw_smtp_to_str: Optional[str] = Field(default=None, alias="SMTP_TO_ADDRS")
    smtp_to_addrs: List[str] = Field(default_factory=list, alias="_INTERNAL_SMTP_TO_LIST")
    smtp_use_ssl: bool = Field(default=True, alias="SMTP_USE_SSL")

    deluge_autoremove_label: str = Field(default="othercat", alias="DELUGE_AUTOREMOVE_LABEL")
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    telegram_bot_token: Optional[str] = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: Optional[str] = Field(default=None, alias="TELEGRAM_CHAT_ID")

    @field_validator(
        "local_torrent_base_local_folder",
        "local_media_base_local_folder",
        mode="before",
    )
    def _validate_directory_path(cls, v: Any, info: ValidationInfo) -> Path:
        if v is None:
            raise ValueError(f"{info.field_name} must be set in environment variables")
        path_obj = Path(v)
        if not path_obj.exists():
            raise ValueError(f"Path for {info.field_name} does not exist: {v}")
        if not path_obj.is_dir():
            raise ValueError(f"Path for {info.field_name} is not a directory: {v}")
        if not os.access(path_obj, os.R_OK):
            raise ValueError(f"Cannot read directory for {info.field_name}: {v}")
        return path_obj

    @field_validator("output_file", mode="before")
    def _validate_output_file_parent(cls, v: Any, info: ValidationInfo) -> Path:
        if v is None:
            raise ValueError(f"{info.field_name} must be set in environment variables")
        path_obj = Path(v)
        parent_dir = path_obj.parent
        if not parent_dir.exists():
            raise ValueError(f"Parent directory for {info.field_name} does not exist: {parent_dir}")
        if not parent_dir.is_dir():
            raise ValueError(f"Parent path for {info.field_name} is not a directory: {parent_dir}")
        if not os.access(parent_dir, os.W_OK):
            raise ValueError(f"Parent directory for {info.field_name} is not writable: {parent_dir}")
        return path_obj

    @model_validator(mode="after")
    def _populate_parsed_lists(self) -> "AppConfig":
        default_ext_str = ".nfo,.srt,.jpg,.sfv,.txt,.png,.sub,.torrent,.plexmatch," ".m3u,.json,.webp,.jpeg,.obj,.ini,.dtshd,.invalid"
        default_sub_str = "music,ebooks,courses"

        effective_ext_str = self.raw_extensions_blacklist_str if self.raw_extensions_blacklist_str is not None else default_ext_str
        if isinstance(effective_ext_str, str):
            self.extensions_blacklist = [item.strip().lower() for item in effective_ext_str.split(",") if item.strip()]
        else:  # Should not happen if default_ext_str is used
            self.extensions_blacklist = []

        effective_sub_str = self.raw_local_subfolders_blacklist_str if self.raw_local_subfolders_blacklist_str is not None else default_sub_str
        if isinstance(effective_sub_str, str):
            self.local_subfolders_blacklist = [item.strip() for item in effective_sub_str.split(",") if item.strip()]
        else:  # Should not happen if default_sub_str is used
            self.local_subfolders_blacklist = []

        # Parse SMTP recipient list
        if self.raw_smtp_to_str:
            self.smtp_to_addrs = [addr.strip() for addr in self.raw_smtp_to_str.split(",") if addr.strip()]
        else:
            self.smtp_to_addrs = []
        return self

    sqlite_cache_path: Path = Field(alias="APP_SQLITE_CACHE_PATH")

    @field_validator("sqlite_cache_path", mode="before")
    def _validate_sqlite_cache_path(cls, v: Any, info: ValidationInfo) -> Path:
        if v is None:
            raise ValueError(f"{info.field_name} must be set in environment variables")
        path_obj = Path(v)
        parent_dir = path_obj.parent
        if not parent_dir.exists():
            raise ValueError(f"Parent directory for {info.field_name} does not exist: {parent_dir}")
        if not parent_dir.is_dir():
            raise ValueError(f"Parent path for {info.field_name} is not a directory: {parent_dir}")
        if not os.access(parent_dir, os.W_OK):
            raise ValueError(f"Parent directory for {info.field_name} is not writable: {parent_dir}")
        return path_obj


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
                status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'marked_for_deletion', 'deleted'
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

    return True


def load_hashes_from_sqlite(db_path: str, folder_path: Path) -> Dict[str, Dict[str, Any]]:
    """Load cache data from the SQLite database for a specific folder.

    Args:
        db_path: Path to the SQLite database file.
        folder_path: Absolute path of the folder to load cache for.

    Returns:
        Dictionary where keys are relative file paths and values are
        dictionaries with 'hash', 'mtime', and 'size' fields.

    Raises:
        sqlite3.Error: If there's an error during database operations.
    """
    cache = {}

    if not os.path.exists(db_path):
        logger.warning(f"SQLite cache file not found at {db_path}")
        return cache

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.trace(f"Querying SQLite cache for folder: {str(folder_path)}")
            cursor.execute(
                """
                SELECT relative_path, file_hash, mtime, file_size
                FROM file_hashes
                WHERE folder_path = ?;
                """,
                (str(folder_path),),
            )
            for row in cursor.fetchall():
                relative_path, file_hash, mtime, file_size = row
                cache[relative_path] = {
                    "hash": file_hash,
                    "mtime": mtime,
                    "size": file_size,
                }

    except sqlite3.Error as e:
        logger.error(f"SQLite error loading hashes for {str(folder_path)} from {db_path}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error loading hashes for {str(folder_path)} from {db_path}: {e}")

    if not cache:
        logger.trace(f"No cache entries found for {str(folder_path)} in {db_path}")
    else:
        logger.trace(f"Loaded {len(cache)} cache entries for {str(folder_path)} from {db_path}")
    return cache


def upsert_hash_to_sqlite(
    db_path: str,
    folder_path: Path,
    relative_path: str,
    file_hash: str,
    mtime: float,
    file_size: int,
) -> bool:
    """Insert or update a file hash entry in the SQLite database.

    Args:
        db_path: Path to the SQLite database file.
        folder_path: Absolute path of the scanned folder.
        relative_path: Path relative to folder_path.
        file_hash: MD5 hash of the file.
        mtime: Modification timestamp of the file.
        file_size: Size of the file in bytes.

    Returns:
        True if the operation was successful, False otherwise.

    Raises:
        sqlite3.Error: If there's an error during database operations.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO file_hashes
                (file_hash, folder_path, relative_path, mtime, file_size)
                VALUES (?, ?, ?, ?, ?)
                """,
                (file_hash, str(folder_path), relative_path, mtime, file_size),
            )
        logger.trace(f"Upserted hash for {relative_path} in {str(folder_path)} into SQLite.")
        return True
    except sqlite3.Error as e:
        logger.error(f"SQLite error upserting hash for {relative_path} in {str(folder_path)}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error upserting hash for {relative_path} in {str(folder_path)}: {e}")
        return False


def get_deluge_files() -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    """Connect to Deluge client and retrieve information about all torrents.

    Uses the global config object for connection parameters.

    Returns:
        A tuple containing:
            - Set of all file paths in Deluge
            - Dictionary mapping file paths to their labels
            - Dictionary mapping file paths to their torrent IDs
    """
    return deluge_get_files(config)


def load_hash_cache(cache_file: str) -> Dict[str, Dict[str, Any]]:
    """Load file hash cache from a JSON file.

    Args:
        cache_file: Path to the JSON cache file.

    Returns:
        Dictionary where keys are file paths and values are
        dictionaries with 'hash', 'mtime', and 'size' fields.
    """
    logger.debug(f"Loading cache from: {cache_file}")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r") as f:
                cache = json.load(f)
            logger.info(f"Loaded {len(cache)} cache entries")
            logger.debug(f"Cache: {str(cache)[:400]}...")
            return cache
        except Exception as e:
            logger.error(f"Error loading cache: {str(e)}")
    else:
        logger.warning("Cache file not found")
    return {}


def save_hash_cache(cache_file: str, hash_cache: Dict[str, Dict[str, Any]]) -> None:
    """Save file hash cache to a JSON file.

    Args:
        cache_file: Path to the JSON cache file.
        hash_cache: Dictionary where keys are file paths and values are
            dictionaries with 'hash', 'mtime', and 'size' fields.

    Raises:
        IOError: If there's an error writing to the cache file.
    """
    logger.debug(f"Saving {len(hash_cache)} entries to hash cache")
    try:
        with open(cache_file, "w") as f:
            json.dump(hash_cache, f)
    except Exception as e:
        logger.error(f"Error saving cache: {str(e)}")


def get_local_files(folder: str, use_sqlite: bool = False, no_progress: bool = False) -> Dict[str, Dict[str, Any]]:
    """Scan a folder for files, get their metadata and hashes (using cache).

    Uses a two-pass system:
    1. Pre-scan: Walks the directory, calls os.stat() once per file, and uses
       should_process_file() (which now includes size check) to build a list
       of eligible files along with their stat_results.
    2. Processing: Iterates the eligible list (timed by tqdm), uses cached hashes
       if mtime matches, or calculates new hashes.

    Args:
        folder: The folder to scan.
        use_sqlite: Whether to use SQLite for caching instead of JSON.
        no_progress: Whether to disable progress bars for this scan.

    Returns:
        A dictionary where keys are relative file paths and values are
        dictionaries with 'hash' and 'size' fields.

    Raises:
        OSError: If there's an error accessing files or directories.
    """
    local_files = {}
    cache_file = None  # Initialize cache_file for potential use in JSON caching
    sqlite_updates_batch = []
    new_hashes_calculated_count = 0

    if use_sqlite:

        hash_cache = load_hashes_from_sqlite(str(config.sqlite_cache_path), folder)
    else:
        cache_file = Path(folder) / ".hash_cache.json"
        hash_cache = load_hash_cache(cache_file)

    logger.info(f"Starting pre-scan for {Path(folder).name} to collect and filter files...")
    paths_to_process_with_stats = []
    for root, dirs, files_in_dir in os.walk(folder):

        current_path = Path(root)
        relative_root_path = current_path.relative_to(folder)

        if relative_root_path.parts and relative_root_path.parts[0] in config.local_subfolders_blacklist:
            logger.trace(f"Skipping blacklisted directory: {Path(folder) / relative_root_path.parts[0]} and all its subdirectories {dirs}.")
            dirs[:] = []  # Don't descend into blacklisted directories
            continue

        for file_name in files_in_dir:
            full_path_str = os.path.join(root, file_name)
            try:
                stat_result = os.stat(full_path_str)
                if os.path.isfile(full_path_str) and should_process_file(Path(full_path_str), stat_result, config):
                    paths_to_process_with_stats.append((full_path_str, stat_result))
            except FileNotFoundError:
                logger.warning(f"File not found during pre-scan: {full_path_str}, skipping.")
            except Exception as e:
                logger.error(f"Error stating file {full_path_str} during pre-scan: {e}, skipping.")

    total_eligible_files = len(paths_to_process_with_stats)
    logger.info(f"Pre-scan complete for {Path(folder).name}. Found {total_eligible_files} eligible files to process.")

    files_since_last_json_save = 0

    with tqdm(
        total=total_eligible_files,
        desc=f"Processing {Path(folder).name}",
        disable=no_progress,
    ) as pbar:
        for full_path_str, stat_result in paths_to_process_with_stats:
            file_size = stat_result.st_size
            mtime = stat_result.st_mtime
            relative_path = os.path.relpath(full_path_str, folder)

            logger.trace(f"Processing: {Path(full_path_str).name} (Size: {file_size} B, mtime: {mtime})")

            cache_key = relative_path
            file_hash = None
            cache_hit = False

            if cache_key in hash_cache:
                cached_data = hash_cache[cache_key]
                cached_mtime = float(cached_data["mtime"])
                hash_algorithm = cached_data.get("hash_algorithm") or infer_algorithm_from_hash(cached_data["hash"])

                if abs(cached_mtime - mtime) <= 2:
                    file_hash = cached_data["hash"]

                    # If we have a hash but it's using the old algorithm (md5), we need to rehash with xxhash64
                    # This allows for gradual migration of the cache
                    if hash_algorithm == "md5":
                        logger.info(f"Upgrading hash for {relative_path} from MD5 to XXHash64")
                        # Mark as cache miss to force rehashing with the new algorithm
                        cache_hit = False
                    else:
                        cache_hit = True
                        logger.trace(f"Cache hit for {relative_path}: hash {file_hash} using {hash_algorithm}")
                else:
                    logger.debug(f"Cache mtime mismatch for {relative_path}: cached {cached_mtime}, current {mtime}")

            if not cache_hit:
                logger.info(f"Cache miss for {relative_path}. Calculating hash.")
                try:
                    file_hash, hash_algorithm = get_file_hash_with_algorithm(Path(full_path_str), "xxh64", no_progress=no_progress)
                    if file_hash:
                        new_hashes_calculated_count += 1
                        hash_cache[cache_key] = {"hash": file_hash, "mtime": mtime, "hash_algorithm": hash_algorithm}
                        logger.debug(f"Updated in-memory cache for {relative_path} with new {hash_algorithm} hash {file_hash}")
                        if use_sqlite:
                            sqlite_updates_batch.append(
                                (
                                    file_hash,
                                    str(folder),
                                    relative_path,
                                    mtime,
                                    file_size,
                                    "xxh64",  # Always use xxh64 for new/updated hashes
                                )
                            )
                        else:
                            files_since_last_json_save += 1
                    else:
                        logger.warning(f"Hash calculation failed for {full_path_str}, skipping file.")
                        pbar.update(1)
                        continue
                except Exception as e:
                    logger.error(f"Error hashing file {full_path_str}: {e}, skipping file.")
                    pbar.update(1)
                    continue

            if not file_hash:
                logger.warning(f"File {relative_path} ended up with no hash. Skipping.")
                pbar.update(1)
                continue

            # Get the hash algorithm from the cache if it exists, otherwise default to xxh64 for new hashes
            cached_entry = hash_cache.get(cache_key, {})
            if "hash_algorithm" in cached_entry:
                hash_algorithm = cached_entry["hash_algorithm"]
            else:
                try:
                    hash_algorithm = infer_algorithm_from_hash(cached_entry.get("hash", ""))
                except ValueError:
                    # Fallback to xxh64 if the hash format can't be inferred (e.g., empty string)
                    hash_algorithm = "xxh64"
            local_files[relative_path] = {"hash": file_hash, "size": file_size, "hash_algorithm": hash_algorithm}

            if not use_sqlite and files_since_last_json_save >= config.cache_save_interval:
                if cache_file:
                    save_hash_cache(cache_file, hash_cache)
                    files_since_last_json_save = 0
                    logger.debug(f"JSON cache saved. {len(hash_cache)} total entries.")

            pbar.update(1)

    if not use_sqlite and files_since_last_json_save > 0 and cache_file:
        save_hash_cache(cache_file, hash_cache)
        logger.debug(f"Final JSON cache save. {len(hash_cache)} total entries.")

    if use_sqlite and sqlite_updates_batch:
        try:
            with sqlite3.connect(str(config.sqlite_cache_path)) as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    ("INSERT OR REPLACE INTO file_hashes " "(file_hash, folder_path, relative_path, mtime, file_size) " "VALUES (?, ?, ?, ?, ?)"),
                    sqlite_updates_batch,
                )
            logger.info(f"Saved/Updated {len(sqlite_updates_batch)} entries in SQLite hash cache for {Path(folder).name}.")
        except sqlite3.Error as e:
            logger.error(f"SQLite error during batch saving to hash cache for {Path(folder).name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during batch saving to SQLite hash cache for {Path(folder).name}: {e}")

    logger.info(f"Finished scanning {Path(folder).name}. Calculated {new_hashes_calculated_count} new hashes. " f"Processed {len(local_files)}/{total_eligible_files} eligible files.")
    return local_files


def find_orphaned_files(skip_media_check: bool = False, use_sqlite: bool = False, no_progress: bool = False) -> int:
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

        # Mark 'local_torrent_folder' files not found in this scan as inactive
        current_source_for_orphans = "local_torrent_folder"
        cursor.execute(
            """
            SELECT id, path FROM orphaned_files
            WHERE source = ? AND status = 'active'
            """,
            (current_source_for_orphans,),
        )

        active_db_orphan_files = {row[1]: row[0] for row in cursor.fetchall()}  # path: id

        current_disk_orphan_files_paths = set()
        for file_info_ot_pre_check in orphaned_torrent_files:
            path_ot_pre_check = file_info_ot_pre_check["path"] if isinstance(file_info_ot_pre_check, dict) else file_info_ot_pre_check
            current_disk_orphan_files_paths.add(path_ot_pre_check)

        orphans_no_longer_seen_ids = []
        for db_path, db_file_id in active_db_orphan_files.items():
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
                f"Marked {len(orphans_no_longer_seen_ids)} previously active '{current_source_for_orphans}' files "
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

            now_iso = datetime.now().isoformat()
            current_source = "local_torrent_folder"

            # Check if this orphaned file (by path and source) already exists
            cursor.execute(
                """
            SELECT id, consecutive_scans FROM orphaned_files
            WHERE path = ? AND source = ?
            """,
                (path, current_source),
            )
            existing_record = cursor.fetchone()

            if existing_record:
                file_id = existing_record[0]
                existing_consecutive_scans = existing_record[1]

                # File exists, update it
                # Also update file_hash, size, size_human if they might have changed for the same path
                cursor.execute(
                    """
                UPDATE orphaned_files
                SET last_seen_at = ?,
                    consecutive_scans = consecutive_scans + 1,
                    file_hash = ?,
                    size = ?,
                    size_human = ?,
                    include_in_report = ?,
                    status = 'active',  -- Ensure it's marked active if seen again
                    torrent_id = COALESCE(torrent_id, ?) -- Populate torrent_id if NULL
                WHERE id = ?
                """,
                    (now_iso, file_hash, size, size_human, include_in_report, file_info.get("torrent_id"), file_id),
                )
                updated_consecutive_scans = existing_consecutive_scans + 1
                logger.debug(
                    (
                        f"Updated existing orphaned file: ID {file_id}, Path {path}, Source {current_source}. "
                        f"Consecutive_scans: {existing_consecutive_scans} -> {updated_consecutive_scans}. Status set to 'active'."
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
            now_iso = datetime.now().isoformat()
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
                    consecutive_scans = consecutive_scans + 1,
                    file_hash = ?,
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
            now_iso = datetime.now().isoformat()
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
                    consecutive_scans = consecutive_scans + 1,
                    file_hash = ?,
                    label = ?,
                    size = ?,
                    size_human = ?,
                    include_in_report = ?,
                    status = 'active'
                WHERE id = ?
                """,
                    (
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
    db_init_sqlite_cache(str(config.sqlite_cache_path))  # Use db version for schema migration

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
                            (file_hash, str(folder_path), relative_path, mtime, file_size),
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


def get_files_to_mark_for_deletion(db_path: Path) -> List[Dict[str, Any]]:
    """
    Retrieves files that are active and meet the criteria for being marked for deletion.
    These are files from 'local_torrent_folder' source, active, seen for >7 scans
    and with a >7 day difference between first and last seen.

    Args:
        db_path (Path): Path to the SQLite database.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a file
                              eligible to be marked for deletion.
                              Keys: 'id', 'path', 'size_human', 'days_seen_difference', 'consecutive_scans'.
    """
    files_to_mark = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT
                    file_id,
                    file_path,
                    file_size,
                    days_seen_difference,
                    consecutive_scans
                FROM view_files_eligible_for_deletion;
                """
            )
            # The view `view_files_eligible_for_deletion` already filters for:
            # of.source = 'local_torrent_folder'
            # AND of.status = 'active'
            # AND of.consecutive_scans > 7
            # AND (julianday(of.last_seen_at) - julianday(of.first_seen_at)) > 7;
            for row in cursor.fetchall():
                files_to_mark.append(
                    {
                        "id": row[0],
                        "path": row[1],
                        "size_human": row[2],
                        "days_seen_difference": row[3],
                        "consecutive_scans": row[4],
                    }
                )
        logger.debug(f"Found {len(files_to_mark)} files eligible to be marked for deletion.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_files_to_mark_for_deletion: {e}")
    return files_to_mark


def get_files_to_actually_delete(db_path: Path) -> List[Dict[str, Any]]:
    """
    Retrieves files that have been marked for deletion.

    Args:
        db_path (Path): Path to the SQLite database.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each representing a file
                              to be deleted. Keys: 'id', 'path'.
    """
    files_to_delete = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, path
                FROM orphaned_files
                WHERE status = 'marked_for_deletion';
                """
            )
            for row in cursor.fetchall():
                files_to_delete.append({"id": row[0], "path": row[1]})
        logger.debug(f"Found {len(files_to_delete)} files marked for actual deletion.")
    except sqlite3.Error as e:
        logger.error(f"SQLite error in get_files_to_actually_delete: {e}")
    return files_to_delete


def process_deletions(force_delete: bool, db_path: Path, torrent_base_folder: Path) -> None:
    """
    Processes file deletions based on the force_delete flag.
    If force_delete is False (dry run), identifies eligible files and marks them for deletion.
    If force_delete is True, deletes files previously marked for deletion.

    Args:
        force_delete (bool): If True, perform actual deletions. Otherwise, dry run.
        db_path (Path): Path to the SQLite database.
    """
    if not db_path.exists():
        logger.warning(f"Deletion processing skipped: Database not found at {db_path}")
        return

    if force_delete:
        logger.info("Force delete enabled. Attempting to delete all 'active' orphaned files from 'local_torrent_folder' source immediately.")
        files_to_remove_directly = []
        try:
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT id, path
                    FROM orphaned_files
                    WHERE source = 'local_torrent_folder' AND status = 'active';
                    """
                )
                for row in cursor.fetchall():
                    files_to_remove_directly.append({"id": row[0], "path": row[1]})
        except sqlite3.Error as e:
            logger.error(f"SQLite error fetching active torrent orphans for force deletion: {e}")
            return  # Can't proceed if we can't fetch

        if not files_to_remove_directly:
            logger.info("No 'active' orphaned files from 'local_torrent_folder' source found to force delete.")
            return

        logger.info(f"Found {len(files_to_remove_directly)} active torrent orphans for immediate deletion.")
        deleted_count = 0

        if not torrent_base_folder.exists() or not torrent_base_folder.is_dir():
            logger.error(f"Provided torrent_base_folder '{torrent_base_folder}' does not exist or is not a directory. " f"Cannot proceed with deletions.")
            return

        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            for file_info in files_to_remove_directly:
                file_id = file_info["id"]
                relative_file_path_str = file_info["path"]
                absolute_file_path = torrent_base_folder / relative_file_path_str

                logger.info(f"Attempting to force delete file: {absolute_file_path}")
                try:
                    if absolute_file_path.exists():
                        os.remove(absolute_file_path)
                        logger.success(f"Successfully force-deleted file: {absolute_file_path}")
                        cursor.execute(
                            """
                            UPDATE orphaned_files
                            SET status = 'deleted', deletion_date = CURRENT_TIMESTAMP
                            WHERE id = ?;
                            """,
                            (file_id,),
                        )
                        deleted_count += 1
                    else:
                        logger.warning(f"File not found during force delete: {absolute_file_path}. " f"Updating status to 'deleted' as it's gone.")
                        # Update status even if file not found, as it's effectively gone from orphan perspective
                        cursor.execute(
                            """
                            UPDATE orphaned_files
                            SET status = 'deleted', deletion_date = CURRENT_TIMESTAMP
                            WHERE id = ? AND status != 'deleted';
                            """,
                            (file_id,),
                        )
                    conn.commit()
                except FileNotFoundError:  # Should be caught by .exists(), but as a fallback
                    logger.warning(f"File not found (caught by except FileNotFoundError) during force delete: {absolute_file_path}. Updating status to 'deleted'.")
                    cursor.execute(
                        "UPDATE orphaned_files SET status = 'deleted', deletion_date = CURRENT_TIMESTAMP WHERE id = ? AND status != 'deleted';",
                        (file_id,),
                    )
                    conn.commit()
                except PermissionError:
                    logger.error(f"Permission denied. Cannot force delete file: {absolute_file_path}")
                except OSError as e:
                    logger.error(f"OS error during force delete of file {absolute_file_path}: {e}")
                except sqlite3.Error as e:
                    logger.error(f"SQLite error updating status for file ID {file_id} ({absolute_file_path}) after force delete attempt: {e}")
                    conn.rollback()
        logger.info(f"Force deletion process completed. Attempted to delete {len(files_to_remove_directly)} files, successfully deleted {deleted_count}.")
    else:
        logger.info("Dry run for deletions. Identifying files eligible for deletion and marking them...")
        files_to_mark = get_files_to_mark_for_deletion(db_path)
        if not files_to_mark:
            logger.info("No new files eligible to be marked for deletion based on current criteria.")
            return

        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            for file_info in files_to_mark:
                file_id = file_info["id"]
                file_path_str = file_info["path"]
                size = file_info["size_human"]
                days_diff = file_info["days_seen_difference"]
                scans = file_info["consecutive_scans"]
                logger.info(f"File eligible for deletion: {file_path_str} (Size: {size}, Seen for ~{days_diff:.0f} days over {scans} scans). " f"Marking for deletion. Run with --force to remove.")
                try:
                    cursor.execute(
                        """
                        UPDATE orphaned_files
                        SET status = 'marked_for_deletion'
                        WHERE id = ? AND status = 'active';
                        """,
                        (file_id,),
                    )
                    conn.commit()
                except sqlite3.Error as e:
                    logger.error(f"SQLite error marking file ID {file_id} ({file_path_str}) for deletion: {e}")
                    conn.rollback()
        logger.info(f"Marked {len(files_to_mark)} files for deletion. Run with --force to actually delete them.")


def process_autoremove_labeling(
    db_path: Path,
    client: "DelugeRPCClient",
    apply_labels: bool,
    target_label_prefix: str = None,
) -> None:
    """
    Processes torrents that are only present on the torrents disk side (not in media)
    and identifies them for re-labeling with a target label (configured via DELUGE_AUTOREMOVE_LABEL)
    after a delay period. This helps identify files that should be managed by the auto-remove plugin.

    Instead of applying labels directly, this function records pending actions in the database
    that will be executed after the configured delay period (RELABEL_ACTION_DELAY_DAYS).

    Args:
        db_path (Path): Path to the SQLite database.
        client (DelugeRPCClient): An active Deluge RPC client instance.
        apply_labels (bool): If True, record pending actions. Otherwise, dry run.
        target_label_prefix (str): The prefix of the label to apply. If None, uses config.deluge_autoremove_label.
    """

    if target_label_prefix is None:
        target_label_prefix = config.deluge_autoremove_label

    # Get latest scan ID for recording as the originating scan
    latest_scan_id = None
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM scan_results ORDER BY created_at DESC LIMIT 1;")
            row = cursor.fetchone()
            if row:
                latest_scan_id = row[0]
            else:
                logger.error("No scan results found in database. Cannot record pending actions.")
                return
    except sqlite3.Error as e:
        logger.error(f"SQLite error fetching latest scan ID: {e}")
        return

    files_to_process = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cursor = conn.cursor()
            # Updated query to use the view with size_human field
            cursor.execute("SELECT file_id, file_path, current_label, torrent_id, size_human FROM vw_autoremove_candidates_latest_scan;")
            for row in cursor.fetchall():
                files_to_process.append(
                    {
                        "file_id": row[0],
                        "path": row[1],
                        "label": row[2],
                        "torrent_id": row[3],
                        "size_human": row[4],
                    }
                )
    except sqlite3.Error as e:
        logger.error(f"SQLite error fetching candidates for '{target_label_prefix}' labeling: {e}")
        return

    if not files_to_process:
        logger.info(f"No torrents found needing the '{target_label_prefix}' label based on the latest scan data.")
        return

    # Calculate the due date for the actions with a safe fallback value
    relabel_delay = getattr(config, "relabel_action_delay_days", 7)  # Default 7 days if not set
    action_due_at = datetime.now(timezone.utc) + timedelta(days=relabel_delay)

    logger.info(f"Found {len(files_to_process)} torrents to potentially label with '{target_label_prefix}'.")

    processed_torrent_hashes = set()
    actions_recorded_count = 0
    actions_would_be_recorded_count = 0

    for item in files_to_process:
        orphaned_file_id = item.get("file_id")
        torrent_id = item.get("torrent_id")
        current_label = item.get("label", "")
        file_path = item.get("path", "Unknown path")

        if not torrent_id:
            logger.warning(f"Item for path '{file_path}' missing 'torrent_id', skipping labeling.")
            continue

        if not orphaned_file_id:
            logger.warning(f"Item for path '{file_path}' missing 'file_id', skipping labeling.")
            continue

        if torrent_id in processed_torrent_hashes:
            # Avoid processing the same torrent multiple times if it has multiple files
            continue

        processed_torrent_hashes.add(torrent_id)

        if current_label and current_label.startswith(target_label_prefix):
            logger.debug(f"Torrent ID {torrent_id} (file: {file_path}) already has label '{current_label}' starting with '{target_label_prefix}'. Skipping.")
            continue

        if apply_labels:
            try:
                # Prepare action params as JSON
                action_params = json.dumps({"torrent_id": torrent_id, "label": target_label_prefix, "current_label": current_label})

                # Register the pending action using the dedicated module
                register_pending_action(
                    db_path=db_path,
                    file_path=file_path,
                    action_type=ActionType.RELABEL,
                    waiting_period_days=config.relabel_action_delay_days,
                    action_params=action_params,
                    scan_id=str(latest_scan_id),
                    orphaned_file_id=orphaned_file_id,
                    torrent_hash=torrent_id,
                    file_size=None,  # We don't have the size in bytes, only human-readable
                    source="torrents",
                )
                logger.info(
                    f"Recorded pending action to apply label '{target_label_prefix}' to torrent ID {torrent_id} (file: {file_path}) on"
                    f" {action_due_at.strftime('%Y-%m-%d')}. Previous label: '{current_label}'."
                )
                actions_recorded_count += 1
            except sqlite3.Error as e:
                logger.error(f"SQLite error recording pending action for torrent ID {torrent_id} (file: {file_path}): {e}")
        else:
            # Dry run - just log what would happen
            logger.info(
                f"[DRY RUN] Would record pending action to apply label '{target_label_prefix}' to torrent ID {torrent_id} (file: {file_path}) on"
                f" {action_due_at.strftime('%Y-%m-%d')}. Previous label: '{current_label}'."
            )
            actions_would_be_recorded_count += 1

    if apply_labels:
        logger.info(f"Finished processing labels. Recorded {actions_recorded_count} pending actions to apply '{target_label_prefix}' after {config.relabel_action_delay_days} days.")
    else:
        logger.info(
            f"Finished processing labels (dry run). Would have recorded {actions_would_be_recorded_count} pending actions to apply"
            f" '{target_label_prefix}' after {config.relabel_action_delay_days} days."
        )


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
    def apply_relabel_callback(file_path: str, action_params: str) -> bool:
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
                return False

            # Check if the torrent still exists in Deluge
            try:
                status = client.core.get_torrent_status(torrent_id, ["name"])
                if not status:  # Empty dict if torrent doesn't exist
                    logger.warning(f"Cannot apply label '{new_label}' to torrent ID {torrent_id} as it no longer exists in Deluge.")
                    return False
            except Exception as e:
                logger.warning(f"Error checking if torrent ID {torrent_id} exists: {e}")
                return False

            # Apply the label
            client.label.set_torrent(torrent_id, new_label)
            logger.info(f"Applied label '{new_label}' to torrent ID {torrent_id} (file: {file_path})")
            return True

        except Exception as e:
            logger.error(f"Error applying label to torrent (file: {file_path}): {e}")
            return False

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
    execute_all_pending_actions(db_path=db_path, apply_relabel_callback=apply_relabel_callback, delete_file_callback=delete_file_callback, dry_run=dry_run)


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
        description="Deluge Orphaned Files - Find and manage orphaned torrent files", epilog="For more information, visit https://github.com/FiveBoroughs/deluge-orphaned-files"
    )
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s {__version__}")

    # Organize arguments into logical groups
    scan_group = parser.add_argument_group("Scanning Options")
    scan_group.add_argument(
        "--skip-media-check",
        action="store_true",
        help="Only check Deluge vs local torrent files (skip media folder comparison)",
    )
    scan_group.add_argument("--no-progress", action="store_true", help="Disable progress bars during scan operations")

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
    results_group.add_argument("--list-scans", action="store_true", help="List all previous scans with IDs and timestamps")
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

    # Logging options
    log_group = parser.add_argument_group("Logging Options")
    log_group.add_argument("--debug", action="store_true", help="Enable debug logging")
    log_group.add_argument("--trace", action="store_true", help="Enable trace logging (most verbose)")

    args = parser.parse_args()

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

    if args.sqlite:
        logger.debug(f"Initializing SQLite database schema at {config.sqlite_cache_path}")
        db_init_sqlite_cache(str(config.sqlite_cache_path))  # Use db version for schema migration

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
    scan_id = find_orphaned_files(
        skip_media_check=args.skip_media_check,
        use_sqlite=args.sqlite,
        no_progress=args.no_progress,
    )

    # Handle auto-remove labels or pending actions if requested
    if args.apply_autoremove_labels:
        logger.info(f"Attempting to apply '{config.deluge_autoremove_label}' labels to eligible torrents based on the latest scan...")
    else:
        logger.info(f"Performing dry-run for '{config.deluge_autoremove_label}' labeling. Use --apply-autoremove-labels to apply changes.")

    if args.execute_pending_actions:
        logger.info("Processing pending actions that are due for execution...")
    else:
        logger.info("Skipping processing of pending actions. Use --execute-pending-actions to check and execute pending actions.")

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
                db_path=config.sqlite_cache_path,
                client=deluge_client,
                apply_labels=args.apply_autoremove_labels,
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
        process_deletions(force_delete=args.force, db_path=config.sqlite_cache_path, torrent_base_folder=config.local_torrent_base_local_folder)
    elif not args.sqlite:
        logger.info(f"Scan results saved to {config.output_file}")
        logger.info("Deletion processing (--force) is only available with --sqlite mode.")

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

    # Only show the formatted table from the database when using SQLite mode
    if scan_id > 0:
        try:
            summary_table = _format_scan_results(config.sqlite_cache_path, scan_id=scan_id)
            logger.info("\n{}", summary_table)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to generate console summary table: {}", exc)


if __name__ == "__main__":
    main()

# Rebind helper functions from scanning module to override legacy definitions below
get_local_files = _scan_get_local_files  # type: ignore  # noqa: F401,F811
load_hash_cache = scan_load_hash_cache  # type: ignore  # noqa: F401,F811
save_hash_cache = scan_save_hash_cache  # type: ignore  # noqa: F401,F811
load_hashes_from_sqlite = scan_load_hashes_from_sqlite  # type: ignore  # noqa: F401,F811

# Override database hash helpers too (gradual deprecation of local copies)
init_sqlite_cache = db_init_sqlite_cache  # type: ignore  # noqa: F401,F811
load_hashes_from_sqlite = db_load_hashes_from_sqlite  # type: ignore  # noqa: F401,F811
upsert_hash_to_sqlite = db_upsert_hash_to_sqlite  # type: ignore  # noqa: F401,F811

# Rebind Deluge wrapper
get_deluge_files = deluge_get_files  # type: ignore  # noqa: F401,F811

# Rebind retention helpers (override legacy in this file)
get_files_to_mark_for_deletion = retention_get_files_to_mark  # type: ignore  # noqa: F401,F811
get_files_to_actually_delete = retention_get_files_to_delete  # type: ignore  # noqa: F401,F811
process_deletions = retention_process_deletions  # type: ignore  # noqa: F401,F811
