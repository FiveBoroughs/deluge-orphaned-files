import os
import json
from pathlib import Path
from datetime import datetime
from deluge_client import DelugeRPCClient
import hashlib
from tqdm import tqdm
import argparse
import sqlite3
from loguru import logger
import sys  # For stdout logging
import logging
from typing import List, Dict, Any, Optional
from pydantic import (
    Field,
    field_validator,
    model_validator,
    ValidationError,
    ValidationInfo,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

__version__ = "0.1.0"

# Configure Loguru
log_file_path = Path(__file__).parent / "deluge_orphaned_files.log"
logger.remove()  # Remove default stderr logger
logger.add(
    sys.stdout,
    level="INFO",
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
)
logger.add(
    log_file_path,
    rotation="1 day",
    retention="30 days",
    level="DEBUG",
    format=(
        "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
        "{name}:{function}:{line} - {message}"
    ),
)

logger.info(f"Logging initialized. Log file: {log_file_path}")


# --- Pydantic Configuration Model ---
class AppConfig(BaseSettings):
    # Required fields (Pydantic raises error if alias not found in environment)
    deluge_host: str = Field(alias="DELUGE_HOST")
    deluge_port: int = Field(alias="DELUGE_PORT")
    deluge_username: str = Field(alias="DELUGE_USERNAME")
    deluge_password: str = Field(alias="DELUGE_PASSWORD")
    deluge_torrent_base_remote_folder: str = Field(
        alias="DELUGE_TORRENT_BASE_REMOTE_FOLDER"
    )
    local_torrent_base_local_folder: Path = Field(
        alias="LOCAL_TORRENT_BASE_LOCAL_FOLDER"
    )
    local_media_base_local_folder: Path = Field(alias="LOCAL_MEDIA_BASE_LOCAL_FOLDER")
    output_file: Path = Field(alias="OUTPUT_FILE")

    # Optional fields with defaults
    # For lists from env vars, we read as string and parse in root_validator
    # Pydantic will use the default value if the alias is not found in os.environ
    cache_save_interval: int = Field(default=25, alias="CACHE_SAVE_INTERVAL")
    min_file_size_mb: int = Field(
        default=10, alias="MIN_FILE_SIZE_MB"
    )  # Minimum file size in MB to process

    # Shadow fields to capture raw environment variable strings
    raw_extensions_blacklist_str: Optional[str] = Field(
        default=None, alias="EXTENSIONS_BLACKLIST"
    )
    raw_local_subfolders_blacklist_str: Optional[str] = Field(
        default=None, alias="LOCAL_SUBFOLDERS_BLACKLIST"
    )

    # Final list fields, populated by model_validator
    extensions_blacklist: List[str] = Field(
        default_factory=list, alias="_DO_NOT_LOAD_EXTENSIONS_BLACKLIST_FROM_ENV_"
    )
    local_subfolders_blacklist: List[str] = Field(
        default_factory=list, alias="_DO_NOT_LOAD_LOCAL_SUBFOLDERS_BLACKLIST_FROM_ENV_"
    )

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
            raise ValueError(
                f"Parent directory for {info.field_name} does not exist: {parent_dir}"
            )
        if not parent_dir.is_dir():
            raise ValueError(
                f"Parent path for {info.field_name} is not a directory: {parent_dir}"
            )
        if not os.access(parent_dir, os.W_OK):
            raise ValueError(
                f"Parent directory for {info.field_name} is not writable: {parent_dir}"
            )
        return path_obj

    @model_validator(mode="after")
    def _populate_parsed_lists(self) -> "AppConfig":
        default_ext_str = (
            ".nfo,.srt,.jpg,.sfv,.txt,.png,.sub,.torrent,.plexmatch,"
            ".m3u,.json,.webp,.jpeg,.obj,.ini,.dtshd,.invalid"
        )
        default_sub_str = "music,ebooks,courses"

        effective_ext_str = (
            self.raw_extensions_blacklist_str
            if self.raw_extensions_blacklist_str is not None
            else default_ext_str
        )
        if isinstance(effective_ext_str, str):
            self.extensions_blacklist = [
                item.strip() for item in effective_ext_str.split(",") if item.strip()
            ]
        else:  # Should not happen if default_ext_str is used
            self.extensions_blacklist = []

        effective_sub_str = (
            self.raw_local_subfolders_blacklist_str
            if self.raw_local_subfolders_blacklist_str is not None
            else default_sub_str
        )
        if isinstance(effective_sub_str, str):
            self.local_subfolders_blacklist = [
                item.strip() for item in effective_sub_str.split(",") if item.strip()
            ]
        else:  # Should not happen if default_sub_str is used
            self.local_subfolders_blacklist = []
        return self

    sqlite_cache_path: Path = Field(alias="APP_SQLITE_CACHE_PATH")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


try:
    config = AppConfig()
    logger.info(
        "Environment configuration loaded and validated successfully using Pydantic."
    )
except ValidationError as e:
    logger.error("!!! Configuration Error !!!")
    for error_detail in e.errors():
        variable_name = (
            error_detail["loc"][0] if error_detail["loc"] else "UnknownField"
        )
        message = error_detail["msg"]
        logger.error(f"  Variable '{variable_name}': {message}")
    logger.error("Please check your .env file or environment variable settings.")
    sys.exit(1)


def print_version_info():
    logger.info(f"Deluge Orphaned Files Checker v{__version__}")


# The verify_paths() function has been removed as its logic is now in validate_env_config().
# Ensure to remove any calls to verify_paths() from your main() function or elsewhere.

# Helper function to check if file should be processed


def should_process_file(filepath: Path, stat_result: os.stat_result) -> bool:
    """Checks if a file should be processed based on various criteria."""
    # Check extension and name blacklist
    if (
        filepath.suffix.lower() in config.extensions_blacklist
        or filepath.name in config.extensions_blacklist
    ):
        logger.trace(f"Skipping {filepath.name} due to extension/name blacklist.")
        return False

    # Check for sample files and featurettes (path-based)
    path_lower = str(filepath).lower()
    if any(
        pattern in path_lower
        for pattern in ["/sample", "/featurettes", "/extras", ".sample", "-sample"]
    ):
        logger.trace(f"Skipping {filepath.name} due to sample/featurette pattern.")
        return False

    # Check minimum file size
    min_size_bytes = config.min_file_size_mb * 1024 * 1024
    if stat_result.st_size < min_size_bytes:
        logger.trace(
            (
                f"Skipping {filepath.name} due to size: {stat_result.st_size} bytes "
                f"< {min_size_bytes} bytes ({config.min_file_size_mb} MB)"
            )
        )
        return False

    return True


def init_sqlite_cache(db_path):
    """
    Initializes the SQLite cache database.
    Creates the database file if it doesn't exist and
    creates the necessary tables if they don't exist.

    Tables:
    - file_hashes: For caching file hashes to improve performance
    - scan_results: For storing metadata about each scan
    - orphaned_files: For tracking files that are orphaned
    - file_scan_history: For tracking file presence in each scan
    """
    db_exists = os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if not db_exists:
        logger.trace(f"Creating SQLite database at {db_path}")

    # Create file_hashes table for caching
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
    # Create index on folder_path for faster lookups
    cursor.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_file_hashes_folder_path ON file_hashes (folder_path);
    """
    )

    # Create scan_results table for storing scan metadata
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

    # Create orphaned_files table for tracking orphaned files
    logger.trace("Ensuring 'orphaned_files' table exists.")
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS orphaned_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_hash TEXT NOT NULL,
        path TEXT NOT NULL,
        source TEXT NOT NULL,  -- 'local_torrent_folder', 'torrents', or 'media'
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

    # Create file_scan_history table for tracking file presence in each scan
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

    # Create indexes for faster queries on file_scan_history
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_fsh_scan_id ON file_scan_history (scan_id);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_fsh_file_id ON file_scan_history (file_id);"
    )

    # Create the view for detailed scan files from the LATEST scan
    logger.trace("Ensuring 'vw_latest_scan_report' view exists.")
    cursor.execute(
        """
    DROP VIEW IF EXISTS vw_detailed_scan_files;
    -- Drop old view if it exists under the old name
    """
    )
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

    conn.commit()
    conn.close()
    logger.info("SQLite cache initialized successfully.")

    # Create view for files eligible for deletion
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    logger.debug("Ensuring 'view_files_eligible_for_deletion' view exists.")
    cursor.execute(
        """
    CREATE VIEW IF NOT EXISTS view_files_eligible_for_deletion AS
    SELECT
        of.id,
        of.path,
        of.source,
        of.label,
        of.size,
        of.status,
        of.first_seen_at,
        of.last_seen_at,
        of.consecutive_scans
    FROM orphaned_files of
    WHERE of.status = 'active'
      AND julianday(of.last_seen_at) - julianday(of.first_seen_at) > 15;
    """
    )
    conn.commit()
    conn.close()
    logger.trace("'view_files_eligible_for_deletion' view created/verified.")
    return True


def load_hashes_from_sqlite(
    db_path: str, folder_path: Path
) -> Dict[str, Dict[str, Any]]:
    """
    Load cache data from the SQLite database for a specific folder.

    Args:
        db_path (str): Path to the SQLite database file
        folder_path (str): Absolute path of the folder to load cache for

    Returns:
        dict: A dictionary where keys are relative file paths and values are
              dictionaries with 'hash' and 'mtime' fields
    """
    cache = {}

    if not os.path.exists(db_path):
        logger.warning(f"SQLite cache file not found at {db_path}")
        return cache

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query for all entries matching the folder_path
        cursor.execute(
            """
        SELECT relative_path, file_hash, mtime, file_size
        FROM file_hashes
        WHERE folder_path = ?
        """,
            (str(folder_path),),
        )

        rows = cursor.fetchall()

        for relative_path, file_hash, mtime, file_size in rows:
            cache[relative_path] = {"hash": file_hash, "mtime": mtime}

        conn.close()
        logger.debug(f"Loaded {len(cache)} entries from SQLite cache for {folder_path}")

    except Exception as e:
        logger.error(f"Error loading from SQLite cache: {str(e)}")

    return cache


def upsert_hash_to_sqlite(
    db_path: str,
    folder_path: Path,
    relative_path: str,
    file_hash: str,
    mtime: float,
    file_size: int,
) -> bool:
    """
    Insert or update a file hash entry in the SQLite database.

    Args:
        db_path (str): Path to the SQLite database file
        folder_path (str): Absolute path of the scanned folder
        relative_path (str): Path relative to folder_path
        file_hash (str): MD5 hash of the file
        mtime (float): Modification timestamp of the file
        file_size (int): Size of the file in bytes

    Returns:
        bool: True if the operation was successful, False otherwise
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Use INSERT OR REPLACE to handle both insert and update cases
        cursor.execute(
            """
        INSERT OR REPLACE INTO file_hashes
        (file_hash, folder_path, relative_path, mtime, file_size)
        VALUES (?, ?, ?, ?, ?)
        """,
            (file_hash, str(folder_path), relative_path, mtime, file_size),
        )

        conn.commit()
        conn.close()
        logger.trace(f"Upserted hash for {relative_path} in {folder_path} into SQLite.")
        return True

    except Exception as e:
        logger.error(f"Error upserting to SQLite cache: {str(e)}")
        return False


def get_deluge_files():
    client = DelugeRPCClient(
        config.deluge_host,
        config.deluge_port,
        config.deluge_username,
        config.deluge_password,
    )
    client.connect()

    logger.debug(f"Connecting to Deluge: {config.deluge_host}:{config.deluge_port}")
    logger.info(
        f"Fetching torrent list from {client.username}@{client.host}:{client.port}..."
    )
    torrent_list = client.call(
        "core.get_torrents_status", {}, ["files", "save_path", "label"]
    )

    all_files = set()
    file_labels = {}
    for torrent_id, torrent_data in torrent_list.items():
        save_path = torrent_data[b"save_path"].decode()
        label = torrent_data.get(b"label", b"").decode() or "No Label"
        for file in torrent_data[b"files"]:
            file_path_in_torrent = file[
                b"path"
            ].decode()  # Path of file *within* the torrent, relative to save_path

            # Normalize path components
            norm_save_path = os.path.normpath(save_path)
            # file_path_in_torrent is already relative, but normalize it too for safety
            norm_file_path_in_torrent = os.path.normpath(file_path_in_torrent)
            norm_deluge_base_remote_folder = os.path.normpath(
                config.deluge_torrent_base_remote_folder
            )

            # Construct the full path of the file on the system Deluge is running on
            full_path_on_deluge_system = os.path.join(
                norm_save_path, norm_file_path_in_torrent
            )
            # Ensure the joined path is also normalized
            full_path_on_deluge_system = os.path.normpath(full_path_on_deluge_system)

            # Calculate the path relative to the configured base folder
            relative_path = os.path.relpath(
                full_path_on_deluge_system, norm_deluge_base_remote_folder
            )

            # It's good practice to warn if the path seems to be outside the expected base
            # This can happen if DELUGE_TORRENT_BASE_REMOTE_FOLDER is
            # misconfigured or
            # if a torrent's save_path is unexpectedly outside this base.
            if ".." in relative_path.split(os.path.sep):
                logger.warning(
                    f"File '{full_path_on_deluge_system}' from torrent "
                    f"appears to be outside the DELUGE_TORRENT_BASE_REMOTE_FOLDER "
                    f"('{norm_deluge_base_remote_folder}'). "
                    f"Relative path: '{relative_path}'. "
                    f"This might lead to inconsistent tracking."
                )

            all_files.add(relative_path)
            file_labels[relative_path] = label

    return all_files, file_labels


def load_hash_cache(cache_file):
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


def save_hash_cache(cache_file, hash_cache):
    logger.debug(f"Saving {len(hash_cache)} entries to hash cache")
    try:
        with open(cache_file, "w") as f:
            json.dump(hash_cache, f)
    except Exception as e:
        logger.error(f"Error saving cache: {str(e)}")


def get_local_files(
    folder: str, use_sqlite: bool = False, no_progress: bool = False
) -> dict:
    """
    Scan a folder for files, get their metadata and hashes (using cache).

    Uses a two-pass system:
    1. Pre-scan: Walks the directory, calls os.stat() once per file, and uses
       should_process_file() (which now includes size check) to build a list
       of eligible files along with their stat_results.
    2. Processing: Iterates the eligible list (timed by tqdm), uses cached hashes
       if mtime matches, or calculates new hashes.

    Args:
        folder (str): The folder to scan.
        use_sqlite (bool): Whether to use SQLite for caching instead of JSON.
        no_progress (bool): Whether to disable progress bars for this scan.

    Returns:
        dict: A dictionary where keys are relative file paths and values are
              dictionaries with 'hash' and 'size' fields.
    """
    local_files = {}
    cache_file = None  # Initialize cache_file for potential use in JSON caching
    sqlite_updates_batch = []
    new_hashes_calculated_count = 0

    if use_sqlite:
        # init_sqlite_cache is now called once in main()
        hash_cache = load_hashes_from_sqlite(str(config.sqlite_cache_path), folder)
    else:
        cache_file = Path(folder) / ".hash_cache.json"
        hash_cache = load_hash_cache(cache_file)

    logger.info(
        f"Starting pre-scan for {Path(folder).name} to collect and filter files..."
    )
    paths_to_process_with_stats = []
    for root, dirs, files_in_dir in os.walk(folder):
        # Prune blacklisted subdirectories early
        current_path = Path(root)
        relative_root_path = current_path.relative_to(folder)

        if (
            relative_root_path.parts
            and relative_root_path.parts[0] in config.local_subfolders_blacklist
        ):
            logger.trace(
                f"Skipping blacklisted directory: {Path(folder) / relative_root_path.parts[0]} and all its subdirectories {dirs}."
            )
            dirs[:] = []  # Don't descend into blacklisted directories
            continue

        for file_name in files_in_dir:
            full_path_str = os.path.join(root, file_name)
            try:
                stat_result = os.stat(full_path_str)  # Single stat call
                if os.path.isfile(full_path_str) and should_process_file(
                    Path(full_path_str), stat_result
                ):
                    paths_to_process_with_stats.append((full_path_str, stat_result))
            except FileNotFoundError:
                logger.warning(
                    f"File not found during pre-scan: {full_path_str}, skipping."
                )
            except Exception as e:
                logger.error(
                    f"Error stating file {full_path_str} during pre-scan: {e}, skipping."
                )

    total_eligible_files = len(paths_to_process_with_stats)
    logger.info(
        f"Pre-scan complete for {Path(folder).name}. Found {total_eligible_files} eligible files to process."
    )

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

            logger.trace(
                f"Processing: {Path(full_path_str).name} (Size: {file_size} B, mtime: {mtime})"
            )

            cache_key = relative_path
            file_hash = None
            cache_hit = False

            if cache_key in hash_cache:
                cached_data = hash_cache[cache_key]
                cached_mtime = float(cached_data["mtime"])
                if abs(cached_mtime - mtime) <= 2:
                    file_hash = cached_data["hash"]
                    cache_hit = True
                    logger.trace(f"Cache hit for {relative_path}: hash {file_hash}")
                else:
                    logger.debug(
                        f"Cache mtime mismatch for {relative_path}: cached {cached_mtime}, current {mtime}"
                    )

            if not cache_hit:
                logger.info(f"Cache miss for {relative_path}. Calculating hash.")
                try:
                    file_hash = get_file_hash(
                        Path(full_path_str), no_progress=no_progress
                    )
                    if file_hash:
                        new_hashes_calculated_count += 1
                        hash_cache[cache_key] = {"hash": file_hash, "mtime": mtime}
                        logger.debug(
                            f"Updated in-memory cache for {relative_path} with new hash {file_hash}"
                        )
                        if use_sqlite:
                            sqlite_updates_batch.append(
                                (
                                    file_hash,
                                    str(folder),
                                    relative_path,
                                    mtime,
                                    file_size,
                                )
                            )
                        else:
                            files_since_last_json_save += 1
                    else:
                        logger.warning(
                            f"Hash calculation failed for {full_path_str}, skipping file."
                        )
                        pbar.update(1)
                        continue
                except Exception as e:
                    logger.error(
                        f"Error hashing file {full_path_str}: {e}, skipping file."
                    )
                    pbar.update(1)
                    continue

            if not file_hash:
                logger.warning(f"File {relative_path} ended up with no hash. Skipping.")
                pbar.update(1)
                continue

            local_files[relative_path] = {"hash": file_hash, "size": file_size}

            if (
                not use_sqlite
                and files_since_last_json_save >= config.cache_save_interval
            ):
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
            conn = sqlite3.connect(str(config.sqlite_cache_path))
            cursor = conn.cursor()
            conn.execute("BEGIN TRANSACTION")
            cursor.executemany(
                ("INSERT OR REPLACE INTO file_hashes "
                 "(file_hash, folder_path, relative_path, mtime, file_size) "
                 "VALUES (?, ?, ?, ?, ?)"),
                sqlite_updates_batch,
            )
            conn.commit()
            logger.info(
                f"Saved/Updated {len(sqlite_updates_batch)} entries in SQLite hash cache for {Path(folder).name}."
            )
        except Exception as e:
            logger.error(
                f"Error batch saving to SQLite hash cache for {Path(folder).name}: {e}"
            )
        finally:
            if conn:
                conn.close()

    logger.info(
        f"Finished scanning {Path(folder).name}. Calculated {new_hashes_calculated_count} new hashes. "
        f"Processed {len(local_files)}/{total_eligible_files} eligible files."
    )
    return local_files


def get_file_hash(file_path: Path, no_progress: bool = False) -> str:
    md5_hash = hashlib.md5()
    file_size = os.path.getsize(file_path)

    # Use a larger chunk size for better performance with large files
    chunk_size = 1024 * 1024  # 1MB chunks instead of 8KB

    logger.debug(f"Calculating MD5 hash for: {file_path}")
    with open(file_path, "rb") as f:
        with tqdm(
            total=file_size,
            unit="B",
            unit_scale=True,
            desc=f"Hashing {Path(file_path).name}",
            leave=False,
            disable=no_progress,
        ) as pbar:
            while chunk := f.read(chunk_size):
                md5_hash.update(chunk)
                pbar.update(len(chunk))
    return md5_hash.hexdigest()


def find_orphaned_files(
    skip_media_check=False, use_sqlite=False, no_progress: bool = False
):
    """
    Find orphaned files by comparing Deluge files with local torrent and media folders.

    Args:
        skip_media_check (bool): Whether to skip checking the media folder
        use_sqlite (bool): Whether to use SQLite for caching and save results only to the database

    Returns:
        int: The scan ID if saved to database, otherwise 0
    """
    scan_start_time = datetime.now()
    scan_id = 0

    try:
        logger.info("Connecting to Deluge and getting file list...")
        deluge_files, file_labels = get_deluge_files()
        logger.info(
            f"Retrieved {len(deluge_files)} files and their labels from Deluge client."
        )

        logger.info("Scanning local torrent folder...")
        local_torrent_files = get_local_files(
            config.local_torrent_base_local_folder, use_sqlite, no_progress=no_progress
        )
        logger.info(f"Found {len(local_torrent_files)} files in local torrent folder.")

        # Get orphaned files with their sizes
        orphaned_torrent_files = [
            {
                "path": path,
                "size": info["size"],
                "size_human": (
                    f"{info['size'] / (1024**3):.2f} GB"
                    if info["size"] >= 1024**3
                    else f"{info['size'] / (1024**2):.2f} MB"
                ),
            }
            for path, info in local_torrent_files.items()
            if path not in deluge_files
        ]
        # Sort by size
        orphaned_torrent_files.sort(key=lambda x: x["size"], reverse=True)

        logger.info(
            "Comparing files in deluge against files in the local torrent folder..."
        )
        logger.info(
            (
                f"Found {len(orphaned_torrent_files)} orphaned files in torrent folder "
                f"(present locally, not in Deluge). Actions planned: Potential deletion after checks."
            )
        )

        if skip_media_check:
            if orphaned_torrent_files:
                logger.info(f"\nFound {len(orphaned_torrent_files)} orphans")
                if use_sqlite:
                    scan_id = save_scan_results_to_db(
                        orphaned_torrent_files, [], [], scan_start_time
                    )
                else:
                    save_scan_results(orphaned_torrent_files, [], [], scan_start_time)
            else:
                logger.info("\nNo orphaned files found.")
            return scan_id

        logger.info("Scanning local media folder...")
        local_media_files = get_local_files(
            config.local_media_base_local_folder, use_sqlite, no_progress=no_progress
        )
        logger.info(f"Found {len(local_media_files)} files in local media folder.")

        # Compare files based on their hashes
        # Exclude files in blacklisted subfolders and with blacklisted extensions
        torrent_hashes = {
            info["hash"]: (name, info["size"], file_labels.get(name, "none"))
            for name, info in local_torrent_files.items()
            if not any(
                name.startswith(subfolder + "/")
                for subfolder in config.local_subfolders_blacklist
            )
        }
        media_hashes = {
            info["hash"]: (name, info["size"])
            for name, info in local_media_files.items()
            if not any(
                name.startswith(subfolder + "/")
                for subfolder in config.local_subfolders_blacklist
            )
        }

        # Pre-filter collections before set operations
        torrent_set = frozenset(torrent_hashes.keys())
        media_set = frozenset(media_hashes.keys())

        # Get files only in torrents with sizes
        only_in_torrents = [
            {
                "path": torrent_hashes[hash][0],
                "label": torrent_hashes[hash][2],
                "size": torrent_hashes[hash][1],
                "size_human": (
                    f"{torrent_hashes[hash][1] / (1024**3):.2f} GB"
                    if torrent_hashes[hash][1] >= 1024**3
                    else f"{torrent_hashes[hash][1] / (1024**2):.2f} MB"
                ),
            }
            for hash in torrent_set - media_set
        ]
        only_in_torrents.sort(
            key=lambda x: (
                "a" if x["label"].startswith("other") else x["label"],
                x["size"],
            ),
            reverse=True,
        )

        # Get files only in media with sizes
        only_in_media = [
            {
                "path": media_hashes[hash][0],
                "size": media_hashes[hash][1],
                "size_human": (
                    f"{media_hashes[hash][1] / (1024**3):.2f} GB"
                    if media_hashes[hash][1] >= 1024**3
                    else f"{media_hashes[hash][1] / (1024**2):.2f} MB"
                ),
            }
            for hash in media_set - torrent_set
        ]
        only_in_media.sort(key=lambda x: x["size"], reverse=True)

        # Save results regardless of whether orphans were found
        logger.info(
            (
                f"\nScan complete. Found {len(orphaned_torrent_files)} orphans, "
                f"{len(only_in_torrents)} files only in torrents, "
                f"{len(only_in_media)} files only in media"
            )
        )

        # Save to database if using SQLite, otherwise save to JSON
        if use_sqlite:
            scan_id = save_scan_results_to_db(
                orphaned_torrent_files, only_in_torrents, only_in_media, scan_start_time
            )
        else:
            save_scan_results(
                orphaned_torrent_files, only_in_torrents, only_in_media, scan_start_time
            )

        return scan_id

    except KeyboardInterrupt:
        logger.warning(
            "\nOperation cancelled by user. Progress has been saved in the cache."
        )
        return scan_id


def save_scan_results_to_db(
    orphaned_torrent_files: list,
    only_in_torrents: list,
    only_in_media: list,
    scan_start_time: datetime,
) -> int:
    """
    Save scan results to the SQLite database.

    Args:
        orphaned_torrent_files: List of files in local torrent folder but not in Deluge
        only_in_torrents: List of files only in torrents (not in media)
        only_in_media: List of files only in media (not in torrents)
        scan_start_time: When the scan started

    Returns:
        int: The ID of the scan record
    """
    scan_end_time = datetime.now()

    try:
        conn = sqlite3.connect(str(config.sqlite_cache_path))
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

        # Process orphaned torrent files
        for file_info in orphaned_torrent_files:
            path = file_info["path"] if isinstance(file_info, dict) else file_info

            # For string entries (old format), we need to get the file info
            if not isinstance(file_info, dict):
                full_path = os.path.join(
                    str(config.local_torrent_base_local_folder), path
                )
                if os.path.exists(full_path):
                    size = os.path.getsize(full_path)
                    size_human = (
                        f"{size / (1024**3):.2f} GB"
                        if size >= 1024**3
                        else f"{size / (1024**2):.2f} MB"
                    )
                else:
                    size = 0
                    size_human = "0 MB"
            else:
                path = file_info["path"]
                size = file_info["size"]
                size_human = file_info["size_human"]

            # Get file hash if available
            file_hash = ""
            full_path = os.path.join(str(config.local_torrent_base_local_folder), path)
            if os.path.exists(full_path):
                try:
                    # Try to get hash from cache first
                    conn_cache = sqlite3.connect(str(config.sqlite_cache_path))
                    cursor_cache = conn_cache.cursor()
                    cursor_cache.execute(
                        """
                    SELECT file_hash FROM file_hashes
                    WHERE folder_path = ? AND relative_path = ?
                    """,
                        (str(config.local_torrent_base_local_folder), path),
                    )

                    result = cursor_cache.fetchone()
                    if result:
                        file_hash = result[0]
                    conn_cache.close()
                except Exception as e:
                    logger.error(f"Error getting file hash from cache: {str(e)}")

            # Determine if this file should be included in the report
            # All orphaned torrent files are included
            include_in_report = True

            # --- MODIFIED SECTION TO HANDLE EXISTING ORPHANED FILES ---
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
                    status = 'active'  -- Ensure it's marked active if seen again
                WHERE id = ?
                """,
                    (now_iso, file_hash, size, size_human, include_in_report, file_id),
                )
                logger.debug(
                    (
                        f"Updated existing orphaned file: ID {file_id}, Path {path}, Source {current_source}. "
                        f"New consecutive_scans: {existing_consecutive_scans + 1}."
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
                        include_in_report,
                    ),
                )
                file_id = cursor.lastrowid
                logger.debug(
                    f"Inserted new orphaned file: ID {file_id}, Path {path}, Source {current_source}."
                )
            # --- END OF MODIFIED SECTION ---

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
            path = file_info["path"]
            label = file_info.get("label", "none")
            size = file_info["size"]
            size_human = file_info["size_human"]

            # Get file hash if available
            file_hash = ""
            full_path = os.path.join(str(config.local_torrent_base_local_folder), path)
            if os.path.exists(full_path):
                try:
                    # Try to get hash from cache first
                    conn_cache = sqlite3.connect(str(config.sqlite_cache_path))
                    cursor_cache = conn_cache.cursor()
                    cursor_cache.execute(
                        """
                    SELECT file_hash FROM file_hashes
                    WHERE folder_path = ? AND relative_path = ?
                    """,
                        (str(config.local_torrent_base_local_folder), path),
                    )

                    result = cursor_cache.fetchone()
                    if result:
                        file_hash = result[0]
                    conn_cache.close()
                except Exception as e:
                    logger.error(
                        f"Error getting file hash from cache for 'torrents' source: {str(e)}"
                    )

            # Determine if this file should be included in the report based on filtering criteria
            include_in_report = (
                size > 100000000
                and not label.startswith("other")
                and not label.startswith("soft")
            )

            # --- MODIFIED SECTION FOR 'torrents' SOURCE ---
            now_iso = datetime.now().isoformat()
            current_source = "torrents"

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
                    status = 'active'  -- Added back
                WHERE id = ?
                """,
                    (
                        now_iso,
                        file_hash,
                        label,
                        size,
                        size_human,
                        include_in_report,
                        file_id,
                    ),
                )
                logger.trace(
                    (
                        f"Updated existing file (source {current_source}): ID {file_id}, Path {path}. "
                        f"New consecutive_scans: {existing_consecutive_scans + 1}."
                    )
                )
            else:
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
                        label,
                        size,
                        size_human,
                        now_iso,  # first_seen_at
                        now_iso,  # last_seen_at
                        include_in_report,
                    ),
                )
                file_id = cursor.lastrowid
                logger.debug(
                    f"Inserted new file (source {current_source}): ID {file_id}, Path {path}."
                )
            # --- END OF MODIFIED SECTION ---

            # Insert into file_scan_history
            cursor.execute(
                """
            INSERT INTO file_scan_history (scan_id, file_id, source)
            VALUES (?, ?, ?)
            """,
                (scan_id, file_id, current_source),
            )

        # Process files only in media
        for file_info in only_in_media:
            path = file_info["path"]
            # For 'media' source, label is not applicable/available from this list
            # Size and size_human are directly from file_info
            size = file_info["size"]
            size_human = file_info["size_human"]

            # Get file hash if available
            file_hash = ""
            # Paths for 'media' source are relative to LOCAL_MEDIA_BASE_LOCAL_FOLDER
            full_path = os.path.join(config.local_media_base_local_folder, path)
            if os.path.exists(full_path):
                try:
                    # Try to get hash from cache first
                    conn_cache = sqlite3.connect(str(config.sqlite_cache_path))
                    cursor_cache = conn_cache.cursor()
                    cursor_cache.execute(
                        """
                    SELECT file_hash FROM file_hashes
                    WHERE folder_path = ? AND relative_path = ?
                    """,
                        (str(config.local_media_base_local_folder), path),
                    )

                    result = cursor_cache.fetchone()
                    if result:
                        file_hash = result[0]
                    conn_cache.close()
                except Exception as e:
                    logger.error(
                        f"Error getting file hash from cache for 'media' source: {str(e)}"
                    )

            # For 'media' source, 'include_in_report' is generally True unless specific rules are added
            # Label is None for media files in this context
            media_label = None
            include_in_report = (
                True  # Default for media files, can be adjusted if needed
            )

            now_iso = datetime.now().isoformat()
            current_source = "media"

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
                    status = 'active'  -- Added back
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
                logger.trace(
                    (
                        f"Updated existing file (source {current_source}): ID {file_id}, Path {path}. "
                        f"New consecutive_scans: {existing_consecutive_scans + 1}."
                    )
                )
            else:
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
                        media_label,
                        size,
                        size_human,
                        now_iso,  # first_seen_at
                        now_iso,  # last_seen_at
                        include_in_report,
                    ),
                )
                file_id = cursor.lastrowid
                logger.debug(
                    f"Inserted new file (source {current_source}): ID {file_id}, Path {path}."
                )

            cursor.execute(
                """
            INSERT INTO file_scan_history (scan_id, file_id, source)
            VALUES (?, ?, ?)
            """,
                (scan_id, file_id, current_source),
            )

        conn.commit()
        conn.close()

        logger.debug(f"Saved scan results to database (scan ID: {scan_id})")
        return scan_id

    except Exception as e:
        logger.error(f"Error saving scan results to database: {str(e)}")
        return 0


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
        conn = sqlite3.connect(str(config.sqlite_cache_path))
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
                conn.close()
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
            conn.close()
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
    orphaned_torrent_files: list,
    only_in_torrents: list,
    only_in_media: list,
    scan_start_time: datetime = None,
) -> None:
    """
    Save scan results to JSON file and optionally to the database.

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
    for root, _, files in os.walk(folder):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, folder)
            current_files.add(relative_path)

    if use_sqlite:
        if not os.path.exists(str(config.sqlite_cache_path)):
            logger.warning(
                f"SQLite cache file not found at {str(config.sqlite_cache_path)}"
            )
            return

        try:
            conn = sqlite3.connect(str(config.sqlite_cache_path))
            cursor = conn.cursor()

            # Get all entries for this folder
            cursor.execute(
                """
            SELECT relative_path FROM file_hashes WHERE folder_path = ?
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
                logger.info(
                    f"Removed {len(stale_files)} stale entries from SQLite cache for {folder}"
                )
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
            logger.info(
                f"Removed {removed} stale entries from JSON hash cache for {folder}"
            )
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
    init_sqlite_cache(str(config.sqlite_cache_path))

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

            logger.info(
                f"Migrating {len(hash_cache)} entries from {folder_desc} JSON cache to SQLite"
            )

            # Connect to the SQLite database
            conn = sqlite3.connect(str(config.sqlite_cache_path))
            cursor = conn.cursor()

            # Use a transaction for better performance
            conn.execute("BEGIN TRANSACTION")

            migrated_count = 0
            with tqdm(
                total=len(hash_cache),
                desc=f"Migrating {folder_desc}",
                disable=no_progress,
            ) as pbar:
                for relative_path, data in hash_cache.items():
                    file_hash = data["hash"]
                    mtime = data["mtime"]

                    # Get the file size if the file exists
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

            # Commit the transaction
            conn.commit()
            conn.close()

            logger.info(
                f"Successfully migrated {migrated_count} entries from {folder_desc} JSON cache to SQLite"
            )
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
        conn = sqlite3.connect(str(config.sqlite_cache_path))
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
        conn.close()

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

            result.append(
                f"{scan_id} | {host} | {scan_start_str} | {scan_end_str} | {file_count} | {created_at_str}"
            )

        return "\n".join(result)

    except Exception as e:
        logger.error(f"Error listing scan history: {str(e)}")
        return f"Error retrieving scan history: {str(e)}"


def main():
    print_version_info()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--clean-cache", action="store_true", help="Clean stale entries from hash cache"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--trace", action="store_true", help="Enable trace logging")
    parser.add_argument(
        "--skip-media-check",
        action="store_true",
        help="Only check Deluge vs local torrent files (skip media folder comparison)",
    )
    parser.add_argument(
        "--sqlite",
        action="store_true",
        help="Use SQLite for caching and save results to database instead of JSON",
    )
    parser.add_argument(
        "--migrate-to-sqlite",
        action="store_true",
        help="Migrate data from JSON hash caches to SQLite database",
    )
    parser.add_argument(
        "--show-last", action="store_true", help="Display the most recent scan results"
    )
    parser.add_argument(
        "--list-scans", action="store_true", help="List all previous scans"
    )
    parser.add_argument(
        "--scan-id", type=int, help="Show results for a specific scan ID"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit the number of scans to show when listing (default: 10)",
    )
    parser.add_argument(
        "--no-progress", action="store_true", help="Disable progress bars"
    )
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    if args.trace:
        logger.setLevel(logging.TRACE)

    if args.sqlite:
        logger.debug(
            f"Initializing SQLite database schema at {config.sqlite_cache_path}"
        )
        init_sqlite_cache(str(config.sqlite_cache_path))

    # Database query operations
    if args.show_last or args.scan_id is not None:
        # Initialize the database if needed
        if not os.path.exists(str(config.sqlite_cache_path)):
            logger.error(
                f"SQLite database not found at {str(config.sqlite_cache_path)}"
            )
            return

        if args.scan_id is not None:
            results = get_formatted_scan_results(args.scan_id)
        else:
            results = get_formatted_scan_results()

        print(results)
        return

    if args.list_scans:
        # Initialize the database if needed
        if not os.path.exists(str(config.sqlite_cache_path)):
            logger.error(
                f"SQLite database not found at {str(config.sqlite_cache_path)}"
            )
            return

        history = list_scan_history(args.limit)
        print(history)
        return

    # Verify paths before proceeding with scan operations
    if args.migrate_to_sqlite:
        migrate_json_to_sqlite(no_progress=args.no_progress)
        return

    if args.clean_cache:
        clean_hash_cache(config.local_torrent_base_local_folder, args.sqlite)
        clean_hash_cache(config.local_media_base_local_folder, args.sqlite)
        return

    # Run the scan
    scan_id = find_orphaned_files(
        skip_media_check=args.skip_media_check,
        use_sqlite=args.sqlite,
        no_progress=args.no_progress,
    )

    # If scan was saved to database, show the scan ID
    if scan_id > 0:
        logger.info(f"Scan saved to database with ID: {scan_id}")
        logger.info(f"To view these results later, use: --scan-id {scan_id}")


if __name__ == "__main__":
    main()
