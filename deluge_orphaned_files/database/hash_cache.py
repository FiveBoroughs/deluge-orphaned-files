"""SQLite-based hash cache helpers.

This module centralises all interactions with the file_hashes table used to
cache XXHash64 hashes for local files. It was extracted from the monolithic
cli.py during the ongoing refactor.

Provides functions for initializing the database schema, loading cached file hashes,
and upserting hash records to the database.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Dict, Any

from ..scanning.hasher import validate_hash

from loguru import logger

__all__ = [
    "init_sqlite_cache",
    "load_hashes_from_sqlite",
    "upsert_hash_to_sqlite",
]


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

_SCHEMA_FILE_HASHES = """
    CREATE TABLE IF NOT EXISTS file_hashes (
        file_hash TEXT NOT NULL,
        folder_path TEXT NOT NULL,
        relative_path TEXT NOT NULL,
        mtime REAL NOT NULL,
        file_size INTEGER NOT NULL,
        hash_algorithm TEXT DEFAULT 'md5',
        PRIMARY KEY (folder_path, relative_path)
    );
    """

_SCHEMA_INDEX_FOLDER_PATH = "CREATE INDEX IF NOT EXISTS idx_file_hashes_folder_path ON file_hashes (folder_path);"


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def init_sqlite_cache(db_path: str | Path) -> None:
    """Create database and required tables if they do not exist.

    If the database already exists but is using an older schema without the
    hash_algorithm column, this function will add the column and set all
    existing records to use 'md5' as the hash algorithm.

    Args:
        db_path: Path to the SQLite database file.

    Raises:
        sqlite3.Error: If there's an error creating the schema.
    """

    db_path = str(db_path)
    if not os.path.exists(db_path):
        logger.trace("SQLite database will be created at {}", db_path)

    try:
        # First ensure the base schema exists
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.trace("Ensuring 'file_hashes' table exists in the SQLite cache.")
            cursor.execute(_SCHEMA_FILE_HASHES)
            cursor.execute(_SCHEMA_INDEX_FOLDER_PATH)
            conn.commit()

        # Check if we need to add the hash_algorithm column
        # Using a completely separate connection to avoid any transaction issues
        columns = []
        success = False
        for attempt in range(1, 4):  # Try up to 3 times
            try:
                logger.debug(f"Attempt {attempt} to update database schema...")
                # Use context manager for connection handling
                with sqlite3.connect(db_path) as conn:
                    conn.isolation_level = None  # Set autocommit mode for DDL statements
                    cursor = conn.cursor()

                # First, check if the table exists at all
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file_hashes'")
                if not cursor.fetchone():
                    logger.warning("file_hashes table doesn't exist yet, will be created with proper schema")
                    # Don't try to alter a non-existent table
                    break

                # Check if the hash_algorithm column exists
                cursor.execute("PRAGMA table_info(file_hashes)")
                columns = [column[1] for column in cursor.fetchall()]
                logger.debug(f"Existing columns in file_hashes: {columns}")

                # Add the hash_algorithm column if it doesn't exist
                if "hash_algorithm" not in columns:
                    logger.info("Adding hash_algorithm column to existing file_hashes table")
                    try:
                        # Try with explicit BEGIN/COMMIT
                        cursor.execute("BEGIN TRANSACTION")
                        cursor.execute("ALTER TABLE file_hashes ADD COLUMN hash_algorithm TEXT DEFAULT 'md5'")
                        cursor.execute("COMMIT")

                        # Verify the column was actually added
                        cursor.execute("PRAGMA table_info(file_hashes)")
                        new_columns = [column[1] for column in cursor.fetchall()]
                        logger.debug(f"Columns after ALTER: {new_columns}")

                        if "hash_algorithm" in new_columns:
                            logger.success("Successfully added hash_algorithm column to file_hashes table")
                            success = True
                            break
                        else:
                            logger.error("Failed to verify hash_algorithm column was added")
                    except sqlite3.Error as inner_exc:
                        logger.error(f"Failed during ALTER TABLE operation: {inner_exc}")
                        try:
                            cursor.execute("ROLLBACK")
                            logger.debug("Rolled back transaction after error")
                        except sqlite3.Error:
                            pass  # Ignore rollback error
                else:
                    logger.debug("hash_algorithm column already exists in file_hashes table")
                    success = True
                    break
            except sqlite3.Error as alter_exc:
                logger.error(f"Failed to add hash_algorithm column (attempt {attempt}): {alter_exc}")
                logger.debug(f"Exception details: {str(alter_exc)}")

        if not success and columns and "hash_algorithm" not in columns:
            logger.warning("Could not add hash_algorithm column after multiple attempts. The application will use fallback mode.")
            # Let's try a different approach - create a new table and copy data
            try:
                logger.info("Attempting table recreation approach...")
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    # Create a new table with the right schema
                    cursor.execute(
                        """CREATE TABLE IF NOT EXISTS file_hashes_new (
                        file_hash TEXT NOT NULL,
                        folder_path TEXT NOT NULL,
                        relative_path TEXT NOT NULL,
                        mtime REAL NOT NULL,
                        file_size INTEGER NOT NULL,
                        hash_algorithm TEXT DEFAULT 'md5',
                        PRIMARY KEY (folder_path, relative_path)
                    )"""
                    )

                    # Copy data from old table
                    cursor.execute(
                        """INSERT INTO file_hashes_new
                                   (file_hash, folder_path, relative_path, mtime, file_size, hash_algorithm)
                                SELECT file_hash, folder_path, relative_path, mtime, file_size, 'md5'
                                FROM file_hashes"""
                    )
                    # Check if data was copied
                    cursor.execute("SELECT COUNT(*) FROM file_hashes_new")
                    count_new = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM file_hashes")
                    count_old = cursor.fetchone()[0]

                    if count_new == count_old:
                        # Swap tables
                        cursor.execute("DROP TABLE file_hashes")
                        cursor.execute("ALTER TABLE file_hashes_new RENAME TO file_hashes")
                        logger.success(f"Successfully recreated file_hashes table with hash_algorithm column. {count_new} records migrated.")
                        success = True
                    else:
                        logger.error(f"Data mismatch during migration: old={count_old}, new={count_new}")
            except sqlite3.Error as e:
                logger.error(f"Failed during table recreation: {e}")

        # Final check
        if success:
            logger.debug("Database schema is up to date with hash_algorithm column")
        else:
            logger.error("Failed to update database schema with hash_algorithm column after all attempts")
            # Force check again
            try:
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA table_info(file_hashes)")
                    final_columns = [column[1] for column in cursor.fetchall()]
                    logger.debug(f"Final columns: {final_columns}")
            except Exception as e:
                logger.error(f"Error during final schema check: {e}")

    except sqlite3.Error as exc:
        logger.error("SQLite error creating/updating schema in {}: {}", db_path, exc)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error creating/updating schema in {}: {}", db_path, exc)


def load_hashes_from_sqlite(db_path: str | Path, folder_path: Path) -> Dict[str, Dict[str, Any]]:
    """Load cached file hashes for a specific folder from the database.

    This function handles both old schema (without hash_algorithm column) and
    new schema databases, providing backward compatibility during migration.

    Args:
        db_path: Path to the SQLite database file.
        folder_path: The folder whose file hashes should be retrieved.

    Returns:
        Dictionary where keys are relative file paths and values are
        dictionaries containing 'hash', 'mtime', 'size', and optionally 'hash_algorithm'.

    Raises:
        sqlite3.Error: If there's an error querying the database.
    """

    db_path = str(db_path)
    cache: Dict[str, Dict[str, Any]] = {}

    if not os.path.exists(db_path):
        logger.warning("SQLite cache file not found at {}", db_path)
        return cache

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Check if the hash_algorithm column exists
            cursor.execute("PRAGMA table_info(file_hashes)")
            columns = [column[1] for column in cursor.fetchall()]
            has_hash_algorithm = "hash_algorithm" in columns

            logger.trace("Querying SQLite cache for folder: {}", folder_path)

            # Use different queries based on schema version
            if has_hash_algorithm:
                cursor.execute(
                    """
                    SELECT relative_path, file_hash, mtime, file_size, hash_algorithm
                    FROM file_hashes
                    WHERE folder_path = ?;
                    """,
                    (str(folder_path),),
                )
                for row in cursor.fetchall():
                    relative_path, file_hash, mtime, file_size, hash_algorithm = row
                    # Ensure hash length matches algorithm expectations
                    try:
                        validate_hash(file_hash, hash_algorithm or "md5")
                    except ValueError as ve:
                        logger.error(
                            "Invalid hash record for %s/%s in cache: %s. Skipping row.",
                            folder_path,
                            relative_path,
                            ve,
                        )
                        continue

                    cache[relative_path] = {
                        "hash": file_hash,
                        "mtime": mtime,
                        "size": file_size,
                        "hash_algorithm": hash_algorithm or "md5",  # Default to md5 if NULL for legacy records
                    }
            else:
                # Legacy schema without hash_algorithm column
                logger.info("Using legacy schema format (without hash_algorithm column)")
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
                    # Validate legacy md5 hashes
                    validate_hash(file_hash, "md5")

                    cache[relative_path] = {
                        "hash": file_hash,
                        "mtime": mtime,
                        "size": file_size,
                        "hash_algorithm": "md5",
                    }
    except sqlite3.Error as exc:
        logger.error("SQLite error loading hashes for {} from {}: {}", folder_path, db_path, exc)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error loading hashes for {} from {}: {}", folder_path, db_path, exc)

    return cache


def upsert_hash_to_sqlite(
    db_path: str | Path,
    folder_path: Path,
    relative_path: str,
    file_hash: str,
    mtime: float,
    file_size: int,
    hash_algorithm: str = "xxh64",
) -> None:
    """Insert or update a file hash record in the database.

    Args:
        db_path: Path to the SQLite database file.
        folder_path: Absolute path of the folder containing the file.
        relative_path: Path of the file relative to folder_path.
        file_hash: Hash of the file content (algorithm specified by hash_algorithm).
        mtime: File's modification timestamp.
        file_size: Size of the file in bytes.
        hash_algorithm: Algorithm used for hashing. Defaults to "xxh64" for new entries.
            Legacy entries will have "md5". This helps with transitioning from MD5 to XXHash64.

    Raises:
        sqlite3.Error: If there's an error updating the database.
    """

    db_path = str(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO file_hashes
                (file_hash, folder_path, relative_path, mtime, file_size, hash_algorithm)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (file_hash, str(folder_path), relative_path, mtime, file_size, hash_algorithm),
            )
    except sqlite3.Error as exc:
        logger.critical("SQLite upsert FAILED for {} in {}: {}", relative_path, db_path, exc)
        raise
    except Exception as exc:  # noqa: BLE001
        logger.critical("Unexpected error in upsert for {} in {}: {}", relative_path, db_path, exc)
        raise
