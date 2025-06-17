"""SQLite-based hash cache helpers.

This module centralises all interactions with the file_hashes table used to
cache MD5 hashes for local files. It was extracted from the monolithic
cli.py during the ongoing refactor.

Provides functions for initializing the database schema, loading cached file hashes,
and upserting hash records to the database.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Dict, Any

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
        PRIMARY KEY (folder_path, relative_path)
    );
    """

_SCHEMA_INDEX_FOLDER_PATH = "CREATE INDEX IF NOT EXISTS idx_file_hashes_folder_path ON file_hashes (folder_path);"


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------


def init_sqlite_cache(db_path: str | Path) -> None:
    """Create database and required tables if they do not exist.
    
    Args:
        db_path: Path to the SQLite database file.
        
    Raises:
        sqlite3.Error: If there's an error creating the schema.
    """

    db_path = str(db_path)
    if not os.path.exists(db_path):
        logger.trace("SQLite database will be created at {}", db_path)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            logger.trace("Ensuring 'file_hashes' table exists in the SQLite cache.")
            cursor.execute(_SCHEMA_FILE_HASHES)
            cursor.execute(_SCHEMA_INDEX_FOLDER_PATH)
    except sqlite3.Error as exc:
        logger.error("SQLite error creating schema in {}: {}", db_path, exc)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error creating schema in {}: {}", db_path, exc)


def load_hashes_from_sqlite(db_path: str | Path, folder_path: Path) -> Dict[str, Dict[str, Any]]:
    """Load cached file hashes for a specific folder from the database.
    
    Args:
        db_path: Path to the SQLite database file.
        folder_path: The folder whose file hashes should be retrieved.
        
    Returns:
        Dictionary where keys are relative file paths and values are
        dictionaries containing 'hash', 'mtime', and 'size' fields.
        
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
            logger.trace("Querying SQLite cache for folder: {}", folder_path)
            cursor.execute(
                """
                SELECT relative_path, file_hash, mtime, file_size
                FROM file_hashes
                WHERE folder_path = ?;
                """,
                (str(folder_path),),
            )
            for relative_path, file_hash, mtime, file_size in cursor.fetchall():
                cache[relative_path] = {
                    "hash": file_hash,
                    "mtime": mtime,
                    "size": file_size,
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
) -> None:
    """Insert or update a file hash record in the database.
    
    Args:
        db_path: Path to the SQLite database file.
        folder_path: Absolute path of the folder containing the file.
        relative_path: Path of the file relative to folder_path.
        file_hash: MD5 hash of the file content.
        mtime: File's modification timestamp.
        file_size: Size of the file in bytes.
        
    Raises:
        sqlite3.Error: If there's an error updating the database.
    """

    db_path = str(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO file_hashes
                (file_hash, folder_path, relative_path, mtime, file_size)
                VALUES (?, ?, ?, ?, ?)
                """,
                (file_hash, str(folder_path), relative_path, mtime, file_size),
            )
    except sqlite3.Error as exc:
        logger.error("SQLite upsert error for {} in {}: {}", relative_path, db_path, exc)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error in upsert for {} in {}: {}", relative_path, db_path, exc)
