"""File filtering helpers for scanning operations.

Currently contains:
    * should_process_file – centralised logic to decide whether a file is worth
      hashing / analysing.
    * load_hash_cache – load a JSON hash cache from a file.
    * save_hash_cache – persist a hash cache to a file as JSON.
    * get_local_files – scan a folder and return a mapping of relative paths to hashes.

The functions are designed to be *pure* and environment-agnostic; everything they
need must be provided via parameters, which keeps the `scanning` sub-package
independent from higher-level CLI concerns.
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Protocol
import sqlite3
from loguru import logger
from tqdm import tqdm
from ..database.hash_cache import load_hashes_from_sqlite  # import centralised helper
from .hasher import get_file_hash


class AppConfigLike(Protocol):  # pragma: no cover – simple structural helper
    """Minimal subset of :class:`cli.AppConfig` required here."""

    extensions_blacklist: list[str]
    min_file_size_mb: int
    local_subfolders_blacklist: list[str]
    sqlite_cache_path: Path


__all__ = [
    "should_process_file",
    "load_hash_cache",
    "load_hashes_from_sqlite",
    "save_hash_cache",
    "get_local_files",
]


def should_process_file(filepath: Path, stat_result: os.stat_result, config: AppConfigLike) -> bool:  # noqa: D401 – detailed docstring below
    """Determine if a file should be processed based on configured criteria.

    Args:
        filepath: Path of the file being evaluated for processing.
        stat_result: Result of os.stat() for the file (already obtained by the
            caller), to avoid double system calls.
        config: Any object that implements the AppConfigLike protocol with
            the required configuration attributes.

    Returns:
        True if the file should be processed, False otherwise.
    """

    # Extension / filename blacklist (case-insensitive)
    if filepath.suffix.lower() in config.extensions_blacklist or filepath.name in config.extensions_blacklist:
        logger.trace("Skipping {} due to extension/name blacklist.", filepath.name)
        return False

    # Ignore obvious samples / extras by substring in path (lower-case search)
    # Normalize path separators for cross-platform compatibility
    path_lower = str(filepath).lower().replace(os.sep, "/")
    if any(pattern in path_lower for pattern in ("/sample", "/featurettes", "/extras", ".sample", "-sample")):
        logger.trace("Skipping {} due to sample/featurette pattern.", filepath.name)
        return False

    # Minimum size filter
    if stat_result.st_size < config.min_file_size_mb * 1024 * 1024:
        logger.trace(
            "Skipping {} because {} < {} MB",
            filepath.name,
            stat_result.st_size,
            config.min_file_size_mb,
        )
        return False

    return True


def load_hash_cache(cache_file: Path) -> dict[str, dict[str, object]]:
    """Load a JSON hash cache from a file.

    Args:
        cache_file: Path to the JSON cache file.

    Returns:
        Dictionary of cached hash data, or an empty dict if the file doesn't
        exist or cannot be parsed.
    """
    logger.debug("Loading cache from: {}", cache_file)

    if not cache_file.exists():
        logger.warning("Cache file not found: {}", cache_file)
        return {}

    try:
        with cache_file.open("r") as fh:
            cache: dict[str, dict[str, object]] = json.load(fh)
        logger.info("Loaded {} cache entries from {}", len(cache), cache_file)
        return cache
    except Exception as exc:  # noqa: BLE001 – we want to log any error
        logger.error("Error loading cache {}: {}", cache_file, exc)
        return {}


def save_hash_cache(cache_file: Path, hash_cache: dict[str, dict[str, object]]) -> None:  # noqa: D401 – simple util
    """Save hash cache data to a JSON file.

    Args:
        cache_file: Path where the cache will be saved.
        hash_cache: Dictionary of hash data to persist.

    Raises:
        Exception: Any error that occurs during saving.
    """

    logger.debug("Saving {} entries to hash cache {}", len(hash_cache), cache_file)
    try:
        with cache_file.open("w") as fh:
            json.dump(hash_cache, fh)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error saving cache {}: {}", cache_file, exc)


def _upsert_hash_to_sqlite(conn: sqlite3.Connection, folder_path: Path, relative_path: str, file_hash: str, mtime: float, file_size: int) -> None:  # noqa: D401 – internal util
    """Insert or replace a row in the file_hashes table.

    Args:
        conn: SQLite database connection.
        folder_path: Absolute path of the scanned folder.
        relative_path: Path relative to folder_path.
        file_hash: MD5 hash of the file.
        mtime: Modification timestamp of the file.
        file_size: Size of the file in bytes.
    """

    conn.execute(
        """
        INSERT OR REPLACE INTO file_hashes
        (file_hash, folder_path, relative_path, mtime, file_size)
        VALUES (?, ?, ?, ?, ?)
        """,
        (file_hash, str(folder_path), relative_path, mtime, file_size),
    )


def get_local_files(
    folder: Path | str,
    config: AppConfigLike,
    use_sqlite: bool = False,
    no_progress: bool = False,
) -> dict[str, dict[str, object]]:
    """Scan a folder and build a mapping of file paths to hash/size information.

    Uses a two-phase approach: first gathering eligible files, then processing
    them with support for caching to avoid rehashing unchanged files.

    Args:
        folder: The directory to scan.
        config: Configuration object implementing AppConfigLike protocol.
        use_sqlite: Whether to use SQLite for hash caching instead of JSON.
        no_progress: Whether to disable progress bars for this scan.

    Returns:
        Dictionary where keys are relative file paths and values are
        dictionaries containing 'hash' and 'size' fields.

    Raises:
        FileNotFoundError: If files disappear during scanning.
        sqlite3.Error: If using SQLite and database operations fail.
    """

    folder = Path(folder)
    local_files: dict[str, dict[str, object]] = {}

    # Choose cache backend
    if use_sqlite:
        hash_cache = load_hashes_from_sqlite(str(config.sqlite_cache_path), folder)  # type: ignore[attr-defined]
    else:
        cache_file = folder / ".hash_cache.json"
        hash_cache = load_hash_cache(cache_file)

    # Phase 1 – gather eligible files quickly (single stat per file)
    paths_with_stats: list[tuple[str, os.stat_result]] = []
    for root, dirs, files in os.walk(folder):
        current_path = Path(root)
        rel_root = current_path.relative_to(folder)

        # Skip blacklisted top-level subfolders only (matches previous behaviour)
        if rel_root.parts and rel_root.parts[0] in config.local_subfolders_blacklist:  # type: ignore[attr-defined]
            logger.trace("Skipping blacklisted directory {}", rel_root.parts[0])
            dirs[:] = []
            continue

        for filename in files:
            full_path = current_path / filename
            try:
                st = full_path.stat()
                if should_process_file(full_path, st, config):
                    paths_with_stats.append((str(full_path), st))
            except FileNotFoundError:
                logger.warning("File disappeared during scan: {}", full_path)
            except Exception as exc:  # noqa: BLE001
                logger.error("Error stating file {}: {}", full_path, exc)

    # Phase 2 – process eligible files
    new_hashes = 0
    sqlite_batch: list[tuple[str, str, str, float, int]] = []

    with tqdm(total=len(paths_with_stats), desc=f"Processing {folder.name}", disable=no_progress) as bar:
        for full_path_str, st in paths_with_stats:
            mtime = st.st_mtime
            rel_path = os.path.relpath(full_path_str, folder)

            cached = hash_cache.get(rel_path)
            # Use tolerance for float comparison to avoid precision issues
            cache_hit = bool(cached and cached.get("mtime") and abs(cached.get("mtime") - mtime) < 2)

            if cache_hit:
                file_hash = cached["hash"]
            else:
                file_hash = get_file_hash(Path(full_path_str), no_progress=no_progress)
                new_hashes += 1

                # Update in-memory cache
                hash_cache[rel_path] = {"hash": file_hash, "mtime": mtime}

                if use_sqlite:
                    sqlite_batch.append((file_hash, str(folder), rel_path, mtime, st.st_size))

            local_files[rel_path] = {"hash": file_hash, "size": st.st_size}

            bar.update(1)

    # Persist cache changes
    if use_sqlite and sqlite_batch:
        try:
            with sqlite3.connect(str(config.sqlite_cache_path)) as conn:  # type: ignore[attr-defined]
                conn.executemany(
                    "INSERT OR REPLACE INTO file_hashes (file_hash, folder_path, relative_path, mtime, file_size) VALUES (?, ?, ?, ?, ?)",
                    sqlite_batch,
                )
        except sqlite3.Error as exc:
            logger.error("SQLite batch upsert error for {}: {}", folder, exc)
    elif not use_sqlite:
        cache_file = folder / ".hash_cache.json"
        save_hash_cache(cache_file, hash_cache)

    logger.info("Finished scanning {}: {} new hashes ({} files total)", folder.name, new_hashes, len(local_files))

    return local_files
