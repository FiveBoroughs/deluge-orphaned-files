"""File hashing utilities used by the orphan-finder.

This module provides file hashing functionality with support for multiple hash algorithms.
It primarily uses XXHash64 for new hashes due to its superior performance compared to MD5,
which is especially beneficial for large media files. While supporting a transition period
where both MD5 and XXHash64 hashes may exist in the system, all new hashes will use XXHash64.

XXHash64 offers several advantages over MD5 for this application:
1. Much faster hashing speed (5-10x faster than MD5)
2. Lower CPU usage during hash computation
3. Still provides excellent hash distribution for file integrity checks

This module is intentionally standalone so it can be imported by any layer
without dragging in heavy application dependencies. It only relies on
`loguru` for logging, `tqdm` for optional progress bars, and `xxhash` for
fast non-cryptographic hashing.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Literal, Tuple

import xxhash
from loguru import logger
from tqdm import tqdm

# Expected hex digest lengths per algorithm
_HASH_LENGTHS: dict[str, int] = {"md5": 32, "xxh64": 16}


def validate_hash(hash_value: str, algorithm: str) -> None:
    """Raise ValueError if *hash_value* length does not match *algorithm* expectation."""
    expected = _HASH_LENGTHS.get(algorithm)
    if expected is None:
        raise ValueError(f"Unknown hash algorithm: {algorithm}")
    if len(hash_value) != expected:
        raise ValueError(f"Hash length mismatch: got {len(hash_value)} chars, expected {expected} for {algorithm}")


__all__ = ["get_file_hash", "get_file_hash_with_algorithm", "infer_algorithm_from_hash"]


def get_file_hash_with_algorithm(file_path: Path, algorithm: Literal["md5", "xxh64"] = "xxh64", no_progress: bool = False) -> Tuple[str, str]:
    """Return the hash of a file using the specified algorithm.

    The file is read in 8 MiB chunks so memory usage stays reasonable. When
    no_progress is False (default) a compact tqdm progress bar is shown.

    Args:
        file_path: Path to the file to hash.
        algorithm: Hash algorithm to use. Either "md5" or "xxh64" (default).
        no_progress: Whether to disable the progress bar. Defaults to False.
            Used for cron jobs or quiet mode.

    Returns:
        Tuple of (hash_digest, algorithm_name):
        - For xxh64: 16-character hexadecimal XXHash64 digest
        - For md5: 32-character hexadecimal MD5 digest

    Raises:
        FileNotFoundError: If the specified file does not exist.
        PermissionError: If the file cannot be read due to permissions.
        ValueError: If an unsupported algorithm is specified.
    """

    if algorithm not in ("md5", "xxh64"):
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

    file_size = os.path.getsize(file_path)
    chunk_size = 8 * 1024 * 1024  # 8 MiB

    logger.debug(f"Calculating {algorithm.upper()} hash for: {file_path}")

    if algorithm == "md5":
        hasher = hashlib.md5()
    else:  # algorithm == "xxh64"
        hasher = xxhash.xxh64()

    with open(file_path, "rb") as f, tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc=f"Hashing {file_path.name}",
        leave=False,
        disable=no_progress,
    ) as pbar:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
            pbar.update(len(chunk))

    digest = hasher.hexdigest()
    # Sanity-check length
    validate_hash(digest, algorithm)
    return digest, algorithm


def infer_algorithm_from_hash(hash_value: str) -> Literal["md5", "xxh64"]:
    """Infer the hashing algorithm based solely on *hash_value* length.

    Args:
        hash_value: Hexadecimal digest string.

    Returns:
        "xxh64" if the digest length matches the 16-char XXHash64 format, otherwise "md5" for 32-char MD5.

    Raises:
        ValueError: If *hash_value* length doesn't correspond to a supported algorithm.
    """

    length = len(hash_value)
    if length == _HASH_LENGTHS["xxh64"]:
        return "xxh64"
    if length == _HASH_LENGTHS["md5"]:
        return "md5"

    raise ValueError(f"Unable to infer hash algorithm from digest of length {length}. Supported lengths: {_HASH_LENGTHS}.")


def get_file_hash(file_path: Path, no_progress: bool = False) -> tuple[str, str]:
    """Return the XXHash64 hash of a file along with the algorithm used.

    This is a wrapper around get_file_hash_with_algorithm that always uses XXHash64.
    Unlike the original implementation, this returns both the hash value and algorithm
    to ensure hash algorithm information is preserved throughout the application.

    The file is read in 8 MiB chunks so memory usage stays reasonable. When
    no_progress is False (default) a compact tqdm progress bar is shown.

    Args:
        file_path: Path to the file to hash.
        no_progress: Whether to disable the progress bar. Defaults to False.
            Used for cron jobs or quiet mode.

    Returns:
        Tuple of (hash_digest, algorithm_name):
        - hash_digest: 16-character hexadecimal XXHash64 digest
        - algorithm_name: String 'xxh64'

    Raises:
        FileNotFoundError: If the specified file does not exist.
        PermissionError: If the file cannot be read due to permissions.
    """

    return get_file_hash_with_algorithm(file_path, "xxh64", no_progress)
