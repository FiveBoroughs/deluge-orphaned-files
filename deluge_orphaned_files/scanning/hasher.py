"""MD5 hashing utilities used by the orphan-finder.

This module is intentionally standalone so it can be imported by any layer
without dragging in heavy application dependencies. It only relies on
`loguru` for logging and `tqdm` for optional progress bars.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from loguru import logger
from tqdm import tqdm

__all__ = ["get_file_hash"]


def get_file_hash(file_path: Path, no_progress: bool = False) -> str:  # noqa: D401 – docstring filled below
    """Return the MD5 hash of a file.

    The file is read in 8 MiB chunks so memory usage stays reasonable.  When
    no_progress is False (default) a compact tqdm progress bar is shown.

    Args:
        file_path: Path to the file to hash.
        no_progress: Whether to disable the progress bar. Defaults to False.
            Used for cron jobs or quiet mode.

    Returns:
        32-character hexadecimal MD5 digest.

    Raises:
        FileNotFoundError: If the specified file does not exist.
        PermissionError: If the file cannot be read due to permissions.
    """

    md5_hash = hashlib.md5()
    file_size = os.path.getsize(file_path)
    chunk_size = 8 * 1024 * 1024  # 8 MiB

    logger.debug(f"Calculating MD5 hash for: {file_path}")

    with open(file_path, "rb") as f, tqdm(
        total=file_size,
        unit="B",
        unit_scale=True,
        desc=f"Hashing {file_path.name}",
        leave=False,
        disable=no_progress,
    ) as pbar:
        while chunk := f.read(chunk_size):
            md5_hash.update(chunk)
            pbar.update(len(chunk))

    return md5_hash.hexdigest()
