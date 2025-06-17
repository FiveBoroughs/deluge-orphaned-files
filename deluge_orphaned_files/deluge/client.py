"""Wrapper utilities to interact with a remote Deluge daemon (RPC).

This module keeps all *Deluge*-specific RPC logic isolated from the rest of the
application, making it easier to test and to eventually swap out the backend if
needed.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple, Set, TypedDict, Any, Protocol, TYPE_CHECKING

try:
    from deluge_client import DelugeRPCClient  # type: ignore
except ModuleNotFoundError:  # pragma: no cover – allow running without deluge-client installed
    DelugeRPCClient = None  # type: ignore
    import importlib
    import sys

    # Provide lightweight stub for type checkers
    if TYPE_CHECKING:
        # 'ModuleType' was previously imported explicitly but never used, leading to an F401 lint error.
        # Removing the unused import resolves the lint issue without affecting runtime behavior.
        pass

    # Insert dummy module so `import deluge_client` elsewhere won't fail after this point
    sys.modules.setdefault("deluge_client", importlib.import_module("types"))

from loguru import logger

__all__ = ["get_deluge_files"]


class DelugeConfig(Protocol):
    """Protocol defining the required configuration attributes for Deluge connection."""
    deluge_host: str
    deluge_port: int
    deluge_username: str
    deluge_password: str
    deluge_torrent_base_remote_folder: str

def get_deluge_files(config: DelugeConfig) -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    """Connect to Deluge and return information about torrent files.
    
    Args:
        config: An object exposing the Deluge connection settings and
            deluge_torrent_base_remote_folder attribute (typically the global
            AppConfig instance).
            
    Returns:
        A tuple containing:
            - Set of all file paths (relative to base folder)
            - Dictionary mapping file paths to their labels
            - Dictionary mapping file paths to their torrent IDs
            
    Raises:
        RuntimeError: If the deluge-client package is not installed.
    """

    if DelugeRPCClient is None:  # pragma: no cover
        raise RuntimeError("The 'deluge-client' package is required for Deluge RPC operations. Please install it (pip install deluge-client).")

    client = DelugeRPCClient(
        host=config.deluge_host,
        port=config.deluge_port,
        username=config.deluge_username,
        password=config.deluge_password,
    )
    client.connect()

    logger.debug("Connecting to Deluge: {}:{}", config.deluge_host, config.deluge_port)
    logger.info(
        "Fetching torrent list from {}@{}:{}...",
        config.deluge_username,
        config.deluge_host,
        config.deluge_port,
    )

    torrent_list = client.call("core.get_torrents_status", {}, ["files", "save_path", "label"])

    all_files: set[str] = set()
    file_labels: Dict[str, str] = {}
    file_torrent_ids: Dict[str, str] = {}

    norm_deluge_base_remote_folder = os.path.normpath(config.deluge_torrent_base_remote_folder)

    for torrent_id, torrent_data in torrent_list.items():
        # Ensure torrent_id is a string, not bytes
        torrent_id_str = torrent_id.decode() if isinstance(torrent_id, bytes) else torrent_id
        save_path = os.path.normpath(torrent_data[b"save_path"].decode())
        label = torrent_data.get(b"label", b"").decode() or "No Label"

        for file in torrent_data[b"files"]:
            file_path_in_torrent = os.path.normpath(file[b"path"].decode())
            full_path = os.path.normpath(os.path.join(save_path, file_path_in_torrent))
            relative_path = os.path.relpath(full_path, norm_deluge_base_remote_folder)

            # Safety check: ensure path does not escape base folder
            if ".." in Path(relative_path).parts:
                logger.warning(
                    "File '{}' appears outside base folder '{}'. Relative path: '{}'.",
                    full_path,
                    norm_deluge_base_remote_folder,
                    relative_path,
                )

            all_files.add(relative_path)
            file_labels[relative_path] = label
            file_torrent_ids[relative_path] = torrent_id_str

    return all_files, file_labels, file_torrent_ids
