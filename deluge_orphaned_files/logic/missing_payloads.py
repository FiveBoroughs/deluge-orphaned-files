"""Reconcile Deluge registrations independently of the legacy file scan/queue."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import time
from contextlib import ExitStack, closing
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from deluge_client import DelugeRPCClient

    from ..settings import AppConfig


_ROOT_MARKER = ".delugeof-root"
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_INFOHASH = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_STATUS_FIELDS = ("state", "is_finished", "is_seed", "progress", "download_payload_rate", "time_added", "save_path", "num_files", "files")
_TABLE = "missing_payload_candidates"
_SCHEMA = """
CREATE TABLE IF NOT EXISTS missing_payload_candidates (
    infohash TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    observations INTEGER NOT NULL,
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL
)
"""


class _Uncertain(ValueError):
    """This result is not reliable evidence of absence."""


def _text(value: object) -> str:
    try:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        if isinstance(value, str):
            value.encode("utf-8")  # Reject lone surrogates rather than probing a different filename.
            return value
    except UnicodeError as exc:
        raise _Uncertain("invalid UTF-8 in Deluge status") from exc
    raise _Uncertain("expected text in Deluge status")


def _fields(raw: object) -> dict:
    if not isinstance(raw, dict):
        raise _Uncertain("expected a Deluge status dictionary")
    fields = {_text(key): value for key, value in raw.items()}
    if len(fields) != len(raw):
        raise _Uncertain("duplicate Deluge status field")
    return fields


def _path_parts(value: object, *, absolute: bool) -> tuple[str, ...]:
    text = _text(value)
    if text.startswith("/") != absolute or "\0" in text or "\\" in text:
        raise _Uncertain(f"invalid path {text!r}")
    parts = text.split("/")
    if absolute:
        parts = parts[1:]
        if parts[-1] == "":
            parts.pop()  # Deluge may return a save directory with a trailing slash.
    if any(part in {"", ".", ".."} for part in parts):
        raise _Uncertain(f"invalid path {text!r}")
    return tuple(parts)


@dataclass(frozen=True)
class _Root:
    remote: tuple[str, ...]
    local: Path
    fd: int
    identity: tuple[int, int]

    def verify(self) -> None:
        # A detached mount can still be read through the old directory FD. Verify
        # that the configured path still leads to that marked directory as well.
        current = self.local.lstat()
        if (current.st_dev, current.st_ino) != self.identity or not stat.S_ISDIR(current.st_mode):
            raise _Uncertain(f"mapped root changed: {self.local}")
        marker = os.stat(_ROOT_MARKER, dir_fd=self.fd, follow_symlinks=False)
        if not stat.S_ISREG(marker.st_mode) or marker.st_dev != current.st_dev:
            raise _Uncertain(f"invalid mount marker in {self.local}")

    def missing(self, path: tuple[str, ...]) -> bool:
        # Walk relative to the verified root without following symlinks. Only
        # ENOENT is absence; ENOTDIR, EACCES, ESTALE, EIO, etc. stay unknown.
        fd = self.fd
        try:
            for part in path[:-1]:
                child = os.open(part, _DIR_FLAGS, dir_fd=fd)
                if fd != self.fd:
                    os.close(fd)
                fd = child
                if os.fstat(fd).st_dev != self.identity[0]:
                    raise _Uncertain(f"payload crosses an unverified filesystem under {self.local}")
            os.stat(path[-1], dir_fd=fd, follow_symlinks=False)
            return False
        except FileNotFoundError:
            return True
        finally:
            if fd != self.fd:
                os.close(fd)


def _open_roots(config: AppConfig, stack: ExitStack) -> tuple[_Root, _Root]:
    if not config.deluge_media_base_remote_folder:
        raise _Uncertain("set DELUGE_MEDIA_BASE_REMOTE_FOLDER for missing-payload reconciliation")
    remote_torrents = _path_parts(config.deluge_torrent_base_remote_folder, absolute=True)
    remote_media = _path_parts(config.deluge_media_base_remote_folder, absolute=True)
    parent, child = sorted((remote_torrents, remote_media), key=len)
    depth = len(parent)
    if child[:depth] == parent:
        raise _Uncertain("Deluge torrent and media roots must not overlap")

    def open_root(remote: tuple[str, ...], local: Path) -> _Root:
        local = local.absolute()
        try:
            fd = os.open(local, _DIR_FLAGS)
            stack.callback(os.close, fd)
            st = os.fstat(fd)
            root = _Root(remote, local, fd, (st.st_dev, st.st_ino))
            root.verify()
        except OSError as exc:
            raise _Uncertain(f"cannot verify {local}/{_ROOT_MARKER}: {exc}") from exc
        return root

    return (
        open_root(remote_torrents, config.local_torrent_base_local_folder),
        open_root(remote_media, config.local_media_base_local_folder),
    )


def _call(client: DelugeRPCClient, method: str, *args):
    try:
        return client.call(method, *args)
    except Exception as exc:
        raise _Uncertain(f"{method} failed: {exc}") from exc


def _inventory(client: DelugeRPCClient) -> dict:
    torrents = _fields(_call(client, "core.get_torrents_status", {}, list(_STATUS_FIELDS)))
    if any(_INFOHASH.fullmatch(infohash) is None for infohash in torrents):
        raise _Uncertain("malformed Deluge inventory infohash")
    return torrents


def _missing_signature(raw: object, roots: tuple[_Root, _Root], daemon: tuple[str, int]) -> str | None:
    status = _fields(raw)
    # Deluge 2.2 explicitly reports Moving/Checking/Downloading. A paused
    # torrent must also still be a libtorrent seed, not merely show 100%.
    if _text(status.get("state")) not in {"Seeding", "Paused"}:
        return None
    finished, seed = status.get("is_finished"), status.get("is_seed")
    progress, rate = status.get("progress"), status.get("download_payload_rate")
    if type(finished) is not bool or type(seed) is not bool or type(progress) not in {int, float} or type(rate) is not int:
        raise _Uncertain("invalid completion or transfer status")
    if not finished or not seed or progress != 100 or rate != 0:
        return None

    added = status.get("time_added")
    files = status.get("files")
    count = status.get("num_files")
    if type(added) is not int or added <= 0 or type(count) is not int or not isinstance(files, (list, tuple)) or not files or len(files) != count:
        raise _Uncertain("missing registration timestamp or incomplete file inventory")
    paths = sorted(_path_parts(_fields(file).get("path"), absolute=False) for file in files)
    if any(current[: len(previous)] == previous for previous, current in zip(paths, paths[1:])):
        raise _Uncertain("duplicate or conflicting payload paths")

    save = _path_parts(status.get("save_path"), absolute=True)
    root = next((r for r in roots if save[: len(r.remote)] == r.remote), None)
    if root is None:
        return None  # A confirmed move outside the roots breaks the streak.
    root_depth = len(root.remote)
    directory = save[root_depth:]
    if not all(root.missing(directory + path) for path in paths):
        return None
    # Reset grace if the registration was re-added, moved/remapped, or its
    # payload layout changed. Sorting above avoids order-only changes.
    identity = (daemon, added, root.remote, str(root.local), root.identity, save, paths)
    return hashlib.sha256(json.dumps(identity).encode()).hexdigest()


def _load_candidates(conn: sqlite3.Connection) -> dict[str, tuple[str, int, float, float]]:
    if not conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (_TABLE,)).fetchone():
        return {}
    return {
        infohash: (signature, observations, first_seen, last_seen)
        for infohash, signature, observations, first_seen, last_seen in conn.execute("SELECT infohash, signature, observations, first_seen, last_seen FROM missing_payload_candidates")
    }


def _forget(conn: sqlite3.Connection, infohash: str) -> None:
    conn.execute("DELETE FROM missing_payload_candidates WHERE infohash = ?", (infohash,))


def _process_observation(
    config: AppConfig,
    client: DelugeRPCClient,
    roots: tuple[_Root, _Root],
    *,
    execute: bool,
    dry_run: bool,
    now: Callable[[], float],
    stack: ExitStack,
) -> None:
    db_path = config.sqlite_cache_path
    conn = None
    if dry_run:
        if db_path.exists():
            conn = stack.enter_context(closing(sqlite3.connect(db_path.absolute().as_uri() + "?mode=ro", uri=True)))
    else:
        conn = stack.enter_context(closing(sqlite3.connect(db_path)))
        stack.enter_context(conn)
        # Serialize writers, including removals, irrespective of the scheduler.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(_SCHEMA)

    inventory = _inventory(client)
    daemon = (config.deluge_host, config.deluge_port)
    observed: dict[str, str | None] = {}
    for infohash, status in inventory.items():
        try:
            observed[infohash] = _missing_signature(status, roots, daemon)
        except (OSError, _Uncertain) as exc:
            logger.warning("Skipping uncertain payload status for {}: {}", infohash, exc)
    for root in roots:
        root.verify()
    observed_at = now()
    previous = _load_candidates(conn) if conn is not None else {}

    if conn is not None and not dry_run:
        for infohash in previous:
            if infohash not in inventory or (infohash in observed and observed[infohash] is None):
                _forget(conn, infohash)

    minimum_observations = max(2, config.deletion_consecutive_scans_threshold)
    grace_seconds = max(0, config.deletion_days_threshold) * 86_400
    for infohash, signature in observed.items():
        if signature is None:
            continue
        old = previous.get(infohash)
        if old is not None and observed_at <= old[3]:
            continue  # Never refresh grace or double-count if the clock has not advanced.
        observations, first_seen = (old[1] + 1, old[2]) if old and signature == old[0] else (1, observed_at)
        due = observations >= minimum_observations and observed_at - first_seen >= grace_seconds
        logger.info(
            "{}Missing payload for {}: {}/{} observations, {:.2f}/{:.2f} days{}",
            "Dry run: " if dry_run else "",
            infohash,
            observations,
            minimum_observations,
            (observed_at - first_seen) / 86_400,
            grace_seconds / 86_400,
            " (would revalidate for registration-only removal)" if dry_run and execute and due else "",
        )
        if dry_run:
            continue
        assert conn is not None
        if execute and due:
            try:
                fresh = _call(client, "core.get_torrent_status", infohash, list(_STATUS_FIELDS))
                fresh_signature = _missing_signature(fresh, roots, daemon)
                for root in roots:
                    root.verify()
            except (OSError, _Uncertain) as exc:
                logger.warning("Cannot revalidate missing payload for {}: {}", infohash, exc)
                continue
            if fresh_signature != signature:
                logger.info("Payload or eligibility changed for {}; clearing missing-payload candidate", infohash)
                _forget(conn, infohash)
                continue
            try:
                removed = _call(client, "core.remove_torrent", infohash, False)
            except _Uncertain as exc:
                logger.warning("Could not remove Deluge registration {}: {}", infohash, exc)
            else:
                if removed is True:
                    _forget(conn, infohash)
                    logger.info("Removed Deluge registration {} (remove_data=False)", infohash)
                    continue
                logger.warning("Deluge did not confirm removal of {} (response: {!r})", infohash, removed)

        conn.execute(
            "INSERT OR REPLACE INTO missing_payload_candidates (infohash, signature, observations, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
            (infohash, signature, observations, first_seen, observed_at),
        )


def reconcile_missing_payloads(
    config: AppConfig,
    client: DelugeRPCClient,
    *,
    execute: bool,
    dry_run: bool,
    now: Callable[[], float] = time.time,
) -> None:
    """Observe all Deluge infohashes once and, if enabled, retire due registrations."""
    try:
        with ExitStack() as stack:
            roots = _open_roots(config, stack)
            _process_observation(config, client, roots, execute=execute, dry_run=dry_run, now=now, stack=stack)
    except (OSError, sqlite3.Error, _Uncertain) as exc:
        logger.warning("Skipping missing-payload reconciliation: {}", exc)
