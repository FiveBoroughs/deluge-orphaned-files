"""Tests for save_scan_results_to_db's file_hash preservation.

An inode-mode scan does no hashing, so its file_hashes lookups miss and the resolved
hash is ''. The UPDATE paths must not overwrite a hash stored by an earlier hash-mode
run with that empty string (prod carries 13k+ cached hashes worth keeping).

cli.py builds its AppConfig singleton at import time from the environment, so the
module fixture points the required path settings at a temp directory *before* the
first import (environment variables outrank the repo's .env in pydantic-settings).
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime

import pytest


@pytest.fixture(scope="module")
def cli(tmp_path_factory):
    """Import deluge_orphaned_files.cli with a valid throwaway environment."""
    base = tmp_path_factory.mktemp("cli-env")
    (base / "torrents").mkdir()
    (base / "media").mkdir()
    env = {
        "DELUGE_HOST": "localhost",
        "DELUGE_PORT": "58846",
        "DELUGE_USERNAME": "test",
        "DELUGE_PASSWORD": "test",
        "DELUGE_TORRENT_BASE_REMOTE_FOLDER": "/data/torrents",
        "LOCAL_TORRENT_BASE_LOCAL_FOLDER": str(base / "torrents"),
        "LOCAL_MEDIA_BASE_LOCAL_FOLDER": str(base / "media"),
        "OUTPUT_FILE": str(base / "orphaned_files.json"),
        "APP_SQLITE_CACHE_PATH": str(base / "orphaned_files.db"),
        "APP_LOG_DIR": str(base / "logs"),
    }
    saved = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        from deluge_orphaned_files import cli as cli_module

        yield cli_module
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _stored(db_path, path, source):
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT file_hash, consecutive_scans FROM orphaned_files WHERE path = ? AND source = ?",
            (path, source),
        ).fetchone()


def _stored_retention_state(db_path, path):
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            "SELECT status, consecutive_scans FROM orphaned_files WHERE path = ? AND source = 'local_torrent_folder'",
            (path,),
        ).fetchone()


def _set_cached_hash(db_path, folder, rel_path, file_hash):
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM file_hashes WHERE folder_path = ? AND relative_path = ?", (str(folder), rel_path))
        if file_hash is not None:
            conn.execute(
                "INSERT INTO file_hashes (file_hash, folder_path, relative_path, mtime, file_size) VALUES (?, ?, ?, 0, 0)",
                (file_hash, str(folder), rel_path),
            )


def test_empty_resolved_hash_does_not_clobber_stored_hash(cli, tmp_path, monkeypatch):
    db_path = tmp_path / "scan.db"
    monkeypatch.setattr(cli.config, "sqlite_cache_path", db_path)
    cli.init_sqlite_cache(db_path)

    torrent_folder = cli.config.local_torrent_base_local_folder
    (torrent_folder / "movie.mkv").write_bytes(b"x")
    entry = {"path": "movie.mkv", "label": "movies", "size": 1, "size_human": "1 MB", "torrent_id": "t1"}

    # Scan 1: hash-mode run — the cache resolves a real hash, which is stored.
    _set_cached_hash(db_path, torrent_folder, "movie.mkv", "cafebabe")
    cli.save_scan_results_to_db([], [entry], [], datetime.now())
    assert _stored(db_path, "movie.mkv", "torrents") == ("cafebabe", 1)

    # Scan 2: inode-mode run — no hashing happened, the cache lookup misses.
    _set_cached_hash(db_path, torrent_folder, "movie.mkv", None)
    cli.save_scan_results_to_db([], [entry], [], datetime.now())
    assert _stored(db_path, "movie.mkv", "torrents") == ("cafebabe", 2)

    # Scan 3: hash-mode again with a changed file — the new hash must still win.
    _set_cached_hash(db_path, torrent_folder, "movie.mkv", "deadbeef")
    cli.save_scan_results_to_db([], [entry], [], datetime.now())
    assert _stored(db_path, "movie.mkv", "torrents") == ("deadbeef", 3)


def test_hash_preserved_for_orphan_and_media_sources_too(cli, tmp_path, monkeypatch):
    db_path = tmp_path / "scan.db"
    monkeypatch.setattr(cli.config, "sqlite_cache_path", db_path)
    cli.init_sqlite_cache(db_path)

    torrent_folder = cli.config.local_torrent_base_local_folder
    media_folder = cli.config.local_media_base_local_folder
    (torrent_folder / "orphan.mkv").write_bytes(b"x")
    (media_folder / "media.mkv").write_bytes(b"x")
    orphan = {"path": "orphan.mkv", "size": 1, "size_human": "1 MB"}
    media = {"path": "media.mkv", "size": 1, "size_human": "1 MB"}

    _set_cached_hash(db_path, torrent_folder, "orphan.mkv", "aaaa1111")
    _set_cached_hash(db_path, media_folder, "media.mkv", "bbbb2222")
    cli.save_scan_results_to_db([orphan], [], [media], datetime.now())

    _set_cached_hash(db_path, torrent_folder, "orphan.mkv", None)
    _set_cached_hash(db_path, media_folder, "media.mkv", None)
    cli.save_scan_results_to_db([orphan], [], [media], datetime.now())

    assert _stored(db_path, "orphan.mkv", "local_torrent_folder") == ("aaaa1111", 2)
    assert _stored(db_path, "media.mkv", "media") == ("bbbb2222", 2)


def test_scan_preserves_current_deletion_marks_and_clears_stale_ones(cli, tmp_path, monkeypatch):
    """A force scan must delete only a previously marked file that is still orphaned.

    A marked row that remains in the current orphan set must keep its mark. A marked row
    absent from the current set (for example because Deluge reacquired it) must become
    inactive before the deletion gate runs.
    """
    db_path = tmp_path / "retention-state.db"
    monkeypatch.setattr(cli.config, "sqlite_cache_path", db_path)
    cli.init_sqlite_cache(db_path)

    current = {"path": "current-orphan.mkv", "size": 1, "size_human": "0.00 MB"}
    reacquired = {"path": "reacquired-by-deluge.mkv", "size": 1, "size_human": "0.00 MB"}

    cli.save_scan_results_to_db([current, reacquired], [], [], datetime.now())
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE orphaned_files SET status = 'marked_for_deletion' WHERE source = 'local_torrent_folder'")

    # The next scan still considers one path orphaned; the other is no longer an orphan.
    cli.save_scan_results_to_db([current], [], [], datetime.now())

    assert _stored_retention_state(db_path, "current-orphan.mkv") == ("marked_for_deletion", 2)
    assert _stored_retention_state(db_path, "reacquired-by-deluge.mkv") == ("inactive", 0)


def test_force_deletion_cycle_deletes_old_marks_then_marks_current_candidates(cli, tmp_path, monkeypatch):
    """The always-force production schedule must still prepare work for its next run."""
    calls = []

    def record_call(*, force_delete, db_path, torrent_base_folder):
        calls.append((force_delete, db_path, torrent_base_folder))

    monkeypatch.setattr(cli, "process_deletions", record_call)

    db_path = tmp_path / "cycle.db"
    torrent_base = tmp_path / "torrents"
    cli._process_deletion_cycle(force_delete=True, db_path=db_path, torrent_base_folder=torrent_base)

    assert calls == [
        (True, db_path, torrent_base),
        (False, db_path, torrent_base),
    ]
