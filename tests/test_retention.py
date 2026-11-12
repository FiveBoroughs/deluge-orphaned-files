"""Behaviour tests for the retention/deletion lifecycle in logic.retention.

These exercise the public process_deletions interface end-to-end: real files on
disk, a real SQLite database, and status transitions recorded in the DB. The
schema created here is the minimal slice retention queries (the full schema is
owned by cli.init_sqlite_cache and covered by test_scan_persistence).
"""

from __future__ import annotations

import sqlite3

import pytest

from deluge_orphaned_files.logic.retention import process_deletions

_SCHEMA = """
CREATE TABLE orphaned_files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    consecutive_scans INTEGER NOT NULL DEFAULT 1,
    size_human TEXT,
    first_seen_at TEXT,
    last_seen_at TEXT,
    deletion_date TEXT
);
CREATE VIEW view_files_eligible_for_deletion AS
SELECT
    id AS file_id,
    path AS file_path,
    size_human AS file_size,
    first_seen_at,
    last_seen_at,
    consecutive_scans,
    julianday(last_seen_at) - julianday(first_seen_at) AS days_seen_difference
FROM orphaned_files
WHERE source = 'local_torrent_folder'
  AND status = 'active'
  AND consecutive_scans >= 4
  AND (julianday(last_seen_at) - julianday(first_seen_at)) >= 4;
"""


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "retention.db"
    with sqlite3.connect(str(path)) as conn:
        conn.executescript(_SCHEMA)
    return path


@pytest.fixture()
def torrent_base(tmp_path):
    base = tmp_path / "torrents"
    base.mkdir()
    return base


def _insert(db_path, path, *, status="marked_for_deletion", consecutive_scans=5, first_seen="2026-01-01T00:00:00", last_seen="2026-01-10T00:00:00"):
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "INSERT INTO orphaned_files (path, source, status, consecutive_scans, size_human, first_seen_at, last_seen_at) VALUES (?, 'local_torrent_folder', ?, ?, '1 MB', ?, ?)",
            (path, status, consecutive_scans, first_seen, last_seen),
        )


def _status(db_path, path):
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute("SELECT status FROM orphaned_files WHERE path = ?", (path,)).fetchone()
    return row[0] if row else None


def test_force_delete_removes_marked_file_in_subdir_and_records_deleted(db_path, torrent_base):
    target = torrent_base / "movies" / "movie.mkv"
    target.parent.mkdir()
    target.write_bytes(b"x")
    _insert(db_path, "movies/movie.mkv")

    process_deletions(force_delete=True, db_path=db_path, torrent_base_folder=torrent_base, scans_threshold=4)

    assert not target.exists()
    assert _status(db_path, "movies/movie.mkv") == "deleted"


def test_force_delete_records_vanished_when_file_already_gone(db_path, torrent_base):
    _insert(db_path, "gone.mkv")

    process_deletions(force_delete=True, db_path=db_path, torrent_base_folder=torrent_base, scans_threshold=4)

    assert _status(db_path, "gone.mkv") == "vanished"


def test_force_delete_skips_marked_file_below_scan_threshold(db_path, torrent_base):
    target = torrent_base / "young.mkv"
    target.write_bytes(b"x")
    _insert(db_path, "young.mkv", consecutive_scans=2)

    process_deletions(force_delete=True, db_path=db_path, torrent_base_folder=torrent_base, scans_threshold=4)

    assert target.exists()
    assert _status(db_path, "young.mkv") == "marked_for_deletion"


def test_force_delete_refuses_path_escaping_base_folder(db_path, torrent_base, tmp_path):
    outside = tmp_path / "outside.mkv"
    outside.write_bytes(b"x")
    _insert(db_path, "../outside.mkv")

    process_deletions(force_delete=True, db_path=db_path, torrent_base_folder=torrent_base, scans_threshold=4)

    assert outside.exists()
    assert _status(db_path, "../outside.mkv") == "marked_for_deletion"


def test_force_delete_removes_symlink_not_its_target(db_path, torrent_base, tmp_path):
    target = tmp_path / "real.mkv"
    target.write_bytes(b"x")
    link = torrent_base / "link.mkv"
    link.symlink_to(target)
    _insert(db_path, "link.mkv")

    process_deletions(force_delete=True, db_path=db_path, torrent_base_folder=torrent_base, scans_threshold=4)

    assert not link.is_symlink() and not link.exists()
    assert target.exists()
    assert _status(db_path, "link.mkv") == "deleted"


def test_dry_run_marks_only_eligible_active_files(db_path, torrent_base):
    _insert(db_path, "eligible.mkv", status="active", consecutive_scans=6)
    _insert(db_path, "too-new.mkv", status="active", consecutive_scans=6, first_seen="2026-01-09T00:00:00", last_seen="2026-01-10T00:00:00")
    _insert(db_path, "too-few-scans.mkv", status="active", consecutive_scans=2)

    process_deletions(force_delete=False, db_path=db_path, torrent_base_folder=torrent_base, scans_threshold=4)

    assert _status(db_path, "eligible.mkv") == "marked_for_deletion"
    assert _status(db_path, "too-new.mkv") == "active"
    assert _status(db_path, "too-few-scans.mkv") == "active"
