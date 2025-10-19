#!/usr/bin/env python3
"""
Smoke test for deluge_orphaned_files.logic.autoremove

Creates a temporary SQLite DB with minimal schema and data, mocks a Deluge
client to simulate cross-seed content groups, and runs process_autoremove_labeling
in both dry-run and live modes to verify cross-seed coordination records pending
actions for clone torrents.
"""

from __future__ import annotations

import os
import sys
import sqlite3
from pathlib import Path


def ensure_tmp_db(db_path: Path, scan_id: int) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.cursor()
        # Minimal schema to satisfy _get_autoremove_candidates
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orphaned_files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                torrent_id TEXT,
                size INTEGER,
                size_human TEXT,
                label TEXT,
                source TEXT,
                status TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS file_scan_history (
                file_id INTEGER,
                scan_id INTEGER
            );
            """
        )

        # Reset any existing rows for id=1
        cur.execute("DELETE FROM orphaned_files WHERE id = 1;")
        cur.execute("DELETE FROM file_scan_history WHERE file_id = 1;")

        # Insert a single candidate file from torrents source without the target label
        cur.execute(
            """
            INSERT INTO orphaned_files (id, path, torrent_id, size, size_human, label, source, status)
            VALUES (1, ?, 'aaaa1111', 104857600, '100 MB', NULL, 'torrents', 'active');
            """,
            ("Movies/Movie.Title.2020/Movie.Title.2020.mkv",),
        )
        cur.execute(
            "INSERT INTO file_scan_history (file_id, scan_id) VALUES (1, ?);",
            (scan_id,),
        )
        conn.commit()


class _Core:
    def get_torrents_status(self, _filter, _keys):
        # Return mapping-like so iteration yields torrent_ids
        return {"aaaa1111": {}, "bbbb2222": {}}

    def get_torrent_status(self, tid, keys):
        # Minimal fields used by the code
        if tid == "aaaa1111":
            return {
                "name": "Movie.Title.2020.1080p.WEB-DL",
                "label": "",
                "state": "Seeding",
                "files": [{"path": "Movies/Movie.Title.2020/Movie.Title.2020.mkv"}],
            }
        else:
            # Cross-seed with same content path, has cross-seed style label
            return {
                "name": "Movie.Title.2020.720p.BluRay",
                "label": "movies.cross-seed",
                "state": "Seeding",
                "files": [{"path": "Movies/Movie.Title.2020/Movie.Title.2020.mkv"}],
            }


class FakeDelugeClient:
    def __init__(self):
        self.connected = True
        self.core = _Core()


def main() -> int:
    # Ensure package root is importable when running from scripts/
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    # Configure environment for the package to load
    os.environ.setdefault("DOF_RELABEL_ACTION_DELAY_DAYS", "3")
    os.environ.setdefault("DOF_DELUGE_AUTOREMOVE_LABEL", "autoremove")

    # Import after env vars set so settings picks them up
    from deluge_orphaned_files.logic.autoremove import process_autoremove_labeling
    from deluge_orphaned_files.logic.pending_actions import init_pending_actions_schema

    db_path = Path(".tmp/smoke.sqlite")
    scan_id = 123
    ensure_tmp_db(db_path, scan_id)
    init_pending_actions_schema(db_path)

    # Start from a clean slate for pending actions
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM pending_actions;")
        conn.commit()

    client = FakeDelugeClient()
    stats_dry = process_autoremove_labeling(
        client=client,
        scan_id=scan_id,
        db_path=db_path,
        dry_run=True,
        target_label_prefix="autoremove",
    )

    print("SMOKE_STATS_DRY:", stats_dry)

    # Clear pending actions then run a live (non-dry) pass to ensure clones are queued
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM pending_actions;")
        conn.commit()

    client = FakeDelugeClient()
    stats_live = process_autoremove_labeling(
        client=client,
        scan_id=scan_id,
        db_path=db_path,
        dry_run=False,
        target_label_prefix="autoremove",
    )

    with sqlite3.connect(str(db_path)) as conn:
        pending_rows = conn.execute("SELECT torrent_id, file_path FROM pending_actions ORDER BY torrent_id").fetchall()

    print("SMOKE_STATS_LIVE:", stats_live)
    print("SMOKE_PENDING:", pending_rows)

    # Expect at least one content group and both candidate + clone torrents queued
    ok_stats = stats_dry.get("content_groups_found", 0) >= 1 and stats_dry.get("torrents_processed", 0) >= 2
    hashes = {row[0] for row in pending_rows}
    ok_pending = {"aaaa1111", "bbbb2222"}.issubset(hashes)
    return 0 if ok_stats and ok_pending else 1


if __name__ == "__main__":
    raise SystemExit(main())
