from __future__ import annotations

import logging
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta
from threading import Barrier
from types import SimpleNamespace

import pytest
from loguru import logger

from deluge_orphaned_files.logic.pending_actions import (
    ActionType,
    execute_pending_actions,
    init_pending_actions_schema,
    register_pending_action,
    register_tracker_removal,
)
from deluge_orphaned_files.logic.tracker_rejections import process_tracker_rejections, run_tracker_rejections


NOW = datetime(2025, 6, 1, 12, 34, 56)
DUE = NOW + timedelta(days=7)
DUE_TEXT = "2025-06-08 12:34:56"
HASH = "ab" * 20
REPLY = "Unregistered torrent"
SECRET = "https://user:password@tracker.example/announce?passkey=my-passkey"
FIELDS = {"state", "private", "is_seed", "tracker_status"}


def _hash(n):
    return f"{n:040x}"


def _status(reply=REPLY, **overrides):
    return {"state": "Seeding", "private": True, "is_seed": True, "tracker_status": reply} | overrides


class FakeDeluge:
    def __init__(self, snapshot=None, *, current=None, removal=True):
        self.core = self
        self.snapshot = {} if snapshot is None else snapshot
        self.current = {} if current is None else current
        self.removal = removal
        self.calls = []

    def get_torrents_status(self, filters, fields):
        assert filters == {}
        assert set(fields) == FIELDS  # No trackers, URLs or names are requested.
        self.calls.append(("discover",))
        if isinstance(self.snapshot, Exception):
            raise self.snapshot
        return self.snapshot

    def get_torrent_status(self, torrent_hash, fields):
        assert set(fields) == FIELDS
        self.calls.append(("check", torrent_hash))
        status = self.current[torrent_hash]
        if isinstance(status, Exception):
            raise status
        return status

    def remove_torrent(self, torrent_hash, remove_data):
        self.calls.append(("remove", torrent_hash, remove_data))
        if isinstance(self.removal, Exception):
            raise self.removal
        return self.removal


def _run(db, deluge, *, now=NOW, dry_run=False, grace=7, daemon="daemon-a"):
    process_tracker_rejections(db, deluge, grace, daemon_key=daemon, dry_run=dry_run, now=now)


def _rows(db):
    with closing(sqlite3.connect(db)) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM pending_actions ORDER BY id")]


@pytest.fixture
def logs():
    messages = []
    sink = logger.add(lambda message: messages.append(str(message)))
    try:
        yield messages
    finally:
        logger.remove(sink)


def test_discovery_matches_only_exact_terminal_replies_and_actual_seeds(tmp_path, logs):
    db = tmp_path / "queue.db"
    deluge = FakeDeluge(
        {
            HASH.upper().encode(): {
                b"state": b"Seeding",
                b"private": True,
                b"is_seed": True,
                b"tracker_status": b" \t eRrOr:  unREGISTERED TORRENT \n",
                b"trackers": [{b"url": SECRET.encode()}, {b"url": b"https://example.test/announce"}],
            },
            _hash(2): _status("Torrent removed by its uploader"),
            _hash(3): _status("Error: Torrent has been deleted."),
            _hash(4): _status("Error: Torrent has been deleted"),
            _hash(5): _status("Warning: Unregistered torrent"),
            _hash(6): _status("Error: Unregistered torrent: " + SECRET),
            _hash(7): _status("Error: Error: Unregistered torrent"),
            _hash(8): _status("Error: Torrent removed by its uploader (try later)"),
            _hash(9): _status("Error: Unregistered  torrent"),
            _hash(10): _status("Error: Announce interval is 30 minutes"),
            _hash(11): _status("Error: Could not resolve host"),
            _hash(12): _status("Error: Timed out"),
            _hash(13): _status("Error: HTTP 500"),
            _hash(14): _status("Error: Invalid passkey"),
            _hash(15): _status("Announce OK"),
            _hash(16): _status(private=False),
            _hash(17): _status(state="Paused"),
            _hash(18): _status(is_seed=False, progress=100, is_finished=True),
            _hash(19): _status(private=1),
            _hash(20): _status(b"Unregistered torrent\xff"),
            _hash(21): _status(["Unregistered torrent"]),
            _hash(22): SECRET,
            SECRET: _status(),
        }
    )

    _run(db, deluge)

    assert [(r["torrent_id"], r["action_details"], r["action_due_at"]) for r in _rows(db)] == [
        (HASH, REPLY, DUE_TEXT),
        (_hash(2), "Torrent removed by its uploader", DUE_TEXT),
        (_hash(3), "Torrent has been deleted.", DUE_TEXT),
    ]
    assert deluge.calls == [("discover",)]
    assert SECRET not in "".join(logs)
    assert b"my-passkey" not in db.read_bytes()


def test_first_due_date_survives_repeated_scans_and_success_cancels(tmp_path):
    db = tmp_path / "queue.db"
    deluge = FakeDeluge({HASH: _status()}, current={HASH: _status(" \tANNOUNCE OK \n")})
    _run(db, deluge)
    first = _rows(db)[0]

    deluge.snapshot = {HASH.encode(): _status("Torrent has been deleted.")}
    _run(db, deluge, now=DUE - timedelta(seconds=1), grace=90)
    assert _rows(db) == [first]
    assert deluge.calls == [("discover",), ("discover",)]

    _run(db, deluge, now=DUE)
    (row,) = _rows(db)
    assert row["action_due_at"] == DUE_TEXT
    assert row["action_details"] == REPLY
    assert row["status"] == "cancelled"
    assert row["processed_at"] == DUE_TEXT
    assert "Announce OK" in row["processing_notes"]
    assert deluge.calls[-2:] == [("discover",), ("check", HASH)]

    _run(db, deluge, now=DUE + timedelta(days=1))
    cancelled, new = _rows(db)
    assert cancelled == row
    assert new["status"] == "pending"
    assert new["action_due_at"] == "2025-06-16 12:34:56"


@pytest.mark.parametrize(
    "current,removal,expected",
    [
        (_status("Error: announce cooldown"), True, "pending"),
        (_status("Error: " + SECRET), True, "pending"),
        (_status("Error: Announce OK"), True, "pending"),
        (RuntimeError(SECRET), True, "pending"),
        (SECRET, True, "pending"),
        ({}, True, "pending"),
        ({"tracker_status": REPLY}, True, "pending"),
        (_status(is_seed=False), True, "cancelled"),
        (_status(private=False), True, "cancelled"),
        (_status(state="Paused"), True, "cancelled"),
        (_status("Announce Sent", state="Paused"), True, "cancelled"),
        ({"private": False}, True, "cancelled"),
        ({"is_seed": False, "tracker_status": "Error: Timed out"}, True, "cancelled"),
        (_status(), False, "pending"),
        (_status(), SECRET, "pending"),
        (_status(), RuntimeError(SECRET), "pending"),
    ],
)
def test_due_actions_defer_or_cancel_safely(tmp_path, logs, current, removal, expected):
    db = tmp_path / "queue.db"
    _run(db, FakeDeluge({HASH: _status()}))
    deluge = FakeDeluge(RuntimeError(SECRET), current={HASH: current}, removal=removal)

    _run(db, deluge, now=DUE)

    (row,) = _rows(db)
    assert row["status"] == expected
    assert row["action_due_at"] == DUE_TEXT
    assert row["processing_notes"]
    assert deluge.calls == [("discover",), ("check", HASH)] + ([("remove", HASH, False)] if current == _status() else [])
    assert "my-passkey" not in "".join(logs)
    assert b"my-passkey" not in db.read_bytes()


def test_temporary_status_keeps_original_grace_then_removes_registration_only(tmp_path):
    db = tmp_path / "queue.db"
    _run(db, FakeDeluge({HASH: _status()}))
    retry = FakeDeluge(current={HASH: _status("Announce Sent")})
    _run(db, retry, now=DUE)
    assert retry.calls[-1] == ("check", HASH)
    assert _rows(db)[0]["status"] == "pending"

    # The bulk snapshot is not authoritative for a due action.
    retry.snapshot = {HASH: _status("Announce OK")}
    retry.current = {HASH: _status("Error: Torrent has been deleted.")}
    _run(db, retry, now=DUE + timedelta(days=1))

    assert retry.calls[-3:] == [("discover",), ("check", HASH), ("remove", HASH, False)]
    (row,) = _rows(db)
    assert row["status"] == "completed"
    assert row["action_due_at"] == DUE_TEXT
    assert row["action_details"] == REPLY
    assert "Torrent has been deleted." in row["processing_notes"]


def test_dry_run_plans_new_actions_without_creating_database(tmp_path, logs):
    db = tmp_path / "does-not-exist.db"
    deluge = FakeDeluge({HASH: _status()})

    _run(db, deluge, dry_run=True)

    assert not db.exists()
    assert deluge.calls == [("discover",)]
    assert any("[DRY RUN] Would queue" in line and REPLY in line and DUE_TEXT in line for line in logs)


def test_preview_reads_legacy_queue_without_migrating_it(tmp_path, logs):
    db = tmp_path / "legacy.db"
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("CREATE TABLE pending_actions (id INTEGER PRIMARY KEY, file_path TEXT)")
    before = db.read_bytes()

    _run(db, FakeDeluge({HASH: _status()}), dry_run=True)

    assert db.read_bytes() == before
    assert any("[DRY RUN] Would queue" in line and HASH in line for line in logs)


def test_dry_run_previews_due_outcomes_without_changing_queue_or_deluge(tmp_path, logs):
    db = tmp_path / "queue.db"
    rejected, recovered, temporary, future = (_hash(n) for n in range(4))
    _run(db, FakeDeluge({h: _status() for h in (rejected, recovered, temporary)}))
    _run(db, FakeDeluge({future: _status()}), now=NOW + timedelta(days=1))
    before = db.read_bytes()
    deluge = FakeDeluge(
        current={
            rejected: _status(),
            recovered: _status("Announce OK"),
            temporary: _status("Error: Timed out"),
        }
    )

    _run(db, deluge, now=DUE, dry_run=True)

    assert db.read_bytes() == before
    assert deluge.calls == [("discover",)] + [("check", h) for h in (rejected, recovered, temporary)]
    for verb, torrent_hash in [("remove", rejected), ("cancel", recovered), ("defer", temporary)]:
        assert any(f"[DRY RUN] Would {verb}" in line and torrent_hash in line and DUE_TEXT in line for line in logs)
    assert any(future in line and "2025-06-09 12:34:56" in line for line in logs)


def test_active_deduplication_is_atomic_and_bound_to_daemon(tmp_path):
    db = tmp_path / "queue.db"
    init_pending_actions_schema(db)
    barrier = Barrier(2)

    def register(due_at):
        barrier.wait(timeout=5)
        return register_tracker_removal(db, "daemon-a", HASH, REPLY, due_at)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(register, [DUE_TEXT, "2025-06-09 12:34:56"])
    assert first == second
    assert len(_rows(db)) == 1

    other = FakeDeluge({HASH: _status()})
    _run(db, other, now=DUE + timedelta(days=2), daemon="daemon-b")
    assert other.calls == [("discover",)]
    assert [(r["source"], r["action_due_at"]) for r in _rows(db)] == [
        ("daemon-a", first.due_at),
        ("daemon-b", "2025-06-17 12:34:56"),
    ]


def test_legacy_orphan_executor_does_not_run_tracker_actions(tmp_path):
    db = tmp_path / "queue.db"
    _run(db, FakeDeluge({HASH: _status()}))
    register_pending_action(db, "orphan.mkv", ActionType.DELETE, -1, scan_id=1)
    deleted = []

    def delete(path):
        deleted.append(path)
        return True

    def relabel(*args):
        pytest.fail(f"Unexpected relabel: {args}")

    assert execute_pending_actions(db, relabel, delete) == (1, 0, 0)
    assert deleted == ["orphan.mkv"]
    assert [r["status"] for r in _rows(db)] == ["pending", "completed"]


def test_rpc_login_error_does_not_log_remote_reply_or_credentials(tmp_path, monkeypatch, logs, caplog):
    import deluge_client

    class FailingConnection:
        def __init__(self, **kwargs):
            assert kwargs["automatic_reconnect"] is False

        def connect(self):
            logging.getLogger("deluge_client.client").debug("Unexpected reply: %s", SECRET)
            raise RuntimeError(SECRET)

    monkeypatch.setattr(deluge_client, "DelugeRPCClient", FailingConnection)
    db = tmp_path / "queue.db"
    config = SimpleNamespace(
        sqlite_cache_path=db,
        deluge_host="localhost",
        deluge_port=58846,
        deluge_username="tester",
        deluge_password="test-password",
        relabel_action_delay_days=7,
    )
    rpc_logger = logging.getLogger("deluge_client.client")
    originally_disabled = rpc_logger.disabled

    with caplog.at_level(logging.DEBUG, logger=rpc_logger.name):
        assert run_tracker_rejections(config, dry_run=False) is False

    assert rpc_logger.disabled == originally_disabled
    assert not db.exists()
    assert "Could not connect to Deluge" in "".join(logs)
    assert "my-passkey" not in "".join(logs) + caplog.text


@pytest.mark.parametrize(
    "flag,dry_run,connected,repair",
    [
        ("--tracker-rejected-torrents", True, True, False),
        ("--apply-tracker-rejected-removals", False, True, False),
        ("--apply-tracker-rejected-removals", False, False, False),
        ("--tracker-rejected-torrents", True, True, True),
    ],
)
def test_cli_tracker_mode_returns_before_orphan_scan(tmp_path, monkeypatch, flag, dry_run, connected, repair):
    for name, value in {
        "DELUGE_HOST": "localhost",
        "DELUGE_PORT": "58846",
        "DELUGE_USERNAME": "tester",
        "DELUGE_PASSWORD": "test",
        "DELUGE_TORRENT_BASE_REMOTE_FOLDER": "/data/torrents",
        "LOCAL_TORRENT_BASE_LOCAL_FOLDER": str(tmp_path),
        "LOCAL_MEDIA_BASE_LOCAL_FOLDER": str(tmp_path),
        "OUTPUT_FILE": str(tmp_path / "report.json"),
        "APP_SQLITE_CACHE_PATH": str(tmp_path / "queue.db"),
        "APP_LOG_DIR": str(tmp_path / "logs"),
    }.items():
        monkeypatch.setenv(name, value)
    from deluge_orphaned_files import cli

    args = ["deluge_orphaned_files", flag]
    if repair:
        args.append("--apply-stuck-repairs")
    monkeypatch.setattr(sys, "argv", args)
    monkeypatch.setattr(cli, "find_orphaned_files", lambda **kwargs: pytest.fail("Unexpected orphan scan"))
    monkeypatch.setattr(cli, "run_stuck_repair", lambda *args, **kwargs: pytest.fail("Unexpected repair"))
    called = []

    def run(config, *, dry_run):
        called.append((config, dry_run))
        return connected

    monkeypatch.setattr(cli, "run_tracker_rejections", run)
    if repair or not connected:
        with pytest.raises(SystemExit) as exc:
            cli.main()
        assert exc.value.code == (2 if repair else 1)
    else:
        cli.main()
    assert called == ([] if repair else [(cli.config, dry_run)])
