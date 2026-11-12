"""Tests for save_scan_results_to_db and the CLI entry point.

An inode-mode scan does no hashing, so its file_hashes lookups miss and the resolved
hash is ''. The UPDATE paths must not overwrite a hash stored by an earlier hash-mode
run with that empty string (prod carries 13k+ cached hashes worth keeping).

Configuration is explicit: each test builds its own AppConfig from a scrubbed
environment pointing at temp directories (the repo's .env is ignored), and
passes it to the helpers. main() tests stub cli.load_config to return it.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

import pytest

from deluge_orphaned_files import cli as cli_module
from deluge_orphaned_files.settings import AppConfig

_NOTIFICATION_ENV = ("SMTP_HOST", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_FROM_ADDR", "SMTP_TO_ADDRS", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")


@pytest.fixture()
def cli():
    return cli_module


@pytest.fixture()
def config(tmp_path, monkeypatch):
    """A validated AppConfig rooted in tmp_path, independent of the host .env."""
    (tmp_path / "torrents").mkdir()
    (tmp_path / "media").mkdir()
    env = {
        "DELUGE_HOST": "localhost",
        "DELUGE_PORT": "58846",
        "DELUGE_USERNAME": "test",
        "DELUGE_PASSWORD": "test",
        "DELUGE_TORRENT_BASE_REMOTE_FOLDER": "/data/torrents",
        "LOCAL_TORRENT_BASE_LOCAL_FOLDER": str(tmp_path / "torrents"),
        "LOCAL_MEDIA_BASE_LOCAL_FOLDER": str(tmp_path / "media"),
        "OUTPUT_FILE": str(tmp_path / "orphaned_files.json"),
        "APP_SQLITE_CACHE_PATH": str(tmp_path / "orphaned_files.db"),
        "APP_LOG_DIR": str(tmp_path / "logs"),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    for key in _NOTIFICATION_ENV:
        monkeypatch.delenv(key, raising=False)
    return AppConfig(_env_file=None)


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


def test_empty_resolved_hash_does_not_clobber_stored_hash(cli, config, tmp_path, monkeypatch):
    db_path = tmp_path / "scan.db"
    config.sqlite_cache_path = db_path
    cli.init_sqlite_cache(db_path, config=config)

    torrent_folder = config.local_torrent_base_local_folder
    (torrent_folder / "movie.mkv").write_bytes(b"x")
    entry = {"path": "movie.mkv", "label": "movies", "size": 1, "size_human": "1 MB", "torrent_id": "t1"}

    # Scan 1: hash-mode run — the cache resolves a real hash, which is stored.
    _set_cached_hash(db_path, torrent_folder, "movie.mkv", "cafebabe")
    cli.save_scan_results_to_db([], [entry], [], datetime(2026, 1, 1), config=config)
    assert _stored(db_path, "movie.mkv", "torrents") == ("cafebabe", 1)

    # Scan 2: inode-mode run — no hashing happened, the cache lookup misses.
    _set_cached_hash(db_path, torrent_folder, "movie.mkv", None)
    cli.save_scan_results_to_db([], [entry], [], datetime(2026, 1, 2), config=config)
    assert _stored(db_path, "movie.mkv", "torrents") == ("cafebabe", 2)

    # Scan 3: hash-mode again with a changed file — the new hash must still win.
    _set_cached_hash(db_path, torrent_folder, "movie.mkv", "deadbeef")
    cli.save_scan_results_to_db([], [entry], [], datetime(2026, 1, 3), config=config)
    assert _stored(db_path, "movie.mkv", "torrents") == ("deadbeef", 3)


def test_hash_preserved_for_orphan_and_media_sources_too(cli, config, tmp_path, monkeypatch):
    db_path = tmp_path / "scan.db"
    config.sqlite_cache_path = db_path
    cli.init_sqlite_cache(db_path, config=config)

    torrent_folder = config.local_torrent_base_local_folder
    media_folder = config.local_media_base_local_folder
    (torrent_folder / "orphan.mkv").write_bytes(b"x")
    (media_folder / "media.mkv").write_bytes(b"x")
    orphan = {"path": "orphan.mkv", "size": 1, "size_human": "1 MB"}
    media = {"path": "media.mkv", "size": 1, "size_human": "1 MB"}

    _set_cached_hash(db_path, torrent_folder, "orphan.mkv", "aaaa1111")
    _set_cached_hash(db_path, media_folder, "media.mkv", "bbbb2222")
    cli.save_scan_results_to_db([orphan], [], [media], datetime(2026, 1, 1), config=config)

    _set_cached_hash(db_path, torrent_folder, "orphan.mkv", None)
    _set_cached_hash(db_path, media_folder, "media.mkv", None)
    cli.save_scan_results_to_db([orphan], [], [media], datetime(2026, 1, 2), config=config)

    assert _stored(db_path, "orphan.mkv", "local_torrent_folder") == ("aaaa1111", 2)
    assert _stored(db_path, "media.mkv", "media") == ("bbbb2222", 2)


def test_scan_preserves_current_deletion_marks_and_clears_stale_ones(cli, config, tmp_path, monkeypatch):
    """A force scan must delete only a previously marked file that is still orphaned.

    A marked row that remains in the current orphan set must keep its mark. A marked row
    absent from the current set (for example because Deluge reacquired it) must become
    inactive before the deletion gate runs.
    """
    db_path = tmp_path / "retention-state.db"
    config.sqlite_cache_path = db_path
    cli.init_sqlite_cache(db_path, config=config)

    current = {"path": "current-orphan.mkv", "size": 1, "size_human": "0.00 MB"}
    reacquired = {"path": "reacquired-by-deluge.mkv", "size": 1, "size_human": "0.00 MB"}

    cli.save_scan_results_to_db([current, reacquired], [], [], datetime(2026, 1, 1), config=config)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE orphaned_files SET status = 'marked_for_deletion' WHERE source = 'local_torrent_folder'")

    # The next scan still considers one path orphaned; the other is no longer an orphan.
    cli.save_scan_results_to_db([current], [], [], datetime(2026, 1, 2), config=config)

    assert _stored_retention_state(db_path, "current-orphan.mkv") == ("marked_for_deletion", 2)
    assert _stored_retention_state(db_path, "reacquired-by-deluge.mkv") == ("inactive", 0)


def test_force_deletion_cycle_deletes_old_marks_then_marks_current_candidates(cli, config, tmp_path, monkeypatch):
    """The always-force production schedule must still prepare work for its next run."""
    calls = []

    def record_call(*, force_delete, db_path, torrent_base_folder, scans_threshold):
        calls.append((force_delete, db_path, torrent_base_folder, scans_threshold))

    monkeypatch.setattr(cli, "process_deletions", record_call)

    db_path = tmp_path / "cycle.db"
    torrent_base = tmp_path / "torrents"
    cli._process_deletion_cycle(force_delete=True, db_path=db_path, torrent_base_folder=torrent_base, scans_threshold=4)

    assert calls == [
        (True, db_path, torrent_base, 4),
        (False, db_path, torrent_base, 4),
    ]


def test_consecutive_scans_count_distinct_days_not_scans(cli, config, tmp_path, monkeypatch):
    """A redeploy burst (many scans in one day) must not inflate consecutive_scans.

    The counter gates deletion and autoremove labeling; run_on_startup means every
    container recreate is another scan, so only sightings on distinct calendar
    days may advance it. scan_start_time is the authoritative sighting time.
    """
    db_path = tmp_path / "days.db"
    config.sqlite_cache_path = db_path
    cli.init_sqlite_cache(db_path, config=config)

    orphan = {"path": "orphan.mkv", "size": 1, "size_human": "0.00 MB"}

    # Two scans on the same calendar day: still one sighting day.
    cli.save_scan_results_to_db([orphan], [], [], datetime(2026, 1, 1, 2, 0, 0), config=config)
    cli.save_scan_results_to_db([orphan], [], [], datetime(2026, 1, 1, 23, 0, 0), config=config)
    assert _stored_retention_state(db_path, "orphan.mkv") == ("active", 1)

    # Next calendar day advances the counter.
    cli.save_scan_results_to_db([orphan], [], [], datetime(2026, 1, 2, 2, 0, 0), config=config)
    assert _stored_retention_state(db_path, "orphan.mkv") == ("active", 2)

    # A legacy row inflated by the old per-scan counting (e.g. 9 scans in one
    # day) must converge to distinct sighting days on its next sighting: the
    # counter can never exceed the calendar span first_seen..last_seen.
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE orphaned_files SET consecutive_scans = 9 WHERE path = 'orphan.mkv'")
    cli.save_scan_results_to_db([orphan], [], [], datetime(2026, 1, 3, 2, 0, 0), config=config)
    assert _stored_retention_state(db_path, "orphan.mkv") == ("active", 3)

    # Absent from a scan: reset. Seen again the same day: fresh run starts at 1.
    cli.save_scan_results_to_db([], [], [], datetime(2026, 1, 4, 2, 0, 0), config=config)
    assert _stored_retention_state(db_path, "orphan.mkv") == ("inactive", 0)
    cli.save_scan_results_to_db([orphan], [], [], datetime(2026, 1, 4, 20, 0, 0), config=config)
    assert _stored_retention_state(db_path, "orphan.mkv") == ("active", 1)


def test_legacy_counters_converted_from_recorded_sightings(cli, config, tmp_path, monkeypatch):
    """Pre-existing DBs hold per-scan counters; conversion must recompute sighting days.

    Covers gaps the span clamp cannot: sightings on Jan 1, Jan 2 and a Jan 10 burst
    are 3 sighting days (not span 10), a scan where the file was absent ends the
    trailing run, and an entirely empty orphan scan (no history rows at all) also
    ends it — 'local_torrent_folder' is recorded on every scan. Runs once, gated
    by PRAGMA user_version.
    """
    db_path = tmp_path / "legacy.db"
    config.sqlite_cache_path = db_path
    cli.init_sqlite_cache(db_path, config=config)

    def _counter(path):
        with sqlite3.connect(str(db_path)) as conn:
            return conn.execute("SELECT consecutive_scans FROM orphaned_files WHERE path = ?", (path,)).fetchone()[0]

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA user_version = 0")  # simulate a database created before the conversion existed
        scan_ids = []
        for start in (
            "2026-01-01T02:00:00",
            "2026-01-02T02:00:00",
            "2026-01-10T01:00:00",
            "2026-01-10T02:00:00",
            "2026-01-10T03:00:00",
            "2026-01-11T02:00:00",  # s6: empty scan — no orphans found, no history rows
            "2026-01-12T02:00:00",  # s7
        ):
            cur = conn.execute("INSERT INTO scan_results (host, base_path, scan_start, scan_end) VALUES ('h', '/data', ?, ?)", (start, start))
            scan_ids.append(cur.lastrowid)
        s1, s2, s3, s4, s5, s6, s7 = scan_ids

        def _row(path, stored):
            cur = conn.execute(
                "INSERT INTO orphaned_files (file_hash, path, source, size, size_human, first_seen_at, last_seen_at, consecutive_scans, status, include_in_report) "
                "VALUES ('', ?, 'local_torrent_folder', 1, '1 MB', '2026-01-01T02:00:00', '2026-01-12T02:00:00', ?, 'active', 1)",
                (path, stored),
            )
            return cur.lastrowid

        burst = _row("burst.mkv", 9)  # sighted every scan up to s5: 3 distinct days
        gapped = _row("gapped.mkv", 9)  # absent from s2: trailing run is Jan 10 only
        _row("no-history.mkv", 7)  # no sightings recorded at all
        reset = _row("reset.mkv", 8)  # full streak, then the empty s6 scan, then sighted again at s7
        sightings = [(s, burst) for s in (s1, s2, s3, s4, s5)] + [(s, gapped) for s in (s1, s3, s4, s5)] + [(s, reset) for s in (s1, s2, s3, s4, s5, s7)]
        for scan_id, file_id in sightings:
            conn.execute("INSERT INTO file_scan_history (scan_id, file_id, source) VALUES (?, ?, 'local_torrent_folder')", (scan_id, file_id))

    cli.init_sqlite_cache(db_path, config=config)

    assert _counter("burst.mkv") == 3
    assert _counter("gapped.mkv") == 1
    assert _counter("no-history.mkv") == 1
    assert _counter("reset.mkv") == 1

    # One-time only: a later init must not recompute again.
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("UPDATE orphaned_files SET consecutive_scans = 44 WHERE path = 'burst.mkv'")
    cli.init_sqlite_cache(db_path, config=config)
    assert _counter("burst.mkv") == 44


def test_dispatch_notifications_respects_no_notify(cli, config, monkeypatch):
    """--no-notify (appended by the scheduler for startup runs) silences both channels.

    The return value reports whether every configured send succeeded, so main()
    can map a failed send to the partial-failure exit code.
    """
    sent = {"email": 0, "telegram": 0}

    def _send_email(**kw):
        sent["email"] += 1
        return True

    def _send_telegram(**kw):
        sent["telegram"] += 1
        return True

    monkeypatch.setattr(cli, "send_scan_report", _send_email)
    monkeypatch.setattr(cli, "send_telegram_report", _send_telegram)
    monkeypatch.setattr(cli, "_format_scan_results", lambda *a, **k: "report body")
    for attr, value in {
        "smtp_host": "smtp.example",
        "smtp_username": "user",
        "smtp_password": "pass",
        "smtp_from_addr": "from@example",
        "smtp_to_addrs": ["to@example"],
        "telegram_bot_token": "token",
        "telegram_chat_id": "123",
    }.items():
        setattr(config, attr, value)

    assert cli._dispatch_notifications(config=config, scan_id=1, no_notify=True) is True
    assert sent == {"email": 0, "telegram": 0}

    assert cli._dispatch_notifications(config=config, scan_id=1, no_notify=False) is True
    assert sent == {"email": 1, "telegram": 1}

    monkeypatch.setattr(cli, "send_telegram_report", lambda **kw: False)
    assert cli._dispatch_notifications(config=config, scan_id=1, no_notify=False) is False


def _run_main(cli, config, monkeypatch, argv):
    monkeypatch.setattr(cli, "load_config", lambda: config)
    monkeypatch.setattr("sys.argv", ["deluge-orphaned-files", *argv])
    return cli.main()


def test_main_exit_codes_usage_and_scan_failure(cli, config, tmp_path, monkeypatch):
    """0 ok / 1 failed / 2 usage-or-config: the scheduler and Docker need real codes."""
    config.sqlite_cache_path = tmp_path / "exit.db"

    # Requesting DB-backed output without --sqlite cannot be served.
    assert _run_main(cli, config, monkeypatch, ["--show-last"]) == 2

    # A test notification without its configuration is a usage error too.
    config.smtp_host = None
    assert _run_main(cli, config, monkeypatch, ["--test-email"]) == 2

    def _preflight_boom(**kwargs):
        raise cli.InodePreflightError("different filesystems")

    monkeypatch.setattr(cli, "find_orphaned_files", _preflight_boom)
    assert _run_main(cli, config, monkeypatch, ["--sqlite"]) == 1

    def _scan_boom(**kwargs):
        raise RuntimeError("deluge unreachable")

    monkeypatch.setattr(cli, "find_orphaned_files", _scan_boom)
    assert _run_main(cli, config, monkeypatch, ["--sqlite"]) == 1


def test_main_exit_codes_success_and_partial(cli, config, tmp_path, monkeypatch):
    import deluge_client

    config.sqlite_cache_path = tmp_path / "exit.db"
    config.smtp_host = None
    config.telegram_bot_token = None

    class _FakeDelugeClient:
        connected = True

        def __init__(self, **kwargs):
            pass

        def connect(self):
            pass

        def disconnect(self):
            pass

    monkeypatch.setattr(deluge_client, "DelugeRPCClient", _FakeDelugeClient)
    monkeypatch.setattr(cli, "find_orphaned_files", lambda **kwargs: 7)
    monkeypatch.setattr(cli, "process_autoremove_labeling", lambda **kwargs: {"errors": 0})
    monkeypatch.setattr(cli, "_process_deletion_cycle", lambda **kwargs: None)
    monkeypatch.setattr(cli, "_format_scan_results", lambda *a, **k: "table")

    assert _run_main(cli, config, monkeypatch, ["--sqlite"]) == 0

    # Labeling reports failures via its returned stats (it swallows exceptions
    # internally); those must surface as partial, not silent success.
    monkeypatch.setattr(cli, "process_autoremove_labeling", lambda **kwargs: {"errors": 2})
    assert _run_main(cli, config, monkeypatch, ["--sqlite"]) == 3
    monkeypatch.setattr(cli, "process_autoremove_labeling", lambda **kwargs: {"errors": 0})

    # Same for pending-action execution failures.
    monkeypatch.setattr(cli, "execute_pending_actions", lambda **kwargs: (0, 1, 0))
    assert _run_main(cli, config, monkeypatch, ["--sqlite", "--execute-pending-actions", "--force"]) == 3
    monkeypatch.setattr(cli, "execute_pending_actions", lambda **kwargs: (1, 0, 0))

    # And for a failed notification send.
    monkeypatch.setattr(cli, "_dispatch_notifications", lambda **kwargs: False)
    assert _run_main(cli, config, monkeypatch, ["--sqlite"]) == 3
