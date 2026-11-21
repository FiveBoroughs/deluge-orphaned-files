"""Observe real temporary paths using an in-memory Deluge RPC stand-in."""

from __future__ import annotations

import errno
import os
import sqlite3
import sys
from contextlib import closing
from types import SimpleNamespace

import pytest

from deluge_orphaned_files.logic.missing_payloads import reconcile_missing_payloads


A, B, C = "a" * 40, "b" * 40, "c" * 40
DAY = 86_400
LIST, STATUS, REMOVE = "core.get_torrents_status", "core.get_torrent_status", "core.remove_torrent"


def torrent(*paths, save=b"/data/torrents/", **overrides):
    files = [{b"path": path} for path in paths or (b"release/movie.mkv",)]
    record = {
        "state": b"Seeding",
        "is_finished": True,
        "is_seed": True,
        "progress": 100.0,
        "download_payload_rate": 0,
        "time_added": 99,
        "save_path": save,
        "num_files": len(files),
        "files": files,
    }
    record.update(overrides)
    return {key.encode(): value for key, value in record.items()}


class RPC:
    def __init__(self, torrents):
        self.torrents = {key.encode(): value for key, value in torrents.items()}
        self.calls = []
        self.before_status = None
        self.inventory = lambda: self.torrents
        self.remove_answers = {}

    def call(self, method, *args):
        self.calls.append((method, *args))
        if method == LIST:
            assert args[0] == {}
            return self.inventory()
        if method == STATUS:
            if self.before_status:
                self.before_status(args[0])
            return self.torrents.get(args[0].encode(), {})
        if method == REMOVE:
            infohash, remove_data = args
            assert remove_data is False
            result = self.remove_answers.get(infohash, True)
            if result is True:
                self.torrents.pop(infohash.encode())
            return result
        pytest.fail(f"Unexpected RPC: {method}")

    def execution_calls(self):
        return [(method, args[0]) for method, *args in self.calls if method in {STATUS, REMOVE}]


@pytest.fixture
def config(tmp_path):
    torrents = tmp_path / "torrents"
    media = tmp_path / "media"
    for root in (torrents, media):
        root.mkdir()
        (root / ".delugeof-root").touch()
    return SimpleNamespace(
        deluge_host="test-daemon",
        deluge_port=58846,
        deluge_torrent_base_remote_folder="/data/torrents",
        deluge_media_base_remote_folder="/data/media",
        local_torrent_base_local_folder=torrents,
        local_media_base_local_folder=media,
        sqlite_cache_path=tmp_path / "scan #1.db",
        deletion_consecutive_scans_threshold=2,
        deletion_days_threshold=0,
    )


def run(config, rpc, at, *, execute=True, dry_run=False):
    reconcile_missing_payloads(config, rpc, execute=execute, dry_run=dry_run, now=lambda: at)


def rows(config):
    with closing(sqlite3.connect(config.sqlite_cache_path)) as conn:
        return {key: (count, first_seen, last_seen) for key, count, first_seen, last_seen in conn.execute("SELECT infohash, observations, first_seen, last_seen FROM missing_payload_candidates")}


def test_tracks_shared_infohashes_and_both_mappings_then_removes_one_at_a_time(config):
    rpc = RPC({A: torrent(), B: torrent(), C: torrent(save="/data/media/movies", state="Paused")})
    run(config, rpc, 100, execute=False)
    assert rows(config) == {A: (1, 100, 100), B: (1, 100, 100), C: (1, 100, 100)}
    assert rpc.execution_calls() == []

    rpc.remove_answers[A] = False
    run(config, rpc, 101)
    assert rpc.execution_calls() == [(STATUS, A), (REMOVE, A), (STATUS, B), (REMOVE, B), (STATUS, C), (REMOVE, C)]
    assert rows(config) == {A: (2, 100, 101)}
    assert [call for call in rpc.calls if call[0] == REMOVE] == [(REMOVE, A, False), (REMOVE, B, False), (REMOVE, C, False)]

    rpc.remove_answers[A] = True
    run(config, rpc, 102)
    assert rows(config) == {}
    assert rpc.execution_calls()[-2:] == [(STATUS, A), (REMOVE, A)]


def test_requires_configured_count_and_grace(config):
    rpc = RPC({A: torrent(), B: torrent()})
    config.deletion_consecutive_scans_threshold = 3
    config.deletion_days_threshold = 2
    run(config, rpc, 100)
    run(config, rpc, 100)  # No duplicate evidence from the same observation time.
    assert rows(config) == {A: (1, 100, 100), B: (1, 100, 100)}
    run(config, rpc, 101)
    rpc.torrents[A.encode()] = torrent(time_added=101)
    run(config, rpc, 102)
    assert rows(config) == {A: (1, 102, 102), B: (3, 100, 102)}
    assert not rpc.execution_calls()

    run(config, rpc, 100 + 2 * DAY)
    assert rows(config) == {A: (2, 102, 100 + 2 * DAY)}
    assert rpc.execution_calls() == [(STATUS, B), (REMOVE, B)]


def test_dry_run_does_not_create_change_or_prune_candidate_state(config):
    config.deletion_consecutive_scans_threshold = 1
    rpc = RPC({A: torrent(), B: torrent()})
    run(config, rpc, 100, dry_run=True)
    assert not config.sqlite_cache_path.exists()
    run(config, rpc, 101, execute=False)
    before = config.sqlite_cache_path.read_bytes()
    rpc.torrents.pop(B.encode())
    run(config, rpc, 102, dry_run=True)
    assert config.sqlite_cache_path.read_bytes() == before
    assert not rpc.execution_calls()
    run(config, rpc, 103)
    assert rows(config) == {}
    assert rpc.execution_calls() == [(STATUS, A), (REMOVE, A)]


def test_present_payload_in_media_resets_streak_even_if_other_files_missing(config):
    rpc = RPC({A: torrent(b"movie.mkv", b"notes.nfo", save="/data/media/movies")})
    run(config, rpc, 100)
    notes = config.local_media_base_local_folder / "movies" / "notes.nfo"
    notes.parent.mkdir()
    notes.touch()
    run(config, rpc, 101)
    assert rows(config) == {}
    assert notes.is_file()
    notes.unlink()
    run(config, rpc, 102)
    assert rows(config) == {A: (1, 102, 102)}
    assert not rpc.execution_calls()


def test_overlapping_remote_roots_never_treat_a_media_payload_as_missing(config):
    config.deluge_torrent_base_remote_folder = "/data"
    movie = config.local_media_base_local_folder / "movie.mkv"
    movie.touch()
    rpc = RPC({A: torrent(b"media/movie.mkv", save="/data")})

    run(config, rpc, 100)
    run(config, rpc, 101)

    assert not config.sqlite_cache_path.exists()
    assert not rpc.execution_calls()
    assert movie.is_file()


def test_move_outside_mapped_roots_then_back_restarts_grace(config):
    rpc = RPC({A: torrent()})
    run(config, rpc, 100)
    rpc.torrents[A.encode()] = torrent(save="/data/torrents-other")
    run(config, rpc, 101)
    assert rows(config) == {}

    rpc.torrents[A.encode()] = torrent()
    run(config, rpc, 102)
    assert rows(config) == {A: (1, 102, 102)}
    assert not rpc.execution_calls()


@pytest.mark.parametrize("state", ["Downloading", "Checking", "Moving", "Queued", "Error", "future-state"])
def test_does_not_accumulate_active_or_unrecognized_states(config, state):
    rpc = RPC({A: torrent(state=state)})
    run(config, rpc, 100)
    run(config, rpc, 101)
    assert rows(config) == {}
    assert not rpc.execution_calls()


@pytest.mark.parametrize("overrides", [{"is_seed": False, "state": "Paused"}, {"download_payload_rate": 3}, {"progress": 99.9}])
def test_requires_completed_seed_with_no_download_traffic(config, overrides):
    rpc = RPC({A: torrent(**overrides)})
    run(config, rpc, 100)
    run(config, rpc, 101)
    assert rows(config) == {}
    assert not rpc.execution_calls()


@pytest.mark.parametrize(
    "record",
    [
        torrent(b"../movie.mkv"),
        torrent(b"/movie.mkv"),
        torrent(b"movie\0.mkv"),
        torrent(b"\xff"),
        torrent(b""),
        torrent(b"release\\movie.mkv"),
        torrent(b"release/movie.mkv", b"release/movie.mkv"),
        torrent(b"release", b"release/movie.mkv"),
        torrent(save="/data/media/../torrents"),
        torrent(num_files=2),
        torrent(files=[]),
        torrent(state=None),
        torrent(is_finished=None),
        None,
    ],
)
def test_malformed_metadata_does_not_advance_evidence(config, record):
    rpc = RPC({A: torrent()})
    run(config, rpc, 100)
    rpc.torrents[A.encode()] = record
    run(config, rpc, 101)
    assert rows(config) == {A: (1, 100, 100)}
    assert not rpc.execution_calls()


@pytest.mark.parametrize("error", [errno.EACCES, errno.ESTALE, errno.EIO])
def test_filesystem_errors_are_not_missing_files(config, monkeypatch, error):
    rpc = RPC({A: torrent(b"movie.mkv")})
    run(config, rpc, 100)
    original = os.stat

    def bad_stat(path, **kwargs):
        if path == "movie.mkv" and kwargs.get("dir_fd") is not None:
            raise OSError(error, os.strerror(error))
        return original(path, **kwargs)

    monkeypatch.setattr(os, "stat", bad_stat)
    run(config, rpc, 101)
    assert rows(config) == {A: (1, 100, 100)}
    assert not rpc.execution_calls()


@pytest.mark.parametrize("placement", ["parent-symlink", "parent-file", "dangling-leaf"])
def test_never_probe_paths_through_obstructions(config, tmp_path, placement):
    root = config.local_torrent_base_local_folder
    if placement == "parent-symlink":
        (root / "release").symlink_to(tmp_path / "unavailable", target_is_directory=True)
    elif placement == "parent-file":
        (root / "release").touch()
    else:
        (root / "release").mkdir()
        (root / "release" / "movie.mkv").symlink_to(tmp_path / "unavailable")
    rpc = RPC({A: torrent()})
    run(config, rpc, 100)
    run(config, rpc, 101)
    assert rows(config) == {}
    assert not rpc.execution_calls()


def test_mount_fallback_and_malformed_rpc_do_not_advance_evidence(config, tmp_path):
    rpc = RPC({A: torrent()})
    run(config, rpc, 100)
    for invalid_reply in (None, {b"invalid-infohash": {}}):
        rpc.inventory = lambda: invalid_reply
        run(config, rpc, 101)
        assert rows(config) == {A: (1, 100, 100)}

    def rpc_failure():
        raise ConnectionError("unavailable inventory")

    rpc.inventory = rpc_failure
    run(config, rpc, 102)
    assert rows(config) == {A: (1, 100, 100)}
    rpc.inventory = lambda: rpc.torrents

    root = config.local_media_base_local_folder
    root.rename(tmp_path / "detached")
    root.mkdir()  # An unmounted fallback exists and is readable yet must not count.
    calls = len(rpc.calls)
    run(config, rpc, 103)
    assert len(rpc.calls) == calls
    assert rows(config) == {A: (1, 100, 100)}
    assert not rpc.execution_calls()


@pytest.mark.parametrize("change", ["file-layout", "save_path", "registration", "daemon", "remount"])
def test_grace_restarts_when_candidate_identity_changes(config, tmp_path, change):
    rpc = RPC({A: torrent()})
    run(config, rpc, 100)
    if change == "file-layout":
        rpc.torrents[A.encode()] = torrent(b"release/renamed.mkv")
    elif change == "save_path":
        rpc.torrents[A.encode()] = torrent(save="/data/media")
    elif change == "registration":
        rpc.torrents[A.encode()] = torrent(time_added=100)
    elif change == "daemon":
        config.deluge_host = "different-instance"
    else:
        root = config.local_torrent_base_local_folder
        root.rename(tmp_path / "detached")
        root.mkdir()
        (root / ".delugeof-root").touch()
    run(config, rpc, 101)
    assert rows(config) == {A: (1, 101, 101)}
    assert not rpc.execution_calls()


@pytest.mark.parametrize("change", ["payload", "checking", "relocated", "rpc", "unmounted"])
def test_revalidation_prevents_removal_after_observation(config, tmp_path, change):
    rpc = RPC({A: torrent()})
    run(config, rpc, 100)

    def before_status(infohash):
        assert infohash == A
        if change == "payload":
            movie = config.local_torrent_base_local_folder / "release" / "movie.mkv"
            movie.parent.mkdir()
            movie.touch()
        elif change == "checking":
            rpc.torrents[A.encode()] = torrent(state="Checking")
        elif change == "relocated":
            rpc.torrents[A.encode()] = torrent(save="/data/media")
        elif change == "rpc":
            raise ConnectionError("unknown fresh state")
        else:
            root = config.local_media_base_local_folder
            root.rename(tmp_path / "detached")
            root.mkdir()

    rpc.before_status = before_status
    run(config, rpc, 101)
    assert rpc.execution_calls() == [(STATUS, A)]
    assert rows(config) == ({A: (1, 100, 100)} if change in {"rpc", "unmounted"} else {})


def test_cli_requires_opt_in_and_honors_pending_actions_dry_run(config, tmp_path, monkeypatch):
    env = {
        "DELUGE_HOST": config.deluge_host,
        "DELUGE_PORT": str(config.deluge_port),
        "DELUGE_USERNAME": "test",
        "DELUGE_PASSWORD": "test",
        "DELUGE_TORRENT_BASE_REMOTE_FOLDER": config.deluge_torrent_base_remote_folder,
        "DELUGE_MEDIA_BASE_REMOTE_FOLDER": config.deluge_media_base_remote_folder,
        "LOCAL_TORRENT_BASE_LOCAL_FOLDER": str(config.local_torrent_base_local_folder),
        "LOCAL_MEDIA_BASE_LOCAL_FOLDER": str(config.local_media_base_local_folder),
        "OUTPUT_FILE": str(tmp_path / "orphaned_files.json"),
        "APP_SQLITE_CACHE_PATH": str(config.sqlite_cache_path),
        "DELETION_CONSECUTIVE_SCANS_THRESHOLD": "2",
        "DELETION_DAYS_THRESHOLD": "0",
        "APP_LOG_DIR": str(tmp_path / "logs"),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    import deluge_client

    from deluge_orphaned_files import cli

    monkeypatch.setattr(cli, "find_orphaned_files", lambda **_: 1)
    monkeypatch.setattr(cli, "process_autoremove_labeling", lambda **_: None)
    monkeypatch.setattr(cli, "_process_deletion_cycle", lambda **_: None)
    legacy_modes = []

    def execute_pending_actions(**kwargs):
        legacy_modes.append(kwargs["dry_run"])
        return 0, 0, 0

    monkeypatch.setattr(cli, "execute_pending_actions", execute_pending_actions)

    rpc = RPC({A: torrent()})
    rpc.connected = True
    rpc.connect = rpc.disconnect = lambda: None
    monkeypatch.setattr(deluge_client, "DelugeRPCClient", lambda **_: rpc)

    def cli_run(*args):
        monkeypatch.setattr(sys, "argv", ["deluge_orphaned_files", "--no-notify", *args])
        return cli.main()

    opt_in = "--reconcile-missing-payloads"
    with pytest.raises(SystemExit) as exc:
        cli_run(opt_in)
    assert exc.value.code == 2
    assert cli_run("--sqlite", "--execute-pending-actions", "--force") == 0
    assert not rpc.calls
    assert cli_run("--sqlite", opt_in) == 0
    before = rows(config)
    assert A in before and before[A][0] == 1
    assert cli_run("--sqlite", opt_in, "--execute-pending-actions") == 0
    assert rows(config) == before
    assert not rpc.execution_calls()
    assert cli_run("--sqlite", opt_in, "--execute-pending-actions", "--force") == 0
    assert rpc.execution_calls() == [(STATUS, A), (REMOVE, A)]
    assert rows(config) == {}
    assert legacy_modes == [False, True, False]
