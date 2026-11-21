"""Tests for stuck cross-seed repair (issue #1).

Safety invariants under test: planning never proposes clobbering a shared
destination or creating a directory; donors must be verified files from a
*different* torrent actually present on disk; an apply run either verifies
100% (resume, backups deleted) or restores the exact original files,
priorities and options; unexpected downloads during the guarded recheck
abort the repair.
"""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from deluge_orphaned_files.logic import stuck_repair
from deluge_orphaned_files.logic.stuck_repair import (
    ACTION_ALREADY_LINKED,
    ACTION_LINK,
    ACTION_REPLACE,
    ACTION_SKIP,
    FilePlan,
    Planner,
    RepairExecutor,
    TorrentPlan,
    is_stuck,
    run_stuck_repair,
)

MB = 1024 * 1024
NOW = 1_800_000_000.0
REMOTE_BASE = "/data/torrents"


def make_file(path: Path, size: int, content: bytes = b"") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(content)
        fh.truncate(size)
    return path


def make_torrent(
    *,
    name="Some.Release-GRP",
    state="Paused",
    label="tvcat.cross-seed",
    progress=99.0,
    is_finished=False,
    time_added=NOW - 48 * 3600,
    save_path=f"{REMOTE_BASE}/cross-seed-links/X",
    files=(("ep.mkv", 100 * MB, 0.5, 4),),
):
    """Decoded torrent dict as run_stuck_repair builds it. files: (path, size, progress, priority)."""
    return {
        "name": name,
        "state": state,
        "label": label,
        "progress": progress,
        "is_finished": is_finished,
        "time_added": time_added,
        "save_path": save_path,
        "files": [{"path": p, "size": s} for p, s, _, _ in files],
        "file_progress": [fp for _, _, fp, _ in files],
        "file_priorities": [pr for _, _, _, pr in files],
    }


@pytest.fixture
def planner(tmp_path) -> Planner:
    return Planner(remote_base=REMOTE_BASE, local_base=str(tmp_path), size_only_min_bytes=50 * MB)


def donor_torrent(tmp_path, *, rel="tv/ep.mkv", size=100 * MB, content=b"donor-content"):
    """Write the donor file and return (torrent dict, local path)."""
    local = make_file(tmp_path / rel, size, content)
    save_path = f"{REMOTE_BASE}/{str(Path(rel).parent)}"
    torrent = make_torrent(
        name="Donor.Release-GRP",
        state="Seeding",
        label="tvcat",
        progress=100.0,
        is_finished=True,
        save_path=save_path,
        files=((Path(rel).name, size, 1.0, 4),),
    )
    return torrent, local


# -- selection ---------------------------------------------------------------


def test_is_stuck_predicate():
    stuck = make_torrent()
    assert is_stuck(stuck, now=NOW, min_age_hours=24)
    assert not is_stuck({**stuck, "state": "Seeding"}, now=NOW, min_age_hours=24)
    assert not is_stuck({**stuck, "is_finished": True}, now=NOW, min_age_hours=24)
    assert not is_stuck({**stuck, "label": "tvcat"}, now=NOW, min_age_hours=24)
    assert not is_stuck({**stuck, "time_added": NOW - 3600}, now=NOW, min_age_hours=24)
    assert is_stuck({**stuck, "label": "cross-seed"}, now=NOW, min_age_hours=24)
    assert is_stuck({**stuck, "label": "cross-seed.cross-seed"}, now=NOW, min_age_hours=24)


# -- planning ------------------------------------------------------------------


def test_plan_links_when_destination_absent(planner, tmp_path):
    donor, _ = donor_torrent(tmp_path)
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    plans = planner.plan_all({"stuck1": make_torrent(), "donor1": donor}, now=NOW, min_age_hours=24)
    assert len(plans) == 1 and plans[0].repairable
    assert plans[0].files[0].action == ACTION_LINK


def test_plan_replaces_existing_partial(planner, tmp_path):
    donor, _ = donor_torrent(tmp_path)
    make_file(tmp_path / "cross-seed-links/X/ep.mkv", 100 * MB, b"partial-junk")
    plans = planner.plan_all({"stuck1": make_torrent(), "donor1": donor}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_REPLACE


def test_plan_detects_already_linked(planner, tmp_path):
    donor, donor_local = donor_torrent(tmp_path)
    dest = tmp_path / "cross-seed-links/X/ep.mkv"
    dest.parent.mkdir(parents=True)
    os.link(donor_local, dest)
    plans = planner.plan_all({"stuck1": make_torrent(), "donor1": donor}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_ALREADY_LINKED
    assert plans[0].repairable


def test_plan_skips_shared_destination(planner, tmp_path):
    """The 3-clone music pattern from prod: identical destinations block repair."""
    donor, _ = donor_torrent(tmp_path)
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    torrents = {"stuck1": make_torrent(), "stuck2": make_torrent(), "donor1": donor}
    plans = planner.plan_all(torrents, now=NOW, min_age_hours=24)
    assert len(plans) == 2
    assert all(p.files[0].action == ACTION_SKIP for p in plans)
    assert all("another torrent" in p.files[0].reason for p in plans)


def test_plan_skips_destination_shared_through_directory_alias(planner, tmp_path):
    donor, _ = donor_torrent(tmp_path)
    directory = tmp_path / "cross-seed-links/X"
    directory.mkdir(parents=True)
    (tmp_path / "alias").symlink_to(directory, target_is_directory=True)
    other = make_torrent(label="tv", save_path=f"{REMOTE_BASE}/alias")

    [plan] = planner.plan_all({"donor": donor, "target": make_torrent(), "other": other}, now=NOW, min_age_hours=24)

    assert not plan.repairable
    assert "another torrent" in plan.blocking_reason


def test_plan_skips_destination_outside_base_through_symlink(planner, tmp_path):
    donor, _ = donor_torrent(tmp_path)
    base = tmp_path / "cross-seed-links"
    base.mkdir()
    (base / "X").symlink_to(tmp_path.parent, target_is_directory=True)

    [plan] = planner.plan_all({"donor": donor, "target": make_torrent()}, now=NOW, min_age_hours=24)

    assert not plan.repairable
    assert "outside torrent base" in plan.blocking_reason


def test_plan_rejects_symlink_donor(planner, tmp_path):
    donor, donor_path = donor_torrent(tmp_path)
    renamed = donor_path.rename(donor_path.with_suffix(".original"))
    donor_path.symlink_to(renamed)
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)

    [plan] = planner.plan_all({"donor": donor, "target": make_torrent()}, now=NOW, min_age_hours=24)

    assert not plan.repairable
    assert "no verified donor" in plan.blocking_reason


def test_plan_skips_when_parent_dir_missing(planner, tmp_path):
    """Never create directories: a missing destination folder blocks repair."""
    donor, _ = donor_torrent(tmp_path)
    plans = planner.plan_all({"stuck1": make_torrent(), "donor1": donor}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_SKIP
    assert "directory missing" in plans[0].files[0].reason


def test_plan_skips_when_stale_backup_exists(planner, tmp_path):
    donor, _ = donor_torrent(tmp_path)
    make_file(tmp_path / "cross-seed-links/X/ep.mkv.delugeof-repair-bak", 1)
    plans = planner.plan_all({"stuck1": make_torrent(), "donor1": donor}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_SKIP
    assert "stale backup" in plans[0].files[0].reason


def test_size_only_match_needs_50mb(planner, tmp_path):
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    small_donor, _ = donor_torrent(tmp_path, rel="tv/renamed.bin", size=10 * MB)
    stuck = make_torrent(files=(("movie.bin", 10 * MB, 0.0, 4),))
    plans = planner.plan_all({"stuck1": stuck, "donor1": small_donor}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_SKIP
    assert "no verified donor" in plans[0].files[0].reason


def test_size_only_match_for_large_files(planner, tmp_path):
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    big_donor, _ = donor_torrent(tmp_path, rel="tv/renamed.mkv", size=60 * MB)
    stuck = make_torrent(files=(("movie.mkv", 60 * MB, 0.0, 4),))
    plans = planner.plan_all({"stuck1": stuck, "donor1": big_donor}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_LINK


def test_size_only_match_rejects_ambiguous_donors(planner, tmp_path):
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    d1, _ = donor_torrent(tmp_path, rel="tv/a.mkv", size=60 * MB)
    d2, _ = donor_torrent(tmp_path, rel="movies/b.mkv", size=60 * MB)
    stuck = make_torrent(files=(("movie.mkv", 60 * MB, 0.0, 4),))
    plans = planner.plan_all({"s": stuck, "d1": d1, "d2": d2}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_SKIP
    assert "ambiguous" in plans[0].files[0].reason


def test_size_only_match_accepts_hardlinked_donor_copies(planner, tmp_path):
    """Two donor paths that are the same inode are one donor, not ambiguity."""
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    d1, local1 = donor_torrent(tmp_path, rel="tv/a.mkv", size=60 * MB)
    local2 = tmp_path / "movies/b.mkv"
    local2.parent.mkdir(parents=True)
    os.link(local1, local2)
    d2 = make_torrent(name=" clone", state="Seeding", label="moviecat", progress=100.0, is_finished=True, save_path=f"{REMOTE_BASE}/movies", files=(("b.mkv", 60 * MB, 1.0, 4),))
    stuck = make_torrent(files=(("movie.mkv", 60 * MB, 0.0, 4),))
    plans = planner.plan_all({"s": stuck, "d1": d1, "d2": d2}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_LINK


def test_donor_must_exist_on_disk_and_be_from_another_torrent(planner, tmp_path):
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    # Donor torrent claims completeness but its file was never written to disk.
    ghost = make_torrent(name="Ghost", state="Seeding", label="tvcat", progress=100.0, is_finished=True, save_path=f"{REMOTE_BASE}/tv", files=(("ep.mkv", 100 * MB, 1.0, 4),))
    plans = planner.plan_all({"stuck1": make_torrent(), "ghost": ghost}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_SKIP

    # A complete file inside the stuck torrent itself is not a donor.
    self_donor = make_torrent(files=(("ep.mkv", 100 * MB, 0.5, 4), ("sub/ep.mkv", 100 * MB, 1.0, 4)))
    make_file(tmp_path / "cross-seed-links/X/sub/ep.mkv", 100 * MB)
    plans = planner.plan_all({"stuck1": self_donor}, now=NOW, min_age_hours=24)
    assert plans[0].files[0].action == ACTION_SKIP


def test_torrent_with_any_unmatchable_file_is_left_alone(planner, tmp_path):
    donor, _ = donor_torrent(tmp_path)
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    stuck = make_torrent(files=(("ep.mkv", 100 * MB, 0.5, 4), ("missing.nfo", 1234, 0.0, 4)))
    plans = planner.plan_all({"stuck1": stuck, "donor1": donor}, now=NOW, min_age_hours=24)
    assert not plans[0].repairable


# -- executor ---------------------------------------------------------------------


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


class FakeClient:
    """Stateful stand-in for DelugeRPCClient (.call interface, bytes keys)."""

    def __init__(
        self,
        *,
        priorities,
        recheck_durations=(1,),
        measure_finished=True,
        measure_progress=100.0,
        check_download_bump=0,
        final_state="Paused",
        measure_file_progress=None,
        fail_prio_set_on=(),
        stale_polls=0,
    ):
        self.calls = []
        self.connected = True
        self.priorities = list(priorities)
        self.options = {"auto_managed": True, "max_download_speed": -1.0}
        self.payload = 1000
        self.checking = False
        self.paused = True
        self.state = "Paused"
        self._polls_left = 0
        self._durations = list(recheck_durations)
        self.measure_finished = measure_finished
        self._finished = measure_finished
        self.measure_progress = measure_progress
        self.check_download_bump = check_download_bump
        self.final_state = final_state
        self.measure_file_progress = measure_file_progress
        self.fail_prio_set_on = set(fail_prio_set_on)
        self._prio_set_count = 0
        self._stale_polls = stale_polls
        self._stale_left = stale_polls
        self.resumed = []
        self.force_rechecks = 0

    def call(self, method, *args):
        self.calls.append((method, args))
        if method == "core.get_torrent_status":
            return self._status(args[1])
        if method == "core.set_torrent_file_priorities":
            # Deluge 2.2 takes a single torrent id here, not a list (S1).
            if not isinstance(args[0], str):
                raise AssertionError(f"set_torrent_file_priorities expects one torrent id, got {args[0]!r}")
            self._prio_set_count += 1
            if self._prio_set_count in self.fail_prio_set_on:
                raise RuntimeError("injected priority-set failure")
            # Deluge resets is_finished when a file goes 0 -> positive (S6).
            if any(int(o) == 0 and int(n) > 0 for o, n in zip(self.priorities, args[1])):
                self._finished = False
            self.priorities = list(args[1])
        elif method == "core.set_torrent_options":
            self.options.update(args[1])
        elif method == "core.force_recheck":
            self.force_rechecks += 1
            self.checking = True
            self.paused = False
            self._polls_left = self._durations.pop(0) if self._durations else 1
            self._stale_left = self._stale_polls
        elif method == "core.pause_torrent":
            self.checking = False
            self.paused = True
            self.state = "Error" if self.state == "Error" else "Paused"
        elif method == "core.resume_torrent":
            self.resumed.append(args[0][0])
        return None

    def _status(self, fields):
        out = {}
        state = self.state
        if self.checking:
            if self._stale_left > 0:
                # Deluge's display state lags the libtorrent check that just
                # started (S7): settled-looking, but hashing has not finished.
                self._stale_left -= 1
            elif self._polls_left > 0:
                self._polls_left -= 1
                state = "Checking"
            else:
                self.checking = False
                state = self.state = self.final_state
                self.paused = state in ("Paused", "Error")
        payload = self.payload + (self.check_download_bump if state == "Checking" and self.force_rechecks == 1 else 0)
        values = {
            "state": state,
            "paused": self.paused,
            "progress": self.measure_progress,
            "is_finished": self._finished,
            "all_time_download": payload,
            "file_priorities": tuple(self.priorities),
            "file_progress": list(self.measure_file_progress) if self.measure_file_progress is not None else [1.0] * len(self.priorities),
            **self.options,
        }
        for f in fields:
            if f in values:
                out[f.encode()] = values[f]
        return out


def make_executor(client, clock, **overrides):
    kwargs = dict(max_download_speed_kib=10.0, download_abort_bytes=MB, recheck_timeout_seconds=7200, poll_seconds=5.0, sleep_fn=clock.sleep, monotonic_fn=clock.monotonic)
    kwargs.update(overrides)
    return RepairExecutor(client, **kwargs)


def make_plan(tmp_path, *, with_dest_content=None):
    donor_local = make_file(tmp_path / "donor/ep.mkv", 64, b"donor-content")
    dest = tmp_path / "dest/ep.mkv"
    dest.parent.mkdir(parents=True)
    action = ACTION_LINK
    if with_dest_content is not None:
        make_file(dest, 64, with_dest_content)
        action = stuck_repair.ACTION_REPLACE
    plan = TorrentPlan(torrent_id="abc", name="Stuck.Release", label="tvcat.cross-seed", age_days=3, progress=99.0)
    plan.files.append(FilePlan(0, "/data/torrents/dest/ep.mkv", dest, 64, action, donor_local=donor_local, donor_remote="/data/torrents/donor/ep.mkv"))
    return plan, dest, donor_local


def test_apply_success_links_resumes_and_restores(tmp_path):
    clock = FakeClock()
    client = FakeClient(priorities=[0, 4], recheck_durations=(2,))
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")
    plan.files[0].size = dest.stat().st_size

    result = make_executor(client, clock).repair(plan)

    assert result.outcome == "repaired"
    assert dest.read_bytes().startswith(b"donor-content")
    assert not Path(str(dest) + ".delugeof-repair-bak").exists()
    assert client.resumed == ["abc"]
    assert client.priorities == [0, 4]
    assert client.options == {"auto_managed": True, "max_download_speed": -1.0}
    assert client.force_rechecks == 1


def test_apply_failure_restores_original_files_and_rechecks(tmp_path):
    clock = FakeClock()
    client = FakeClient(priorities=[4], recheck_durations=(1, 1), measure_finished=False, measure_progress=99.5, measure_file_progress=[0.995])
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, clock).repair(plan)

    assert result.outcome == "failed"
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert not Path(str(dest) + ".delugeof-repair-bak").exists()
    assert client.resumed == []
    assert client.force_rechecks == 2  # verification recheck + rollback recheck
    assert client.priorities == [4]
    assert client.options == {"auto_managed": True, "max_download_speed": -1.0}


def test_downloads_during_recheck_abort_and_roll_back(tmp_path):
    clock = FakeClock()
    client = FakeClient(priorities=[4], recheck_durations=(5, 1), check_download_bump=2 * MB)
    plan, dest, _ = make_plan(tmp_path)

    result = make_executor(client, clock).repair(plan)

    assert result.outcome == "aborted-download"
    assert not dest.exists()  # our link was rolled back; destination was absent before
    assert client.resumed == []
    assert ("core.pause_torrent", (["abc"],)) in client.calls


def test_recheck_timeout_rolls_back(tmp_path):
    clock = FakeClock()
    client = FakeClient(priorities=[4], recheck_durations=(10_000, 1))
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, clock, recheck_timeout_seconds=30).repair(plan)

    assert result.outcome == "timed-out"
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert client.resumed == []


# -- orchestration -------------------------------------------------------------------


def encode_raw(torrents):
    raw = {}
    for tid, t in torrents.items():
        entry = {}
        for k, v in t.items():
            if k == "files":
                entry[b"files"] = [{b"path": f["path"].encode(), b"size": f["size"]} for f in v]
            elif isinstance(v, str):
                entry[k.encode()] = v.encode()
            else:
                entry[k.encode()] = v
        raw[tid.encode()] = entry
    return raw


class OrchestrationClient:
    connected = True

    def __init__(self, torrents):
        self._raw = encode_raw(torrents)
        self.mutations = []

    def call(self, method, *args):
        if method == "core.get_torrents_status":
            return self._raw
        self.mutations.append((method, args))
        return None

    def disconnect(self):
        pass


@pytest.fixture
def config(tmp_path):
    return SimpleNamespace(
        deluge_host="x",
        deluge_port=1,
        deluge_username="x",
        deluge_password="x",
        deluge_torrent_base_remote_folder=REMOTE_BASE,
        local_torrent_base_local_folder=tmp_path,
        sqlite_cache_path=tmp_path / "cache.db",
        repair_min_age_hours=24.0,
        repair_min_free_gb=0.001,
        repair_size_only_min_mb=50.0,
        repair_max_download_speed_kib=10.0,
        repair_download_abort_mb=1.0,
        repair_recheck_timeout_seconds=7200,
    )


def test_dry_run_plans_without_touching_anything(config, tmp_path):
    donor, _ = donor_torrent(tmp_path)
    (tmp_path / "cross-seed-links/X").mkdir(parents=True)
    client = OrchestrationClient({"stuck1": make_torrent(), "donor1": donor})

    stats = run_stuck_repair(config, apply=False, client=client, now_fn=lambda: NOW)

    assert stats["selected"] == 1 and stats["repairable"] == 1
    assert client.mutations == []
    assert not (tmp_path / "cross-seed-links/X/ep.mkv").exists()


def test_lock_held_skips_run(config, tmp_path):
    lock_path = tmp_path / "stuck-repair.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:

        class ExplodingClient:
            connected = True

            def call(self, *a):
                raise AssertionError("client used despite held lock")

        stats = run_stuck_repair(config, apply=False, client=ExplodingClient(), now_fn=lambda: NOW)
        assert stats.get("lock_held") is True
    finally:
        os.close(fd)


def test_low_free_space_skips_run(config):
    config.repair_min_free_gb = 1e12
    stats = run_stuck_repair(config, apply=False, client=OrchestrationClient({}), now_fn=lambda: NOW)
    assert stats.get("low_free_space") is True


def test_connection_failure_is_a_clean_skip(config, monkeypatch):
    class RefusingClient:
        def __init__(self, **kwargs):
            pass

        def connect(self):
            raise ConnectionRefusedError("nope")

    monkeypatch.setattr("deluge_client.DelugeRPCClient", RefusingClient)
    stats = run_stuck_repair(config, apply=False, now_fn=lambda: NOW)
    assert stats.get("connection_failed") is True


# -- sol-loop round 1 regressions ---------------------------------------------------


def test_link_failure_puts_original_back(tmp_path, monkeypatch):
    """S2: if os.link fails after the rename, the original must not be stranded as a backup."""
    clock = FakeClock()
    client = FakeClient(priorities=[4])
    plan, dest, donor = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    link = os.link

    def failing_link(src, dst, **kwargs):
        if src == donor:
            raise OSError(18, "Invalid cross-device link")
        return link(src, dst, **kwargs)

    monkeypatch.setattr(stuck_repair.os, "link", failing_link)
    result = make_executor(client, clock).repair(plan)

    assert result.outcome == "error"
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert not Path(str(dest) + ".delugeof-repair-bak").exists()


def test_priorities_restore_failure_leaves_original_in_place(tmp_path):
    clock = FakeClock()
    # Priority-set calls: 1 guard-zero, 2 measure-restore, 3 rollback-recheck-zero,
    # 4 rollback-restore (fails).
    client = FakeClient(priorities=[4], recheck_durations=(1, 1), measure_finished=False, measure_progress=99.5, measure_file_progress=[0.995], fail_prio_set_on={4})
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, clock).repair(plan)

    assert dest.read_bytes().startswith(b"partial-junk!")
    assert result.outcome == "error"
    assert client.force_rechecks == 2


def test_error_state_never_counts_as_verified(tmp_path):
    """S4: Deluge reports progress=100 in Error state; that must not delete backups or resume."""
    clock = FakeClock()
    client = FakeClient(priorities=[4], recheck_durations=(1, 1), final_state="Error", measure_finished=True, measure_progress=100.0)
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, clock).repair(plan)

    assert result.outcome != "repaired"
    assert client.resumed == []
    assert dest.read_bytes().startswith(b"partial-junk!")


def test_instant_recheck_completes_after_start_grace(tmp_path):
    """S5: a recheck finishing between polls is completion via the grace path, not 'never started'."""
    clock = FakeClock()
    client = FakeClient(priorities=[4], recheck_durations=(0,))
    plan, dest, _ = make_plan(tmp_path)

    result = make_executor(client, clock).repair(plan)

    assert result.outcome == "repaired"
    assert client.resumed == ["abc"]
    assert clock.t > stuck_repair.RECHECK_START_GRACE_SECONDS


def test_stale_settled_polls_do_not_complete_check_early(tmp_path):
    """S7: settled-looking polls before Checking appears must not be read as done."""
    clock = FakeClock()
    client = FakeClient(priorities=[4], recheck_durations=(1,), stale_polls=3)
    plan, dest, _ = make_plan(tmp_path)

    result = make_executor(client, clock).repair(plan)

    assert result.outcome == "repaired"
    assert client.force_rechecks == 1  # no premature measurement -> rollback -> second recheck
    assert clock.t < stuck_repair.RECHECK_START_GRACE_SECONDS


@pytest.mark.parametrize("missing", ["state", "paused", "file_priorities", "auto_managed", "max_download_speed", "all_time_download"])
def test_incomplete_snapshot_leaves_torrent_and_files_untouched(tmp_path, missing):
    class IncompleteStatus(FakeClient):
        def _status(self, fields):
            result = super()._status(fields)
            result.pop(missing.encode(), None)
            return result

    client = IncompleteStatus(priorities=[4])
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, FakeClock()).repair(plan)

    assert result.outcome == "error"
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert {method for method, _ in client.calls} == {"core.get_torrent_status"}


@pytest.mark.parametrize(
    ("invalid_counter", "persistent", "expected_rechecks"),
    [(None, False, 2), (1, False, 2), (None, True, 1)],
    ids=["missing", "reset", "missing-through-rollback"],
)
def test_download_counter_lost_during_recheck_rolls_back(tmp_path, invalid_counter, persistent, expected_rechecks):
    class LostCounter(FakeClient):
        def _status(self, fields):
            result = super()._status(fields)
            if self.force_rechecks == 1 and b"all_time_download" in result and (persistent or "state" in fields):
                result[b"all_time_download"] = invalid_counter
            return result

    client = LostCounter(priorities=[4])
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, FakeClock()).repair(plan)

    assert result.outcome == "error"
    assert "cannot enforce download abort guard" in result.detail
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert client.force_rechecks == expected_rechecks
    assert client.paused and client.state == "Paused"
    assert not Path(str(dest) + stuck_repair.BACKUP_SUFFIX).exists()
    assert client.resumed == []
    assert client.priorities == [4]
    assert client.options == {"auto_managed": True, "max_download_speed": -1.0}


def test_counter_decrease_above_baseline_is_not_accepted(tmp_path):
    class CounterReset(FakeClient):
        def __init__(self):
            super().__init__(priorities=[4], recheck_durations=(2, 1))
            self.samples = iter((self.payload + 200, self.payload + 100))

        def _status(self, fields):
            status = super()._status(fields)
            if self.force_rechecks == 1 and "state" in fields and "all_time_download" in fields:
                status[b"all_time_download"] = next(self.samples, self.payload + 100)
            return status

    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")
    client = CounterReset()

    result = make_executor(client, FakeClock()).repair(plan)

    assert "reset during recheck" in result.detail
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert not client.resumed


@pytest.mark.parametrize("late_counter", [None, 2 * MB], ids=["lost", "over-budget"])
def test_pause_confirmation_is_still_guarded(tmp_path, late_counter):
    class LateCounter(FakeClient):
        def _status(self, fields):
            status = super()._status(fields)
            if self.force_rechecks == 1 and self.paused and "state" in fields and "all_time_download" in fields:
                status[b"all_time_download"] = None if late_counter is None else self.payload + late_counter
            return status

    client = LateCounter(priorities=[4], final_state="Seeding")
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, FakeClock()).repair(plan)

    assert result.outcome == ("error" if late_counter is None else "aborted-download")
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert not client.resumed


def test_rollback_recheck_timeout_does_not_start_another_recheck(tmp_path):
    client = FakeClient(priorities=[4], recheck_durations=(1, 10_000), measure_file_progress=[0.995])
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, FakeClock(), recheck_timeout_seconds=30).repair(plan)

    assert result.outcome == "error"
    assert "rollback: recheck timed out" in result.detail
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert client.force_rechecks == 2
    assert client.paused and client.priorities == [4]
    assert client.options == {"auto_managed": True, "max_download_speed": -1.0}


@pytest.mark.parametrize("pause", ["immediate", "delayed", "lost-reply"])
def test_rollback_keeps_guard_until_repaused_after_auto_management(tmp_path, pause):
    class AutoRestarts(FakeClient):
        restarted = False
        pending_pause = 0

        def call(self, method, *args):
            if method == "core.pause_torrent" and self.restarted and pause == "delayed":
                self.pending_pause = 2
                return None
            result = super().call(method, *args)
            if method == "core.set_torrent_options" and args[1].get("auto_managed"):
                assert self.options["max_download_speed"] == 10.0
                assert self.priorities == [0]
                self.restarted = True
                self.state = "Downloading"
                self.paused = False
                if pause == "lost-reply":
                    raise ConnectionError("auto-management reply lost")
            return result

        def _status(self, fields):
            if self.pending_pause and "paused" in fields:
                self.pending_pause -= 1
                if self.pending_pause == 0:
                    super().call("core.pause_torrent", ["abc"])
            return super()._status(fields)

    client = AutoRestarts(priorities=[4], measure_file_progress=[0.995])
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, FakeClock()).repair(plan)

    assert client.restarted
    if pause != "immediate":
        assert result.outcome == "error"
        expected = "guard reasserted" if pause == "delayed" else "auto-management reply lost"
        assert expected in result.detail
        assert client.priorities == [0]
        assert client.options["max_download_speed"] == 10.0
        if pause == "delayed":
            assert client.options["auto_managed"] is False
    else:
        assert result.outcome == "failed"
        assert client.priorities == [4]
        assert client.options == {"auto_managed": True, "max_download_speed": -1.0}
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert client.state == "Paused" and client.paused
    assert not client.resumed


@pytest.mark.parametrize("drift", [False, True])
def test_unconfirmed_pause_preserves_backup_and_guard(tmp_path, drift):
    class RefusesPause(FakeClient):
        deactivated_at = None

        def _status(self, fields):
            if drift and self.force_rechecks == 1 and "all_time_download" in fields and "state" in fields:
                self.options["auto_managed"] = True
                self.priorities = [4]
            return super()._status(fields)

        def call(self, method, *args):
            if method == "core.pause_torrent":
                return None
            if method == "core.set_torrent_options" and drift and self.force_rechecks == 1:
                self.deactivated_at = clock.t
            return super().call(method, *args)

    clock = FakeClock()
    client = RefusesPause(priorities=[4], final_state="Seeding", measure_file_progress=[0.995])
    plan, dest, donor = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, clock).repair(plan)

    assert result.outcome == "error"
    assert ("auto_managed" if drift else "could not confirm") in result.detail
    assert dest.samefile(donor)
    assert Path(str(dest) + stuck_repair.BACKUP_SUFFIX).read_bytes().startswith(b"partial-junk!")
    assert client.force_rechecks == 1
    assert client.resumed == []
    assert client.priorities == [0]
    assert client.options == {"auto_managed": False, "max_download_speed": 10.0}
    if drift:
        assert client.deactivated_at < stuck_repair.PAUSE_SETTLE_SECONDS


def test_backup_created_during_apply_is_not_overwritten(tmp_path, monkeypatch):
    client = FakeClient(priorities=[4])
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")
    backup = Path(str(dest) + stuck_repair.BACKUP_SUFFIX)
    link = os.link

    def wait_for_collision(src, dst, **kwargs):
        if dst == backup:
            backup.write_bytes(b"keep me")
        return link(src, dst, **kwargs)

    monkeypatch.setattr(stuck_repair.os, "link", wait_for_collision)
    result = make_executor(client, FakeClock()).repair(plan)

    assert result.outcome == "error"
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert backup.read_bytes() == b"keep me"
    assert client.force_rechecks == 0


def test_backup_cleanup_failure_does_not_roll_back_verified_links(tmp_path, monkeypatch):
    client = FakeClient(priorities=[4, 4])
    plan, first, donor = make_plan(tmp_path, with_dest_content=b"first-original")
    second = make_file(first.with_name("second.mkv"), 64, b"second-original")
    plan.files.append(FilePlan(1, f"{REMOTE_BASE}/dest/second.mkv", second, 64, ACTION_REPLACE, donor_local=donor))
    first_backup = Path(str(first) + stuck_repair.BACKUP_SUFFIX)
    second_backup = Path(str(second) + stuck_repair.BACKUP_SUFFIX)
    unlink = Path.unlink

    def fail_one_backup(path, *args, **kwargs):
        if path == second_backup:
            raise PermissionError("cannot delete second backup")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_one_backup)
    result = make_executor(client, FakeClock()).repair(plan)

    assert result.outcome == "repaired"
    assert "cannot delete second backup" in result.detail
    assert first.samefile(donor) and second.samefile(donor)
    assert not first_backup.exists()
    assert second_backup.read_bytes().startswith(b"second-original")
    assert client.force_rechecks == 1
    assert client.resumed == ["abc"]


@pytest.mark.parametrize(
    ("field", "value", "detail"),
    [("auto_managed", True, "auto_managed"), ("max_download_speed", 0.0, "max_download_speed"), ("file_priorities", (4,), "priorities no longer guarded")],
)
def test_guard_readback_drift_during_check_rolls_back(tmp_path, field, value, detail):
    class GuardDrift(FakeClient):
        def _status(self, fields):
            result = super()._status(fields)
            if self.force_rechecks == 1 and fields[0] == "state" and "all_time_download" in fields:
                result[field.encode()] = value
            return result

    client = GuardDrift(priorities=[4])
    plan, dest, _ = make_plan(tmp_path, with_dest_content=b"partial-junk!")

    result = make_executor(client, FakeClock()).repair(plan)

    assert result.outcome == "error"
    assert detail in result.detail
    assert dest.read_bytes().startswith(b"partial-junk!")
    assert client.resumed == []
    assert client.force_rechecks == 2
