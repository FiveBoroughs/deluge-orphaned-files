"""Tests for the internal cron scheduler (daemon mode).

The invariants that matter operationally: fires happen at wall-clock cron
times (DST included), a failed or overrunning run never kills or duplicates
the schedule, shutdown during the idle wait exits promptly, and the
no-CRON_SCHEDULE path execs the CLI exactly like the old one-shot container.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import pytest

from deluge_orphaned_files import scheduler
from deluge_orphaned_files.scheduler import Scheduler, next_run, parse_bool, validate_schedule

UTC = timezone.utc


class FakeClock:
    """Deterministic clock: sleep() just advances the visible time."""

    def __init__(self, start: datetime) -> None:
        self.t = start

    def now(self) -> datetime:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)


class RecordingRunner:
    def __init__(self, clock: FakeClock, *, exit_codes=(0,), run_duration: timedelta = timedelta(0)) -> None:
        self._clock = clock
        self._exit_codes = list(exit_codes)
        self._run_duration = run_duration
        self.calls: list[tuple[datetime, list[str]]] = []

    def __call__(self, argv) -> int:
        self.calls.append((self._clock.now(), list(argv)))
        self._clock.t += self._run_duration
        if self._exit_codes:
            return self._exit_codes.pop(0)
        return 0


def make_scheduler(clock: FakeClock, runner, tmp_path, **kwargs) -> Scheduler:
    kwargs.setdefault("heartbeat_path", str(tmp_path / "heartbeat"))
    return Scheduler(
        kwargs.pop("expression", "0 * * * *"),
        ["--sqlite", "--force"],
        runner=runner,
        now_fn=clock.now,
        sleep_fn=clock.sleep,
        **kwargs,
    )


# -- cron math ---------------------------------------------------------------


def test_next_run_is_strictly_after():
    """Landing exactly on a fire time must not re-fire for that same minute."""
    after = datetime(2026, 1, 1, 1, 0, tzinfo=UTC)
    assert next_run("0 1 * * *", after) == datetime(2026, 1, 2, 1, 0, tzinfo=UTC)


def test_next_run_supports_steps():
    after = datetime(2026, 1, 1, 0, 7, tzinfo=UTC)
    assert next_run("*/15 * * * *", after) == datetime(2026, 1, 1, 0, 15, tzinfo=UTC)


def test_next_run_keeps_wall_clock_across_dst():
    """A 12:00 job stays at 12:00 local when the UTC offset changes."""
    try:
        from zoneinfo import ZoneInfo

        paris = ZoneInfo("Europe/Paris")
    except Exception:
        pytest.skip("tzdata for Europe/Paris unavailable")
    after = datetime(2026, 3, 28, 12, 0, tzinfo=paris)  # CET (+01), day before spring-forward
    nxt = next_run("0 12 * * *", after)
    assert (nxt.day, nxt.hour, nxt.minute) == (29, 12, 0)
    assert nxt.utcoffset() == timedelta(hours=2)  # CEST


def test_validate_schedule_rejects_garbage():
    with pytest.raises(ValueError, match="invalid cron expression"):
        validate_schedule("not a cron")


def test_parse_bool_spellings_and_default():
    assert parse_bool("true", False) is True
    assert parse_bool("Off", True) is False
    assert parse_bool("1", False) is True
    assert parse_bool(None, True) is True
    assert parse_bool("banana", False) is False


# -- scheduler loop ------------------------------------------------------------


def test_startup_run_then_hourly_fires(tmp_path):
    clock = FakeClock(datetime(2026, 1, 1, 0, 30, tzinfo=UTC))
    runner = RecordingRunner(clock)
    sched = make_scheduler(clock, runner, tmp_path, run_on_startup=True, max_runs=3)

    assert sched.run() == 0
    times = [when for when, _ in runner.calls]
    assert times == [
        datetime(2026, 1, 1, 0, 30, tzinfo=UTC),  # startup
        datetime(2026, 1, 1, 1, 0, tzinfo=UTC),  # scheduled
        datetime(2026, 1, 1, 2, 0, tzinfo=UTC),  # scheduled
    ]
    assert all(argv == ["--sqlite", "--force"] for _, argv in runner.calls)


def test_without_startup_run_first_fire_is_the_schedule(tmp_path):
    clock = FakeClock(datetime(2026, 1, 1, 0, 30, tzinfo=UTC))
    runner = RecordingRunner(clock)
    sched = make_scheduler(clock, runner, tmp_path, run_on_startup=False, max_runs=1)

    sched.run()
    assert [when for when, _ in runner.calls] == [datetime(2026, 1, 1, 1, 0, tzinfo=UTC)]


def test_failed_runs_do_not_stop_the_schedule(tmp_path):
    clock = FakeClock(datetime(2026, 1, 1, 0, 30, tzinfo=UTC))
    runner = RecordingRunner(clock, exit_codes=[1, 7, 0])
    sched = make_scheduler(clock, runner, tmp_path, run_on_startup=True, max_runs=3)

    assert sched.run() == 0
    assert len(runner.calls) == 3


def test_overrunning_run_skips_missed_fire_without_catch_up(tmp_path):
    """A 90-minute run on an hourly schedule skips the missed hour, never overlaps."""
    clock = FakeClock(datetime(2026, 1, 1, 0, 30, tzinfo=UTC))
    runner = RecordingRunner(clock, run_duration=timedelta(minutes=90))
    sched = make_scheduler(clock, runner, tmp_path, run_on_startup=False, max_runs=2)

    sched.run()
    times = [when for when, _ in runner.calls]
    assert times == [datetime(2026, 1, 1, 1, 0, tzinfo=UTC), datetime(2026, 1, 1, 3, 0, tzinfo=UTC)]


def test_stop_during_idle_wait_exits_without_firing(tmp_path):
    clock = FakeClock(datetime(2026, 1, 1, 0, 30, tzinfo=UTC))
    runner = RecordingRunner(clock)
    holder: list[Scheduler] = []

    def stopping_sleep(seconds: float) -> None:
        holder[0].stop()
        clock.sleep(seconds)

    sched = Scheduler(
        "0 * * * *",
        [],
        run_on_startup=False,
        runner=runner,
        now_fn=clock.now,
        sleep_fn=stopping_sleep,
        heartbeat_path=str(tmp_path / "heartbeat"),
    )
    holder.append(sched)

    assert sched.run() == 0
    assert runner.calls == []


def test_heartbeat_file_is_touched(tmp_path):
    clock = FakeClock(datetime(2026, 1, 1, 0, 30, tzinfo=UTC))
    runner = RecordingRunner(clock)
    sched = make_scheduler(clock, runner, tmp_path, run_on_startup=True, max_runs=1)

    sched.run()
    assert (tmp_path / "heartbeat").exists()


# -- entrypoint dispatch ---------------------------------------------------------


def test_main_without_schedule_execs_cli_once(monkeypatch):
    """No CRON_SCHEDULE == old one-shot container: replace process with the CLI."""
    exec_calls = []
    monkeypatch.setattr(scheduler.os, "execvpe", lambda file, argv, env: exec_calls.append((file, argv)))

    result = scheduler.main(["--sqlite", "--force"], environ={})

    assert exec_calls == [(sys.executable, [sys.executable, "-m", "deluge_orphaned_files.cli", "--sqlite", "--force"])]
    assert result == 127  # only reached because the fake exec returned


def test_main_with_invalid_schedule_exits_2():
    """A typo'd schedule must fail loudly at startup, not idle forever."""
    assert scheduler.main([], environ={"CRON_SCHEDULE": "61 * * * *"}) == 2
