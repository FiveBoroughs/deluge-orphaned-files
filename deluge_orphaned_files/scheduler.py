"""Internal cron scheduler: keep the container alive and run the CLI on a schedule.

This module is the Docker ENTRYPOINT. Behaviour depends on the environment:

- ``CRON_SCHEDULE`` unset/empty: one-shot mode. The process is replaced
  (``exec``) by ``python -m deluge_orphaned_files.cli <args>`` — identical to
  running the CLI directly, so manual/docker-run usage is unchanged.
- ``CRON_SCHEDULE`` set (5-field cron expression): daemon mode. The CLI is run
  as a subprocess once per cron fire, forever. ``RUN_ON_STARTUP`` (default
  ``true``) additionally runs it once immediately, mirroring the old
  "container start == run" behaviour of the external cron setup.

Cron expressions are evaluated in container local time (the ``TZ`` env var),
using timezone-aware datetimes so DST transitions shift fire times with the
wall clock. Runs never overlap: the next fire is computed only after the
previous run finishes, so a run that overruns its slot simply skips the
missed fire(s) instead of queuing a catch-up.

A heartbeat file (``HEARTBEAT_PATH``) is refreshed while waiting and while a
child run is in progress; the Docker HEALTHCHECK uses its mtime to detect a
stuck scheduler. This module deliberately does not import ``.settings`` or
``.cli``: it must start (and report config errors) even when the CLI's own
configuration is missing, and child processes give each run a fresh
interpreter (no leaked state, memory, or half-open Deluge sessions).
"""

import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from croniter import croniter
from loguru import logger

CLI_MODULE = "deluge_orphaned_files.cli"
DEFAULT_HEARTBEAT_PATH = "/tmp/delugeof-heartbeat"
# Long sleeps are sliced so SIGTERM is honoured and the heartbeat stays fresh
# without busy-waiting.
WAIT_SLICE_SECONDS = 30.0
# After forwarding SIGTERM to a running child, escalate to SIGKILL once the
# child's own shutdown has had this long.
CHILD_TERM_GRACE_SECONDS = 10.0

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def parse_bool(value: Optional[str], default: bool) -> bool:
    """Parse an env-var boolean; unrecognised values fall back to ``default``."""
    if value is None or not value.strip():
        return default
    normalised = value.strip().lower()
    if normalised in _TRUE_VALUES:
        return True
    if normalised in _FALSE_VALUES:
        return False
    logger.warning("Unrecognised boolean value {!r}; using default {}", value, default)
    return default


def validate_schedule(expression: str) -> None:
    """Raise ``ValueError`` if ``expression`` is not a valid cron schedule."""
    if not croniter.is_valid(expression):
        raise ValueError(f"invalid cron expression: {expression!r} (expected 5 fields: minute hour day month weekday)")


def next_run(expression: str, after: datetime) -> datetime:
    """Return the first cron fire strictly after ``after`` (tz-aware in, tz-aware out)."""
    return croniter(expression, after).get_next(datetime)


def _local_now() -> datetime:
    return datetime.now().astimezone()


class Scheduler:
    """Run ``argv`` once per cron fire until stopped."""

    def __init__(
        self,
        expression: str,
        argv: Sequence[str],
        *,
        run_on_startup: bool = True,
        runner: Optional[Callable[[Sequence[str]], int]] = None,
        now_fn: Callable[[], datetime] = _local_now,
        sleep_fn: Optional[Callable[[float], None]] = None,
        heartbeat_path: Optional[str] = DEFAULT_HEARTBEAT_PATH,
        max_runs: Optional[int] = None,
    ) -> None:
        validate_schedule(expression)
        self._expression = expression
        self._argv = list(argv)
        self._run_on_startup = run_on_startup
        self._runner = runner if runner is not None else self._run_child
        self._now_fn = now_fn
        self._heartbeat_path = Path(heartbeat_path) if heartbeat_path else None
        self._max_runs = max_runs
        self._stop = threading.Event()
        # Default sleeper is the stop event itself: sleep ends the instant
        # shutdown is requested. Tests inject a fake-clock sleeper.
        self._sleep_fn = sleep_fn if sleep_fn is not None else self._stop.wait
        self._stop_at: Optional[float] = None
        self._child: Optional[subprocess.Popen] = None
        self._runs_completed = 0

    # -- lifecycle ---------------------------------------------------------

    def stop(self) -> None:
        """Request shutdown: wake any wait and terminate a running child."""
        if not self._stop.is_set():
            logger.info("Shutdown requested; stopping scheduler")
            self._stop_at = time.monotonic()
            self._stop.set()
        child = self._child
        if child is not None and child.poll() is None:
            child.terminate()

    def run(self) -> int:
        logger.info(
            "Scheduler started: CRON_SCHEDULE={!r} run_on_startup={} args={}",
            self._expression,
            self._run_on_startup,
            self._argv,
        )
        self._touch_heartbeat()
        if self._run_on_startup:
            self._run_once("startup")
        while not self._stop.is_set() and not self._max_runs_reached():
            target = next_run(self._expression, self._now_fn())
            logger.info("Next scheduled run at {}", target.isoformat())
            if not self._wait_until(target):
                break
            self._run_once("scheduled")
        logger.info("Scheduler stopped after {} run(s)", self._runs_completed)
        return 0

    # -- internals ---------------------------------------------------------

    def _max_runs_reached(self) -> bool:
        return self._max_runs is not None and self._runs_completed >= self._max_runs

    def _run_once(self, kind: str) -> None:
        if self._stop.is_set() or self._max_runs_reached():
            return
        self._touch_heartbeat()
        logger.info("Starting {} run: {} -m {} {}", kind, sys.executable, CLI_MODULE, " ".join(self._argv))
        started = time.monotonic()
        try:
            exit_code = self._runner(self._argv)
        except Exception as exc:  # the scheduler must outlive a broken run
            logger.exception("Runner raised unexpectedly: {}", exc)
            exit_code = -1
        self._runs_completed += 1
        self._touch_heartbeat()
        elapsed = time.monotonic() - started
        if exit_code == 0:
            logger.info("{} run finished OK in {:.1f}s", kind.capitalize(), elapsed)
        else:
            logger.error("{} run failed with exit code {} after {:.1f}s; scheduler continues", kind.capitalize(), exit_code, elapsed)

    def _run_child(self, argv: Sequence[str]) -> int:
        proc = subprocess.Popen([sys.executable, "-m", CLI_MODULE, *argv])
        self._child = proc
        try:
            while True:
                try:
                    return proc.wait(timeout=WAIT_SLICE_SECONDS)
                except subprocess.TimeoutExpired:
                    self._touch_heartbeat()
                    if self._stop.is_set() and self._stop_at is not None and time.monotonic() - self._stop_at > CHILD_TERM_GRACE_SECONDS and proc.poll() is None:
                        logger.warning("Child did not exit on SIGTERM; sending SIGKILL")
                        proc.kill()
        finally:
            self._child = None

    def _wait_until(self, target: datetime) -> bool:
        """Sleep until ``target``; return False if shutdown was requested."""
        while not self._stop.is_set():
            remaining = (target - self._now_fn()).total_seconds()
            if remaining <= 0:
                return True
            self._touch_heartbeat()
            self._sleep_fn(min(remaining, WAIT_SLICE_SECONDS))
        return False

    def _touch_heartbeat(self) -> None:
        if self._heartbeat_path is None:
            return
        try:
            self._heartbeat_path.touch()
        except OSError as exc:
            logger.warning("Could not update heartbeat {}: {}", self._heartbeat_path, exc)


def main(argv: Optional[Sequence[str]] = None, environ: Optional[dict] = None) -> int:
    args: List[str] = list(sys.argv[1:] if argv is None else argv)
    env = os.environ if environ is None else environ
    expression = (env.get("CRON_SCHEDULE") or "").strip()

    if not expression:
        # One-shot mode: become the CLI process (keeps today's PID-1, signal
        # and exit-code behaviour for manual runs byte-for-byte).
        logger.info("CRON_SCHEDULE not set; running CLI once")
        os.execvpe(sys.executable, [sys.executable, "-m", CLI_MODULE, *args], env)
        return 127  # exec only returns on failure; keeps type-checkers honest

    try:
        validate_schedule(expression)
    except ValueError as exc:
        logger.error("CRON_SCHEDULE: {}", exc)
        return 2

    scheduler = Scheduler(
        expression,
        args,
        run_on_startup=parse_bool(env.get("RUN_ON_STARTUP"), default=True),
        heartbeat_path=env.get("HEARTBEAT_PATH") or DEFAULT_HEARTBEAT_PATH,
    )
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: scheduler.stop())
    return scheduler.run()


if __name__ == "__main__":
    sys.exit(main())
