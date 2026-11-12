"""Repair stuck cross-seed torrents whose missing files already exist on disk.

Implements issue #1. cross-seed injects partial matches paused and only
resumes them when little is missing; the rest stay paused indefinitely even
when every missing wanted file is already on disk, verified complete as part
of another torrent (same release from another tracker, or a renamed copy).
This module finds those torrents, hardlinks donor files into place, and
force-rechecks under a strict guard so Deluge cannot start downloading:

- every file priority is set to 0 and the download speed is capped with
  auto-management off for the whole check (Deluge resumes a torrent during
  ``force_recheck``, and after a daemon restart resumes torrents that were
  mid-recheck, so the guard has to hold for the whole check),
- payload downloads are polled; exceeding ``REPAIR_DOWNLOAD_ABORT_MB``
  pauses the torrent and aborts the repair,
- original priorities/options are restored afterwards in all paths.

Safety rules, all from the issue: destinations another torrent also uses are
skipped, directories are never created (a directory created by the wrong
user later breaks Deluge's own file creation), existing partial files are
kept as ``*.delugeof-repair-bak`` until verification succeeds, torrents are
never removed, and the only deletions are the backups of torrents that
verified 100%. Repairs are all-or-nothing per torrent: a torrent with one
unmatchable missing file is reported and left untouched. Torrents still
stuck after a failed attempt are reported only (issue open question); their
original files are restored and rechecked so Deluge's piece map matches the
disk again.

Run standalone via ``--repair-stuck-torrents`` (dry-run: log planned links)
or ``--apply-stuck-repairs``.
"""

from __future__ import annotations

import fcntl
import json
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from loguru import logger

BACKUP_SUFFIX = ".delugeof-repair-bak"
# Minimum wait before a settled state may be read as "check completed"
# when Checking was never sampled (see _guarded_recheck).
RECHECK_START_GRACE_SECONDS = 120.0
# After pausing a torrent mid-check (abort/timeout), wait this long for it to
# actually leave the Checking/Downloading states before touching its files.
PAUSE_SETTLE_SECONDS = 60.0

STATUS_FIELDS = [
    "name",
    "state",
    "label",
    "progress",
    "time_added",
    "save_path",
    "total_size",
    "is_finished",
    "files",
    "file_progress",
    "file_priorities",
]
POLL_FIELDS = ["state", "progress", "is_finished", "all_time_download", "total_payload_download"]
MEASURE_FIELDS = ["state", "progress", "is_finished", "file_progress", "file_priorities"]
# The option subset the guard touches, and therefore snapshots/restores.
GUARD_OPTION_KEYS = ["auto_managed", "max_download_speed"]


def _d(value):
    return value.decode() if isinstance(value, bytes) else value


def _decode_torrent(raw: dict) -> dict:
    torrent = {_d(k): v for k, v in raw.items()}
    torrent["files"] = [{_d(k): _d(v) for k, v in f.items()} for f in raw.get(b"files", [])]
    for key in ("name", "state", "label", "save_path"):
        if key in torrent:
            torrent[key] = _d(torrent[key])
    return torrent


def _remote_full_path(torrent: dict, file_entry: dict) -> str:
    return os.path.normpath(os.path.join(torrent["save_path"], file_entry["path"]))


def _to_local(remote_path: str, remote_base: str, local_base: str) -> Optional[Path]:
    relative = os.path.relpath(remote_path, remote_base)
    if ".." in Path(relative).parts:
        return None
    return Path(local_base) / relative


def _stat_key(path: Path) -> Optional[Tuple[int, int, int]]:
    """(st_dev, st_ino, st_size) or None if the path does not exist."""
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_dev, st.st_ino, st.st_size)


# --------------------------------------------------------------------------
# Planning (no mutations; shared by dry-run and apply)
# --------------------------------------------------------------------------

ACTION_LINK = "link"  # destination absent: hardlink the donor in
ACTION_REPLACE = "replace"  # destination exists (partial): back it up, then link
ACTION_ALREADY_LINKED = "already-linked"  # destination already is the donor inode
ACTION_SKIP = "skip"  # cannot repair this file; blocks its torrent


@dataclass
class FilePlan:
    torrent_file_index: int
    remote_dest: str
    local_dest: Optional[Path]
    size: int
    action: str
    donor_local: Optional[Path] = None
    donor_remote: Optional[str] = None
    reason: str = ""  # for ACTION_SKIP


@dataclass
class TorrentPlan:
    torrent_id: str
    name: str
    label: str
    age_days: float
    progress: float
    files: List[FilePlan] = field(default_factory=list)

    @property
    def repairable(self) -> bool:
        return bool(self.files) and all(f.action != ACTION_SKIP for f in self.files)

    @property
    def blocking_reason(self) -> str:
        for f in self.files:
            if f.action == ACTION_SKIP:
                return f.reason
        return "no-missing-wanted-files" if not self.files else ""


def is_stuck(torrent: dict, *, now: float, min_age_hours: float) -> bool:
    label = torrent.get("label") or ""
    if torrent.get("state") != "Paused" or torrent.get("is_finished"):
        return False
    if not (label == "cross-seed" or label.endswith(".cross-seed")):
        return False
    added = float(torrent.get("time_added") or now)
    return (now - added) >= min_age_hours * 3600


@dataclass
class DonorIndex:
    # Values are (remote_path, owning_torrent_id): a donor must come from a
    # *different* torrent than the one being repaired (issue #1 step 2).
    by_name_size: Dict[Tuple[str, int], List[Tuple[str, str]]] = field(default_factory=dict)
    by_size: Dict[int, List[Tuple[str, str]]] = field(default_factory=dict)


def build_donor_index(torrents: Dict[str, dict], *, size_only_min_bytes: int) -> DonorIndex:
    """Index remote paths of files Deluge has verified complete (progress 1.0)."""
    index = DonorIndex()
    for torrent_id, torrent in torrents.items():
        progress = torrent.get("file_progress") or []
        for i, file_entry in enumerate(torrent["files"]):
            if i < len(progress) and float(progress[i]) >= 1.0:
                remote = _remote_full_path(torrent, file_entry)
                index.by_name_size.setdefault((os.path.basename(file_entry["path"]), int(file_entry["size"])), []).append((remote, torrent_id))
                if int(file_entry["size"]) >= size_only_min_bytes:
                    index.by_size.setdefault(int(file_entry["size"]), []).append((remote, torrent_id))
    return index


def _destination_owners(torrents: Dict[str, dict]) -> Dict[str, Set[str]]:
    owners: Dict[str, Set[str]] = {}
    for torrent_id, torrent in torrents.items():
        for file_entry in torrent["files"]:
            owners.setdefault(_remote_full_path(torrent, file_entry), set()).add(torrent_id)
    return owners


class Planner:
    def __init__(self, *, remote_base: str, local_base: str, size_only_min_bytes: int) -> None:
        self._remote_base = os.path.normpath(remote_base)
        self._local_base = os.path.normpath(local_base)
        self._size_only_min_bytes = size_only_min_bytes

    def plan_all(self, torrents: Dict[str, dict], *, now: float, min_age_hours: float) -> List[TorrentPlan]:
        donors = build_donor_index(torrents, size_only_min_bytes=self._size_only_min_bytes)
        owners = _destination_owners(torrents)
        plans = []
        for torrent_id, torrent in torrents.items():
            if is_stuck(torrent, now=now, min_age_hours=min_age_hours):
                plans.append(self._plan_torrent(torrent_id, torrent, donors, owners, now))
        plans.sort(key=lambda p: -p.progress)
        return plans

    def _plan_torrent(self, torrent_id: str, torrent: dict, donors: DonorIndex, owners: Dict[str, Set[str]], now: float) -> TorrentPlan:
        plan = TorrentPlan(
            torrent_id=torrent_id,
            name=torrent.get("name") or torrent_id,
            label=torrent.get("label") or "",
            age_days=(now - float(torrent.get("time_added") or now)) / 86400,
            progress=float(torrent.get("progress") or 0.0),
        )
        progress = torrent.get("file_progress") or []
        priorities = torrent.get("file_priorities") or []
        for i, file_entry in enumerate(torrent["files"]):
            priority = int(priorities[i]) if i < len(priorities) else 1
            file_progress = float(progress[i]) if i < len(progress) else 0.0
            if priority > 0 and file_progress < 1.0:
                plan.files.append(self._plan_file(torrent_id, torrent, i, file_entry, donors, owners))
        return plan

    def _plan_file(self, torrent_id, torrent, i, file_entry, donors, owners) -> FilePlan:
        remote_dest = _remote_full_path(torrent, file_entry)
        size = int(file_entry["size"])
        local_dest = _to_local(remote_dest, self._remote_base, self._local_base)
        base = dict(torrent_file_index=i, remote_dest=remote_dest, local_dest=local_dest, size=size)

        if local_dest is None:
            return FilePlan(**base, action=ACTION_SKIP, reason="destination outside torrent base folder")
        if owners.get(remote_dest, set()) - {torrent_id}:
            return FilePlan(**base, action=ACTION_SKIP, reason="destination also used by another torrent")
        if not local_dest.parent.is_dir():
            # Never create directories (issue #1): dirs created by the wrong
            # user later break Deluge's own file creation with EACCES.
            return FilePlan(**base, action=ACTION_SKIP, reason="destination directory missing (refusing to create it)")
        if Path(str(local_dest) + BACKUP_SUFFIX).exists():
            return FilePlan(**base, action=ACTION_SKIP, reason="stale backup from an earlier repair exists")

        donor_remote, donor_local, donor_key, donor_error = self._choose_donor(torrent_id, file_entry, donors)
        if donor_local is None:
            return FilePlan(**base, action=ACTION_SKIP, reason=donor_error)

        dest_key = _stat_key(local_dest)
        if dest_key is None:
            return FilePlan(**base, action=ACTION_LINK, donor_local=donor_local, donor_remote=donor_remote)
        if dest_key[:2] == donor_key[:2]:
            return FilePlan(**base, action=ACTION_ALREADY_LINKED, donor_local=donor_local, donor_remote=donor_remote)
        return FilePlan(**base, action=ACTION_REPLACE, donor_local=donor_local, donor_remote=donor_remote)

    def _choose_donor(self, torrent_id, file_entry, donors: DonorIndex):
        """Pick a verified donor: name+size first, then unambiguous size-only.

        Returns (remote, local, stat_key, error). Candidates are checked
        against the disk here: Deluge believing a file complete does not
        guarantee it still exists at the expected size.
        """
        size = int(file_entry["size"])

        def existing(candidates: List[Tuple[str, str]]):
            found = []
            for remote, owner in sorted(set(candidates)):
                if owner == torrent_id:
                    continue
                local = _to_local(remote, self._remote_base, self._local_base)
                if local is None:
                    continue
                key = _stat_key(local)
                if key is not None and key[2] == size:
                    found.append((remote, local, key))
            return found

        chosen = existing(donors.by_name_size.get((os.path.basename(file_entry["path"]), size), []))
        if chosen:
            return chosen[0][0], chosen[0][1], chosen[0][2], ""

        if size >= self._size_only_min_bytes:
            by_inode: Dict[Tuple[int, int], Tuple[str, Path, Tuple[int, int, int]]] = {}
            for remote, local, key in existing(donors.by_size.get(size, [])):
                by_inode.setdefault(key[:2], (remote, local, key))
            if len(by_inode) == 1:
                remote, local, key = next(iter(by_inode.values()))
                return remote, local, key, ""
            if len(by_inode) > 1:
                return None, None, None, "size-only donor match is ambiguous (multiple distinct files)"
        return None, None, None, "no verified donor file on disk"


# --------------------------------------------------------------------------
# Execution (mutations; apply mode only)
# --------------------------------------------------------------------------


@dataclass
class RepairResult:
    torrent_id: str
    name: str
    outcome: str  # repaired | failed | aborted-download | timed-out | error
    detail: str = ""


class RepairError(RuntimeError):
    pass


def _call(client, method: str, *args):
    return client.call(method, *args)


def _torrent_status(client, torrent_id: str, fields: Sequence[str]) -> dict:
    raw = _call(client, "core.get_torrent_status", torrent_id, list(fields))
    return {_d(k): _d(v) for k, v in raw.items()}


def _payload_downloaded(status: dict) -> Optional[int]:
    for key in ("all_time_download", "total_payload_download"):
        if key in status and status[key] is not None:
            return int(status[key])
    return None


class RepairExecutor:
    """Apply one TorrentPlan: link files, guarded recheck, verify or roll back."""

    def __init__(
        self,
        client,
        *,
        max_download_speed_kib: float,
        download_abort_bytes: int,
        recheck_timeout_seconds: float,
        poll_seconds: float = 5.0,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._max_download_speed_kib = max_download_speed_kib
        self._download_abort_bytes = download_abort_bytes
        self._recheck_timeout_seconds = recheck_timeout_seconds
        self._poll_seconds = poll_seconds
        self._sleep = sleep_fn
        self._monotonic = monotonic_fn

    # -- public -------------------------------------------------------------

    def repair(self, plan: TorrentPlan) -> RepairResult:
        tid = plan.torrent_id
        logger.info("Repairing {} ({}): {} file(s)", plan.name, tid, len(plan.files))
        linked: List[FilePlan] = []  # files we created/replaced, for rollback
        snapshot: Optional[dict] = None
        try:
            snapshot = self._snapshot(tid)
            for file_plan in plan.files:
                if self._apply_file_plan(file_plan):
                    linked.append(file_plan)

            _downloaded, ok = self._guarded_recheck(tid, snapshot)
            if ok == "done":
                # Measure only after priorities are back: with every file at
                # priority 0 there is nothing "wanted", so progress during
                # the check itself is meaningless. The speed cap is still on.
                self._set_priorities(tid, snapshot["file_priorities"])
                status = _torrent_status(self._client, tid, MEASURE_FIELDS)
                # Neither progress nor is_finished can declare success here:
                # Deluge reports progress=100.0 in Error state, and it resets
                # is_finished=False whenever a file goes from priority 0 to
                # positive — which the restore above just did. The checked
                # piece map is the only reliable verdict: every wanted file
                # complete, in a non-Error state.
                file_progress = status.get("file_progress") or []
                file_priorities = status.get("file_priorities") or []
                wanted_complete = bool(file_progress) and len(file_progress) == len(file_priorities) and all(float(fp) >= 1.0 for fp, pr in zip(file_progress, file_priorities) if int(pr) > 0)
                if status.get("state") != "Error" and wanted_complete:
                    self._restore_options(tid, snapshot)
                    _call(self._client, "core.resume_torrent", [tid])
                    for file_plan in linked:
                        self._discard_backup(file_plan)
                    logger.success("Repaired {}: verified 100%, resumed", plan.name)
                    return RepairResult(tid, plan.name, "repaired")
                detail = f"state={status.get('state')} progress={float(status.get('progress') or 0.0):.2f}% is_finished={status.get('is_finished')} wanted_complete={wanted_complete} after recheck"
                outcome = "failed"
            else:
                detail = ok
                outcome = "aborted-download" if ok.startswith("downloaded") else ("timed-out" if ok == "recheck timed out" else "error")

            logger.warning("Repair of {} did not verify ({}); rolling back", plan.name, detail)
            self._rollback(tid, snapshot, linked)
            return RepairResult(tid, plan.name, outcome, detail)
        except Exception as exc:
            logger.exception("Repair of {} raised: {}", plan.name, exc)
            self._best_effort_restore(tid, snapshot, linked)
            return RepairResult(tid, plan.name, "error", str(exc))

    # -- snapshot / guard -----------------------------------------------------

    def _snapshot(self, tid: str) -> dict:
        status = _torrent_status(self._client, tid, ["file_priorities", *GUARD_OPTION_KEYS])
        if "file_priorities" not in status:
            raise RepairError("torrent status carries no file_priorities")
        payload = _payload_downloaded(_torrent_status(self._client, tid, POLL_FIELDS))
        if payload is None:
            raise RepairError("no payload download counter in torrent status; cannot enforce download abort guard")
        snapshot = {"file_priorities": list(status["file_priorities"])}
        for key in GUARD_OPTION_KEYS:
            if key in status:
                snapshot[key] = status[key]
        return snapshot

    def _set_priorities(self, tid: str, priorities: Sequence[int]) -> None:
        # Deluge 2.2 core.set_torrent_file_priorities takes ONE torrent id
        # (it wraps it in a list internally); passing [tid] breaks the lookup.
        _call(self._client, "core.set_torrent_file_priorities", tid, list(priorities))
        actual = _torrent_status(self._client, tid, ["file_priorities"]).get("file_priorities")
        if actual is None or [int(p) for p in actual] != [int(p) for p in priorities]:
            raise RepairError(f"file priorities readback mismatch: set {list(priorities)}, daemon reports {actual}")

    def _set_guard_options(self, tid: str) -> None:
        _call(self._client, "core.set_torrent_options", [tid], {"auto_managed": False, "max_download_speed": self._max_download_speed_kib})
        actual = _torrent_status(self._client, tid, GUARD_OPTION_KEYS)
        if actual.get("auto_managed") not in (False, 0):
            raise RepairError(f"auto_managed readback is {actual.get('auto_managed')!r}, expected False")
        speed = actual.get("max_download_speed")
        if speed is None or float(speed) < 0 or float(speed) > self._max_download_speed_kib:
            raise RepairError(f"max_download_speed readback is {speed!r}, expected <= {self._max_download_speed_kib}")

    def _restore_options(self, tid: str, snapshot: dict) -> None:
        options = {key: snapshot[key] for key in GUARD_OPTION_KEYS if key in snapshot}
        if options:
            _call(self._client, "core.set_torrent_options", [tid], options)

    # -- files ------------------------------------------------------------------

    def _apply_file_plan(self, file_plan: FilePlan) -> bool:
        """Create/replace the destination link. Returns True if rollback must undo it."""
        dest = file_plan.local_dest
        donor = file_plan.donor_local
        if file_plan.action == ACTION_ALREADY_LINKED or dest is None or donor is None:
            return False
        if file_plan.action == ACTION_REPLACE:
            os.rename(dest, str(dest) + BACKUP_SUFFIX)
        try:
            os.link(donor, dest)
        except OSError:
            # Undo the rename right here: the caller only journals files this
            # method reports as applied, so a failure must not strand the
            # original as a backup with the destination missing.
            if file_plan.action == ACTION_REPLACE:
                backup = Path(str(dest) + BACKUP_SUFFIX)
                if backup.exists() and not dest.exists():
                    os.rename(backup, dest)
            raise
        logger.info("Linked {} -> {}", donor, dest)
        return True

    def _discard_backup(self, file_plan: FilePlan) -> None:
        if file_plan.action != ACTION_REPLACE or file_plan.local_dest is None:
            return
        backup = Path(str(file_plan.local_dest) + BACKUP_SUFFIX)
        backup.unlink(missing_ok=True)

    def _rollback_files(self, linked: List[FilePlan]) -> None:
        """Restore journaled files, consuming the journal as entries succeed.

        Popping each restored entry makes rollback safe to repeat (the
        best-effort path retries after a partial rollback): a second pass
        must never unlink an original the first pass already put back.
        """
        while linked:
            file_plan = linked[-1]
            dest = file_plan.local_dest
            if dest is not None:
                dest.unlink(missing_ok=True)
                if file_plan.action == ACTION_REPLACE:
                    backup = Path(str(dest) + BACKUP_SUFFIX)
                    if backup.exists():
                        os.rename(backup, dest)
            linked.pop()

    # -- recheck ------------------------------------------------------------------

    def _guarded_recheck(self, tid: str, snapshot: dict) -> Tuple[int, str]:
        """Zero priorities, cap speed, force recheck, watch payload downloads.

        Returns (bytes_downloaded, "done") or (bytes, reason-string).
        Priorities/options are intentionally left guarded on return; the
        caller decides when to restore them (measure-then-resume needs
        priorities back while the speed cap is still on).
        """
        self._set_priorities(tid, [0] * len(snapshot["file_priorities"]))
        self._set_guard_options(tid)
        baseline = _payload_downloaded(_torrent_status(self._client, tid, POLL_FIELDS)) or 0
        _call(self._client, "core.force_recheck", [tid])

        started = self._monotonic()
        seen_checking = False
        while True:
            status = _torrent_status(self._client, tid, POLL_FIELDS)
            payload = _payload_downloaded(status)
            downloaded = (payload - baseline) if payload is not None else 0
            if downloaded > self._download_abort_bytes:
                logger.error("Download guard tripped for {}: {} bytes downloaded during recheck", tid, downloaded)
                self._pause_and_settle(tid)
                return downloaded, f"downloaded {downloaded} bytes during recheck (limit {self._download_abort_bytes})"
            state = status.get("state")
            if state == "Error":
                return downloaded, "torrent entered Error state during recheck"
            elapsed = self._monotonic() - started
            if state == "Checking":
                seen_checking = True
            else:
                if seen_checking:
                    return downloaded, "done"
                # Settled without ever sampling Checking is ambiguous. It can
                # mean the check already finished between polls — or that it
                # has not started yet: libtorrent clears piece accounting
                # when a check starts while Deluge's display state lags, so
                # neither "settled" nor changed piece counters prove
                # completion. (Daemon events would, but the deluge-client
                # package cannot deliver them to a local callback.) Only
                # after the start grace do we proceed and let the caller's
                # wanted-file measurement decide; a check that never ran
                # leaves it incomplete and rolls back safely.
                if elapsed > RECHECK_START_GRACE_SECONDS:
                    logger.warning("No Checking state observed for {} within {}s; proceeding to measurement", tid, RECHECK_START_GRACE_SECONDS)
                    return downloaded, "done"
            if elapsed > self._recheck_timeout_seconds:
                self._pause_and_settle(tid)
                return downloaded, "recheck timed out"
            self._sleep(self._poll_seconds)

    def _pause_and_settle(self, tid: str) -> None:
        _call(self._client, "core.pause_torrent", [tid])
        deadline = self._monotonic() + PAUSE_SETTLE_SECONDS
        while self._monotonic() < deadline:
            if _torrent_status(self._client, tid, ["state"]).get("state") in ("Paused", "Seeding", "Error"):
                return
            self._sleep(min(self._poll_seconds, 5.0))

    def _rollback(self, tid: str, snapshot: dict, linked: List[FilePlan]) -> None:
        """Restore original files, re-recheck so the piece map matches disk, leave paused."""
        self._pause_and_settle(tid)
        self._rollback_files(linked)
        try:
            self._guarded_recheck(tid, snapshot)
        except Exception as exc:
            logger.warning("Rollback recheck for {} failed: {}", tid, exc)
        self._set_priorities(tid, snapshot["file_priorities"])
        self._restore_options(tid, snapshot)
        _call(self._client, "core.pause_torrent", [tid])

    def _best_effort_restore(self, tid: str, snapshot: Optional[dict], linked: List[FilePlan]) -> None:
        try:
            if snapshot:
                self._rollback(tid, snapshot, linked)
            else:
                _call(self._client, "core.pause_torrent", [tid])
                self._rollback_files(linked)
        except Exception as exc:
            logger.error("Best-effort restore for {} also failed: {} — check this torrent manually", tid, exc)


# --------------------------------------------------------------------------
# Run orchestration: lock, state, reporting
# --------------------------------------------------------------------------


def _acquire_lock(lock_path: Path):
    """Exclusive non-blocking flock; returns the open fd or None if held."""
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    return fd


def _load_state(state_path: Path) -> dict:
    try:
        return json.loads(state_path.read_text())
    except (OSError, ValueError):
        return {}


def _save_state(state_path: Path, state: dict) -> None:
    state_path.write_text(json.dumps(state, indent=1, sort_keys=True))


def _log_plan(plan: TorrentPlan) -> None:
    verdict = "REPAIRABLE" if plan.repairable else f"SKIP ({plan.blocking_reason})"
    logger.info("[{}] {} ({}) {:.2f}% age {:.1f}d label={}", verdict, plan.name, plan.torrent_id, plan.progress, plan.age_days, plan.label)
    for f in plan.files:
        if f.action == ACTION_SKIP:
            logger.info("  skip {}: {}", f.remote_dest, f.reason)
        elif f.action == ACTION_ALREADY_LINKED:
            logger.info("  already linked: {}", f.remote_dest)
        else:
            logger.info("  {} {} -> {}", f.action, f.donor_remote, f.remote_dest)


def run_stuck_repair(
    config,
    *,
    apply: bool,
    client=None,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic_fn: Callable[[], float] = time.monotonic,
    now_fn: Callable[[], float] = time.time,
    poll_seconds: float = 5.0,
) -> Dict[str, object]:
    """Entry point for --repair-stuck-torrents / --apply-stuck-repairs."""
    stats: Dict[str, object] = {"selected": 0, "repairable": 0, "repaired": 0, "failed": 0, "skipped_reasons": {}}
    state_dir = Path(config.sqlite_cache_path).parent
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_fd = _acquire_lock(state_dir / "stuck-repair.lock")
    if lock_fd is None:
        logger.error("Another stuck-torrent repair run holds the lock; skipping")
        stats["lock_held"] = True
        return stats

    own_client = client is None
    try:
        free_gb = shutil.disk_usage(config.local_torrent_base_local_folder).free / 1e9
        if free_gb < float(config.repair_min_free_gb):
            logger.error("Free space {:.1f} GB below REPAIR_MIN_FREE_GB={}; skipping repair run", free_gb, config.repair_min_free_gb)
            stats["low_free_space"] = True
            return stats

        if own_client:
            from deluge_client import DelugeRPCClient

            try:
                client = DelugeRPCClient(host=config.deluge_host, port=config.deluge_port, username=config.deluge_username, password=config.deluge_password)
                client.connect()
            except Exception as exc:
                logger.error("Could not connect to Deluge at {}:{} ({}); skipping repair run", config.deluge_host, config.deluge_port, exc)
                stats["connection_failed"] = True
                client = None
                return stats
            if not client.connected:
                logger.error("Failed to connect to Deluge; skipping repair run")
                stats["connection_failed"] = True
                return stats

        raw = _call(client, "core.get_torrents_status", {}, STATUS_FIELDS)
        torrents = {_d(tid): _decode_torrent(t) for tid, t in raw.items()}
        now = now_fn()
        planner = Planner(
            remote_base=str(config.deluge_torrent_base_remote_folder),
            local_base=str(config.local_torrent_base_local_folder),
            size_only_min_bytes=int(float(config.repair_size_only_min_mb) * 1024 * 1024),
        )
        plans = planner.plan_all(torrents, now=now, min_age_hours=float(config.repair_min_age_hours))
        stats["selected"] = len(plans)
        stats["repairable"] = sum(1 for p in plans if p.repairable)
        reasons: Dict[str, int] = {}
        for plan in plans:
            _log_plan(plan)
            if not plan.repairable:
                reasons[plan.blocking_reason] = reasons.get(plan.blocking_reason, 0) + 1
        stats["skipped_reasons"] = reasons

        if not apply:
            logger.info("Dry-run: {} stuck torrent(s), {} repairable. Use --apply-stuck-repairs to execute.", len(plans), stats["repairable"])
            return stats

        state_path = state_dir / "stuck-repair-state.json"
        state = _load_state(state_path)
        executor = RepairExecutor(
            client,
            max_download_speed_kib=float(config.repair_max_download_speed_kib),
            download_abort_bytes=int(float(config.repair_download_abort_mb) * 1024 * 1024),
            recheck_timeout_seconds=float(config.repair_recheck_timeout_seconds),
            poll_seconds=poll_seconds,
            sleep_fn=sleep_fn,
            monotonic_fn=monotonic_fn,
        )
        for plan in plans:
            entry = state.setdefault(plan.torrent_id, {"first_stuck_seen": now, "attempts": 0, "last_attempt": None, "last_outcome": None, "name": plan.name})
            if not plan.repairable:
                entry["last_outcome"] = f"skipped: {plan.blocking_reason}"
                continue
            entry["attempts"] = int(entry.get("attempts") or 0) + 1
            entry["last_attempt"] = now
            result = executor.repair(plan)
            entry["last_outcome"] = result.outcome + (f" ({result.detail})" if result.detail else "")
            if result.outcome == "repaired":
                stats["repaired"] = int(stats["repaired"]) + 1
                del state[plan.torrent_id]
            else:
                stats["failed"] = int(stats["failed"]) + 1
            _save_state(state_path, state)
        _save_state(state_path, state)
        still_stuck = [f"{v.get('name')} attempts={v.get('attempts')} last={v.get('last_outcome')}" for v in state.values()]
        if still_stuck:
            logger.warning("Still stuck (report only, nothing removed): {}", "; ".join(still_stuck))
        logger.info("Repair run done: {}", {k: v for k, v in stats.items()})
        return stats
    finally:
        if own_client and client is not None:
            try:
                if client.connected:
                    client.disconnect()
            except Exception:
                pass
        os.close(lock_fd)
