"""Find and safely remove Deluge registrations with explicit terminal tracker replies."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

from loguru import logger

from .pending_actions import (
    ActionStatus,
    PendingTrackerRemoval,
    get_pending_tracker_removals,
    init_pending_actions_schema,
    record_tracker_removal_outcome,
    register_tracker_removal,
)

if TYPE_CHECKING:
    from deluge_client import DelugeRPCClient

    from ..settings import AppConfig

_RPC_LOGGER = logging.getLogger("deluge_client.client")

# Deluge's Torrent._create_status_funcs exposes these fields. is_seed is
# libtorrent's is_seeding flag; Deluge's Seeding state alone also covers
# torrents whose wanted files (but not the whole torrent) are complete.
_STATUS_FIELDS = ("state", "private", "is_seed", "tracker_status")
_TERMINAL_REPLIES = {
    reply.casefold(): reply
    for reply in (
        "Unregistered torrent",
        "Torrent removed by its uploader",
        "Torrent has been deleted.",
    )
}
_INFOHASH = re.compile(r"[0-9a-fA-F]{40}")


def _text(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            pass
    return None


def _status_field(status: dict, key: str) -> object:
    # deluge-client normally returns byte keys and values; decoded clients are
    # also supported. Do not decode or inspect unrelated fields in the reply.
    return status.get(key, status.get(key.encode()))


def _normalized_status(status: dict) -> str:
    return (_text(_status_field(status, "tracker_status")) or "").strip().casefold()


def _terminal_reply(normalized_status: str) -> str | None:
    return _TERMINAL_REPLIES.get(normalized_status.removeprefix("error:").strip())


def _eligible(status: dict) -> bool | None:
    state = _text(_status_field(status, "state"))
    private = _status_field(status, "private")
    is_seed = _status_field(status, "is_seed")
    if (state and state != "Seeding") or private is False or is_seed is False:
        return False
    if state == "Seeding" and private is True and is_seed is True:
        return True
    return None


def _discover(client: DelugeRPCClient) -> dict:
    try:
        snapshot = client.core.get_torrents_status({}, list(_STATUS_FIELDS))
    except Exception:
        logger.warning("Could not read Deluge tracker statuses; no new removals discovered.")
        return {}
    if not isinstance(snapshot, dict):
        logger.warning("Deluge returned an unexpected tracker status list; no new removals discovered.")
        return {}
    return snapshot


def _check_due(client: DelugeRPCClient, torrent_hash: str, *, dry_run: bool) -> tuple[ActionStatus, str]:
    try:
        status = client.core.get_torrent_status(torrent_hash, list(_STATUS_FIELDS))
    except Exception:
        return ActionStatus.PENDING, "tracker status could not be verified"
    if not isinstance(status, dict) or not status:
        return ActionStatus.PENDING, "tracker status could not be verified"

    current = _normalized_status(status)
    if current == "announce ok":
        return ActionStatus.CANCELLED, "Deluge reports Announce OK"
    eligible = _eligible(status)
    if eligible is None:
        return ActionStatus.PENDING, "torrent eligibility could not be verified"
    if not eligible:
        return ActionStatus.CANCELLED, "torrent is no longer complete, private, and seeding"
    reply = _terminal_reply(current)
    if reply is None:
        return ActionStatus.PENDING, "no exact terminal tracker reply is currently verified"

    if not dry_run:
        try:
            removed = client.core.remove_torrent(torrent_hash, False)  # remove_data=False
        except Exception:
            return ActionStatus.PENDING, f"Deluge did not confirm removal ({reply})"
        if removed is not True:
            return ActionStatus.PENDING, f"Deluge did not confirm removal ({reply})"
    return ActionStatus.COMPLETED, f"exact terminal reply confirmed ({reply})"


def process_tracker_rejections(
    db_path: Path,
    client: DelugeRPCClient,
    waiting_period_days: int,
    *,
    daemon_key: str,
    dry_run: bool,
    now: datetime | None = None,
) -> None:
    """Plan or queue terminal tracker replies; recheck due actions against Deluge."""
    if not dry_run:
        init_pending_actions_schema(db_path)
    pending = get_pending_tracker_removals(db_path, daemon_key)
    snapshot = _discover(client)
    now = now if now is not None else datetime.now()
    checked_at = now.isoformat(sep=" ", timespec="seconds")
    new_due_at = (now + timedelta(days=waiting_period_days)).isoformat(sep=" ", timespec="seconds")

    for raw_hash, status in snapshot.items():
        torrent_hash = _text(raw_hash)
        if torrent_hash is None or not _INFOHASH.fullmatch(torrent_hash) or not isinstance(status, dict):
            continue
        torrent_hash = torrent_hash.lower()
        if torrent_hash in pending or _eligible(status) is not True:
            continue
        reply = _terminal_reply(_normalized_status(status))
        if reply is None:
            continue
        pending[torrent_hash] = PendingTrackerRemoval(None, torrent_hash, reply, new_due_at) if dry_run else register_tracker_removal(db_path, daemon_key, torrent_hash, reply, new_due_at)

    for action in pending.values():
        logger.info(
            "{} tracker removal for {}: {}; due at {} (keep local data)",
            "[DRY RUN] Would queue" if action.id is None else "Pending",
            action.torrent_hash,
            action.reply,
            action.due_at,
        )
        if action.due_at > checked_at:
            continue
        outcome, reason = _check_due(client, action.torrent_hash, dry_run=dry_run)
        verb = {
            ActionStatus.PENDING: "defer" if dry_run else "Deferred",
            ActionStatus.CANCELLED: "cancel" if dry_run else "Cancelled",
            ActionStatus.COMPLETED: "remove" if dry_run else "Removed",
        }[outcome]
        logger.info(
            "{}{} for {} (due at {}): {}",
            "[DRY RUN] Would " if dry_run else "",
            f"{verb} tracker registration (keep local data)" if outcome is ActionStatus.COMPLETED else f"{verb} tracker registration removal",
            action.torrent_hash,
            action.due_at,
            reason,
        )
        if not dry_run and action.id is not None:
            record_tracker_removal_outcome(db_path, action.id, outcome, reason, checked_at)
    if not pending:
        logger.info("No tracker registration removals planned or pending.")


def run_tracker_rejections(config: AppConfig, *, dry_run: bool) -> bool:
    """Run only the tracker queue workflow with an independent Deluge connection."""
    from deluge_client import DelugeRPCClient

    # deluge-client debug logging can print the raw login reply.
    rpc_logging_disabled = _RPC_LOGGER.disabled
    _RPC_LOGGER.disabled = True
    try:
        client = DelugeRPCClient(
            host=config.deluge_host,
            port=config.deluge_port,
            username=config.deluge_username,
            password=config.deluge_password,
            # Do not replay a removal RPC automatically after reconnecting.
            automatic_reconnect=False,
        )
        client.connect()
    except Exception:
        logger.error("Could not connect to Deluge for tracker rejection cleanup.")
        return False
    finally:
        _RPC_LOGGER.disabled = rpc_logging_disabled

    endpoint = f"{config.deluge_host.casefold()}\0{config.deluge_port}"
    daemon_key = sha256(endpoint.encode("utf-8")).hexdigest()
    try:
        process_tracker_rejections(
            config.sqlite_cache_path,
            client,
            config.relabel_action_delay_days,
            daemon_key=daemon_key,
            dry_run=dry_run,
        )
    finally:
        try:
            client.disconnect()
        except Exception:
            logger.warning("Could not disconnect from Deluge after tracker rejection cleanup.")
    return True
