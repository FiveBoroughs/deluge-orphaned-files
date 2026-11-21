"""Build read-only, exact-v1 sidecar plans using Deluge 2.2 status and local data."""

from __future__ import annotations

import itertools
import json
import re
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from .sidecar_metainfo import DiskFile, Entry, Metainfo, PieceReader, PlanError, ReadTree, absolute_path, load_metainfo, relative_path

_INVENTORY_FIELDS = ["download_location", "files", "is_seed"]
_TARGET_FIELDS = [
    "download_location",
    "files",
    "orig_files",
    "state",
    "label",
    "paused",
    "is_finished",
    "file_priorities",
    "num_pieces",
    "piece_length",
    "total_size",
    "pieces",
]


def torrent_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", value):
        raise PlanError(f"expected an exact 40-digit hexadecimal v1 torrent ID: {value!r}")
    return value.lower()


@dataclass(frozen=True)
class Request:
    sidecars: frozenset[PurePosixPath]
    media_sources: tuple[str, ...]


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PlanError(f"duplicate manifest key: {key}")
        result[key] = value
    return result


def read_manifest(path: Path) -> dict[str, Request]:
    manifest = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_no_duplicate_keys)
    if not isinstance(manifest, dict) or not manifest:
        raise PlanError("manifest must map exact torrent IDs to sidecars and media_sources")
    requests = {}
    for key, spec in manifest.items():
        tid = torrent_id(key)
        if tid in requests or not isinstance(spec, dict) or spec.keys() != {"sidecars", "media_sources"}:
            raise PlanError(f"invalid or duplicate manifest entry: {key}")
        if not isinstance(spec["sidecars"], list) or not isinstance(spec["media_sources"], list):
            raise PlanError(f"sidecars and media_sources must be lists: {tid}")
        paths = frozenset(map(relative_path, spec["sidecars"]))
        refs = tuple(map(torrent_id, spec["media_sources"]))
        if not paths or len(paths) != len(spec["sidecars"]) or not refs or len(set(refs)) != len(refs) or tid in refs:
            raise PlanError(f"give distinct sidecars and distinct independent media_sources: {tid}")
        requests[tid] = Request(paths, refs)
    return requests


def _text(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _fields(raw) -> dict:
    if not isinstance(raw, dict):
        raise PlanError("invalid Deluge RPC response")
    return {_text(key): _text(value) for key, value in raw.items()}


def _entries(raw) -> tuple[Entry, ...]:
    if not isinstance(raw, (list, tuple)):
        raise PlanError("Deluge did not return a file layout")
    result = []
    for index, entry in enumerate(raw):
        f = _fields(entry)
        if type(f.get("index")) is not int or f["index"] != index or any(type(f.get(key)) is not int or f[key] < 0 for key in ("offset", "size")):
            raise PlanError("Deluge returned a malformed or unordered file layout")
        result.append(Entry(relative_path(f.get("path")), f["offset"], f["size"]))
    return tuple(result)


@dataclass(frozen=True)
class Layout:
    location: PurePosixPath
    entries: tuple[Entry, ...]

    def path(self, entry: Entry) -> PurePosixPath:
        return self.location / entry.path


def _layout(status: dict) -> Layout:
    return Layout(absolute_path(status.get("download_location")), _entries(status.get("files")))


@dataclass(frozen=True)
class InventoryTorrent:
    layout: Layout
    seeding: bool


def _inventory(client) -> dict[str, InventoryTorrent]:
    raw = client.call("core.get_torrents_status", {}, _INVENTORY_FIELDS)
    if not isinstance(raw, dict):
        raise PlanError("invalid Deluge inventory")
    inventory = {}
    for key, value in raw.items():
        # Other active torrents may be v2, but their paths still count as owners.
        tid = _text(key)
        status = _fields(value)
        if type(status.get("is_seed")) is not bool:
            raise PlanError(f"incomplete Deluge inventory status for {tid}")
        inventory[tid] = InventoryTorrent(_layout(status), status["is_seed"])
    return inventory


def _missing_pieces(status: dict, meta: Metainfo) -> set[int]:
    pieces = status.get("pieces")
    if not isinstance(pieces, (list, tuple)) or len(pieces) != meta.count or any(type(piece) is not int or piece not in (0, 1, 3) for piece in pieces):
        raise PlanError("Deluge did not return a complete, quiescent v1 piece map (0/1 absent, 3 verified)")
    return {index for index, piece in enumerate(pieces) if piece != 3}


def _target_evidence(status: dict, layout: Layout, meta: Metainfo) -> tuple[tuple[int, ...], set[int]]:
    label = status.get("label")
    if (
        status.get("state") != "Paused"
        or status.get("paused") is not True
        or status.get("is_finished") is not False
        or not isinstance(label, str)
        or not (label == "cross-seed" or label.endswith(".cross-seed"))
    ):
        raise PlanError("target is not an individually paused, incomplete cross-seed torrent")
    if _layout(status) != layout or _entries(status.get("orig_files")) != meta.files:
        raise PlanError("Deluge file layout differs from the inventory or v1 metainfo")
    if tuple((entry.offset, entry.size) for entry in layout.entries) != tuple((entry.offset, entry.size) for entry in meta.files):
        raise PlanError("mapped Deluge files do not match v1 offsets and sizes")
    for key, expected in [("num_pieces", meta.count), ("piece_length", meta.piece_length), ("total_size", meta.size)]:
        if type(status.get(key)) is not int or status[key] != expected:
            raise PlanError(f"Deluge {key} disagrees with v1 metainfo")
    priorities = status.get("file_priorities")
    if not isinstance(priorities, (list, tuple)) or len(priorities) != len(meta.files) or any(type(value) is not int or not 1 <= value <= 7 for value in priorities):
        raise PlanError("the planner requires unchanged, wanted priorities for every file")
    return tuple(priorities), _missing_pieces(status, meta)


def _groups(meta: Metainfo, indices: list[int]):
    start = end = 0
    members = []
    for index in indices:
        pieces = meta.file_pieces(index)
        if members and pieces.start >= end:
            yield members, range(start, end)
            members = []
        if not members:
            start, end = pieces.start, pieces.stop
        else:
            end = max(end, pieces.stop)
        members.append(index)
    if members:
        yield members, range(start, end)


class _Planner:
    def __init__(self, client, tree: ReadTree, base: PurePosixPath, metadata: Path, inventory: dict[str, InventoryTorrent]) -> None:
        self.client = client
        self.tree = tree
        self.base = base
        self.metadata = metadata
        self.inventory = inventory
        self.owners = defaultdict(list)
        for tid, tor in inventory.items():
            for entry in tor.layout.entries:
                self.owners[tor.layout.path(entry).as_posix()].append(tid)
        self.owned_paths = sorted(self.owners)
        self.reference_files: dict[str, list[DiskFile]] = {}

    def _local(self, path: PurePosixPath) -> PurePosixPath:
        if path == self.base:
            raise PlanError(f"torrent file path is the mapped base directory: {path}")
        try:
            return path.relative_to(self.base)
        except ValueError as exc:
            raise PlanError(f"path is outside the configured Deluge mapping: {path}") from exc

    def _status(self, tid: str, fields: list[str]) -> dict:
        return _fields(self.client.call("core.get_torrent_status", tid, fields))

    def _meta(self, tid: str) -> Metainfo:
        return load_metainfo(self.metadata / f"{tid}.torrent", tid)

    def _check_owner(self, path: PurePosixPath, tid: str) -> None:
        full = path.as_posix()
        child_prefix = full + "/"
        child = bisect_left(self.owned_paths, child_prefix)
        if self.owners[full] != [tid] or any(parent.as_posix() in self.owners for parent in path.parents) or (child < len(self.owned_paths) and self.owned_paths[child].startswith(child_prefix)):
            raise PlanError(f"ambiguous or shared torrent destination: {path}")

    def _reference(self, tid: str) -> list[DiskFile]:
        if tid not in self.reference_files:
            tor = self.inventory.get(tid)
            if tor is None or not tor.seeding:
                raise PlanError(f"media source is not a complete torrent in Deluge: {tid}")
            meta = self._meta(tid)
            status = self._status(tid, ["download_location", "files", "orig_files", "is_seed"])
            if (
                status.get("is_seed") is not True
                or _layout(status) != tor.layout
                or _entries(status.get("orig_files")) != meta.files
                or tuple((e.offset, e.size) for e in tor.layout.entries) != tuple((e.offset, e.size) for e in meta.files)
            ):
                raise PlanError(f"media source layout disagrees with v1 metainfo: {tid}")
            files = []
            for entry in tor.layout.entries:
                disk = self.tree.snapshot(self._local(tor.layout.path(entry)), entry.size)
                if disk is None:
                    raise PlanError(f"media source is missing a file: {tor.layout.path(entry)}")
                files.append(disk)
            with PieceReader(self.tree) as reader:
                for index in range(meta.count):
                    if not reader.matches(meta, files, index):
                        raise PlanError(f"media source {tid} fails v1 piece hash {index}")
            self.reference_files[tid] = files
        return self.reference_files[tid]

    def _donor_candidates(self, tid: str, keys: set[tuple[str, int]]) -> dict[tuple[str, int], list[DiskFile]]:
        candidates = defaultdict(dict)
        for donor_id, tor in self.inventory.items():
            if donor_id == tid or not tor.layout.location.is_relative_to(self.base):
                continue
            for entry in tor.layout.entries:
                key = entry.path.name, entry.size
                if key not in keys:
                    continue
                try:
                    disk = self.tree.snapshot(self._local(tor.layout.path(entry)), entry.size)
                except (OSError, PlanError):
                    continue  # never use a donor that cannot be read with the same path restrictions
                if disk is not None:
                    candidates[key].setdefault(disk.inode, disk)
        return {key: sorted(disks.values(), key=lambda disk: disk.relative) for key, disks in candidates.items()}

    def _find_donors(self, tid: str, meta: Metainfo, layout: Layout, missing_files: list[int], sources: list[DiskFile | None]):
        keys = {index: (layout.entries[index].path.name, layout.entries[index].size) for index in missing_files}
        candidates = self._donor_candidates(tid, set(keys.values()))
        donors = {}
        unresolved = set()
        with PieceReader(self.tree) as reader:
            for indices, pieces in _groups(meta, missing_files):
                choices = [candidates.get(keys[i], ()) for i in indices]
                # Donors for sidecars sharing a piece must verify together. Never treat
                # incomplete evidence from one matching basename/size as a proven donor.
                for attempt, selected in enumerate(itertools.product(*choices)):
                    if attempt == 256:
                        raise PlanError("too many local donor combinations to prove; disambiguate donors first")
                    for index, disk in zip(indices, selected):
                        sources[index] = disk
                    if all(reader.matches(meta, sources, piece) for piece in pieces):
                        donors.update(zip(indices, selected))
                        break
                else:
                    unresolved.update(pieces)
                    for index in indices:
                        sources[index] = None
        return donors, unresolved

    def _unchanged(self, source: DiskFile) -> None:
        if self.tree.snapshot(source.relative, source.fingerprint[2]) != source:
            raise PlanError(f"file changed while planning: {source.relative}")

    def plan(self, tid: str, request: Request) -> dict:
        if tid not in self.inventory:
            raise PlanError("exact torrent ID not present in Deluge")
        layout = self.inventory[tid].layout
        meta = self._meta(tid)
        evidence = _target_evidence(self._status(tid, _TARGET_FIELDS), layout, meta)
        _, live_missing = evidence
        if not live_missing:
            raise PlanError("target has no missing v1 pieces")
        if not request.sidecars <= {e.path for e in layout.entries}:
            raise PlanError("manifest sidecar paths do not match mapped Deluge files")
        files = []
        local_paths = []
        missing_files = []
        sidecar_pieces = set()
        for index, entry in enumerate(layout.entries):
            dest = layout.path(entry)
            self._check_owner(dest, tid)
            local = self._local(dest)
            file = self.tree.snapshot(local, entry.size)
            if file is None:
                if entry.path not in request.sidecars or not entry.size:
                    raise PlanError(f"missing or unverified non-sidecar file: {dest}")
                missing_files.append(index)
                sidecar_pieces.update(meta.file_pieces(index))
            files.append(file)
            local_paths.append(local)
        if not missing_files or sidecar_pieces != live_missing:
            unexpected = sorted(live_missing - sidecar_pieces)[:8]
            unconfirmed = sorted(sidecar_pieces - live_missing)[:8]
            raise PlanError(f"Deluge missing pieces differ from absent sidecar spans; unexplained={unexpected}, not marked missing={unconfirmed}")
        with PieceReader(self.tree) as reader:
            for piece in range(meta.count):
                if piece not in live_missing and not reader.matches(meta, files, piece):
                    raise PlanError(f"local hash failure on Deluge-complete v1 piece {piece}")
        if not any(entry.size and entry.path not in request.sidecars for entry in layout.entries):
            raise PlanError("no non-sidecar media files to independently verify")
        donors, unresolved = self._find_donors(tid, meta, layout, missing_files, files)
        missing_indices = set(missing_files)
        # An otherwise-present sidecar sharing an unresolved piece also has
        # unauthenticated bytes in the target and needs independent proof.
        unproven = {
            file.inode
            for i, (entry, file) in enumerate(zip(layout.entries, files))
            if i not in missing_indices and file is not None and entry.size and (entry.path not in request.sidecars or any(piece in unresolved for piece in meta.file_pieces(i)))
        }
        used_references = []
        for ref in request.media_sources:
            reference = self._reference(ref)
            proven = unproven & {file.inode for file in reference}
            if proven:
                unproven.difference_update(proven)
                used_references.append(ref)
            if not unproven:
                break
        if unproven:
            raise PlanError("some local inodes are not backed by a fully v1-verified media source")
        if _target_evidence(self._status(tid, _TARGET_FIELDS), layout, meta) != evidence:
            raise PlanError("Deluge target status changed while planning")
        for ref in used_references:
            for source in self.reference_files[ref]:
                self._unchanged(source)
        for index, (entry, local, source) in enumerate(zip(layout.entries, local_paths, files)):
            if index in missing_indices:
                if self.tree.snapshot(local, entry.size) is not None:
                    raise PlanError(f"sidecar destination changed while planning: {local}")
            if source is not None:
                self._unchanged(source)
        return {
            "torrent_id": tid,
            "status": "download-candidate" if unresolved else "local-only",
            "media_sources": used_references,
            "missing_piece_indices": sorted(live_missing),
            "missing_piece_bytes": sum(meta.piece_size(piece) for piece in live_missing),
            "missing_sidecar_bytes": sum(layout.entries[index].size for index in missing_files),
            "remaining_piece_indices": sorted(unresolved),
            "remaining_piece_bytes": sum(meta.piece_size(piece) for piece in unresolved),
            "remaining_sidecar_bytes": sum(layout.entries[index].size for index in missing_files if index not in donors),
            "sidecars": [
                {
                    "destination": str(layout.path(layout.entries[index])),
                    "bytes": layout.entries[index].size,
                    "verified_donor": str(self.tree.root / donors[index].relative) if index in donors else None,
                }
                for index in missing_files
            ],
        }


def plan_sidecars(client, *, local_base: Path, remote_base: str, metadata: Path, requests: dict[str, Request]) -> dict:
    """Report observable evidence only; no disk or RPC mutations are issued."""
    version = _text(client.call("daemon.get_version"))
    if not isinstance(version, str) or not re.fullmatch(r"2\.2\.\d+(?:\+[-.\w]+)?", version):
        raise PlanError(f"expected Deluge 2.2.x status contract, got {version!r}")
    if client.call("daemon.authorized_call", "core.get_known_accounts") is not True:
        raise PlanError("Deluge admin access is required for a complete torrent inventory")
    inventory = _inventory(client)
    with ReadTree(local_base) as tree:
        planner = _Planner(client, tree, absolute_path(remote_base), metadata, inventory)
        plans = []
        for tid, request in requests.items():
            try:
                plans.append(planner.plan(tid, request))
            except (PlanError, OSError, UnicodeError) as exc:
                plans.append({"torrent_id": tid, "status": "blocked", "reason": str(exc)})
    final_inventory = _inventory(client)
    if {tid: tor.layout for tid, tor in inventory.items()} != {tid: tor.layout for tid, tor in final_inventory.items()}:
        raise PlanError("Deluge inventory or file paths changed while planning; rerun with a stable inventory")
    plans.sort(key=lambda plan: (plan["status"] == "blocked", plan.get("remaining_piece_bytes", 0), plan["torrent_id"]))
    return {
        "mode": "read-only",
        "plans": plans,
        "preferred_download_torrent_id": next((p["torrent_id"] for p in plans if p["status"] == "download-candidate"), None),
    }
