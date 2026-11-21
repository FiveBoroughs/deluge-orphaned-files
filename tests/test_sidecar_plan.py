import hashlib
import json
import os
from copy import deepcopy
from pathlib import PurePosixPath

import pytest

from deluge_orphaned_files.logic.sidecar_metainfo import PlanError, load_metainfo
from deluge_orphaned_files.logic.sidecar_repair import Request, plan_sidecars
from deluge_orphaned_files.sidecar_plan import main


BASE = "/deluge/data"
MEDIA = b"abcdefghij"


def bencode(value):
    if isinstance(value, bytes):
        return str(len(value)).encode() + b":" + value
    if isinstance(value, int):
        return b"i%de" % value
    if isinstance(value, dict):
        return b"d" + b"".join(bencode(key) + bencode(val) for key, val in sorted(value.items())) + b"e"
    return b"l" + b"".join(map(bencode, value)) + b"e"


def write_metainfo(directory, name, files, piece_length, extra=None):
    payload = b"".join(data for _, data in files)
    piece_hashes = []
    for start in range(0, len(payload), piece_length):
        end = start + piece_length
        piece_hashes.append(hashlib.sha1(payload[start:end]).digest())
    info = {
        b"name": name.encode(),
        b"pieces": b"".join(piece_hashes),
        b"piece length": piece_length,
    }
    if len(files) == 1 and not files[0][0]:
        info[b"length"] = len(payload)
    else:
        info[b"files"] = [{b"length": len(data), b"path": [part.encode() for part in path.split("/")]} for path, data in files]
    info.update(extra or {})
    tid = hashlib.sha1(bencode(info)).hexdigest()
    (directory / f"{tid}.torrent").write_bytes(bencode({b"info": info}))
    return tid, len(info[b"pieces"]) // 20


def entries(name, files):
    result = []
    offset = 0
    for index, (path, data) in enumerate(files):
        result.append({"path": f"{name}/{path}" if path else name, "offset": offset, "size": len(data), "index": index})
        offset += len(data)
    return result


class ReadOnlyClient:
    def __init__(self):
        self.status = {}
        self.history = []
        self.admin = True
        self.inventory_reads = 0
        self.last_inventory = None
        self.on_get_status = lambda tid: None
        self.connections = []

    def connect(self):
        self.connections.append("connect")

    def disconnect(self):
        self.connections.append("disconnect")

    def call(self, method, *args):
        self.history.append((method, args))
        if method == "daemon.get_version":
            return b"2.2.0"
        if method == "daemon.authorized_call":
            assert args == ("core.get_known_accounts",)
            return self.admin
        if method == "core.get_torrents_status":
            self.inventory_reads += 1
            assert args[0] == {}
            status = self.last_inventory if self.inventory_reads > 1 and self.last_inventory is not None else self.status
            return {tid: {key: tor[key] for key in args[1]} for tid, tor in status.items()}
        if method == "core.get_torrent_status":
            self.on_get_status(args[0])
            return {key: self.status[args[0]][key] for key in args[1]}
        pytest.fail(f"unexpected or mutating RPC: {method}")


class Dataset:
    def __init__(self, tmp_path):
        self.tree = tmp_path / "data"
        self.tree.mkdir()
        self.metainfo = tmp_path / "metainfo"
        self.metainfo.mkdir()
        self.client = ReadOnlyClient()
        (self.tree / "reference").mkdir()
        self.original = self.tree / "reference" / "media.mkv"
        self.original.write_bytes(MEDIA)
        self.ref, count = write_metainfo(self.metainfo, "media.mkv", [("", MEDIA)], 4)
        self.add_status(self.ref, "reference", "media.mkv", [("", MEDIA)], 4, count, is_seed=True)
        self.requests = {}

    def add_status(self, tid, directory, original_name, files, piece_length, count, *, current_name=None, is_seed=False):
        self.client.status[tid] = {
            "download_location": f"{BASE}/{directory}",
            "files": entries(current_name or original_name, files),
            "orig_files": entries(original_name, files),
            "label": "site.cross-seed",
            "state": "Seeding" if is_seed else "Paused",
            "paused": not is_seed,
            "is_seed": is_seed,
            "is_finished": is_seed,
            "num_pieces": count,
            "piece_length": piece_length,
            "total_size": sum(len(data) for _, data in files),
            "file_priorities": [4] * len(files),
            "file_progress": [1.0 if is_seed else 0.0] * len(files),
            "pieces": None if is_seed else [3] * count,
        }
        return self.client.status[tid]

    def target(self, name, files, missing, *, piece_length=4, map_name="Mapped", physically_present_sidecars=()):
        tid, count = write_metainfo(self.metainfo, name, files, piece_length)
        status = self.add_status(tid, name, name, files, piece_length, count, current_name=map_name)
        status["pieces"] = [1 if piece in missing else 3 for piece in range(count)]
        directory = self.tree / name / map_name
        directory.mkdir(parents=True)
        os.link(self.original, directory / "media.mkv")
        for filename, data in files:
            if filename in physically_present_sidecars:
                (directory / filename).write_bytes(data)
        sidecars = frozenset(PurePosixPath(map_name, filename) for filename, _ in files if filename != "media.mkv")
        self.requests[tid] = Request(sidecars, (self.ref,))
        return tid, directory

    def donor(self, key, name, data):
        directory = self.tree / key
        directory.mkdir()
        path = directory / name
        path.write_bytes(data)
        tid = hashlib.sha1(key.encode()).hexdigest()
        self.add_status(tid, key, name, [("", data)], 4, 1, is_seed=True)
        return path

    def plan(self):
        return plan_sidecars(self.client, local_base=self.tree, remote_base=BASE, metadata=self.metainfo, requests=self.requests)


@pytest.fixture
def sample(tmp_path):
    return Dataset(tmp_path)


def test_v1_boundary_and_short_final_piece_are_counted_without_mutations(sample):
    tid, directory = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    state = deepcopy(sample.client.status)
    st = (directory / "media.mkv").stat()
    result = sample.plan()
    assert result["preferred_download_torrent_id"] == tid
    plan = result["plans"][0]
    assert plan["status"] == "download-candidate"
    assert plan["media_sources"] == [sample.ref]
    assert plan["missing_piece_indices"] == plan["remaining_piece_indices"] == [2]
    assert plan["missing_piece_bytes"] == plan["remaining_piece_bytes"] == 3
    assert plan["missing_sidecar_bytes"] == plan["remaining_sidecar_bytes"] == 1
    assert sample.client.status == state
    assert list(directory.iterdir()) == [directory / "media.mkv"]
    assert (directory / "media.mkv").stat().st_ino == st.st_ino == sample.original.stat().st_ino
    assert (directory / "media.mkv").read_bytes() == MEDIA
    assert sample.client.inventory_reads == 2


def test_absent_sidecar_parent_counts_toward_missing_pieces(sample):
    _, directory = sample.target("Nested", [("media.mkv", MEDIA), ("Subs/end.srt", b"!")], {2})
    plan = sample.plan()["plans"][0]
    assert plan["status"] == "download-candidate"
    assert plan["missing_piece_bytes"] == 3
    assert not (directory / "Subs").exists()


def test_checks_all_shared_sidecar_donor_hashes_and_rank_after_local_donors(sample):
    small, _ = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"12345")], {2, 3})
    large, _ = sample.target(
        "Large",
        [("intro.srt", b"?"), ("meta.nfo", b"+"), ("media.mkv", MEDIA), ("tail.txt", b"abcd")],
        {0, 3},
    )
    sample.donor("a-wrong", "intro.srt", b"-")
    verified_intro = sample.donor("b-right", "intro.srt", b"?")
    verified_meta = sample.donor("c-right", "meta.nfo", b"+")
    # Libtorrent may not mark a sidecar complete if its shared piece is absent.
    sample.client.status[hashlib.sha1(b"c-right").hexdigest()]["file_progress"] = [0.0]
    plans = sample.plan()
    assert [p["torrent_id"] for p in plans["plans"]] == [large, small]
    assert plans["preferred_download_torrent_id"] == large
    plan = plans["plans"][0]
    assert plan["missing_piece_indices"] == [0, 3]
    assert plan["missing_piece_bytes"] == 8
    assert plan["missing_sidecar_bytes"] == 6
    assert plan["remaining_piece_indices"] == [3]
    assert plan["remaining_piece_bytes"] == 4
    assert plan["remaining_sidecar_bytes"] == 4
    assert [s["verified_donor"] for s in plan["sidecars"]] == [str(verified_intro), str(verified_meta), None]


def test_all_local_pieces_are_ranked_as_download_free(sample):
    tid, _ = sample.target("Local", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    expected = sample.donor("donor", "end.srt", b"!")
    plan = sample.plan()
    assert plan["preferred_download_torrent_id"] is None
    assert plan["plans"][0]["torrent_id"] == tid
    assert plan["plans"][0]["status"] == "local-only"
    assert plan["plans"][0]["remaining_piece_bytes"] == 0
    assert plan["plans"][0]["sidecars"][0]["verified_donor"] == str(expected)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("pieces", [3, 3], "quiescent v1 piece map"),
        ("pieces", [3, 3, 2], "quiescent v1 piece map"),
        ("pieces", None, "quiescent v1 piece map"),
        ("pieces", [1, 3, 1], "unexplained=[0]"),
        ("pieces", [1, 3, 3], "not marked missing=[2]"),
        ("file_priorities", [4, 0], "wanted priorities"),
        ("piece_length", 8, "disagrees with v1 metainfo"),
        ("paused", False, "individually paused"),
    ],
)
def test_insufficient_or_inconsistent_deluge_evidence_is_blocked(sample, field, value, reason):
    tid, _ = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    sample.client.status[tid][field] = value
    result = sample.plan()
    assert result["preferred_download_torrent_id"] is None
    assert result["plans"][0]["status"] == "blocked"
    assert reason in result["plans"][0]["reason"]


def test_rejects_corruption_hidden_inside_the_missing_boundary_piece(sample):
    sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    sample.original.write_bytes(MEDIA[:8] + b"zz")
    result = sample.plan()["plans"][0]
    assert result["status"] == "blocked"
    assert "media source" in result["reason"] and "fails v1 piece hash 2" in result["reason"]


def test_rejects_corruption_in_a_deluge_complete_piece(sample):
    sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    sample.original.write_bytes(b"zz" + MEDIA[2:])
    assert "local hash failure on Deluge-complete v1 piece 0" in sample.plan()["plans"][0]["reason"]


def test_renamed_copy_cannot_prove_shared_media_inode(sample):
    _, target = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    path = target / "media.mkv"
    path.unlink()
    path.write_bytes(MEDIA)
    assert "not backed by a fully v1-verified media source" in sample.plan()["plans"][0]["reason"]


def test_missing_piece_with_a_present_sidecar_needs_independent_proof(sample):
    files = [("present.nfo", b"!"), ("absent.srt", b"?"), ("media.mkv", MEDIA)]
    sample.target("Boundary", files, {0}, physically_present_sidecars=("present.nfo",))
    assert "not backed by a fully v1-verified media source" in sample.plan()["plans"][0]["reason"]


def test_refuses_a_second_torrent_claiming_the_same_absent_destination(sample):
    tid, _ = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    sample.client.status["f" * 40] = deepcopy(sample.client.status[tid])
    sample.client.status["f" * 40]["files"].pop(0)
    sample.client.status["f" * 40]["files"][0]["index"] = 0
    assert "ambiguous or shared torrent destination" in sample.plan()["plans"][0]["reason"]


def test_rejects_a_symlink_at_expected_absent_destination(sample):
    _, directory = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    (directory / "end.srt").symlink_to(sample.tree / "absent")
    assert "non-symlink file" in sample.plan()["plans"][0]["reason"]


def test_detects_new_file_during_plan(sample):
    tid, directory = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    target_reads = []

    def create_during_final_status(torrent):
        if torrent == tid:
            target_reads.append(torrent)
            if len(target_reads) == 2:
                (directory / "end.srt").write_bytes(b"!")

    sample.client.on_get_status = create_during_final_status
    assert "sidecar destination changed while planning" in sample.plan()["plans"][0]["reason"]
    assert (directory / "end.srt").read_bytes() == b"!"


def test_inventory_change_disqualifies_an_observation(sample):
    sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    sample.client.last_inventory = {sample.ref: sample.client.status[sample.ref]}
    with pytest.raises(PlanError, match="inventory or file paths changed"):
        sample.plan()


def test_rejects_hidden_inventory_without_admin(sample):
    sample.client.admin = False
    with pytest.raises(PlanError, match="admin access"):
        sample.plan()
    assert sample.client.inventory_reads == 0


def test_metainfo_identity_and_hybrid_fail_closed(sample):
    tid, _ = write_metainfo(sample.metainfo, "hybrid", [("", b"abcd")], 2, {b"meta version": 2})
    path = sample.metainfo / f"{tid}.torrent"
    with pytest.raises(PlanError, match="infohash"):
        load_metainfo(path, "0" * 40)
    with pytest.raises(PlanError, match="v2 and hybrid"):
        load_metainfo(path, tid)


def test_cli_uses_exact_id_and_does_not_load_mutating_scan_settings(sample, tmp_path, monkeypatch, capsys):
    tid, _ = sample.target("Small", [("media.mkv", MEDIA), ("end.srt", b"!")], {2})
    other, _ = sample.target("Other", [("media.mkv", MEDIA), ("end.srt", b"??")], {2})
    manifest = tmp_path / "targets.json"
    manifest.write_text(json.dumps({k: {"sidecars": [str(s) for s in v.sidecars], "media_sources": list(v.media_sources)} for k, v in sample.requests.items()}))
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(
        f"DELUGE_HOST=localhost\nDELUGE_PORT=58846\nDELUGE_USERNAME=test\nDELUGE_PASSWORD=test\n" f"DELUGE_TORRENT_BASE_REMOTE_FOLDER={BASE}/\nLOCAL_TORRENT_BASE_LOCAL_FOLDER={sample.tree}\n"
    )
    monkeypatch.setenv("OUTPUT_FILE", str(tmp_path / "not-created" / "out.json"))
    monkeypatch.setenv("APP_SQLITE_CACHE_PATH", str(tmp_path / "not-created" / "db.sqlite"))
    for key in ("DELUGE_HOST", "DELUGE_PORT", "DELUGE_USERNAME", "DELUGE_PASSWORD", "DELUGE_TORRENT_BASE_REMOTE_FOLDER", "LOCAL_TORRENT_BASE_LOCAL_FOLDER"):
        monkeypatch.delenv(key, raising=False)

    def connect(**kwargs):
        assert not kwargs["automatic_reconnect"]
        return sample.client

    monkeypatch.setattr("deluge_client.DelugeRPCClient", connect)
    common = ["--manifest", str(manifest), "--metainfo-dir", str(sample.metainfo), "--torrent"]
    assert main(common + [tid[:8]]) == 1
    assert "exact 40-digit" in capsys.readouterr().err
    assert sample.client.connections == []
    assert main(common + [tid.upper()]) == 0
    out = json.loads(capsys.readouterr().out)
    assert [p["torrent_id"] for p in out["plans"]] == [tid]
    assert not any(args and args[0] == other for _, args in sample.client.history)
    assert sample.client.connections == ["connect", "disconnect"]
    assert not (tmp_path / "not-created").exists()
