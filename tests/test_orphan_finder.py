"""Tests for the orphan-detection guard against the Deluge snapshot / scan race.

`compute_orphans` snapshots Deluge once and then compares that snapshot against a
filesystem scan that can run for hours. Anything created in between (cross-seed injects
nightly, overlapping the scan window) is absent from the snapshot but is *not* an orphan.
These tests pin that behaviour down.
"""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from deluge_orphaned_files.logic import orphan_finder

HOUR = 3600.0


@pytest.fixture
def config() -> SimpleNamespace:
    """Minimal config stub; only the torrent folder is read when media check is skipped."""
    return SimpleNamespace(
        local_torrent_base_local_folder=Path("/torrents"),
        local_media_base_local_folder=Path("/media"),
        # Mirrors the deployed LOCAL_SUBFOLDERS_BLACKLIST. Note 'tv' is absent: Sonarr
        # consumes tv_unpackerred/, so those stay deletion candidates.
        local_subfolders_blacklist=["music", "cg", "courses", "ebooks", "soft", "comics"],
    )


def _patch(monkeypatch, *, deluge_paths: set[str], local_files: dict) -> None:
    """Stub out the Deluge RPC call and the filesystem scan used by compute_orphans."""
    monkeypatch.setattr(orphan_finder, "deluge_get_files", lambda config: (deluge_paths, {}, {}))
    monkeypatch.setattr(orphan_finder, "scan_get_local_files", lambda **kwargs: local_files)


def _entry(size: int = 1024, mtime: float | None = None) -> dict:
    entry = {"hash": "deadbeef", "size": size, "hash_algorithm": "xxh64"}
    if mtime is not None:
        entry["mtime"] = mtime
    return entry


def test_file_created_after_snapshot_is_not_an_orphan(monkeypatch, config):
    """The 3494b3ea case: cross-seed created the file 27 min after the snapshot was taken."""
    _patch(
        monkeypatch,
        deluge_paths=set(),
        local_files={"cross-seed-links/Disclosure Day.mkv": _entry(mtime=time.time() + HOUR)},
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    assert orphans == []


def test_file_older_than_snapshot_is_an_orphan(monkeypatch, config):
    """A genuine leftover — on disk before we asked Deluge, and still unknown to it."""
    _patch(
        monkeypatch,
        deluge_paths=set(),
        local_files={"movies/Oppenheimer.mkv": _entry(size=2048, mtime=time.time() - HOUR)},
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    assert [o["path"] for o in orphans] == ["movies/Oppenheimer.mkv"]
    assert orphans[0]["size"] == 2048


def test_mixed_batch_keeps_genuine_orphans_and_defers_new_files(monkeypatch, config):
    """A new file appearing mid-scan must not suppress detection of real orphans."""
    now = time.time()
    _patch(
        monkeypatch,
        deluge_paths={"movies/Silo S03E08.mkv"},
        local_files={
            "movies/Silo S03E08.mkv": _entry(mtime=now - HOUR),  # live torrent
            "movies/Scary Movie.mkv": _entry(size=4096, mtime=now - HOUR),  # genuine orphan
            "movies/The Invite.mkv": _entry(size=8192, mtime=now - HOUR),  # genuine orphan
            "cross-seed-links/injected.mkv": _entry(mtime=now + HOUR),  # created mid-scan
        },
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    # Sorted by size, descending.
    assert [o["path"] for o in orphans] == ["movies/The Invite.mkv", "movies/Scary Movie.mkv"]


def test_file_without_mtime_is_still_classified(monkeypatch, config):
    """Entries from an older cache that predate the mtime field keep the previous behaviour."""
    _patch(
        monkeypatch,
        deluge_paths=set(),
        local_files={"movies/Chien.51.mkv": _entry()},
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    assert [o["path"] for o in orphans] == ["movies/Chien.51.mkv"]


def test_files_known_to_deluge_are_never_orphans(monkeypatch, config):
    _patch(
        monkeypatch,
        deluge_paths={"movies/Silo S03E08.mkv"},
        local_files={"movies/Silo S03E08.mkv": _entry(mtime=time.time() - HOUR)},
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    assert orphans == []


def test_blacklisted_subfolder_and_its_unpackerred_dir_are_excluded(monkeypatch, config):
    """`cg` has no arr consuming it, so cg_unpackerred/ is hand-managed and must be kept."""
    now = time.time()
    _patch(
        monkeypatch,
        deluge_paths=set(),
        local_files={
            "cg/artbook.cbz": _entry(mtime=now - HOUR),
            "cg_unpackerred/artbook.cbz": _entry(mtime=now - HOUR),
            "soft_unpackerred/installer.iso": _entry(mtime=now - HOUR),
        },
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    assert orphans == []


def test_unpackerred_dir_without_a_blacklisted_parent_stays_a_candidate(monkeypatch, config):
    """tv_unpackerred/ is Sonarr's import staging — transient, so still a candidate."""
    _patch(
        monkeypatch,
        deluge_paths=set(),
        local_files={"tv_unpackerred/Silo S03E08.mkv": _entry(mtime=time.time() - HOUR)},
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    assert [o["path"] for o in orphans] == ["tv_unpackerred/Silo S03E08.mkv"]


def test_deluge_partial_allocations_are_never_candidates(monkeypatch, config):
    """Deleting a .parts file under an active download corrupts it."""
    now = time.time()
    _patch(
        monkeypatch,
        deluge_paths=set(),
        local_files={
            "cross-seed-links/Show.S01E01.mkv.parts": _entry(mtime=now - HOUR),
            "cross-seed-links/.hidden-scratch": _entry(mtime=now - HOUR),
            "cross-seed-links/Genuine Leftover.mkv": _entry(size=4096, mtime=now - HOUR),
        },
    )

    orphans, _, _ = orphan_finder.compute_orphans(config=config, skip_media_check=True)

    assert [o["path"] for o in orphans] == ["cross-seed-links/Genuine Leftover.mkv"]


def test_scanner_reports_mtime(tmp_path):
    """The guard above is only as good as the mtime the scanner hands it."""
    from deluge_orphaned_files.scanning.file_scanner import get_local_files

    target = tmp_path / "movie.mkv"
    target.write_bytes(b"x" * 1024)

    scan_config = SimpleNamespace(
        extensions_blacklist=[],
        min_file_size_mb=0,
        local_subfolders_blacklist=[],
        sqlite_cache_path=tmp_path / "cache.db",
    )

    local_files = get_local_files(folder=tmp_path, config=scan_config, no_progress=True)

    assert local_files["movie.mkv"]["mtime"] == pytest.approx(target.stat().st_mtime)
