"""Tests for --use-inodes mode: the same-filesystem preflight, scanner parity with hash mode,
and the inode-based torrent/media comparison.

The preflight must fail *before* any scan (the walks can run for hours) and must fail
with the diagnostic InodePreflightError, never a raw OSError — cli.main() converts it
into a clean error message and nonzero exit.
"""

from __future__ import annotations

import os
import time
from types import SimpleNamespace

import pytest

from deluge_orphaned_files.logic import orphan_finder
from deluge_orphaned_files.scanning.file_scanner import get_local_files, get_local_files_inodes

HOUR = 3600.0


@pytest.fixture
def config(tmp_path) -> SimpleNamespace:
    """Config whose folders really exist (same filesystem, so the preflight passes)."""
    torrents = tmp_path / "torrents"
    media = tmp_path / "media"
    torrents.mkdir()
    media.mkdir()
    return SimpleNamespace(
        local_torrent_base_local_folder=torrents,
        local_media_base_local_folder=media,
        local_subfolders_blacklist=["music"],
    )


def test_hardlink_preflight_runs_before_deluge_and_scans(monkeypatch, config):
    """A misconfigured mount must fail fast, not after a multi-hour torrent scan."""

    def failing_check(dir1, dir2):
        raise ValueError("boom")

    def too_late(*args, **kwargs):
        raise AssertionError("Deluge/scan ran before the same-filesystem preflight")

    monkeypatch.setattr(orphan_finder, "_assert_same_filesystem", failing_check)
    monkeypatch.setattr(orphan_finder, "deluge_get_files", too_late)
    monkeypatch.setattr(orphan_finder, "scan_get_local_files_inodes", too_late)

    with pytest.raises(ValueError, match="boom"):
        orphan_finder.compute_orphans(config=config, use_inodes=True)


def test_preflight_passes_on_same_filesystem_and_needs_no_write_access(tmp_path):
    """The stat-based check must work on read-only mounts and create no files.

    A probe hardlink is the wrong test: link(2) fails with EXDEV across two bind
    mounts of the same filesystem (the Docker deployment), where inode comparison
    is perfectly valid.
    """
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    a.chmod(0o555)
    b.chmod(0o555)
    try:
        shared_dev = orphan_finder._assert_same_filesystem(a, b)
    finally:
        a.chmod(0o755)
        b.chmod(0o755)

    assert shared_dev == os.stat(a).st_dev
    assert list(a.iterdir()) == []
    assert list(b.iterdir()) == []


def test_preflight_rejects_roots_on_different_filesystems(tmp_path, monkeypatch):
    """The scanners key on (st_dev, st_ino); differing root devices must fail fast."""
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()

    real_stat = os.stat

    class _DevShifted:
        def __init__(self, st):
            self._st = st
            self.st_dev = st.st_dev + 1

        def __getattr__(self, attr):
            return getattr(self._st, attr)

    def fake_stat(path, *args, **kwargs):
        st = real_stat(path, *args, **kwargs)
        if str(path).startswith(str(b)):
            return _DevShifted(st)
        return st

    monkeypatch.setattr(orphan_finder.os, "stat", fake_stat)

    with pytest.raises(orphan_finder.InodePreflightError, match="different filesystems"):
        orphan_finder._assert_same_filesystem(a, b)


def test_preflight_reports_an_unmounted_directory(tmp_path):
    a = tmp_path / "a"
    a.mkdir()

    with pytest.raises(orphan_finder.InodePreflightError, match="Is the media directory mounted"):
        orphan_finder._assert_same_filesystem(a, tmp_path / "missing")


def test_inode_mode_pairs_hardlinks_and_respects_blacklist(monkeypatch, config):
    now = time.time() - HOUR
    torrent_files = {
        "linked.mkv": {"inode": (1, 100), "size": 2048, "mtime": now},
        "unlinked.mkv": {"inode": (1, 200), "size": 4096, "mtime": now},
        "music/blacklisted.flac": {"inode": (1, 300), "size": 1024, "mtime": now},
    }
    media_files = {
        "movies/linked.mkv": {"inode": (1, 100), "size": 2048, "mtime": now},
        "movies/media-only.mkv": {"inode": (1, 400), "size": 512, "mtime": now},
    }
    scans = iter([torrent_files, media_files])
    monkeypatch.setattr(orphan_finder, "deluge_get_files", lambda config: (set(torrent_files), {}, {}))
    monkeypatch.setattr(orphan_finder, "scan_get_local_files_inodes", lambda **kwargs: next(scans))

    orphans, only_in_torrents, only_in_media = orphan_finder.compute_orphans(config=config, use_inodes=True)

    assert orphans == []
    assert [e["path"] for e in only_in_torrents] == ["unlinked.mkv"]
    assert [e["path"] for e in only_in_media] == ["movies/media-only.mkv"]


def test_inode_mode_reports_one_entry_per_physical_file(monkeypatch, config):
    """Hardlink fan-out: N names for one inode must yield one row, one size, and a
    deterministic representative (lexicographically smallest path) on both sides."""
    now = time.time() - HOUR
    torrent_files = {
        "movies/f.mkv": {"inode": (1, 100), "size": 2048, "mtime": now},
        "cross-seed-links/f.mkv": {"inode": (1, 100), "size": 2048, "mtime": now},
    }
    media_files = {
        "tv/b-name.mkv": {"inode": (1, 400), "size": 512, "mtime": now},
        "tv/a-name.mkv": {"inode": (1, 400), "size": 512, "mtime": now},
    }
    scans = iter([torrent_files, media_files])
    monkeypatch.setattr(orphan_finder, "deluge_get_files", lambda config: (set(torrent_files), {}, {}))
    monkeypatch.setattr(orphan_finder, "scan_get_local_files_inodes", lambda **kwargs: next(scans))

    _, only_in_torrents, only_in_media = orphan_finder.compute_orphans(config=config, use_inodes=True)

    assert [e["path"] for e in only_in_torrents] == ["cross-seed-links/f.mkv"]
    assert sum(e["size"] for e in only_in_torrents) == 2048
    assert [e["path"] for e in only_in_media] == ["tv/a-name.mkv"]
    assert sum(e["size"] for e in only_in_media) == 512


def test_unpackerred_dirs_are_excluded_from_only_in_sections(monkeypatch, config):
    """The cg_unpackerred incident must not be reproducible via only_in_* in inode mode."""
    config.local_subfolders_blacklist = ["cg"]
    now = time.time() - HOUR
    torrent_files = {
        "cg_unpackerred/artbook.cbz": {"inode": (1, 100), "size": 2048, "mtime": now},
        "tv_unpackerred/show.mkv": {"inode": (1, 200), "size": 4096, "mtime": now},
    }
    media_files = {
        "cg_unpackerred/other.cbz": {"inode": (1, 300), "size": 1024, "mtime": now},
    }
    scans = iter([torrent_files, media_files])
    monkeypatch.setattr(orphan_finder, "deluge_get_files", lambda config: (set(torrent_files), {}, {}))
    monkeypatch.setattr(orphan_finder, "scan_get_local_files_inodes", lambda **kwargs: next(scans))

    _, only_in_torrents, only_in_media = orphan_finder.compute_orphans(config=config, use_inodes=True)

    assert [e["path"] for e in only_in_torrents] == ["tv_unpackerred/show.mkv"]
    assert only_in_media == []


def test_unpackerred_dirs_are_excluded_from_only_in_sections_in_hash_mode(monkeypatch, config):
    """Same guarantee for the default hash mode's only_in_* sections."""
    config.local_subfolders_blacklist = ["cg"]
    now = time.time() - HOUR
    torrent_files = {
        "cg_unpackerred/artbook.cbz": {"hash": "aaa", "size": 2048, "mtime": now},
        "tv_unpackerred/show.mkv": {"hash": "bbb", "size": 4096, "mtime": now},
    }
    media_files = {
        "cg_unpackerred/other.cbz": {"hash": "ccc", "size": 1024, "mtime": now},
    }
    scans = iter([torrent_files, media_files])
    monkeypatch.setattr(orphan_finder, "deluge_get_files", lambda config: (set(torrent_files), {}, {}))
    monkeypatch.setattr(orphan_finder, "scan_get_local_files", lambda **kwargs: next(scans))

    _, only_in_torrents, only_in_media = orphan_finder.compute_orphans(config=config)

    assert [e["path"] for e in only_in_torrents] == ["tv_unpackerred/show.mkv"]
    assert only_in_media == []


def test_inode_scanner_sees_the_same_files_as_the_hash_scanner(tmp_path):
    """Symlinks are followed in both modes, and inode entries carry the mtime the
    snapshot race-guard depends on."""
    scan_config = SimpleNamespace(
        extensions_blacklist=[],
        min_file_size_mb=0,
        local_subfolders_blacklist=[],
        sqlite_cache_path=tmp_path / "cache.db",
    )
    target = tmp_path / "movie.mkv"
    target.write_bytes(b"x" * 1024)
    (tmp_path / "link.mkv").symlink_to(target)

    # Inode scan first: the hash scan leaves a .hash_cache.json behind.
    by_inode = get_local_files_inodes(folder=tmp_path, config=scan_config, no_progress=True)
    by_hash = get_local_files(folder=tmp_path, config=scan_config, no_progress=True)

    assert set(by_inode) == set(by_hash) == {"movie.mkv", "link.mkv"}
    assert by_inode["link.mkv"]["inode"] == by_inode["movie.mkv"]["inode"]
    assert by_inode["movie.mkv"]["mtime"] == pytest.approx(target.stat().st_mtime)
