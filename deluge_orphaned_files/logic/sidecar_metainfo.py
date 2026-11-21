"""Read-only BitTorrent v1 verification for sidecar planning."""

from __future__ import annotations

import hashlib
import os
import stat
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator, Sequence


class PlanError(ValueError):
    """Evidence is incomplete or contradicts a safe plan."""


def relative_path(value: str) -> PurePosixPath:
    if not isinstance(value, str) or "\\" in value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise PlanError(f"unsafe file path: {value!r}")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise PlanError(f"unsafe relative file path: {value!r}")
    return PurePosixPath(value)


def absolute_path(value: str) -> PurePosixPath:
    if not isinstance(value, str) or not value.startswith("/"):
        raise PlanError(f"not an absolute Deluge path: {value!r}")
    return PurePosixPath("/") if value == "/" else PurePosixPath("/") / relative_path(value.rstrip("/")[1:])


@dataclass(frozen=True)
class Entry:
    path: PurePosixPath
    offset: int
    size: int


@dataclass(frozen=True)
class Metainfo:
    files: tuple[Entry, ...]
    ends: tuple[int, ...]
    piece_length: int
    hashes: bytes

    @property
    def size(self) -> int:
        return self.ends[-1]

    @property
    def count(self) -> int:
        return len(self.hashes) // 20

    def piece_size(self, index: int) -> int:
        return min(self.piece_length, self.size - index * self.piece_length)

    def file_pieces(self, index: int) -> range:
        file = self.files[index]
        if not file.size:
            return range(0)
        return range(file.offset // self.piece_length, (self.ends[index] - 1) // self.piece_length + 1)

    def segments(self, index: int) -> Iterator[tuple[int, int, int]]:
        offset = index * self.piece_length
        end = offset + self.piece_size(index)
        file_index = bisect_right(self.ends, offset)
        while offset < end:
            file = self.files[file_index]
            stop = min(end, self.ends[file_index])
            if stop > offset:
                yield file_index, offset - file.offset, stop - offset
                offset = stop
            file_index += 1


class _Decoder:
    # Keep the original info bytes; re-encoding could change the infohash.
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0
        self.info_span = (0, 0)

    def _token(self, sep: bytes) -> bytes:
        start = self.pos
        end = self.data.find(sep, start)
        if end < 0 or end - start > 20:
            raise PlanError("invalid bencode token")
        token = self.data[start:end]
        self.pos = end + 1
        return token

    def _peek(self) -> int:
        if self.pos == len(self.data):
            raise PlanError("truncated bencode")
        return self.data[self.pos]

    @staticmethod
    def _number(token: bytes, *, signed: bool = False) -> int:
        digits = token[1:] if signed and token.startswith(b"-") else token
        if not digits.isdigit() or (digits.startswith(b"0") and token != b"0"):
            raise PlanError("invalid bencode number")
        return int(token)

    def value(self, depth: int = 0):
        if depth > 32:
            raise PlanError("metainfo is nested too deeply")
        marker = self._peek()
        if 48 <= marker <= 57:
            length = self._number(self._token(b":"))
            start = self.pos
            end = start + length
            if end > len(self.data):
                raise PlanError("truncated bencode string")
            result = self.data[start:end]
            self.pos = end
            return result
        self.pos += 1
        if marker == ord("i"):
            return self._number(self._token(b"e"), signed=True)
        if marker == ord("l"):
            items = []
            while self._peek() != ord("e"):
                items.append(self.value(depth + 1))
            self.pos += 1
            return items
        if marker == ord("d"):
            items = {}
            previous = None
            while self._peek() != ord("e"):
                key = self.value(depth + 1)
                if not isinstance(key, bytes) or (previous is not None and key <= previous):
                    raise PlanError("unordered, duplicate, or non-string bencode key")
                start = self.pos
                items[key] = self.value(depth + 1)
                if not depth and key == b"info":
                    self.info_span = start, self.pos
                previous = key
            self.pos += 1
            return items
        raise PlanError("unknown bencode marker")


def _component(raw: object) -> str:
    if not isinstance(raw, bytes):
        raise PlanError("missing metainfo path component")
    try:
        value = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PlanError("metainfo has non-UTF-8 paths") from exc
    if "/" in value:
        raise PlanError(f"invalid metainfo path component: {value!r}")
    relative_path(value)
    return value


def _no_special_files(info: dict) -> None:
    attrs = info.get(b"attr", b"")
    if not isinstance(attrs, bytes) or b"p" in attrs or b"l" in attrs or b"symlink path" in info:
        raise PlanError("pad files and symlink files are unsupported")


def load_metainfo(path: Path, torrent_id: str) -> Metainfo:
    limit = 16 * 1024 * 1024
    with path.open("rb") as src:
        data = src.read(limit + 1)
    if len(data) > limit:
        raise PlanError("metainfo exceeds 16 MiB")
    decoder = _Decoder(data)
    top = decoder.value()
    if not isinstance(top, dict) or decoder.pos != len(data) or not isinstance(top.get(b"info"), dict):
        raise PlanError("expected one complete bencoded torrent with an info dictionary")
    start, end = decoder.info_span
    if hashlib.sha1(memoryview(data)[start:end], usedforsecurity=False).hexdigest() != torrent_id:
        raise PlanError("metainfo infohash does not match the exact Deluge torrent ID")
    info = top[b"info"]
    if b"meta version" in info or b"file tree" in info:
        raise PlanError("v2 and hybrid metainfo are unsupported")
    _no_special_files(info)
    name = _component(info.get(b"name.utf-8", info.get(b"name")))
    raw_files = info.get(b"files")
    if b"files" in info:
        if b"length" in info or not isinstance(raw_files, list) or not raw_files:
            raise PlanError("invalid multi-file metainfo")
    else:
        raw_files = [info]
    files = []
    ends = []
    offset = 0
    for entry in raw_files:
        if not isinstance(entry, dict) or type(entry.get(b"length")) is not int or entry[b"length"] < 0:
            raise PlanError("invalid metainfo file length")
        _no_special_files(entry)
        parts = entry.get(b"path.utf-8", entry.get(b"path")) if b"files" in info else []
        if not isinstance(parts, list) or (b"files" in info and not parts):
            raise PlanError("invalid metainfo file path")
        size = entry[b"length"]
        files.append(Entry(PurePosixPath(name, *map(_component, parts)), offset, size))
        offset += size
        ends.append(offset)
    piece_length = info.get(b"piece length")
    hashes = info.get(b"pieces")
    if type(piece_length) is not int or piece_length <= 0 or not offset or not isinstance(hashes, bytes):
        raise PlanError("invalid v1 piece length, content size, or hashes")
    if len(hashes) != 20 * ((offset - 1) // piece_length + 1):
        raise PlanError("v1 piece count does not match the ordered file lengths")
    return Metainfo(tuple(files), tuple(ends), piece_length, hashes)


@dataclass(frozen=True)
class DiskFile:
    relative: PurePosixPath
    fingerprint: tuple[int, int, int, int, int]

    @property
    def inode(self) -> tuple[int, int]:
        return self.fingerprint[:2]


def _fingerprint(st: os.stat_result, path: PurePosixPath) -> tuple[int, int, int, int, int]:
    if not stat.S_ISREG(st.st_mode):
        raise PlanError(f"not a regular, non-symlink file: {path}")
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY


class ReadTree:
    """Read through directory FDs: never follow symlinks beneath the trusted mount."""

    def __init__(self, root: Path) -> None:
        if not root.is_absolute():
            raise PlanError("local torrent base must be absolute")
        self.root = root

    def __enter__(self) -> ReadTree:
        self.fd = os.open(self.root, _DIRECTORY_FLAGS)
        return self

    def __exit__(self, *exc) -> None:
        os.close(self.fd)

    def parent_fd(self, path: PurePosixPath) -> int:
        fd = os.dup(self.fd)
        try:
            for part in path.parts[:-1]:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=fd)
                os.close(fd)
                fd = child
        except BaseException:
            os.close(fd)
            raise
        return fd

    def snapshot(self, path: PurePosixPath, size: int) -> DiskFile | None:
        try:
            fd = self.parent_fd(path)
        except FileNotFoundError:
            return None
        try:
            st = os.stat(path.name, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return None
        finally:
            os.close(fd)
        fingerprint = _fingerprint(st, path)
        if st.st_size != size:
            raise PlanError(f"incorrect on-disk size: {path} (expected {size}, found {st.st_size})")
        return DiskFile(path, fingerprint)


class PieceReader:
    def __init__(self, tree: ReadTree) -> None:
        self.tree = tree
        self.source: DiskFile | None = None
        self.fd: int | None = None

    def __enter__(self) -> PieceReader:
        return self

    def __exit__(self, *exc) -> None:
        self._close()

    def _close(self) -> None:
        if self.fd is not None:
            fd, self.fd = self.fd, None
            try:
                self._check(fd, self.source)
            finally:
                os.close(fd)
        self.source = None

    @staticmethod
    def _check(fd: int, source: DiskFile) -> None:
        if _fingerprint(os.fstat(fd), source.relative) != source.fingerprint:
            raise PlanError(f"file changed while verifying piece hashes: {source.relative}")

    def _select(self, source: DiskFile) -> int:
        if self.fd is not None and source == self.source:
            return self.fd
        self._close()
        fd = self.tree.parent_fd(source.relative)
        try:
            self.fd = os.open(source.relative.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        finally:
            os.close(fd)
        self.source = source
        self._check(self.fd, source)
        return self.fd

    def matches(self, meta: Metainfo, sources: Sequence[DiskFile | None], piece: int) -> bool:
        hasher = hashlib.sha1(usedforsecurity=False)
        for index, offset, count in meta.segments(piece):
            source = sources[index]
            if source is None:
                raise PlanError("cannot hash a piece with absent constituent bytes")
            fd = self._select(source)
            while count:
                chunk = os.pread(fd, min(count, 1024 * 1024), offset)
                if not chunk:
                    raise PlanError(f"unexpected EOF while hashing: {source.relative}")
                hasher.update(chunk)
                offset += len(chunk)
                count -= len(chunk)
        start, end = 20 * piece, 20 * (piece + 1)
        return hasher.digest() == meta.hashes[start:end]
