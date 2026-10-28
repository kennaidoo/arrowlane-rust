"""ctypes binding for the arrowlane Rust core.

This loads the shared library built by `cargo build --release` and exposes the
selector and the frame inspector. It is the low-ceremony path: no extension
module build, no ABI coupling to a specific CPython version.

Usage:
    from arrowlane_rs import Core

    core = Core()                       # finds the library next to this file
    decision = core.select(rows=500_000, arrow_bytes=13_000_000)
    print(decision.lane, decision.reason, decision.predicted_ms)
"""

from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
from dataclasses import dataclass
from pathlib import Path

TOPO_SAME_HOST = 0
TOPO_SAME_NETWORK = 1
TOPO_WIDE_AREA = 2

POLICY_LATENCY = 0
POLICY_THROUGHPUT = 1
POLICY_CPU = 2

LANE_NAMES = {0: "?", 1: "json", 2: "ipc", 3: "ipc-zstd", 4: "shm"}

AL_OK = 0
AL_ERR_INVALID = -1
AL_ERR_FRAME = -2
AL_ERR_PANIC = -3
AL_ERR_BUFFER = -4


class AlBuffer(ctypes.Structure):
    """An owned byte buffer handed back by the library."""

    _fields_ = [
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
        ("cap", ctypes.c_size_t),
    ]


class AlDecision(ctypes.Structure):
    """A flattened selection result.

    `reason` is declared as a raw pointer, not `c_char_p`, because reading a
    `c_char_p` field makes ctypes build a Python bytes copy and lose the original
    address. The library has to free the address it allocated, so the raw pointer
    must survive the read.
    """

    _fields_ = [
        ("lane_id", ctypes.c_uint8),
        ("predicted_ms", ctypes.c_double),
        ("predicted_bytes", ctypes.c_uint64),
        ("reason", ctypes.c_void_p),
    ]


@dataclass(frozen=True)
class Decision:
    """A selection result in Python form."""

    lane: str
    reason: str
    predicted_ms: float
    predicted_bytes: int


def library_name() -> str:
    """The file name of the shared library on this platform."""
    if sys.platform == "win32":
        return "arrowlane_ffi.dll"
    if sys.platform == "darwin":
        return "libarrowlane_ffi.dylib"
    return "libarrowlane_ffi.so"


def candidate_paths() -> list[Path]:
    """Where the shared library might be, most specific first."""
    here = Path(__file__).resolve().parent
    repo = here.parent
    target = repo / "target"
    name = library_name()
    paths = [
        repo / name,
        target / "release" / name,
        target / "debug" / name,
        here / name,
    ]
    found = ctypes.util.find_library("arrowlane_ffi")
    if found:
        paths.append(Path(found))
    return paths


def load(path: str | Path | None = None) -> ctypes.CDLL:
    """Load the shared library, or raise with the paths it tried."""
    if path is not None:
        return ctypes.CDLL(str(path))
    tried: list[str] = []
    for candidate in candidate_paths():
        tried.append(str(candidate))
        if candidate.exists():
            return ctypes.CDLL(str(candidate))
    raise OSError(
        "arrowlane_ffi library not found. Build it with `cargo build --release`. Tried:\n  "
        + "\n  ".join(tried)
    )


class Core:
    """The Rust core, loaded through its C ABI."""

    def __init__(self, path: str | Path | None = None) -> None:
        self._lib = load(path)
        self._lib.al_version.restype = ctypes.c_char_p
        self._lib.al_select.restype = ctypes.c_int
        self._lib.al_select.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint8,
            ctypes.c_uint8,
            ctypes.c_bool,
            ctypes.POINTER(AlDecision),
        ]
        self._lib.al_free_reason.restype = None
        self._lib.al_free_reason.argtypes = [ctypes.c_void_p]
        self._lib.al_inspect.restype = AlBuffer
        self._lib.al_inspect.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t]
        self._lib.al_repack.restype = AlBuffer
        self._lib.al_repack.argtypes = [
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_size_t,
            ctypes.c_uint8,
        ]
        self._lib.al_free.restype = None
        self._lib.al_free.argtypes = [AlBuffer]

    @property
    def version(self) -> str:
        """Library version."""
        raw = self._lib.al_version()
        return raw.decode("utf-8") if raw else "?"

    def select(
        self,
        rows: int,
        arrow_bytes: int,
        topology: int = TOPO_SAME_NETWORK,
        policy: int = POLICY_LATENCY,
        shm_ready: bool = False,
    ) -> Decision:
        """Choose a lane. Raises on any error."""
        out = AlDecision()
        code = self._lib.al_select(rows, arrow_bytes, topology, policy, shm_ready, ctypes.byref(out))
        if code != AL_OK:
            raise RuntimeError(f"al_select failed with code {code}")
        try:
            if out.reason:
                raw = ctypes.string_at(ctypes.cast(out.reason, ctypes.c_void_p))
                reason = raw.decode("utf-8")
            else:
                reason = "?"
        finally:
            if out.reason:
                self._lib.al_free_reason(out.reason)
        return Decision(
            lane=LANE_NAMES.get(out.lane_id, "?"),
            reason=reason,
            predicted_ms=out.predicted_ms,
            predicted_bytes=out.predicted_bytes,
        )

    def _taken(self, buffer: AlBuffer) -> bytes:
        """Copy a returned buffer into Python bytes and free the original."""
        try:
            if not buffer.data or buffer.len == 0:
                return b""
            return ctypes.string_at(ctypes.cast(buffer.data, ctypes.c_void_p), buffer.len)
        finally:
            self._lib.al_free(buffer)

    def inspect(self, wire: bytes) -> bytes:
        """Return a JSON summary of a frame as bytes, or b"" when it is invalid."""
        if not wire:
            return b""
        array = (ctypes.c_uint8 * len(wire)).from_buffer_copy(wire)
        return self._taken(self._lib.al_inspect(array, len(wire)))

    def repack(self, wire: bytes, lane_id: int) -> bytes:
        """Re-encode a frame on another lane, or return b"" when it is invalid."""
        if not wire:
            return b""
        array = (ctypes.c_uint8 * len(wire)).from_buffer_copy(wire)
        return self._taken(self._lib.al_repack(array, len(wire), lane_id))


__all__ = [
    "AL_ERR_BUFFER",
    "AL_ERR_FRAME",
    "AL_ERR_INVALID",
    "AL_ERR_PANIC",
    "AL_OK",
    "Core",
    "Decision",
    "LANE_NAMES",
    "POLICY_CPU",
    "POLICY_LATENCY",
    "POLICY_THROUGHPUT",
    "TOPO_SAME_HOST",
    "TOPO_SAME_NETWORK",
    "TOPO_WIDE_AREA",
    "library_name",
    "load",
]
