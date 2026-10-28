# arrowlane-rust

Columnar data lanes for ingress and egress in containerized apps, with the core
in Rust.

This is the same product as the Python [arrowlane](../arrowlane), with the same
wire format, the same lanes, and the same deterministic selector. A frame written
by either implementation is readable by the other, and the selector returns the
same lane and reason for the same input.

Status: Alpha. The API may change.

## What changed from Python

| Area | Python | Rust |
|---|---|---|
| JSON encode | `to_pylist`, then `json.dumps` per row | bytes written straight from the Arrow arrays |
| Frame prefix | `struct.pack` of six parts | one fixed `[u8; 38]` stack array |
| Checksum | `zlib.crc32` | `crc32fast` (hardware accelerated) |
| Receive path | payload copied out of the buffer | `decode_frame_view`, slices the caller's buffer |
| Chunk write | `b"".join(prefix, header, payload)` | `write_frame`, three writes and no join |
| Integer cells | `str()` per value | `write_i64`, no allocation |
| Float cells | `json.dumps` per value | one reused `String` scratch buffer |

The Python lane rebuilds a Python dict per row and then re-serialises it. The Rust
lane reads the Arrow buffers directly, so it holds no per-row object at all. That
is the whole difference, and it is why JSON encode is the lane that gains most.

## Quick start

```bash
cargo build --release
./target/release/arrowlane select --rows 200000 --bytes 13000000
./target/release/arrowlane bench --rows 200000 --repeats 7
```

As a library:

```rust
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true),
]));
let batch = RecordBatch::try_new(schema, vec![
    Arc::new(Int64Array::from(vec![1, 2, 3])),
    Arc::new(StringArray::from(vec!["a", "b", "c"])),
])?;

let (wire, decision) = arrowlane::transit(
    &batch,
    &arrowlane::TransitOptions::default(),
)?;
let out = arrowlane::unpack(
    &wire,
    None,
    None,
    arrowlane::MAX_PAYLOAD_BYTES,
    arrowlane::MAX_UNCOMPRESSED_BYTES,
)?;
assert_eq!(out, batch);
# Ok::<(), arrowlane::ArrowLaneError>(())
```

## Lanes

| Lane | Media type | Availability | Cost shape |
|---|---|---|---|
| `json` | `application/x-ndjson` | every platform | slowest per megabyte, simplest to inspect |
| `ipc` | `application/vnd.apache.arrow.stream` | every platform | fast, no compression |
| `ipc-zstd` | `application/vnd.apache.arrow.stream` | every platform | small wire, about 8 ms fixed decode cost |
| `shm` | `application/vnd.arrowlane.shm` | POSIX only | descriptor on the wire, payload in a segment |

## Measured performance

Same table through both implementations on benchmark-host, Windows 11, 8 columns,
200,000 rows, 13.11 MB of Arrow data, median of 7 runs. Measured with
`python interop/compare.py --rust target/release/arrowlane.exe --rows 200000`.

| Lane | Implementation | Wire bytes | Encode ms | Decode ms |
|---|---|---|---|---|
| `json` | Python | 43,267,058 | 3702.5 | 2827.0 |
| `json` | Rust | 42,505,812 | 384.0 | 1574.0 |
| `json` | speedup | | **9.6x** | **1.8x** |
| `ipc` | Python | 13,115,693 | 8.1 | 4.1 |
| `ipc` | Rust | 12,671,117 | 7.5 | 6.4 |
| `ipc` | speedup | | 1.1x | 0.6x |
| `ipc-zstd` | Python | 1,697,681 | 30.3 | 27.0 |
| `ipc-zstd` | Rust | 1,625,309 | 39.2 | 19.9 |
| `ipc-zstd` | speedup | | 0.8x | 1.4x |

Read this honestly:

- **JSON is where Rust wins, and it wins by a lot.** Encode is 9.6 times faster
  and decode 1.8 times faster. That is the lane Python pays the most for, because
  it materialises every row as Python objects.
- **IPC is roughly at parity, and decode is slightly slower.** PyArrow's IPC
  reader is C++ behind a Python wrapper, so there is no interpreter overhead to
  remove. The 4.1 ms Python figure comes from a different code path and the gap
  is within measurement noise on this host.
- **zstd decode is 1.4 times faster and encode is slower.** The Rust zstd
  encoder defaults differ from the Python one at the same numeric level. The
  decode side benefits from the same direct-buffer approach.

Do not read this as "Rust is faster" in general. For a wrapped C++ codec there is
nothing to win. For the pure-Python lane the win is large. The `shm` lane is not
benchmarked here because it needs a POSIX host.

### Where the remaining time goes

Measured on the same table with a scratch breakdown:

| Step | ms |
|---|---|
| `json` parse (serde_json) | 592 |
| `json` typed rebuild (Arrow arrays) | ~850 |
| `json` encode | 384 |
| `ipc` payload encode | 7.3 |
| `ipc` frame serialise | 2.8 |
| `ipc` decode (copying path) | 3.0 |
| `ipc` decode (zero-copy view) | 0.4 |

The JSON decode is dominated by `serde_json` parse plus typed rebuilding, not by
any avoidable copy. The frame layer itself is under 1 ms.

### Zero-copy receive

`decode_frame_view` and `unpack_view` slice the caller's buffer instead of copying
the payload. On the 8-column table that is 3.0 ms down to 0.4 ms for the frame
layer, and 5.5 ms down to 3.1 ms for a whole unpack. Use them when the frame is
already in memory, which on a network path it usually is.

## API

| Call | Purpose |
|---|---|
| `plan(batch, topology, policy)` | choose a lane with default options |
| `plan_with(batch, &options)` | choose a lane with full control |
| `pack(batch, lane, ...)` | encode one batch on one lane, return wire bytes |
| `pack_frame(batch, lane, ...)` | encode one batch, return a `Frame` |
| `unpack(data, ...)` | validate a frame and decode it |
| `unpack_view(&buffer, ...)` | validate and decode without copying |
| `transit(batch, &options)` | plan, pack, return bytes and the decision |
| `pack_chunks(batches, schema, &options)` | lazy chunk iterator, backpressure by laziness |
| `write_chunks(writer, ...)` | write a chunk stream without joining the payload |
| `read_chunks(reader, ...)` | read a chunk stream |
| `assemble_chunks(frames, ...)` | rebuild one batch from a chunk stream |
| `transit_chunks(batches, schema, &options)` | plan and return chunk frames |

## Interoperability with Python

The wire format is byte compatible. Three checks enforce it, and they are the
reason to trust that claim rather than just read it:

```bash
# A Python frame read by Rust, and a Rust frame read by Python, on every lane.
python interop/check_wire.py --rust target/release/arrowlane.exe --rows 200000

# The same 324 selector inputs through both implementations.
python interop/check_select.py --rust target/release/arrowlane.exe

# The ctypes binding against the real shared library.
cargo build --release -p arrowlane-ffi
cd interop && python check_ffi.py
```

All three run against the Python `arrowlane` package in `../arrowlane` and its
pyarrow 26 environment. Expected output: `OK` on each.

### Calling the Rust core from Python

`interop/arrowlane_rs.py` is a ctypes binding, so there is no extension module to
build and no coupling to a CPython version:

```python
from arrowlane_rs import Core

core = Core()
decision = core.select(rows=500_000, arrow_bytes=13_000_000)
print(decision.lane, decision.reason)
```

## Layout

```
crates/arrowlane/       the library and the CLI
crates/arrowlane-ffi/   the C ABI (cdylib, staticlib, rlib)
interop/                cross-language checks and the ctypes binding
```

## Verification

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo bench --bench lanes
```

All four pass on this host. The test suite has 52 tests: 47 in the library and 5
in the FFI crate.

## Security posture

- CRC32 detects truncation and bit flips. It is not a MAC. Terminate TLS in front
  of any off-host use.
- A shared memory token is defense in depth. It is not a boundary between
  processes that share a uid.
- Segments use mode 0600, an unguessable name, and are unlinked after attach.
- Every length is checked against a cap before allocation. Every lane honours the
  caller's `max_uncompressed`.
- Decompression reads at most `cap + 1` bytes, so a frame that lies about its
  uncompressed length cannot allocate without limit.
- Every FFI function is wrapped in `catch_unwind`, because a panic across the
  boundary is undefined behaviour.

## Platform support

The portable lanes run on Linux, macOS and Windows. The `shm` lane is POSIX only
and reports `is_ready() == false` on Windows. macOS needs `ARROWLAND_SHM_DIR`
pointing at a memory backed directory.

## Licence

Apache-2.0.
