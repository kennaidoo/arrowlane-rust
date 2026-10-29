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
| JSON encode | `to_pylist`, then one `orjson` call and an array split | bytes written straight from the Arrow arrays |
| Frame prefix | `struct.pack` of six parts | one fixed `[u8; 38]` stack array |
| Checksum | `zlib.crc32` | `crc32fast` (hardware accelerated) |
| Receive path | payload copied out of the buffer | `decode_frame_view`, slices the caller's buffer |
| Chunk write | `b"".join(prefix, header, payload)` | `write_frame`, three writes and no join |
| Integer cells | `str()` per value | `write_i64`, no allocation |
| Float cells | `json.dumps` per value | one reused `String` scratch buffer |

The Python lane still rebuilds a Python dict per row, but it serializes the row
list once and splits the outer array. The Rust lane reads the Arrow buffers
directly, so it holds no per-row object at all. That is the whole difference,
and it is why JSON encode is the lane that gains most.

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

The maintained cross-language record is `arrowlane/docs/docker-benchmark.md` in
the Python repository. It uses the canonical fixture at 200,000 rows and five
repeats.

The local comparison command remains:

```bash
python interop/compare.py --rust target/release/arrowlane.exe --rows 200000
```

Report median and spread. JSON is the lane where Rust wins clearly. IPC decode
is slower because the Rust path copies the payload into the Arrow reader. Do
not read the record as "Rust is faster" in general. The `shm` lane needs a POSIX
platform and is not in the Docker table.

### Zero-copy receive

`decode_frame_view` and `unpack_view` slice the caller's buffer instead of copying
the payload. Use them when the frame is already in memory, which on a network
path it usually is. The IPC decoder still copies into the Arrow reader; the view
removes the frame copy, not the decoder copy.

## Container benchmark image

`bench/Dockerfile` builds a dynamic glibc binary and copies the loader, libc
and libgcc into a `scratch` image. The image is 8.6 MB and keeps glibc speed.
Static musl was rejected because it regressed IPC performance.

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
| `read_chunks(reader, ...)` | lazy chunk reader, one batch per item |
| `unpack_chunks(frames, ...)` | lazy decoder over frames already in memory |
| `assemble_chunks(frames, ...)` | rebuild one batch from a chunk stream |
| `transit_chunks(batches, schema, &options)` | plan and return a lazy chunk iterator |

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

All four pass. The test suite has 56 tests: 51 in the library and 5 in the FFI
crate.

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
- Every FFI entry point that can panic is wrapped in `catch_unwind`. The release
  profile uses `panic = "unwind"` so the catch is effective.

## Platform support

The portable lanes run on Linux x86_64, Linux aarch64, macOS arm64, Windows
x86_64 and Windows aarch64. The `shm` lane is POSIX only and reports
`is_ready() == false` on Windows. macOS and FreeBSD need `ARROWLAND_SHM_DIR`
pointing at a memory backed or tmpfs directory.

Windows aarch64 and FreeBSD aarch64 are supported from source. Prebuilt release
artifacts cover Linux x86_64, Linux aarch64, macOS arm64 and Windows x86_64.

## Agent docs

- `llms.txt` is the short machine-readable entry point.
- `docs/ai/manifest.json` is the static capability manifest.
- `docs/ai/quickstart.md` and `docs/ai/api.md` cover the public calls.
- `arrowlane describe --compact` prints the live manifest.

## Licence

Apache-2.0. See `LICENSE`.
