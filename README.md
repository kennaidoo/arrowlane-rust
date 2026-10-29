# arrowlane-rust

Rust implementation of the arrowlane frame, lane selector and chunk streams.

> Status: Alpha source preview. No release artifacts yet. The API may change.

This is the same wire format as the Python [arrowlane](https://github.com/kennaidoo/arrowlane). A frame
written by either implementation is readable by the other, and the selector
returns the same lane and reason for the same input.

## Why arrowlane-rust

- Encode directly from Arrow buffers, without building a Python object per row.
- Decode a frame as a view over the caller's buffer.
- Use the same lanes and selector as the Python package.
- Call the Rust core from Python through a small ctypes binding.

## Quick start

```bash
cargo build --release
./target/release/arrowlane select --rows 200000 --bytes 13000000
./target/release/arrowlane bench --rows 200000 --repeats 5
```

## Library example

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

## Install

There is no crates.io release yet. Use the repository as a path dependency, or
build the binary from a checkout.

## Lanes

| Lane | Media type | Availability | Tradeoff |
|---|---|---|---|
| `json` | `application/x-ndjson` | Every platform | Easiest to inspect, largest |
| `ipc` | `application/vnd.apache.arrow.stream` | Every platform | Fast, no compression |
| `ipc-zstd` | `application/vnd.apache.arrow.stream` | Every platform | Small wire, fixed decode cost |
| `shm` | `application/vnd.arrowlane.shm` | POSIX only | Descriptor on the wire, payload in a segment |

## Limits

- The Arrow IPC decoder still copies the payload into the Arrow reader. `decode_frame_view` removes the frame copy, not the decoder copy.
- The `shm` lane is POSIX only and reports `is_ready() == false` on Windows.
- CRC32 is not a MAC. Terminate TLS in front of off-host use.
- There is no prebuilt release artifact yet.

## Platforms

The portable lanes run on Linux x86_64, Linux aarch64, macOS arm64, Windows
x86_64 and Windows aarch64. macOS and FreeBSD need `ARROWLAND_SHM_DIR` for the
shared-memory lane.

Prebuilt artifacts are planned for Linux x86_64, Linux aarch64, macOS arm64 and
Windows x86_64.

## Interoperability

```bash
python interop/check_wire.py --rust target/release/arrowlane.exe --rows 50000
python interop/check_select.py --rust target/release/arrowlane.exe
cd interop && python check_ffi.py
```

The Python environment lives beside this repository in `../arrowlane`.

## Performance

The maintained cross-language record is
[`arrowlane/docs/docker-benchmark.md`](https://github.com/kennaidoo/arrowlane/blob/main/docs/docker-benchmark.md).
JSON is the lane where Rust wins clearly. IPC decode is slower because the Rust
path copies the payload into the Arrow reader. Do not read the record as "Rust
is faster" in general.

Run the local comparison with:

```bash
python interop/compare.py --rust target/release/arrowlane.exe --rows 200000
```

## Documentation

- [docs/ai/quickstart.md](docs/ai/quickstart.md): build and first call.
- [docs/ai/api.md](docs/ai/api.md): public calls and return shapes.
- [docs/ai/manifest.json](docs/ai/manifest.json): static capability manifest.
- [llms.txt](llms.txt): short entry point for retrieval tools.
- `arrowlane describe --compact`: live manifest.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) for the process and [AGENTS.md](AGENTS.md)
for the coding-agent commands.

## Security

Report vulnerabilities privately through the repository Security tab. Do not
open a public issue. See [SECURITY.md](SECURITY.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
