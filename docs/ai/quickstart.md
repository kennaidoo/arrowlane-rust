# Quick start

## Build the CLI and the C ABI

```bash
cargo build --release
```

The binaries are in `target/release`:

- `arrowlane` is the CLI.
- `arrowlane_ffi.dll` on Windows, `libarrowlane_ffi.so` on Linux, or
  `libarrowlane_ffi.dylib` on macOS is the C ABI.

## Use the library

```rust
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, true),
]));
let batch = RecordBatch::try_new(
    schema,
    vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
    ],
)?;

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

## Stream a large table

```rust,no_run
# use arrow_array::RecordBatch;
# use arrowlane::chunks::{ChunkOptions, pack_chunks};
# fn run(batches: Vec<RecordBatch>, schema: std::sync::Arc<arrow_schema::Schema>) -> arrowlane::Result<()> {
let options = ChunkOptions {
    lane: "ipc".into(),
    rows_per_chunk: 50_000,
    ..Default::default()
};
let frames = pack_chunks(batches, schema, &options)?;
for frame in frames {
    let wire = frame?;
    // Write the wire bytes to a blocking sink here. The next chunk is not
    // encoded until the next loop iteration asks for it.
    let _ = wire;
}
# Ok(())
# }
```

On the receive side, `read_chunks` and `unpack_chunks` return lazy iterators.
Each item is one decoded batch. `assemble_chunks` is the explicit join step.

## Inspect the capabilities

```bash
arrowlane describe --compact
```

The output is JSON. It reports the frame limits, lanes, streaming limits, selector
inputs, and the documentation entry points.
