# API reference

## Transit

| Call | Returns | Notes |
|---|---|---|
| `plan(batch, topology, policy)` | `Decision` | default options |
| `plan_with(batch, &TransitOptions)` | `Decision` | full selector control |
| `pack(batch, lane, token, directory, level, max_payload)` | `Vec<u8>` | explicit lane |
| `pack_frame(...)` | `Frame` | no wire serialisation |
| `unpack(data, token, directory, max_payload, max_uncompressed)` | `RecordBatch` | parses and decodes once |
| `unpack_view(&Buffer, ...)` | `RecordBatch` | zero copy frame slice |
| `unpack_frame(frame, token, directory, max_uncompressed)` | `RecordBatch` | decodes a parsed frame |
| `transit(batch, &TransitOptions)` | `(Vec<u8>, Decision)` | plan, pack, decide |

`unpack_frame` is the call to use when a caller already parsed the frame for its
metadata. It avoids parsing the wire twice.

## Chunks

```text
ChunkOptions {
    lane: Cow<str>,
    token, directory, level,
    rows_per_chunk: usize,
    max_payload: usize,
    topology: Topology,
    policy: Policy,
    allowed: Option<Vec<String>>,
    min_gain: f64,
}
```

| Call | Returns | Buffering |
|---|---|---|
| `pack_chunks(batches, schema, &options)` | lazy iterator of wire frames | one chunk in flight |
| `write_chunks(writer, ...)` | `(chunks, bytes)` | one chunk per write |
| `read_chunks(reader, ...)` | lazy iterator of `RecordBatch` | one frame per item |
| `unpack_chunks(frames, ...)` | lazy iterator of `RecordBatch` | one frame per item |
| `assemble_chunks(frames, ...)` | one `RecordBatch` | joins all chunks |
| `transit_chunks(batches, schema, &options)` | `(lazy iterator, Decision)` | one chunk in flight |

`transit_chunks` plans from the total row and byte counts. It does not
concatenate the input batches. It passes `topology`, `policy`, `allowed`, and
`min_gain` from `ChunkOptions` into the selector.

The reader and decoder iterators enforce:

- one stream id for the whole stream;
- zero based, contiguous chunk indexes;
- no frame after the `last` frame;
- a `last` frame before end of stream;
- a non-empty stream.

## Frame and limits

```text
HEADER_SIZE = 38
MAX_HEADER_BYTES = 65_535
MAX_PAYLOAD_BYTES = 536_870_912
MAX_UNCOMPRESSED_BYTES = 1_073_741_824
MAX_CHUNKS = 1_000_000
DEFAULT_CHUNK_ROWS = 50_000
MAX_CHUNK_ROWS = 10_000_000
```

`decode_frame_view` slices the caller's `Buffer`. `decode_frame` copies the
payload into an owned `Buffer`. Use the view on a receive path when the frame is
already in memory.

## FFI

The C ABI exposes:

- `al_version`
- `al_select`
- `al_inspect`
- `al_repack`
- `al_free`
- `al_free_reason`

Returned `AlBuffer` values are owned by the caller. Free the data with `al_free`
and the decision reason with `al_free_reason`. The release profile uses
`panic = "unwind"` so the boundary can catch panics.

## Errors

Every library call returns `arrowlane::Result<T>`, which is
`Result<T, ArrowLaneError>`. The variants are `Frame`, `Config`, `Chunk`,
`LaneUnavailable`, `Segment`, `Auth`, `Limit`, `Arrow`, and `Io`.
