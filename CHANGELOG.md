# Changelog

## Unreleased

### Fixed

- Reject a shared memory descriptor whose offset plus length overflows or leaves
  the mapped segment. A hostile descriptor can no longer panic the receiver.
- Use `panic = "unwind"` for the release profile so the FFI `catch_unwind`
  boundary is effective.
- Wrap `al_free` and `al_free_reason` in the same panic boundary.
- Check the frame header `ncols` against the decoded schema.
- Do not select the `shm` lane in `transit` or `transit_chunks` unless a token is
  supplied. This matches the Python `transit` rule and avoids a Linux-only
  `LaneUnavailable` failure.
- Decode an already parsed frame with `unpack_frame` instead of parsing the wire
  twice in `al_inspect` and the CLI.

### Changed

- The benchmark image uses a dynamic glibc binary in distroless. A static musl
  image was rejected because it regressed IPC performance.
- Docker build uses a Cargo registry and target cache.
- `read_chunks` and `unpack_chunks` now return lazy iterators instead of
  collecting the whole stream.
- `transit_chunks` returns a lazy iterator. It plans from row and byte totals,
  accepts topology, policy, allowlist and `min_gain`, and no longer concatenates
  the whole input or leaks the lane name.
- `ChunkOptions.lane` is a `Cow<str>` so a planned lane can be owned.
- `ChunkOptions` now carries `topology`, `policy`, `allowed` and `min_gain`.
- Move `tempfile` to the dev dependencies.
- Pin the toolchain to 1.99.1 with rustfmt and clippy.

### Added

- `LICENSE`, `SECURITY.md`, a Windows/Linux/macOS CI matrix, `llms.txt`,
  `docs/ai/manifest.json`, `docs/ai/quickstart.md`, `docs/ai/api.md`, and
  `docs/performance.md`.
- Regression tests for the descriptor range overflow, the `ncols` check, the
  streaming chunk reader, and `transit_chunks`.

### Removed

- The ignored `--table` option from `pack`.
- The absolute executable path from `describe` output.
