# Contributing

This repository is the Rust implementation of arrowlane. The Python and Zig
implementations live in separate repositories.

## Before you start

- Open an issue for behaviour changes, new lanes, or wire-format changes.
- Keep a change small and focused. One concern per pull request.
- Do not change a lane id. Add a new id instead.
- Do not publish a performance claim without a reproducible measurement.

## Local checks

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
python interop/check_wire.py --rust target/release/arrowlane.exe
```

The Python interoperability environment lives beside this repository in
`../arrowlane`.

## Documentation

Update the page that describes the behaviour you changed. Keep `docs/ai/` and
`llms.txt` accurate for agents.

## Commit messages

Use `type(scope): description`, for example:

```text
fix(frame): reject a truncated payload before allocation
```

## Security

Do not report a vulnerability in a public issue. Follow `SECURITY.md`.
