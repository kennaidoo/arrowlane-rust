# Agent guide

Read CONTRIBUTING.md before changing code.

## Commands

- Format check: `cargo fmt --all --check`
- Lint: `cargo clippy --workspace --all-targets -- -D warnings`
- Test: `cargo test --workspace`
- Release build: `cargo build --release --workspace`
- Wire check: `python interop/check_wire.py --rust target/release/arrowlane.exe`

## Boundaries

- Do not create an issue or pull request unless the human asks.
- Do not change a lane id.
- Do not commit generated files or benchmark results.
- Do not run benchmarks in CI.
- Keep changes focused. Report changed files, checks run and open risks.

## References

- `docs/ai/quickstart.md` and `docs/ai/api.md` for the public contract.
- `docs/ai/manifest.json` for the static manifest.
- `llms.txt` for the short retrieval entry point.
