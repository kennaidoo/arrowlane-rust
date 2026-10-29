# Performance

The maintained cross-language record is `arrowlane/docs/docker-benchmark.md` in
the Python repository. It uses the canonical eight-column fixture, 200,000 rows
and five repeats.

The Rust `interop/compare.py` command remains the correct way to measure a new
host:

```bash
python interop/compare.py --rust target/release/arrowlane.exe --rows 200000
```

Report median and spread. Do not treat one run as a baseline. The `shm` lane is
not in the Docker table and needs a POSIX platform.
