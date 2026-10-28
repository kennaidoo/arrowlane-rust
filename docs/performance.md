# Performance

These numbers compare the Rust port with the Python package on the same table.
They are a regression check, not a general claim that Rust is faster.

## Method

- Host: Windows 11 x86_64.
- Python: CPython 3.15 with pyarrow 26.
- Table: 8 columns, 200,000 rows, 13.11 MB of Arrow data.
- Command: `python interop/compare.py --rust target/release/arrowlane.exe --rows 200000`.
- Repeats: 3. The script reports the median.
- Wire bytes are measured from the encoded frame. The Arrow size is used only to
  normalise the wire ratio.

## Result

| Lane | Encode, Python | Encode, Rust | Encode speedup | Decode, Python | Decode, Rust | Decode speedup |
|---|---|---|---|---|---|---|
| json | 2684.6 ms | 314.8 ms | 8.5x | 2102.3 ms | 1390.4 ms | 1.5x |
| ipc | 7.1 ms | 6.2 ms | 1.1x | 3.5 ms | 5.4 ms | 0.6x |
| ipc-zstd | 30.5 ms | 30.7 ms | 1.0x | 23.4 ms | 14.5 ms | 1.6x |

## Reading the result

- JSON encode is the clear win. It writes from the Arrow arrays instead of
  building a Python object per row.
- IPC decode is slower. The Rust path still copies the payload into the Arrow
  reader. The 5.4 ms versus 3.5 ms gap is a regression, not noise.
- zstd decode is faster; zstd encode is at parity in this run. The encode ratio
  moved between 0.8x and 1.0x across repeated runs.
- The `shm` lane needs a POSIX platform and is not in this table.

## Rules for a new measurement

1. Build with `cargo build --release --workspace`.
2. Run at least two row counts that bracket the production shape.
3. Report median and spread, not one run.
4. Do not call a difference noise without a spread.
5. Check that Python and Rust use the same sample table. `interop/compare.py`
   builds both from the same rules.
