"""Rust and Python lane comparison.

Measures the same table through both implementations on this host and prints a
table. Both sides encode and decode the identical Arrow data, so the numbers are
comparable.

Usage:
    python interop/compare.py --rust target/release/arrowlane.exe --rows 200000
"""

from __future__ import annotations

import argparse
import datetime
import statistics
import subprocess
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

import arrowlane


def sample_table(rows: int) -> pa.Table:
    """The same table the Rust `bench` subcommand builds."""
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
            ("ok", pa.bool_()),
            ("day", pa.date32()),
            ("amount", pa.decimal128(9, 2)),
            ("ts", pa.timestamp("us", tz="UTC")),
            ("blob", pa.binary()),
        ]
    )
    columns = [
        pa.array(range(rows), type=pa.int64()),
        pa.array([f"name-{index}" for index in range(rows)]),
        pa.array([index / 7.0 for index in range(rows)]),
        pa.array([index % 2 == 0 for index in range(rows)]),
        pa.array(
            [datetime.date(2026, 1, index % 28 + 1) for index in range(rows)],
            type=pa.date32(),
        ),
        pa.array([Decimal("12.34") for _ in range(rows)], type=pa.decimal128(9, 2)),
        pa.array(
            [datetime.datetime(2026, 10, 28, 12, 0, tzinfo=datetime.UTC) for _ in range(rows)],
            type=pa.timestamp("us", tz="UTC"),
        ),
        pa.array([b"\x00\x01\x02" for _ in range(rows)], type=pa.binary()),
    ]
    return pa.Table.from_arrays(columns, schema=schema)


def median_ms(repeats: int, body) -> float:
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        body()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()

    table = sample_table(args.rows)
    arrow_bytes = table.nbytes
    print(f"rows={args.rows} columns={table.num_columns} arrow_bytes={arrow_bytes}")

    rust_out = subprocess.run(
        [
            str(args.rust),
            "bench",
            "--rows",
            str(args.rows),
            "--repeats",
            str(args.repeats),
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    rust_rows = {}
    for line in rust_out.splitlines():
        parts = line.split()
        if len(parts) == 4 and parts[0] in {"json", "ipc", "ipc-zstd"}:
            rust_rows[parts[0]] = (int(parts[1]), float(parts[2]), float(parts[3]))

    print(
        f"{'lane':<10} {'impl':<7} {'wire_bytes':>12} {'ratio':>7} "
        f"{'encode_ms':>10} {'decode_ms':>10}"
    )
    for lane in ("json", "ipc", "ipc-zstd"):
        wire = arrowlane.pack(table, lane=lane)
        py_encode = median_ms(args.repeats, lambda: arrowlane.pack(table, lane=lane))
        py_decode = median_ms(args.repeats, lambda: arrowlane.unpack(wire))
        ratio = len(wire) / arrow_bytes
        print(
            f"{lane:<10} {'python':<7} {len(wire):>12} {ratio:>7.3f} "
            f"{py_encode:>10.1f} {py_decode:>10.1f}"
        )
        if lane in rust_rows:
            r_wire, r_encode, r_decode = rust_rows[lane]
            print(
                f"{lane:<10} {'rust':<7} {r_wire:>12} {r_wire / arrow_bytes:>7.3f} "
                f"{r_encode:>10.1f} {r_decode:>10.1f}"
            )
            print(
                f"{lane:<10} {'speedup':<7} {'':>12} {'':>7} "
                f"{py_encode / r_encode:>9.1f}x {py_decode / r_decode:>9.1f}x"
            )

    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp = Path(raw_tmp)
        path = tmp / "interop.aln"
        subprocess.run(
            [
                str(args.rust),
                "pack",
                "--lane",
                "ipc",
                "--rows",
                str(args.rows),
                "--out",
                str(path),
            ],
            check=True,
            capture_output=True,
        )
        print(f"\nrust wrote ipc frame: {path.stat().st_size} bytes, readable by python: ", end="")
        arrowlane.unpack(path.read_bytes())
        print("yes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
