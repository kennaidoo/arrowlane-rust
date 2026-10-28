"""Cross-language wire compatibility check.

Writes a frame with the Python package, reads it with the Rust binary, then writes
a frame with Rust and reads it with Python. A mismatch fails the run.

Usage:
    python interop/check_wire.py --rust target/release/arrowlane.exe
"""

from __future__ import annotations

import argparse
import datetime
import json
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

import arrowlane


def sample_table(rows: int = 500) -> pa.Table:
    """A table with the types the JSON lane has to tag.

    This must stay in step with `sample_batch` in `crates/arrowlane/src/main.rs`,
    because the harness compares a Rust frame against a Python table.
    """
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
        pa.array([f"row-{index}" if index % 7 else None for index in range(rows)]),
        pa.array([index / 3.0 for index in range(rows)]),
        pa.array([index % 2 == 0 for index in range(rows)]),
        pa.array(
            [datetime.date(2026, 1, index % 28 + 1) for index in range(rows)],
            type=pa.date32(),
        ),
        pa.array([Decimal("12.34") for _ in range(rows)], type=pa.decimal128(9, 2)),
        pa.array(
            [
                datetime.datetime(2026, 10, 28, 12, 0, tzinfo=datetime.UTC)
                for _ in range(rows)
            ],
            type=pa.timestamp("us", tz="UTC"),
        ),
        pa.array([b"\x00\x01\x02" for _ in range(rows)], type=pa.binary()),
    ]
    return pa.Table.from_arrays(columns, schema=schema)


def rust(cmd: list[str], rust_bin: Path, stdin: bytes | None = None) -> bytes:
    result = subprocess.run(
        [str(rust_bin), *cmd], input=stdin, capture_output=True, check=False
    )
    if result.returncode != 0:
        raise SystemExit(
            f"rust {cmd} failed rc={result.returncode}\n"
            f"stdout={result.stdout[:2000]!r}\nstderr={result.stderr[:2000]!r}"
        )
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=500)
    args = parser.parse_args()

    if not args.rust.exists():
        raise SystemExit(f"rust binary not found: {args.rust}")

    table = sample_table(args.rows)
    failures: list[str] = []

    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp = Path(raw_tmp)

        # Direction 1: Python writes, Rust reads.
        for lane in ("json", "ipc", "ipc-zstd"):
            wire = arrowlane.pack(table, lane=lane)
            path = tmp / f"py-{lane}.aln"
            path.write_bytes(wire)
            out = rust(["unpack", str(path), "--json"], args.rust)
            payload = json.loads(out)
            if payload["rows"] != table.num_rows:
                failures.append(
                    f"py->rust {lane}: rows {payload['rows']} != {table.num_rows}"
                )
            if payload["lane"] != lane:
                failures.append(f"py->rust {lane}: lane read as {payload['lane']}")

        # Direction 2: Rust writes, Python reads.
        for lane in ("json", "ipc", "ipc-zstd"):
            path = tmp / f"rs-{lane}.aln"
            rust(
                [
                    "pack",
                    "--lane",
                    lane,
                    "--rows",
                    str(args.rows),
                    "--out",
                    str(path),
                ],
                args.rust,
            )
            if not path.exists():
                failures.append(f"rust->py {lane}: no file written")
                continue
            wire = path.read_bytes()
            try:
                out = arrowlane.unpack(wire)
            except Exception as exc:  # noqa: BLE001 - report any decode failure
                failures.append(f"rust->py {lane}: python could not read it: {exc}")
                continue
            if out.num_rows != table.num_rows:
                failures.append(
                    f"rust->py {lane}: rows {out.num_rows} != {table.num_rows}"
                )
            if out.schema != table.schema:
                failures.append(f"rust->py {lane}: schema differs\n{out.schema}\n{table.schema}")
            else:
                for column in table.column_names:
                    if not out[column].equals(table[column]):
                        failures.append(f"rust->py {lane}: column {column} differs")
                        break

    if failures:
        print("FAIL")
        for line in failures:
            print(f"  {line}")
        return 1
    print(f"OK: {args.rows} rows, 3 lanes, both directions, values and schema identical")
    return 0


if __name__ == "__main__":
    sys.exit(main())
