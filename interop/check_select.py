"""Selector parity check.

Runs the same workload through the Python selector and the Rust selector and
fails on any difference in lane or reason.

Usage:
    python interop/check_select.py --rust target/release/arrowlane.exe
"""

from __future__ import annotations

import argparse
import itertools
import json
import subprocess
import sys
from pathlib import Path

from arrowlane.select import Policy, Topology, Workload, select_plan

from arrowlane.costmodel import load_default

ROWS = (0, 1, 100, 1_000, 10_000, 100_000, 500_000)
BYTES = (0, 64, 4_096, 1_000_000, 13_000_000, 60_000_000)


def rust_select(
    rust_bin: Path, rows: int, arrow_bytes: int, topology: str, policy: str
) -> dict[str, object]:
    result = subprocess.run(
        [
            str(rust_bin),
            "select",
            "--rows",
            str(rows),
            "--bytes",
            str(arrow_bytes),
            "--topology",
            topology,
            "--policy",
            policy,
            "--json",
        ],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"rust select failed rc={result.returncode}\n"
            f"stdout={result.stdout[:1000]!r}\nstderr={result.stderr[:1000]!r}"
        )
    return json.loads(result.stdout)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rust", required=True, type=Path)
    args = parser.parse_args()
    if not args.rust.exists():
        raise SystemExit(f"rust binary not found: {args.rust}")

    model = load_default()
    failures: list[str] = []
    checked = 0
    for rows, arrow_bytes in itertools.product(ROWS, BYTES):
        # The Python Workload rejects a non-empty table with zero bytes, and so
        # does the Rust one, so those pairs are not comparable through the CLI.
        if rows > 0 and arrow_bytes == 0:
            continue
        for topology in ("same_host", "same_network", "wide_area"):
            for policy in ("latency", "throughput", "cpu"):
                expected = select_plan(
                    Workload(rows=rows, arrow_bytes=arrow_bytes),
                    topology=Topology(topology),
                    policy=Policy(policy),
                    model=model,
                )
                got = rust_select(args.rust, rows, arrow_bytes, topology, policy)
                checked += 1
                if got["lane"] != expected.lane or got["reason"] != expected.reason:
                    failures.append(
                        f"rows={rows} bytes={arrow_bytes} topo={topology} policy={policy}: "
                        f"python {expected.lane}/{expected.reason} "
                        f"vs rust {got['lane']}/{got['reason']}"
                    )

    if failures:
        print(f"FAIL after {checked} cases")
        for line in failures:
            print(f"  {line}")
        return 1
    print(f"OK: {checked} selector cases agree on lane and reason")
    return 0


if __name__ == "__main__":
    sys.exit(main())
