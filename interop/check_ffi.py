"""Smoke test for the ctypes binding, using real frames.

Run after `cargo build --release -p arrowlane-ffi`:
    python interop/check_ffi.py
"""

from __future__ import annotations

import datetime
import json
import sys
from decimal import Decimal

import pyarrow as pa

import arrowlane
from arrowlane.costmodel import load_default
from arrowlane.select import Policy, Topology, Workload, select_plan
from arrowlane_rs import POLICY_CPU, POLICY_LATENCY, TOPO_SAME_HOST, Core


def table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(1000), type=pa.int64()),
            "name": [f"row-{index}" for index in range(1000)],
            "day": pa.array([datetime.date(2026, 1, 1)] * 1000, type=pa.date32()),
            "amount": pa.array([Decimal("1.25")] * 1000, type=pa.decimal128(9, 2)),
        }
    )


def main() -> int:
    core = Core()
    print(f"library version: {core.version}")
    failures: list[str] = []

    decision = core.select(rows=500_000, arrow_bytes=13_000_000)
    print(f"select: lane={decision.lane} reason={decision.reason} ms={decision.predicted_ms:.3f}")
    # Compare against the Python selector rather than a hardcoded guess.
    expected = select_plan(
        Workload(rows=500_000, arrow_bytes=13_000_000),
        topology=Topology.SAME_NETWORK,
        policy=Policy.LATENCY,
        model=load_default(),
    )
    if decision.lane != expected.lane:
        failures.append(f"select disagrees with python: {decision.lane} vs {expected.lane}")

    cpu = core.select(rows=500_000, arrow_bytes=13_000_000, topology=TOPO_SAME_HOST, policy=POLICY_CPU)
    if cpu.lane == "json":
        failures.append("cpu policy on a large batch should not fall back to json")

    tiny = core.select(rows=2, arrow_bytes=64)
    if tiny.lane != "json":
        failures.append(f"a tiny batch should stay on json, got {tiny.lane}")

    data = table()
    wire = arrowlane.pack(data, lane="ipc")
    summary_raw = core.inspect(wire)
    if not summary_raw:
        failures.append("inspect returned nothing for a valid frame")
    else:
        summary = json.loads(summary_raw)
        print(f"inspect: {summary}")
        if summary["rows"] != data.num_rows:
            failures.append(f"inspect rows {summary['rows']} != {data.num_rows}")
        if summary["lane"] != "ipc":
            failures.append(f"inspect lane {summary['lane']} != ipc")

    # Repack through the Rust core and read the result with Python.
    zstd_wire = core.repack(wire, 3)
    if not zstd_wire:
        failures.append("repack returned nothing")
    else:
        out = arrowlane.unpack(zstd_wire)
        if not out.equals(data):
            failures.append("repack through rust changed the data")
        else:
            print(f"repack: {len(wire)} ipc bytes -> {len(zstd_wire)} ipc-zstd bytes, data identical")

    garbage = core.inspect(b"not a frame at all")
    if garbage:
        failures.append("inspect accepted garbage")

    if failures:
        print("FAIL")
        for line in failures:
            print(f"  {line}")
        return 1
    print("OK: FFI select, inspect and repack all behave")
    return 0


if __name__ == "__main__":
    sys.exit(main())
