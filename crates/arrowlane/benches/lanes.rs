//! Lane benchmarks. A plain harness so the numbers do not depend on criterion.
//!
//! Run with `cargo bench --bench lanes`. Every lane is measured end to end:
//! `plan`, encode, frame serialise, frame parse, decode.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrowlane::{MAX_PAYLOAD_BYTES, MAX_UNCOMPRESSED_BYTES, Policy, TransitOptions};

const LANES: [&str; 3] = ["json", "ipc", "ipc-zstd"];

fn sample(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, false),
        Field::new("ok", DataType::Boolean, false),
        Field::new("day", DataType::Date32, true),
        Field::new("amount", DataType::Decimal128(9, 2), true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("blob", DataType::Binary, true),
    ]));
    let rows_i = rows as i64;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..rows_i)),
        Arc::new(StringArray::from_iter_values(
            (0..rows_i).map(|index| format!("name-{index}")),
        )),
        Arc::new(Float64Array::from_iter_values(
            (0..rows_i).map(|index| index as f64 / 7.0),
        )),
        Arc::new(BooleanArray::from_iter(
            (0..rows_i).map(|index| Some(index % 2 == 0)),
        )),
        Arc::new(Date32Array::from_iter_values(
            (0..rows_i).map(|index| (index % 28) as i32),
        )),
        Arc::new(
            Decimal128Array::from_iter_values((0..rows_i).map(|_| 1_234i128))
                .with_precision_and_scale(9, 2)
                .expect("valid decimal"),
        ),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(
                (0..rows_i).map(|_| 1_793_000_000_000_000i64),
            )
            .with_timezone("UTC"),
        ),
        Arc::new(BinaryArray::from_iter_values(
            (0..rows_i).map(|_| &b"\x00\x01\x02"[..]),
        )),
    ];
    RecordBatch::try_new(schema, columns).expect("valid batch")
}

fn median(samples: &mut [f64]) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    samples[samples.len() / 2]
}

fn time<F: FnMut()>(repeats: usize, mut body: F) -> f64 {
    let mut samples = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        let start = Instant::now();
        body();
        samples.push(start.elapsed().as_secs_f64() * 1e3);
    }
    median(&mut samples)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let rows: usize = args
        .iter()
        .position(|arg| arg == "--rows")
        .and_then(|index| args.get(index + 1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000);
    let repeats: usize = args
        .iter()
        .position(|arg| arg == "--repeats")
        .and_then(|index| args.get(index + 1))
        .and_then(|value| value.parse().ok())
        .unwrap_or(7);

    let batch = sample(rows);
    let arrow_bytes = batch.get_array_memory_size();
    println!(
        "rows={rows} columns={} arrow_bytes={arrow_bytes} repeats={repeats}",
        batch.num_columns()
    );
    println!(
        "{:<10} {:>12} {:>10} {:>10} {:>12} {:>12} {:>10}",
        "lane", "wire_bytes", "ratio", "enc_ms", "decode_ms", "roundtrip_ms", "MB/s"
    );

    for lane in LANES {
        let wire =
            arrowlane::pack(&batch, lane, None, None, None, MAX_PAYLOAD_BYTES).expect("pack works");
        let wire_len = wire.len();
        let encode = time(repeats, || {
            let _ = arrowlane::pack(&batch, lane, None, None, None, MAX_PAYLOAD_BYTES)
                .expect("pack works");
        });
        let decode = time(repeats, || {
            let _ = arrowlane::unpack(&wire, None, None, MAX_PAYLOAD_BYTES, MAX_UNCOMPRESSED_BYTES)
                .expect("unpack works");
        });
        let roundtrip = time(repeats, || {
            let encoded = arrowlane::pack(&batch, lane, None, None, None, MAX_PAYLOAD_BYTES)
                .expect("pack works");
            let _ = arrowlane::unpack(
                &encoded,
                None,
                None,
                MAX_PAYLOAD_BYTES,
                MAX_UNCOMPRESSED_BYTES,
            )
            .expect("unpack works");
        });
        let mb = arrow_bytes as f64 / 1e6;
        let throughput = if roundtrip > 0.0 {
            mb / (roundtrip / 1e3)
        } else {
            0.0
        };
        println!(
            "{:<10} {:>12} {:>10.3} {:>10.3} {:>12.3} {:>12.3} {:>10.1}",
            lane,
            wire_len,
            wire_len as f64 / arrow_bytes as f64,
            encode,
            decode,
            roundtrip,
            throughput
        );
    }

    // Plan cost is independent of the data size, so measure it on its own.
    let plan_ms = time(repeats, || {
        let options = TransitOptions {
            topology: arrowlane::Topology::SameNetwork,
            policy: Policy::Latency,
            ..Default::default()
        };
        let _ = arrowlane::transit(&batch, &options).expect("transit works");
    });
    println!("transit (plan + encode) median: {plan_ms:.3} ms");
}
