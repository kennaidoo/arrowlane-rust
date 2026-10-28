//! Command line entry point. Subcommands mirror the Python CLI.

use arrowlane::costmodel::load_default;
use arrowlane::select::{Policy, Selection, Topology, Workload, select_plan};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        usage();
        std::process::exit(2);
    }
    let exit = match args[0].as_str() {
        "select" => cmd_select(&args[1..]),
        "describe" => cmd_describe(&args[1..]),
        "pack" => cmd_pack(&args[1..]),
        "unpack" => cmd_unpack(&args[1..]),
        "bench" => cmd_bench(&args[1..]),
        "-h" | "--help" | "help" => {
            usage();
            0
        }
        "--version" => {
            println!("{}", arrowlane::VERSION);
            0
        }
        other => {
            eprintln!("unknown subcommand {other:?}");
            usage();
            2
        }
    };
    if exit != 0 {
        std::process::exit(exit);
    }
}

fn usage() {
    eprintln!(
        "arrowlane {version}

usage:
  arrowlane select --rows N --bytes N [--topology T] [--policy P] [--json]
  arrowlane describe [--compact]
  arrowlane pack --lane L [--rows N] [--out FILE]
  arrowlane unpack FILE [--json]
  arrowlane bench --rows N --repeats R
  arrowlane --version

topologies: same_host, same_network, wide_area
policies:   latency, throughput, cpu
lanes:      json, ipc, ipc-zstd",
        version = arrowlane::VERSION
    );
}

/// Build a sample batch that matches `interop/check_wire.py` exactly.
///
/// The schema is all nullable and the values follow the same rules as the Python
/// fixture, so a schema or value difference means a wire bug rather than a
/// difference between two hand written samples.
fn sample_batch(rows: usize) -> arrowlane::Result<arrow_array::RecordBatch> {
    use std::sync::Arc;

    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
        StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("ok", DataType::Boolean, true),
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
    // 2026-01-01 as days since the Unix epoch, matching `datetime.date(2026, 1, 1)`.
    let base_day = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .expect("valid date")
        .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
        .num_days() as i32;
    // 2026-10-28T12:00:00Z in microseconds, matching `datetime(2026, 10, 28, 12, tzinfo=UTC)`.
    let ts_micros = 1_793_188_800_000_000i64;
    let columns: Vec<arrow_array::ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..rows_i)),
        Arc::new(StringArray::from_iter(
            (0..rows_i).map(|index| (index % 7 != 0).then(|| format!("row-{index}"))),
        )),
        Arc::new(Float64Array::from_iter_values(
            (0..rows_i).map(|index| index as f64 / 3.0),
        )),
        Arc::new(BooleanArray::from_iter(
            (0..rows_i).map(|index| Some(index % 2 == 0)),
        )),
        Arc::new(Date32Array::from_iter_values(
            (0..rows_i).map(|index| base_day + (index % 28) as i32),
        )),
        Arc::new(
            Decimal128Array::from_iter_values((0..rows_i).map(|_| 1_234i128))
                .with_precision_and_scale(9, 2)
                .map_err(|err| arrowlane::ArrowLaneError::Config(err.to_string()))?,
        ),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values((0..rows_i).map(|_| ts_micros))
                .with_timezone("UTC"),
        ),
        Arc::new(BinaryArray::from_iter_values(
            (0..rows_i).map(|_| &b"\x00\x01\x02"[..]),
        )),
    ];
    arrow_array::RecordBatch::try_new(schema, columns)
        .map_err(|err| arrowlane::ArrowLaneError::Config(err.to_string()))
}

fn cmd_pack(args: &[String]) -> i32 {
    let lane = flag_value(args, "--lane").unwrap_or_else(|| "ipc".to_string());
    let rows: usize = flag_value(args, "--rows")
        .and_then(|value| value.parse().ok())
        .unwrap_or(500);
    let out = flag_value(args, "--out");
    let level = flag_value(args, "--level").and_then(|value| value.parse().ok());
    let batch = match sample_batch(rows) {
        Ok(batch) => batch,
        Err(err) => {
            eprintln!("{err}");
            return 1;
        }
    };
    let wire = match arrowlane::pack(
        &batch,
        &lane,
        None,
        None,
        level,
        arrowlane::MAX_PAYLOAD_BYTES,
    ) {
        Ok(wire) => wire,
        Err(err) => {
            eprintln!("{err}");
            return 1;
        }
    };
    match out {
        Some(path) => {
            if let Err(err) = std::fs::write(&path, &wire) {
                eprintln!("{err}");
                return 1;
            }
            eprintln!("wrote {} bytes to {path}", wire.len());
        }
        None => {
            use std::io::Write;
            if let Err(err) = std::io::stdout().write_all(&wire) {
                eprintln!("{err}");
                return 1;
            }
        }
    }
    0
}

fn cmd_unpack(args: &[String]) -> i32 {
    let Some(path) = args.first().filter(|arg| !arg.starts_with('-')) else {
        eprintln!("unpack needs a FILE");
        return 2;
    };
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(err) => {
            eprintln!("{err}");
            return 1;
        }
    };
    let frame = match arrowlane::decode_frame(
        &data,
        arrowlane::MAX_PAYLOAD_BYTES,
        arrowlane::MAX_UNCOMPRESSED_BYTES,
    ) {
        Ok(frame) => frame,
        Err(err) => {
            eprintln!("{err}");
            return 1;
        }
    };
    let lane = frame.lane();
    let uncompressed = frame.uncompressed_len;
    let batch = match arrowlane::unpack_frame(frame, None, None, arrowlane::MAX_UNCOMPRESSED_BYTES)
    {
        Ok(batch) => batch,
        Err(err) => {
            eprintln!("{err}");
            return 1;
        }
    };
    if has_flag(args, "--json") {
        let schema: Vec<serde_json::Value> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                serde_json::json!({
                    "name": field.name(),
                    "type": format!("{:?}", field.data_type()),
                    "nullable": field.is_nullable(),
                })
            })
            .collect();
        let payload = serde_json::json!({
            "lane": lane,
            "rows": batch.num_rows(),
            "columns": batch.num_columns(),
            "wire_bytes": data.len(),
            "uncompressed_bytes": uncompressed,
            "schema": schema,
        });
        match serde_json::to_string_pretty(&payload) {
            Ok(text) => println!("{text}"),
            Err(err) => {
                eprintln!("{err}");
                return 1;
            }
        }
    } else {
        println!(
            "lane={} rows={} columns={} wire_bytes={}",
            lane.unwrap_or("?"),
            batch.num_rows(),
            batch.num_columns(),
            data.len()
        );
    }
    0
}

fn cmd_bench(args: &[String]) -> i32 {
    let rows: usize = flag_value(args, "--rows")
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000);
    let repeats: usize = flag_value(args, "--repeats")
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let batch = match sample_batch(rows) {
        Ok(batch) => batch,
        Err(err) => {
            eprintln!("{err}");
            return 1;
        }
    };
    println!("rows={rows} repeats={repeats}");
    println!(
        "{:<10} {:>12} {:>12} {:>12}",
        "lane", "wire_bytes", "enc_ms", "dec_ms"
    );
    for lane in ["json", "ipc", "ipc-zstd"] {
        let mut enc_samples: Vec<f64> = Vec::with_capacity(repeats);
        let mut dec_samples: Vec<f64> = Vec::with_capacity(repeats);
        let mut wire_len = 0usize;
        for _ in 0..repeats {
            let start = std::time::Instant::now();
            let wire =
                match arrowlane::pack(&batch, lane, None, None, None, arrowlane::MAX_PAYLOAD_BYTES)
                {
                    Ok(wire) => wire,
                    Err(err) => {
                        eprintln!("{err}");
                        return 1;
                    }
                };
            enc_samples.push(start.elapsed().as_secs_f64() * 1e3);
            wire_len = wire.len();
            let start = std::time::Instant::now();
            if let Err(err) = arrowlane::unpack(
                &wire,
                None,
                None,
                arrowlane::MAX_PAYLOAD_BYTES,
                arrowlane::MAX_UNCOMPRESSED_BYTES,
            ) {
                eprintln!("{err}");
                return 1;
            }
            dec_samples.push(start.elapsed().as_secs_f64() * 1e3);
        }
        println!(
            "{:<10} {:>12} {:>12.3} {:>12.3}",
            lane,
            wire_len,
            median(&mut enc_samples),
            median(&mut dec_samples)
        );
    }
    0
}

fn median(samples: &mut [f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    samples[samples.len() / 2]
}

fn flag_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1))
        .cloned()
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

fn cmd_select(args: &[String]) -> i32 {
    let rows: u64 = match flag_value(args, "--rows").and_then(|v| v.parse().ok()) {
        Some(rows) => rows,
        None => {
            eprintln!("select needs --rows N");
            return 2;
        }
    };
    let bytes: u64 = match flag_value(args, "--bytes").and_then(|v| v.parse().ok()) {
        Some(bytes) => bytes,
        None => {
            eprintln!("select needs --bytes N");
            return 2;
        }
    };
    let topology = flag_value(args, "--topology").unwrap_or_else(|| "same_network".to_string());
    let topology: Topology = match topology.parse() {
        Ok(topology) => topology,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let policy = flag_value(args, "--policy").unwrap_or_else(|| "latency".to_string());
    let policy: Policy = match policy.parse() {
        Ok(policy) => policy,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let workload = match Workload::new(rows, bytes) {
        Ok(workload) => workload,
        Err(err) => {
            eprintln!("{err}");
            return 2;
        }
    };
    let mut selection = Selection::new(workload, topology);
    selection.policy = policy;
    selection.shm_ready = flag_value(args, "--shm-ready").is_some_and(|v| v == "true");
    let decision = match select_plan(load_default(), &selection) {
        Ok(decision) => decision,
        Err(err) => {
            eprintln!("{err}");
            return 1;
        }
    };
    if has_flag(args, "--json") {
        match serde_json::to_string_pretty(&serde_json::json!({
            "lane": decision.lane,
            "reason": decision.reason,
            "predicted_ms": (decision.predicted_ms * 10_000.0).round() / 10_000.0,
            "predicted_bytes": decision.predicted_bytes,
            "scores": decision.rounded_scores(),
            "rejected": decision.rejected,
        })) {
            Ok(text) => println!("{text}"),
            Err(err) => {
                eprintln!("{err}");
                return 1;
            }
        }
    } else {
        println!("lane: {}", decision.lane);
        println!("reason: {}", decision.reason);
        println!("predicted_ms: {:.4}", decision.predicted_ms);
        println!("predicted_bytes: {}", decision.predicted_bytes);
        for (name, score) in decision.rounded_scores() {
            println!("  {name}: {score:.4}");
        }
        for (name, why) in &decision.rejected {
            println!("  rejected {name}: {why}");
        }
    }
    0
}

fn cmd_describe(args: &[String]) -> i32 {
    let model = load_default();
    let mut lanes: Vec<serde_json::Value> = Vec::new();
    for (name, info) in arrowlane::lanes::lane_infos() {
        lanes.push(serde_json::json!({
            "name": name,
            "id": info.id,
            "media_type": info.media_type,
            "available": arrowlane::available_names().contains(&name),
        }));
    }
    let payload = serde_json::json!({
        "kind": "arrowlane-rust",
        "version": arrowlane::VERSION,
        "frame": {
            "media_type": arrowlane::FRAME_MEDIA_TYPE,
            "header_size": arrowlane::HEADER_SIZE,
            "max_payload": arrowlane::MAX_PAYLOAD_BYTES,
            "max_uncompressed": arrowlane::MAX_UNCOMPRESSED_BYTES,
        },
        "streaming": {
            "chunked": true,
            "default_chunk_rows": arrowlane::DEFAULT_CHUNK_ROWS,
            "max_chunk_rows": arrowlane::MAX_CHUNK_ROWS,
            "max_chunks": arrowlane::MAX_CHUNKS,
            "backpressure": "lazy iterator, one chunk in flight",
        },
        "lanes": lanes,
        "topologies": Topology::all().iter().map(|t| t.as_str()).collect::<Vec<_>>(),
        "policies": Policy::all().iter().map(|p| p.as_str()).collect::<Vec<_>>(),
        "costmodel_schema_version": model.schema_version(),
        "shm_ready": arrowlane::shm::is_ready(None),
        "docs": {
            "readme": "README.md",
            "agent": "llms.txt",
        },
    });
    let compact = has_flag(args, "--compact") || has_flag(args, "--json");
    let encoded = if compact {
        serde_json::to_string(&payload)
    } else {
        serde_json::to_string_pretty(&payload)
    };
    match encoded {
        Ok(text) => {
            println!("{text}");
            0
        }
        Err(err) => {
            eprintln!("{err}");
            1
        }
    }
}
