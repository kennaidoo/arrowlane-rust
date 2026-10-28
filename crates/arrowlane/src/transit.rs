//! End to end transit: plan a lane, pack a batch, unpack it again.

use std::path::Path;

use arrow_array::RecordBatch;

use crate::costmodel::{CostModel, load_default};
use crate::error::Result;
use crate::frame::{
    Frame, MAX_PAYLOAD_BYTES, MAX_UNCOMPRESSED_BYTES, decode_frame, decode_frame_view, encode_frame,
};
use crate::lanes::lane_for;
use crate::select::{Decision, Policy, Selection, Topology, Workload, select_plan};

/// Return the lane for a name.
pub fn lane(
    name: &str,
    token: Option<&str>,
    directory: Option<&Path>,
) -> Result<Box<dyn crate::lanes::Lane>> {
    lane_for(name, token, directory)
}

/// Choose a lane for this batch, using the defaults in [`TransitOptions`].
pub fn plan(batch: &RecordBatch, topology: Topology, policy: Policy) -> Result<Decision> {
    plan_with(
        batch,
        &TransitOptions {
            topology,
            policy,
            ..Default::default()
        },
    )
}

/// Choose a lane for this batch with full control over the selection.
pub fn plan_with(batch: &RecordBatch, options: &TransitOptions<'_>) -> Result<Decision> {
    let workload = Workload::new(batch.num_rows() as u64, arrow_bytes(batch))?;
    let mut selection = Selection::new(workload, options.topology);
    selection.policy = options.policy;
    selection.allowed = options.allowed.clone();
    selection.shm_ready = options.token.is_some() || crate::shm::is_ready(options.directory);
    selection.min_gain = options.min_gain;
    let model = match options.model {
        Some(model) => model.clone(),
        None => load_default().clone(),
    };
    select_plan(&model, &selection)
}

/// Logical size of the batch buffers in bytes.
pub fn arrow_bytes(batch: &RecordBatch) -> u64 {
    batch.get_array_memory_size() as u64
}

/// Encode the batch on one lane and return the wire frame.
pub fn pack(
    batch: &RecordBatch,
    lane_name: &str,
    token: Option<&str>,
    directory: Option<&Path>,
    level: Option<i32>,
    max_payload: usize,
) -> Result<Vec<u8>> {
    let chosen = lane(lane_name, token, directory)?;
    let frame = chosen.encode(batch, level)?;
    encode_frame(&frame, max_payload)
}

/// Encode the batch on one lane and return the frame, without serialising it.
pub fn pack_frame(
    batch: &RecordBatch,
    lane_name: &str,
    token: Option<&str>,
    directory: Option<&Path>,
    level: Option<i32>,
) -> Result<Frame> {
    lane(lane_name, token, directory)?.encode(batch, level)
}

/// Validate a wire frame and decode it on the lane it names.
pub fn unpack(
    data: &[u8],
    token: Option<&str>,
    directory: Option<&Path>,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<RecordBatch> {
    let frame = decode_frame(data, max_payload, max_uncompressed)?;
    let chosen = lane(frame.try_lane()?, token, directory)?;
    chosen.decode(&frame, Some(max_uncompressed))
}

/// Validate and decode a frame that is already in memory, without copying it.
pub fn unpack_view(
    data: &arrow_buffer::Buffer,
    token: Option<&str>,
    directory: Option<&Path>,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<RecordBatch> {
    let frame = decode_frame_view(data, max_payload, max_uncompressed)?;
    let chosen = lane(frame.try_lane()?, token, directory)?;
    chosen.decode(&frame, Some(max_uncompressed))
}

/// Options for [`transit`].
#[derive(Debug, Clone)]
pub struct TransitOptions<'a> {
    /// Where the endpoints sit.
    pub topology: Topology,
    /// What the caller wants optimised.
    pub policy: Policy,
    /// Cost model override.
    pub model: Option<&'a CostModel>,
    /// Lane allowlist.
    pub allowed: Option<Vec<String>>,
    /// Shared memory token.
    pub token: Option<&'a str>,
    /// Shared memory directory.
    pub directory: Option<&'a Path>,
    /// Compression level for the zstd lane.
    pub level: Option<i32>,
    /// Minimum fractional gain before a lane other than `json` is chosen.
    pub min_gain: f64,
}

impl Default for TransitOptions<'_> {
    fn default() -> Self {
        Self {
            topology: Topology::SameNetwork,
            policy: Policy::Latency,
            model: None,
            allowed: None,
            token: None,
            directory: None,
            level: None,
            min_gain: 0.10,
        }
    }
}

/// Plan a lane, encode the batch, and return the bytes with the decision.
pub fn transit(batch: &RecordBatch, options: &TransitOptions<'_>) -> Result<(Vec<u8>, Decision)> {
    let decision = plan_with(batch, options)?;
    let wire = pack(
        batch,
        &decision.lane,
        options.token,
        options.directory,
        options.level,
        MAX_PAYLOAD_BYTES,
    )?;
    Ok((wire, decision))
}

/// Default limits, for callers that do not want to name them.
pub const DEFAULT_MAX_PAYLOAD: usize = MAX_PAYLOAD_BYTES;
/// Default uncompressed limit.
pub const DEFAULT_MAX_UNCOMPRESSED: usize = MAX_UNCOMPRESSED_BYTES;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|index| format!("name-{index}")),
                )),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn transit_roundtrips_on_every_portable_lane() {
        let source = batch(1000);
        for lane_name in ["json", "ipc", "ipc-zstd"] {
            let wire = pack(&source, lane_name, None, None, None, DEFAULT_MAX_PAYLOAD).unwrap();
            let out = unpack(
                &wire,
                None,
                None,
                DEFAULT_MAX_PAYLOAD,
                DEFAULT_MAX_UNCOMPRESSED,
            )
            .unwrap();
            assert_eq!(out, source, "lane {lane_name}");
        }
    }

    #[test]
    fn transit_picks_a_fast_lane_for_a_large_batch() {
        let source = batch(200_000);
        let options = TransitOptions::default();
        let (wire, decision) = transit(&source, &options).unwrap();
        assert_ne!(decision.lane, "json");
        let out = unpack(
            &wire,
            None,
            None,
            DEFAULT_MAX_PAYLOAD,
            DEFAULT_MAX_UNCOMPRESSED,
        )
        .unwrap();
        assert_eq!(out, source);
    }

    #[test]
    fn view_decode_matches_owned_decode() {
        let source = batch(100);
        let wire = pack(&source, "ipc", None, None, None, DEFAULT_MAX_PAYLOAD).unwrap();
        let buffer = arrow_buffer::Buffer::from(wire);
        let out = unpack_view(
            &buffer,
            None,
            None,
            DEFAULT_MAX_PAYLOAD,
            DEFAULT_MAX_UNCOMPRESSED,
        )
        .unwrap();
        assert_eq!(out, source);
    }
}
