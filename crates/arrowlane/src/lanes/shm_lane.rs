//! Shared memory lane: Arrow IPC in a POSIX segment, descriptor on the wire.

use std::path::PathBuf;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_schema::Schema;
use serde_json::Value;

use crate::error::{ArrowLaneError, FrameError, Result};
use crate::frame::Frame;
use crate::shm::{self, Descriptor};

use super::ipc_lane::{decode_ipc_buffer, encode_ipc};
use super::{Lane, LaneInfo, lane_header};

/// Static description.
pub const INFO: LaneInfo = LaneInfo {
    id: 4,
    name: "shm",
    media_type: "application/vnd.arrowlane.shm",
    posix_only: true,
};

/// Write the table to a segment and put only the descriptor on the wire.
#[derive(Debug, Clone, Default)]
pub struct ShmLane {
    directory: Option<PathBuf>,
    token: Option<String>,
    verify: bool,
}

impl ShmLane {
    /// Build a lane for one call.
    pub fn new(directory: Option<PathBuf>, token: Option<String>) -> Self {
        Self {
            directory,
            token,
            verify: true,
        }
    }

    /// Disable the CRC check on read. Only for a trusted local caller.
    pub fn without_verify(mut self) -> Self {
        self.verify = false;
        self
    }

    /// True when this lane can write a segment here.
    pub fn ready(&self) -> bool {
        shm::is_ready(self.directory.as_deref())
    }

    /// Remove a segment that was written but never sent.
    pub fn release(&self, frame: &Frame) -> Result<()> {
        let descriptor = Descriptor::from_bytes(frame.payload.as_ref())?;
        shm::discard(&descriptor, self.directory.as_deref());
        Ok(())
    }
}

/// A batch whose buffers point into a live mapping.
///
/// The mapping must outlive the batch, so it is returned alongside the batch and
/// dropped afterwards.
pub struct MappedBatch {
    /// The decoded batch. Its buffers borrow the mapping.
    pub batch: RecordBatch,
    /// The mapping the batch points into.
    pub segment: shm::Segment,
}

impl Lane for ShmLane {
    fn info(&self) -> LaneInfo {
        INFO
    }

    fn encode(&self, batch: &RecordBatch, _level: Option<i32>) -> Result<Frame> {
        if !cfg!(unix) {
            return Err(ArrowLaneError::LaneUnavailable(
                "the shm lane requires a POSIX platform".to_string(),
            ));
        }
        let token = self.token.as_deref().ok_or_else(|| {
            ArrowLaneError::LaneUnavailable("the shm lane requires a shared token".to_string())
        })?;
        let raw = encode_ipc(batch)?;
        let uncompressed_len = raw.len() as u64;
        let descriptor = shm::write_segment(&raw, self.directory.as_deref(), Some(token))?;
        let header = lane_header(
            batch.schema().as_ref(),
            INFO.media_type,
            [(
                "segment".to_string(),
                Value::String(descriptor.name.clone()),
            )],
        );
        Ok(Frame {
            lane_id: INFO.id,
            payload: Buffer::from(descriptor.to_bytes()),
            rows: batch.num_rows() as u64,
            uncompressed_len,
            header,
            flags: 0,
            chunk: None,
        })
    }

    fn decode(&self, frame: &Frame, max_uncompressed: Option<usize>) -> Result<RecordBatch> {
        let token = self.token.as_deref().ok_or_else(|| {
            ArrowLaneError::LaneUnavailable("the shm lane requires a shared token".to_string())
        })?;
        let descriptor = Descriptor::from_bytes(frame.payload.as_ref())?;
        if descriptor.length != frame.uncompressed_len {
            return Err(ArrowLaneError::Segment(
                "descriptor length does not match the frame".to_string(),
            ));
        }
        if let Some(cap) = max_uncompressed
            && descriptor.length > cap as u64
        {
            return Err(FrameError::oversize("segment exceeds the uncompressed cap").into());
        }
        let segment =
            shm::read_segment(&descriptor, self.directory.as_deref(), token, self.verify)?;
        // SAFETY: the mapping is held in `segment`, which outlives the buffer
        // built here, and the CRC over the same range was just verified.
        let bytes = Buffer::from_slice_ref(
            &segment.mapping[segment.offset..segment.offset + segment.length],
        );
        let batch = decode_ipc_buffer(&bytes)?;
        drop(segment);
        Ok(batch)
    }
}

/// Decode a `shm` frame and keep the mapping alive next to the batch.
///
/// Use this when a table must outlive one call and `decode` cannot guarantee the
/// mapping lifetime. It is slower than `decode` because it maps the segment
/// twice, so prefer `decode` inside a request.
pub fn decode_mapped(
    lane: &ShmLane,
    frame: &Frame,
    max_uncompressed: Option<usize>,
) -> Result<MappedBatch> {
    let token = lane.token.as_deref().ok_or_else(|| {
        ArrowLaneError::LaneUnavailable("the shm lane requires a shared token".to_string())
    })?;
    let descriptor = Descriptor::from_bytes(frame.payload.as_ref())?;
    if let Some(cap) = max_uncompressed
        && descriptor.length > cap as u64
    {
        return Err(FrameError::oversize("segment exceeds the uncompressed cap").into());
    }
    let segment = shm::read_segment(&descriptor, lane.directory.as_deref(), token, lane.verify)?;
    let bytes =
        Buffer::from_slice_ref(&segment.mapping[segment.offset..segment.offset + segment.length]);
    let batch = decode_ipc_buffer(&bytes)?;
    Ok(MappedBatch { batch, segment })
}

/// Build a header carrying the schema for a lane that needs one.
pub fn shm_header(schema: &Schema) -> crate::frame::Header {
    lane_header(schema, INFO.media_type, [])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::DataType;

    fn batch() -> RecordBatch {
        let schema = std::sync::Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "a",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("valid batch")
    }

    #[test]
    fn encode_requires_a_token() {
        let lane = ShmLane::new(None, None);
        assert!(lane.encode(&batch(), None).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn roundtrip_through_a_segment() {
        let dir = tempfile::tempdir().expect("temp dir");
        let lane = ShmLane::new(
            Some(dir.path().to_path_buf()),
            Some("token-0123456789".to_string()),
        );
        if !lane.ready() {
            return;
        }
        let frame = lane.encode(&batch(), None).unwrap();
        assert_eq!(frame.lane_id, INFO.id);
        let out = lane.decode(&frame, None).unwrap();
        assert_eq!(out, batch());
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn wrong_token_is_rejected() {
        let dir = tempfile::tempdir().expect("temp dir");
        let lane = ShmLane::new(
            Some(dir.path().to_path_buf()),
            Some("token-0123456789".to_string()),
        );
        if !lane.ready() {
            return;
        }
        let frame = lane.encode(&batch(), None).unwrap();
        let other = ShmLane::new(
            Some(dir.path().to_path_buf()),
            Some("token-9876543210".to_string()),
        );
        assert!(other.decode(&frame, None).is_err());
    }
}
