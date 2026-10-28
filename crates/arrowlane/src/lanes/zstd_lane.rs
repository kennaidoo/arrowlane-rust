//! Arrow IPC compressed with zstd.
//!
//! Decompression is bounded: the decoder stops at `cap + 1` bytes, so a frame
//! that lies about its uncompressed length cannot allocate without limit.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use serde_json::Value;

use crate::error::{FrameError, Result};
use crate::frame::{Frame, MAX_UNCOMPRESSED_BYTES};

use super::ipc_lane::{check_header_schema, decode_ipc, encode_ipc};
use super::{Lane, LaneInfo, checked_payload, lane_header};

/// Static description.
pub const INFO: LaneInfo = LaneInfo {
    id: 3,
    name: "ipc-zstd",
    media_type: "application/vnd.apache.arrow.stream",
    posix_only: false,
};

/// Default compression level.
pub const DEFAULT_LEVEL: i32 = 3;
/// Lowest accepted level.
pub const MIN_LEVEL: i32 = 1;
/// Highest accepted level.
pub const MAX_LEVEL: i32 = 19;

/// Decompress at most `cap` bytes. Raise an oversize error if more arrive.
///
/// The bounded read is the whole point: a hostile frame that declares a small
/// uncompressed length cannot make this allocate more than the cap.
pub fn bounded_decompress(payload: &[u8], cap: usize) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::read::Decoder::new(payload)
        .map_err(|err| FrameError::payload(format!("payload is not a valid zstd frame: {err}")))?;
    let mut out: Vec<u8> = Vec::with_capacity(cap.min(payload.len().saturating_mul(4)));
    let limit = (cap as u64).saturating_add(1);
    let mut bounded = std::io::Read::take(&mut decoder, limit);
    std::io::Read::read_to_end(&mut bounded, &mut out)
        .map_err(|err| FrameError::payload(format!("payload is not a valid zstd frame: {err}")))?;
    if out.len() > cap {
        return Err(FrameError::oversize(format!("decompressed data exceeds {cap} bytes")).into());
    }
    Ok(out)
}

/// Arrow IPC inside one zstd frame.
#[derive(Debug, Default, Clone, Copy)]
pub struct ZstdLane;

impl Lane for ZstdLane {
    fn info(&self) -> LaneInfo {
        INFO
    }

    fn encode(&self, batch: &RecordBatch, level: Option<i32>) -> Result<Frame> {
        let chosen = level.unwrap_or(DEFAULT_LEVEL);
        if !(MIN_LEVEL..=MAX_LEVEL).contains(&chosen) {
            return Err(crate::error::ArrowLaneError::Config(format!(
                "zstd level must be between {MIN_LEVEL} and {MAX_LEVEL}"
            )));
        }
        let raw = encode_ipc(batch)?;
        let uncompressed_len = raw.len() as u64;
        let payload = zstd::stream::encode_all(raw.as_slice(), chosen)
            .map_err(|err| FrameError::payload(format!("payload cannot be compressed: {err}")))?;
        let header = lane_header(
            batch.schema().as_ref(),
            INFO.media_type,
            [
                ("codec".to_string(), Value::String("zstd".to_string())),
                ("zstd_level".to_string(), Value::from(chosen)),
            ],
        );
        Ok(Frame {
            lane_id: INFO.id,
            payload: Buffer::from(payload),
            rows: batch.num_rows() as u64,
            uncompressed_len,
            header,
            flags: 0,
            chunk: None,
        })
    }

    fn decode(&self, frame: &Frame, max_uncompressed: Option<usize>) -> Result<RecordBatch> {
        let cap = max_uncompressed.unwrap_or(MAX_UNCOMPRESSED_BYTES);
        if frame.uncompressed_len > cap as u64 {
            return Err(FrameError::oversize(format!(
                "frame declares {} bytes, cap is {cap}",
                frame.uncompressed_len
            ))
            .into());
        }
        checked_payload(frame, cap)?;
        let raw = bounded_decompress(frame.payload.as_ref(), cap)?;
        if raw.len() as u64 != frame.uncompressed_len {
            return Err(FrameError::payload(format!(
                "length mismatch: declared {}, decompressed {}",
                frame.uncompressed_len,
                raw.len()
            ))
            .into());
        }
        let batch = decode_ipc(&raw)?;
        check_header_schema(frame, &batch)?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..5000)),
                Arc::new(StringArray::from_iter_values(
                    (0..5000).map(|_| "x".repeat(20)),
                )),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn roundtrip_compresses_and_restores() {
        let lane = ZstdLane;
        let plain = encode_ipc(&batch()).unwrap();
        let frame = lane.encode(&batch(), Some(3)).unwrap();
        assert!(frame.payload.len() < plain.len());
        assert_eq!(frame.uncompressed_len, plain.len() as u64);
        assert_eq!(lane.decode(&frame, None).unwrap(), batch());
    }

    #[test]
    fn header_records_the_codec_and_level() {
        let frame = ZstdLane.encode(&batch(), None).unwrap();
        assert_eq!(
            frame.header.get("codec").and_then(Value::as_str),
            Some("zstd")
        );
        assert_eq!(
            frame.header.get("zstd_level").and_then(Value::as_i64),
            Some(i64::from(DEFAULT_LEVEL))
        );
    }

    #[test]
    fn declared_size_over_cap_is_rejected() {
        let frame = ZstdLane.encode(&batch(), None).unwrap();
        assert!(ZstdLane.decode(&frame, Some(100)).is_err());
    }

    #[test]
    fn actual_size_over_cap_is_rejected() {
        let payload = zstd::stream::encode_all(&vec![b'x'; 1_000_000][..], 3).unwrap();
        let err = bounded_decompress(&payload, 1000).unwrap_err();
        assert!(matches!(
            err,
            crate::error::ArrowLaneError::Frame(FrameError::Oversize(_))
        ));
    }

    #[test]
    fn level_out_of_range_is_rejected() {
        assert!(ZstdLane.encode(&batch(), Some(99)).is_err());
    }

    #[test]
    fn length_mismatch_is_rejected() {
        let frame = ZstdLane.encode(&batch(), None).unwrap();
        let broken = Frame {
            uncompressed_len: frame.uncompressed_len + 1,
            ..frame
        };
        assert!(ZstdLane.decode(&broken, None).is_err());
    }
}
