//! The wire frame: a 38 byte prefix, a JSON header, and an opaque payload.
//!
//! The prefix is fixed width so a reader can validate lengths and the checksum
//! before it trusts, allocates for, or parses the payload.
//!
//! The layout is byte compatible with the Python package:
//!
//! | Offset | Size | Field |
//! |---|---|---|
//! | 0 | 4 | magic `ALN1` |
//! | 4 | 1 | format version, 1 |
//! | 5 | 1 | lane id |
//! | 6 | 2 | flags, little endian |
//! | 8 | 8 | payload length |
//! | 16 | 8 | uncompressed length |
//! | 24 | 8 | row count |
//! | 32 | 4 | CRC32 over the payload |
//! | 36 | 2 | header length |
//! | 38 | n | header, UTF-8 JSON |
//! | 38+n | p | payload |

use std::collections::BTreeMap;
use std::io::{Read, Write};

use arrow_buffer::Buffer;
use arrow_schema::Schema;
use serde::Deserialize;
use serde_json::Value;

use crate::error::{FrameError, Result};

/// Frame magic.
pub const MAGIC: [u8; 4] = *b"ALN1";
/// Frame format version.
pub const VERSION: u8 = 1;
/// Media type for a whole frame.
pub const FRAME_MEDIA_TYPE: &str = "application/vnd.arrowlane.frame";
/// Header cap in bytes.
pub const MAX_HEADER_BYTES: usize = 65_535;
/// Payload cap in bytes.
pub const MAX_PAYLOAD_BYTES: usize = 536_870_912;
/// Uncompressed payload cap in bytes.
pub const MAX_UNCOMPRESSED_BYTES: usize = 1_073_741_824;
/// Chunk cap per stream.
pub const MAX_CHUNKS: usize = 1_000_000;
/// Size of the fixed prefix.
pub const HEADER_SIZE: usize = 38;

/// Lane id for a lane name.
pub const fn lane_id(name: &str) -> Option<u8> {
    match name.as_bytes() {
        b"json" => Some(1),
        b"ipc" => Some(2),
        b"ipc-zstd" => Some(3),
        b"shm" => Some(4),
        _ => None,
    }
}

/// Lane name for a lane id.
pub const fn lane_name(id: u8) -> Option<&'static str> {
    match id {
        1 => Some("json"),
        2 => Some("ipc"),
        3 => Some("ipc-zstd"),
        4 => Some("shm"),
        _ => None,
    }
}

/// Lane name to id, both directions.
pub static LANE_IDS: [(&str, u8); 4] = [("json", 1), ("ipc", 2), ("ipc-zstd", 3), ("shm", 4)];
/// Lane id to name, both directions.
pub static ID_LANES: [(u8, &str); 4] = [(1, "json"), (2, "ipc"), (3, "ipc-zstd"), (4, "shm")];

/// Frame header. A `BTreeMap` keeps the JSON keys sorted, so the bytes are
/// deterministic for a given header.
pub type Header = BTreeMap<String, Value>;

/// Where one frame sits in a chunked stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkInfo {
    /// Stream identity, 32 lowercase hex characters.
    pub id: String,
    /// Zero based position in the stream.
    pub index: u64,
    /// True for the final chunk.
    pub last: bool,
}

impl ChunkInfo {
    /// Build chunk metadata, rejecting an id that the wire format forbids.
    pub fn new(id: impl Into<String>, index: u64, last: bool) -> Result<Self> {
        let info = Self {
            id: id.into(),
            index,
            last,
        };
        info.validate()?;
        Ok(info)
    }

    /// Reject a malformed chunk id or index.
    pub fn validate(&self) -> Result<()> {
        if self.id.len() != 32
            || !self
                .id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(crate::error::ArrowLaneError::Chunk(
                "chunk id must be 32 lowercase hex characters".to_string(),
            ));
        }
        if self.index >= MAX_CHUNKS as u64 {
            return Err(crate::error::ArrowLaneError::Chunk(format!(
                "chunk index must be 0..{}",
                MAX_CHUNKS - 1
            )));
        }
        Ok(())
    }

    /// Serialise the chunk position for the frame header.
    pub fn to_value(&self) -> Value {
        serde_json::json!({"id": self.id, "index": self.index, "last": self.last})
    }

    /// Parse untrusted chunk metadata from a frame header.
    pub fn from_value(value: &Value) -> Result<Self> {
        #[derive(Deserialize)]
        struct Raw {
            id: String,
            index: u64,
            last: bool,
        }
        if !value.is_object() {
            return Err(crate::error::ArrowLaneError::Chunk(
                "chunk header is not an object".to_string(),
            ));
        }
        let raw: Raw = serde_json::from_value(value.clone()).map_err(|_| {
            crate::error::ArrowLaneError::Chunk(
                "chunk header has the wrong field types".to_string(),
            )
        })?;
        Self::new(raw.id, raw.index, raw.last)
    }
}

/// One encoded payload with the metadata needed to decode it.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Lane id for the payload encoding.
    pub lane_id: u8,
    /// Encoded bytes.
    pub payload: Buffer,
    /// Row count.
    pub rows: u64,
    /// Length of the payload before compression.
    pub uncompressed_len: u64,
    /// Frame header, including the Arrow schema.
    pub header: Header,
    /// Reserved flag bits.
    pub flags: u16,
    /// Chunk position, when this frame is part of a stream.
    pub chunk: Option<ChunkInfo>,
}

impl Frame {
    /// Lane name for this frame, or `None` when the id is unregistered.
    pub fn lane(&self) -> Option<&'static str> {
        lane_name(self.lane_id)
    }

    /// Lane name for this frame, as an error when the id is unregistered.
    pub fn try_lane(&self) -> Result<&'static str> {
        lane_name(self.lane_id).ok_or(FrameError::UnknownLane(self.lane_id).into())
    }
}

/// Build a header that carries the Arrow schema and a producer mark.
///
/// The schema is encoded the way `pyarrow.Schema.serialize` encodes it, which is
/// an encapsulated IPC message rather than a bare flatbuffer, so either side can
/// read the other's header.
pub fn header_for(schema: &Schema, extra: impl IntoIterator<Item = (String, Value)>) -> Header {
    let mut header = Header::new();
    header.insert(
        "schema".to_string(),
        Value::String(crate::schema_codec::encode_schema_b64(schema)),
    );
    header.insert("ncols".to_string(), Value::from(schema.fields().len()));
    header.insert(
        "producer".to_string(),
        Value::String(format!("arrowlane/{}", env!("CARGO_PKG_VERSION"))),
    );
    for (key, value) in extra {
        header.insert(key, value);
    }
    header
}

/// Read the Arrow schema out of a frame header.
pub fn schema_of(header: &Header) -> Result<Schema> {
    let raw = header
        .get("schema")
        .and_then(Value::as_str)
        .ok_or_else(|| FrameError::header("header carries no schema"))?;
    crate::schema_codec::decode_schema_b64(raw)
}

/// Header bytes, including chunk metadata when the frame carries it.
fn header_bytes(frame: &Frame) -> Result<Vec<u8>> {
    let raw = match &frame.chunk {
        Some(chunk) => {
            let mut header = frame.header.clone();
            header.insert("chunk".to_string(), chunk.to_value());
            serde_json::to_vec(&header)
        }
        None => serde_json::to_vec(&frame.header),
    }
    .map_err(|err| FrameError::header(format!("header cannot be serialised: {err}")))?;
    if raw.len() > MAX_HEADER_BYTES {
        return Err(FrameError::oversize(format!(
            "header {} exceeds {MAX_HEADER_BYTES}",
            raw.len()
        ))
        .into());
    }
    Ok(raw)
}

/// Fields carried by the fixed prefix, in either direction.
#[derive(Debug, Clone, Copy)]
struct Prefix {
    lane_id: u8,
    flags: u16,
    payload_len: usize,
    uncompressed_len: u64,
    rows: u64,
    crc: u32,
    header_len: usize,
}

fn write_prefix(out: &mut [u8; HEADER_SIZE], fields: &Prefix) {
    out[0..4].copy_from_slice(&MAGIC);
    out[4] = VERSION;
    out[5] = fields.lane_id;
    out[6..8].copy_from_slice(&fields.flags.to_le_bytes());
    out[8..16].copy_from_slice(&(fields.payload_len as u64).to_le_bytes());
    out[16..24].copy_from_slice(&fields.uncompressed_len.to_le_bytes());
    out[24..32].copy_from_slice(&fields.rows.to_le_bytes());
    out[32..36].copy_from_slice(&fields.crc.to_le_bytes());
    out[36..38].copy_from_slice(&(fields.header_len as u16).to_le_bytes());
}

fn read_prefix(prefix: &[u8], max_payload: usize, max_uncompressed: usize) -> Result<Prefix> {
    if prefix.len() < HEADER_SIZE {
        return Err(FrameError::truncated(format!(
            "{} bytes is shorter than {HEADER_SIZE}",
            prefix.len()
        ))
        .into());
    }
    let magic: [u8; 4] = prefix[0..4].try_into().expect("four bytes");
    if magic != MAGIC {
        return Err(FrameError::BadMagic(magic).into());
    }
    let version = prefix[4];
    if version != VERSION {
        return Err(FrameError::UnsupportedVersion(version).into());
    }
    let lane_id = prefix[5];
    if lane_name(lane_id).is_none() {
        return Err(FrameError::UnknownLane(lane_id).into());
    }
    let flags = u16::from_le_bytes(prefix[6..8].try_into().expect("two bytes"));
    let payload_len = u64::from_le_bytes(prefix[8..16].try_into().expect("eight bytes"));
    let uncompressed_len = u64::from_le_bytes(prefix[16..24].try_into().expect("eight bytes"));
    let rows = u64::from_le_bytes(prefix[24..32].try_into().expect("eight bytes"));
    let crc = u32::from_le_bytes(prefix[32..36].try_into().expect("four bytes"));
    let header_len = u16::from_le_bytes(prefix[36..38].try_into().expect("two bytes")) as usize;
    if header_len > MAX_HEADER_BYTES {
        return Err(FrameError::oversize(format!(
            "header {header_len} exceeds {MAX_HEADER_BYTES}"
        ))
        .into());
    }
    if payload_len > max_payload as u64 {
        return Err(
            FrameError::oversize(format!("payload {payload_len} exceeds {max_payload}")).into(),
        );
    }
    if uncompressed_len > max_uncompressed as u64 {
        return Err(FrameError::oversize(format!(
            "uncompressed length {uncompressed_len} exceeds {max_uncompressed}"
        ))
        .into());
    }
    Ok(Prefix {
        lane_id,
        flags,
        payload_len: payload_len as usize,
        uncompressed_len,
        rows,
        crc,
        header_len,
    })
}

/// Serialise a frame into prefix, header and payload without joining them.
pub fn encode_frame_parts(
    frame: &Frame,
    max_payload: usize,
) -> Result<([u8; HEADER_SIZE], Vec<u8>, Buffer)> {
    if max_payload > MAX_PAYLOAD_BYTES {
        return Err(FrameError::oversize(format!(
            "max_payload {max_payload} exceeds {MAX_PAYLOAD_BYTES}"
        ))
        .into());
    }
    if frame.payload.len() > max_payload {
        return Err(FrameError::oversize(format!(
            "payload {} exceeds {max_payload}",
            frame.payload.len()
        ))
        .into());
    }
    let header = header_bytes(frame)?;
    let crc = crc32fast::hash(frame.payload.as_ref());
    let mut prefix = [0u8; HEADER_SIZE];
    write_prefix(
        &mut prefix,
        &Prefix {
            lane_id: frame.lane_id,
            flags: frame.flags,
            payload_len: frame.payload.len(),
            uncompressed_len: frame.uncompressed_len,
            rows: frame.rows,
            crc,
            header_len: header.len(),
        },
    );
    Ok((prefix, header, frame.payload.clone()))
}

/// Serialise a frame. Rejects an oversize payload or header.
pub fn encode_frame(frame: &Frame, max_payload: usize) -> Result<Vec<u8>> {
    let (prefix, header, payload) = encode_frame_parts(frame, max_payload)?;
    let mut out = Vec::with_capacity(HEADER_SIZE + header.len() + payload.len());
    out.extend_from_slice(&prefix);
    out.extend_from_slice(&header);
    out.extend_from_slice(payload.as_ref());
    Ok(out)
}

/// Write one frame in parts, so the payload is never copied into a joined buffer.
pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame, max_payload: usize) -> Result<usize> {
    let (prefix, header, payload) = encode_frame_parts(frame, max_payload)?;
    writer.write_all(&prefix)?;
    writer.write_all(&header)?;
    writer.write_all(payload.as_ref())?;
    Ok(HEADER_SIZE + header.len() + payload.len())
}

fn read_exact_or_eof<R: Read>(reader: &mut R, size: usize) -> Result<Option<Vec<u8>>> {
    if size == 0 {
        return Ok(Some(Vec::new()));
    }
    let mut buffer = vec![0u8; size];
    let mut filled = 0;
    while filled < size {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err.into()),
        }
    }
    if filled == 0 {
        return Ok(None);
    }
    if filled < size {
        buffer.truncate(filled);
        return Err(
            FrameError::truncated(format!("stream ended after {filled} of {size} bytes")).into(),
        );
    }
    Ok(Some(buffer))
}

/// Read one frame from a stream. Returns `None` at a clean end of stream.
pub fn read_frame<R: Read>(
    reader: &mut R,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<Option<Frame>> {
    let Some(prefix) = read_exact_or_eof(reader, HEADER_SIZE)? else {
        return Ok(None);
    };
    let fields = read_prefix(&prefix, max_payload, max_uncompressed)?;
    let header = read_exact_or_eof(reader, fields.header_len)?
        .ok_or_else(|| FrameError::truncated("stream ended inside a frame"))?;
    let payload = read_exact_or_eof(reader, fields.payload_len)?
        .ok_or_else(|| FrameError::truncated("stream ended inside a frame"))?;
    Ok(Some(decode_frame_parts(
        &prefix,
        &header,
        Buffer::from(payload),
        max_payload,
        max_uncompressed,
    )?))
}

/// Validate prefix, header and payload that were read separately.
pub fn decode_frame_parts(
    prefix: &[u8],
    header: &[u8],
    payload: Buffer,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<Frame> {
    let fields = read_prefix(prefix, max_payload, max_uncompressed)?;
    if header.len() != fields.header_len {
        return Err(FrameError::truncated(format!(
            "header declares {} bytes, carries {}",
            fields.header_len,
            header.len()
        ))
        .into());
    }
    if payload.len() != fields.payload_len {
        return Err(FrameError::truncated(format!(
            "payload declares {} bytes, carries {}",
            fields.payload_len,
            payload.len()
        ))
        .into());
    }
    let parsed: Value = serde_json::from_slice(header)
        .map_err(|err| FrameError::header(format!("header is not UTF-8 JSON: {err}")))?;
    let Value::Object(map) = parsed else {
        return Err(FrameError::header("header is not a JSON object").into());
    };
    let chunk = match map.get("chunk") {
        Some(value) => Some(
            ChunkInfo::from_value(value)
                .map_err(|err| FrameError::header(format!("chunk metadata is invalid: {err}")))?,
        ),
        None => None,
    };
    if crc32fast::hash(payload.as_ref()) != fields.crc {
        return Err(FrameError::ChecksumMismatch.into());
    }
    Ok(Frame {
        lane_id: fields.lane_id,
        payload,
        rows: fields.rows,
        uncompressed_len: fields.uncompressed_len,
        header: map.into_iter().collect(),
        flags: fields.flags,
        chunk,
    })
}

/// Parse and validate a frame. Every check runs before the payload is trusted.
pub fn decode_frame(data: &[u8], max_payload: usize, max_uncompressed: usize) -> Result<Frame> {
    let fields = read_prefix(data, max_payload, max_uncompressed)?;
    let body_start = HEADER_SIZE + fields.header_len;
    if body_start + fields.payload_len != data.len() {
        return Err(FrameError::truncated(format!(
            "frame declares {} bytes, carries {}",
            body_start + fields.payload_len,
            data.len()
        ))
        .into());
    }
    decode_frame_parts(
        &data[..HEADER_SIZE],
        &data[HEADER_SIZE..body_start],
        Buffer::from(data[body_start..].to_vec()),
        max_payload,
        max_uncompressed,
    )
}

/// Parse a frame from a buffer without copying the payload.
///
/// The payload becomes a slice of the caller's buffer, so no bytes move. Use
/// this on the receive path when the frame is already in memory.
pub fn decode_frame_view(
    data: &Buffer,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<Frame> {
    let bytes = data.as_ref();
    let fields = read_prefix(bytes, max_payload, max_uncompressed)?;
    let body_start = HEADER_SIZE + fields.header_len;
    if body_start + fields.payload_len != bytes.len() {
        return Err(FrameError::truncated(format!(
            "frame declares {} bytes, carries {}",
            body_start + fields.payload_len,
            bytes.len()
        ))
        .into());
    }
    let payload = data.slice_with_length(body_start, fields.payload_len);
    decode_frame_parts(
        &bytes[..HEADER_SIZE],
        &bytes[HEADER_SIZE..body_start],
        payload,
        max_payload,
        max_uncompressed,
    )
}
