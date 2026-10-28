//! Lane registry and the lane contract.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;

use crate::error::{ArrowLaneError, Result};
use crate::frame::{Frame, Header};

pub mod ipc_lane;
pub mod json_lane;
pub mod shm_lane;
pub mod zstd_lane;

/// Static description of a lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneInfo {
    /// Lane id on the wire.
    pub id: u8,
    /// Lane name on the wire.
    pub name: &'static str,
    /// Media type of the payload.
    pub media_type: &'static str,
    /// True when the lane needs a POSIX platform.
    pub posix_only: bool,
}

/// What a lane must provide.
pub trait Lane: Send + Sync {
    /// Static description.
    fn info(&self) -> LaneInfo;

    /// Encode a batch into a frame.
    fn encode(&self, batch: &RecordBatch, level: Option<i32>) -> Result<Frame>;

    /// Decode a frame into a batch.
    fn decode(&self, frame: &Frame, max_uncompressed: Option<usize>) -> Result<RecordBatch>;
}

/// Return the lane for a name.
///
/// The `shm` lane is constructed per call because it carries a directory and a
/// token, matching the Python package.
pub fn lane_for(
    name: &str,
    token: Option<&str>,
    directory: Option<&std::path::Path>,
) -> Result<Box<dyn Lane>> {
    match name {
        "json" => Ok(Box::new(json_lane::JsonLane)),
        "ipc" => Ok(Box::new(ipc_lane::IpcLane)),
        "ipc-zstd" => Ok(Box::new(zstd_lane::ZstdLane)),
        "shm" => Ok(Box::new(shm_lane::ShmLane::new(
            directory.map(std::path::Path::to_path_buf),
            token.map(str::to_string),
        ))),
        other => Err(ArrowLaneError::Frame(crate::error::FrameError::BadPayload(
            format!("unknown lane {other:?}"),
        ))),
    }
}

/// The lane for an already described frame.
pub fn lane_for_frame(
    frame: &Frame,
    token: Option<&str>,
    directory: Option<&std::path::Path>,
) -> Result<Box<dyn Lane>> {
    let name = frame.try_lane()?;
    lane_for(name, token, directory)
}

/// Registered lane names, in registration order.
pub fn lane_names() -> Vec<&'static str> {
    vec!["json", "ipc", "ipc-zstd", "shm"]
}

/// Registered lanes this platform can serve.
pub fn available_names() -> Vec<&'static str> {
    lane_names()
        .into_iter()
        .filter(|name| *name != "shm" || cfg!(unix))
        .collect()
}

/// Registered lanes this platform and directory can serve.
pub fn available_for(directory: Option<&std::path::Path>) -> Vec<&'static str> {
    if cfg!(unix) && crate::shm::is_ready(directory) {
        return lane_names();
    }
    lane_names()
        .into_iter()
        .filter(|name| *name != "shm")
        .collect()
}

/// Every lane's static description, keyed by name.
pub fn lane_infos() -> BTreeMap<&'static str, LaneInfo> {
    [
        json_lane::INFO,
        ipc_lane::INFO,
        zstd_lane::INFO,
        shm_lane::INFO,
    ]
    .into_iter()
    .map(|info| (info.name, info))
    .collect()
}

/// Extract the payload and check it against a cap.
pub(crate) fn checked_payload(frame: &Frame, cap: usize) -> Result<&[u8]> {
    if frame.payload.len() > cap {
        return Err(crate::error::FrameError::oversize(format!(
            "payload {} exceeds {cap}",
            frame.payload.len()
        ))
        .into());
    }
    Ok(frame.payload.as_ref())
}

/// A header with the lane's media type and any extra keys.
pub(crate) fn lane_header(
    schema: &arrow_schema::Schema,
    media_type: &str,
    extra: impl IntoIterator<Item = (String, serde_json::Value)>,
) -> Header {
    let mut all: Vec<(String, serde_json::Value)> = vec![(
        "media_type".to_string(),
        serde_json::Value::String(media_type.to_string()),
    )];
    all.extend(extra);
    crate::frame::header_for(schema, all)
}
