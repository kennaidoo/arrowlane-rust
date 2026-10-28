//! arrowlane: columnar data lanes for ingress and egress in containerized apps.
//!
//! The wire format is byte compatible with the Python package of the same name,
//! so either side can read what the other writes.
//!
//! The hot path is Rust: encoding writes bytes straight from the Arrow buffers,
//! the frame prefix is a fixed 38 byte stack array, the checksum is
//! `crc32fast`, and decoding can borrow the caller's buffer instead of copying
//! it. See `docs/performance.md` for the measured comparison with Python.

pub mod chunks;
pub mod costmodel;
pub mod error;
pub mod frame;
pub mod lanes;
pub mod schema_codec;
pub mod select;
pub mod shm;
pub mod transit;

pub use chunks::{
    DEFAULT_CHUNK_ROWS, MAX_CHUNK_ROWS, assemble_chunks, pack_chunks, read_chunks, transit_chunks,
    unpack_chunks, write_chunks,
};
pub use costmodel::{CostModel, LaneCost};
pub use error::{ArrowLaneError, FrameError, Result};
pub use frame::{
    ChunkInfo, FRAME_MEDIA_TYPE, Frame, HEADER_SIZE, Header, ID_LANES, LANE_IDS, MAX_CHUNKS,
    MAX_HEADER_BYTES, MAX_PAYLOAD_BYTES, MAX_UNCOMPRESSED_BYTES, decode_frame, decode_frame_view,
    encode_frame, read_frame, write_frame,
};
pub use lanes::{Lane, LaneInfo, available_for, available_names, lane_for, lane_names};
pub use select::{Decision, Policy, Selection, Topology, Workload, select_plan};
pub use transit::{
    TransitOptions, pack, pack_frame, plan, plan_with, transit, unpack, unpack_view,
};

/// Package version, matching the Python package's version line.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
