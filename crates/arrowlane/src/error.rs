//! Error hierarchy. Every failure from this crate is an [`ArrowLaneError`].

use thiserror::Error;

/// Root of every arrowlane error.
#[derive(Debug, Error)]
pub enum ArrowLaneError {
    /// The frame cannot be trusted or decoded.
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),

    /// Configuration is missing or invalid.
    #[error("config error: {0}")]
    Config(String),

    /// A chunk stream is malformed, incomplete, or out of order.
    #[error("chunk error: {0}")]
    Chunk(String),

    /// The lane exists but this platform or directory cannot serve it.
    #[error("lane unavailable: {0}")]
    LaneUnavailable(String),

    /// A shared memory segment is missing, malformed, or the wrong size.
    #[error("segment error: {0}")]
    Segment(String),

    /// The caller is not authorised.
    #[error("auth error: {0}")]
    Auth(String),

    /// A runtime limit was hit.
    #[error("limit exceeded: {0}")]
    Limit(String),

    /// Underlying Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// An I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// The frame cannot be trusted or decoded.
#[derive(Debug, Error)]
pub enum FrameError {
    /// The frame does not start with the arrowlane magic.
    #[error("expected magic ALN1, got {0:?}")]
    BadMagic([u8; 4]),

    /// The frame format version is not supported.
    #[error("frame version {0}, expected 1")]
    UnsupportedVersion(u8),

    /// The frame is shorter than its own length fields claim.
    #[error("truncated frame: {0}")]
    Truncated(String),

    /// A length field exceeds a configured limit.
    #[error("oversize frame: {0}")]
    Oversize(String),

    /// The payload does not match the frame CRC.
    #[error("payload does not match the frame CRC")]
    ChecksumMismatch,

    /// The lane id is not registered.
    #[error("lane id {0} is not registered")]
    UnknownLane(u8),

    /// The frame header is not valid JSON or carries no schema.
    #[error("bad header: {0}")]
    BadHeader(String),

    /// The payload is not a valid encoding for its lane.
    #[error("bad payload: {0}")]
    BadPayload(String),
}

impl FrameError {
    /// Wrap a message in a [`FrameError::BadPayload`].
    pub fn payload(message: impl Into<String>) -> Self {
        Self::BadPayload(message.into())
    }

    /// Wrap a message in a [`FrameError::Truncated`].
    pub fn truncated(message: impl Into<String>) -> Self {
        Self::Truncated(message.into())
    }

    /// Wrap a message in a [`FrameError::Oversize`].
    pub fn oversize(message: impl Into<String>) -> Self {
        Self::Oversize(message.into())
    }

    /// Wrap a message in a [`FrameError::BadHeader`].
    pub fn header(message: impl Into<String>) -> Self {
        Self::BadHeader(message.into())
    }
}

/// Convenience result type for this crate.
pub type Result<T> = std::result::Result<T, ArrowLaneError>;
