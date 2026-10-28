//! Arrow schema serialisation for the frame header.
//!
//! `pyarrow.Schema.serialize` returns an encapsulated IPC message: a
//! continuation marker, a little endian length, then the flatbuffer. An empty
//! Arrow IPC stream is exactly that message, so writing a stream with no record
//! batches gives bytes the Python package can read, and reading one back gives
//! the schema it wrote.

use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow_schema::Schema;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::error::{FrameError, Result};

/// Encode a schema as an encapsulated IPC message.
pub fn encode_schema(schema: &Schema) -> Result<Vec<u8>> {
    let mut sink: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut sink, schema)
            .map_err(|err| FrameError::payload(format!("schema cannot be encoded: {err}")))?;
        writer
            .finish()
            .map_err(|err| FrameError::payload(format!("schema cannot be encoded: {err}")))?;
    }
    Ok(sink)
}

/// Decode a schema from an encapsulated IPC message.
pub fn decode_schema(raw: &[u8]) -> Result<Schema> {
    let reader = StreamReader::try_new(Cursor::new(raw), None)
        .map_err(|err| FrameError::header(format!("schema is not a valid Arrow schema: {err}")))?;
    Ok(Schema::clone(reader.schema().as_ref()))
}

/// Encode a schema as base64 for a JSON header.
pub fn encode_schema_b64(schema: &Schema) -> String {
    STANDARD.encode(encode_schema(schema).unwrap_or_default())
}

/// Decode a base64 schema from a JSON header.
pub fn decode_schema_b64(raw: &str) -> Result<Schema> {
    let bytes = STANDARD
        .decode(raw)
        .map_err(|err| FrameError::header(format!("schema is not valid base64: {err}")))?;
    decode_schema(&bytes)
}
