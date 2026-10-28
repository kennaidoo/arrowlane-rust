//! Chunked frames for large tables.
//!
//! Each chunk is a complete frame with `id`, `index` and `last` metadata. The
//! producer is a lazy iterator, so it encodes chunk n+1 only when the consumer
//! asks for it. A blocking write or a bounded queue therefore applies backpressure
//! to the producer.

use std::io::{Read, Write};
use std::path::Path;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::error::{ArrowLaneError, FrameError, Result};
use crate::frame::{
    ChunkInfo, Frame, MAX_CHUNKS, MAX_PAYLOAD_BYTES, MAX_UNCOMPRESSED_BYTES, read_frame,
    write_frame,
};
use crate::lanes::lane_for;
use crate::select::{Decision, Policy, Topology};
use crate::transit::TransitOptions;

/// Rows per chunk when the caller does not choose.
pub const DEFAULT_CHUNK_ROWS: usize = 50_000;
/// Highest accepted chunk size.
pub const MAX_CHUNK_ROWS: usize = 10_000_000;

fn validate(rows_per_chunk: usize, max_payload: usize) -> Result<()> {
    if rows_per_chunk == 0 || rows_per_chunk > MAX_CHUNK_ROWS {
        return Err(ArrowLaneError::Chunk(format!(
            "rows_per_chunk must be 1..{MAX_CHUNK_ROWS}"
        )));
    }
    if max_payload == 0 || max_payload > MAX_PAYLOAD_BYTES {
        return Err(ArrowLaneError::Chunk(format!(
            "max_payload must be 1..{MAX_PAYLOAD_BYTES}"
        )));
    }
    Ok(())
}

fn split_batch(batch: &RecordBatch) -> Result<(RecordBatch, RecordBatch)> {
    let middle = batch.num_rows() / 2;
    if middle == 0 {
        return Err(ArrowLaneError::Chunk(
            "cannot split a one row chunk".to_string(),
        ));
    }
    let head = batch.slice(0, middle);
    let tail = batch.slice(middle, batch.num_rows() - middle);
    Ok((head, tail))
}

/// Split a batch at a row offset, keeping the schema.
fn split_batch_at(batch: &RecordBatch, rows: usize) -> Result<(RecordBatch, RecordBatch)> {
    if rows == 0 || rows >= batch.num_rows() {
        return Err(ArrowLaneError::Chunk(
            "split point must be inside the batch".to_string(),
        ));
    }
    let head = batch.slice(0, rows);
    let tail = batch.slice(rows, batch.num_rows() - rows);
    Ok((head, tail))
}

/// Options for the chunk writes.
#[derive(Debug, Clone)]
pub struct ChunkOptions<'a> {
    /// Lane to use.
    pub lane: &'a str,
    /// Shared memory token.
    pub token: Option<&'a str>,
    /// Shared memory directory.
    pub directory: Option<&'a Path>,
    /// Compression level.
    pub level: Option<i32>,
    /// Rows per chunk.
    pub rows_per_chunk: usize,
    /// Payload cap per chunk.
    pub max_payload: usize,
}

impl Default for ChunkOptions<'_> {
    fn default() -> Self {
        Self {
            lane: "ipc",
            token: None,
            directory: None,
            level: None,
            rows_per_chunk: DEFAULT_CHUNK_ROWS,
            max_payload: MAX_PAYLOAD_BYTES,
        }
    }
}

/// A lazy chunk stream. Encodes chunk n+1 only when the consumer calls `next`.
pub struct ChunkFrames<'a> {
    chosen: Box<dyn crate::lanes::Lane>,
    pending: Vec<RecordBatch>,
    stream_id: String,
    index: u64,
    level: Option<i32>,
    max_payload: usize,
    finished: bool,
    _options: std::marker::PhantomData<&'a ()>,
}

impl<'a> ChunkFrames<'a> {
    /// Build a lazy chunk stream from a batch list.
    pub fn new(
        batches: Vec<RecordBatch>,
        schema: std::sync::Arc<Schema>,
        options: &ChunkOptions<'a>,
    ) -> Result<Self> {
        validate(options.rows_per_chunk, options.max_payload)?;
        let chosen = lane_for(options.lane, options.token, options.directory)?;
        let mut pending: Vec<RecordBatch> = Vec::new();
        for batch in batches {
            let mut remaining = batch;
            while remaining.num_rows() > 0 && remaining.num_rows() > options.rows_per_chunk {
                let (head, tail) = split_batch_at(&remaining, options.rows_per_chunk)?;
                pending.push(head);
                remaining = tail;
            }
            if remaining.num_rows() > 0 {
                pending.push(remaining);
            }
        }
        if pending.is_empty() {
            pending.push(RecordBatch::new_empty(schema.clone()));
        }
        // Reverse so `pop` yields in stream order.
        pending.reverse();
        Ok(Self {
            chosen,
            pending,
            stream_id: uuid::Uuid::new_v4().simple().to_string(),
            index: 0,
            level: options.level,
            max_payload: options.max_payload,
            finished: false,
            _options: std::marker::PhantomData,
        })
    }

    /// Stream identity.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Whether the stream has emitted its final chunk.
    pub fn is_finished(&self) -> bool {
        self.finished && self.pending.is_empty()
    }
}

impl Iterator for ChunkFrames<'_> {
    type Item = Result<Frame>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let batch = self.pending.pop()?;
            let frame = match self.chosen.encode(&batch, self.level) {
                Ok(frame) => frame,
                Err(err) => {
                    if batch.num_rows() <= 1 {
                        return Some(Err(err));
                    }
                    if let Err(split_err) = requeue_split(&mut self.pending, &batch) {
                        return Some(Err(split_err));
                    }
                    continue;
                }
            };
            if frame.payload.len() > self.max_payload {
                if batch.num_rows() <= 1 {
                    return Some(Err(FrameError::oversize(format!(
                        "one row encodes to {} bytes, over {}",
                        frame.payload.len(),
                        self.max_payload
                    ))
                    .into()));
                }
                let (head, tail) = match split_batch(&batch) {
                    Ok(pair) => pair,
                    Err(err) => return Some(Err(err)),
                };
                self.pending.push(tail);
                self.pending.push(head);
                continue;
            }
            if self.index as usize >= MAX_CHUNKS {
                return Some(Err(ArrowLaneError::Chunk(format!(
                    "stream exceeds {MAX_CHUNKS} chunks"
                ))));
            }
            let last = self.pending.is_empty();
            let info = match ChunkInfo::new(self.stream_id.clone(), self.index, last) {
                Ok(info) => info,
                Err(err) => return Some(Err(err)),
            };
            self.index += 1;
            if last {
                self.finished = true;
            }
            return Some(Ok(Frame {
                chunk: Some(info),
                ..frame
            }));
        }
    }
}

fn requeue_split(pending: &mut Vec<RecordBatch>, batch: &RecordBatch) -> Result<()> {
    let (head, tail) = split_batch(batch)?;
    pending.push(tail);
    pending.push(head);
    Ok(())
}

/// Yield one wire frame per chunk. The iterator is the backpressure signal.
pub fn pack_chunks<'a>(
    batches: Vec<RecordBatch>,
    schema: std::sync::Arc<Schema>,
    options: &ChunkOptions<'a>,
) -> Result<impl Iterator<Item = Result<Vec<u8>>> + 'a> {
    let frames = ChunkFrames::new(batches, schema, options)?;
    let max_payload = options.max_payload;
    Ok(frames.map(move |frame| {
        let frame = frame?;
        crate::frame::encode_frame(&frame, max_payload)
    }))
}

/// Write a chunk stream to a writer. Returns `(chunks, bytes)`.
pub fn write_chunks<W: Write>(
    writer: &mut W,
    batches: Vec<RecordBatch>,
    schema: std::sync::Arc<Schema>,
    options: &ChunkOptions<'_>,
) -> Result<(usize, usize)> {
    validate(options.rows_per_chunk, options.max_payload)?;
    let frames = ChunkFrames::new(batches, schema, options)?;
    let mut chunks = 0;
    let mut total = 0;
    for frame in frames {
        let frame = frame?;
        total += write_frame(writer, &frame, options.max_payload)?;
        chunks += 1;
    }
    Ok((chunks, total))
}

fn decode_chunks<I>(
    frames: I,
    token: Option<&str>,
    directory: Option<&Path>,
    max_uncompressed: usize,
) -> Result<Vec<RecordBatch>>
where
    I: IntoIterator<Item = Result<Frame>>,
{
    let mut stream_id: Option<String> = None;
    let mut last_seen = false;
    let mut batches: Vec<RecordBatch> = Vec::new();
    for (expected, frame) in frames.into_iter().enumerate() {
        let frame = frame?;
        let info = frame
            .chunk
            .as_ref()
            .ok_or_else(|| ArrowLaneError::Chunk("frame carries no chunk metadata".to_string()))?
            .clone();
        match &stream_id {
            None => stream_id = Some(info.id.clone()),
            Some(existing) if *existing != info.id => {
                return Err(ArrowLaneError::Chunk(
                    "chunk id changed mid stream".to_string(),
                ));
            }
            Some(_) => {}
        }
        if last_seen {
            return Err(ArrowLaneError::Chunk(
                "chunk after the last chunk".to_string(),
            ));
        }
        if info.index != expected as u64 {
            return Err(ArrowLaneError::Chunk(format!(
                "chunk index {}, expected {expected}",
                info.index
            )));
        }
        let chosen = lane_for(frame.try_lane()?, token, directory)?;
        let batch = chosen.decode(&frame, Some(max_uncompressed))?;
        batches.push(batch);
        last_seen = info.last;
    }
    if !last_seen {
        return Err(ArrowLaneError::Chunk(
            "stream ended before the last chunk".to_string(),
        ));
    }
    if batches.is_empty() {
        return Err(ArrowLaneError::Chunk("chunk stream is empty".to_string()));
    }
    Ok(batches)
}

/// Decode a chunk stream and return one batch per chunk.
pub fn unpack_chunks(
    frames: Vec<Vec<u8>>,
    token: Option<&str>,
    directory: Option<&Path>,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<Vec<RecordBatch>> {
    let decoded = frames
        .into_iter()
        .map(|wire| crate::frame::decode_frame(&wire, max_payload, max_uncompressed));
    decode_chunks(decoded, token, directory, max_uncompressed)
}

/// Decode a chunk stream into one batch.
pub fn assemble_chunks(
    frames: Vec<Vec<u8>>,
    token: Option<&str>,
    directory: Option<&Path>,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<RecordBatch> {
    let batches = unpack_chunks(frames, token, directory, max_payload, max_uncompressed)?;
    let schema = batches[0].schema();
    arrow_select::concat::concat_batches(&schema, &batches)
        .map_err(|err| FrameError::payload(format!("batches cannot be joined: {err}")).into())
}

/// Read a chunk stream from a reader and return one batch per chunk.
pub fn read_chunks<R: Read>(
    reader: &mut R,
    token: Option<&str>,
    directory: Option<&Path>,
    max_payload: usize,
    max_uncompressed: usize,
) -> Result<Vec<RecordBatch>> {
    let mut frames: Vec<Result<Frame>> = Vec::new();
    loop {
        match read_frame(reader, max_payload, max_uncompressed) {
            Ok(None) => break,
            Ok(Some(frame)) => frames.push(Ok(frame)),
            Err(err) => {
                frames.push(Err(err));
                break;
            }
        }
    }
    decode_chunks(frames, token, directory, max_uncompressed)
}

/// Plan a lane and return a lazy chunk iterator with the decision.
pub fn transit_chunks<'a>(
    batches: Vec<RecordBatch>,
    schema: std::sync::Arc<Schema>,
    options: &ChunkOptions<'a>,
) -> Result<(Vec<Vec<u8>>, Decision)> {
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    let total_bytes: u64 = batches.iter().map(crate::transit::arrow_bytes).sum();
    let transit_options = TransitOptions {
        topology: Topology::SameNetwork,
        policy: Policy::Latency,
        ..Default::default()
    };
    let synthetic = if total_rows == 0 {
        RecordBatch::new_empty(schema.clone())
    } else {
        arrow_select::concat::concat_batches(&schema, &batches)
            .map_err(|err| FrameError::payload(format!("batches cannot be joined: {err}")))?
    };
    let _ = total_bytes;
    let decision = crate::transit::plan_with(
        &synthetic,
        &TransitOptions {
            topology: transit_options.topology,
            policy: transit_options.policy,
            token: options.token,
            directory: options.directory,
            ..Default::default()
        },
    )?;
    let mut chunk_options = options.clone();
    chunk_options.lane = Box::leak(decision.lane.clone().into_boxed_str());
    let frames: Vec<Vec<u8>> =
        pack_chunks(batches, schema, &chunk_options)?.collect::<Result<_>>()?;
    Ok((frames, decision))
}

/// Default uncompressed cap for chunk reads.
pub const DEFAULT_MAX_UNCOMPRESSED: usize = MAX_UNCOMPRESSED_BYTES;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batches(rows: usize) -> Vec<RecordBatch> {
        vec![
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                    Arc::new(StringArray::from_iter_values(
                        (0..rows).map(|index| format!("name-{index}")),
                    )),
                ],
            )
            .expect("valid batch"),
        ]
    }

    #[test]
    fn chunks_roundtrip_and_mark_the_last_one() {
        let options = ChunkOptions {
            lane: "ipc",
            rows_per_chunk: 250,
            ..Default::default()
        };
        let frames = pack_chunks(batches(1000), schema(), &options)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(frames.len(), 4);
        let decoded = unpack_chunks(
            frames,
            None,
            None,
            MAX_PAYLOAD_BYTES,
            MAX_UNCOMPRESSED_BYTES,
        )
        .unwrap();
        assert_eq!(decoded.len(), 4);
        assert_eq!(
            decoded.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1000
        );
    }

    #[test]
    fn assemble_rebuilds_the_whole_table() {
        let options = ChunkOptions {
            lane: "ipc-zstd",
            rows_per_chunk: 300,
            ..Default::default()
        };
        let frames = pack_chunks(batches(1000), schema(), &options)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let assembled = assemble_chunks(
            frames,
            None,
            None,
            MAX_PAYLOAD_BYTES,
            MAX_UNCOMPRESSED_BYTES,
        )
        .unwrap();
        assert_eq!(assembled, batches(1000)[0]);
    }

    #[test]
    fn empty_batch_produces_one_chunk() {
        let options = ChunkOptions {
            lane: "ipc",
            rows_per_chunk: 100,
            ..Default::default()
        };
        let frames = pack_chunks(vec![RecordBatch::new_empty(schema())], schema(), &options)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn zero_rows_per_chunk_is_rejected() {
        let options = ChunkOptions {
            lane: "ipc",
            rows_per_chunk: 0,
            ..Default::default()
        };
        assert!(pack_chunks(batches(10), schema(), &options).is_err());
    }

    #[test]
    fn a_truncated_stream_is_rejected() {
        let options = ChunkOptions {
            lane: "ipc",
            rows_per_chunk: 250,
            ..Default::default()
        };
        let mut frames = pack_chunks(batches(1000), schema(), &options)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        frames.pop();
        assert!(
            unpack_chunks(
                frames,
                None,
                None,
                MAX_PAYLOAD_BYTES,
                MAX_UNCOMPRESSED_BYTES
            )
            .is_err()
        );
    }

    #[test]
    fn write_then_read_roundtrips() {
        let options = ChunkOptions {
            lane: "ipc",
            rows_per_chunk: 200,
            ..Default::default()
        };
        let mut sink: Vec<u8> = Vec::new();
        let (chunks, bytes) = write_chunks(&mut sink, batches(600), schema(), &options).unwrap();
        assert_eq!(chunks, 3);
        assert_eq!(bytes, sink.len());
        let mut cursor = std::io::Cursor::new(sink);
        let decoded = read_chunks(
            &mut cursor,
            None,
            None,
            MAX_PAYLOAD_BYTES,
            MAX_UNCOMPRESSED_BYTES,
        )
        .unwrap();
        assert_eq!(decoded.len(), 3);
    }
}
