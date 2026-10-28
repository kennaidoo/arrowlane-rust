//! C ABI for arrowlane.
//!
//! The shape is deliberately small: a caller hands over a frame in memory and
//! gets either a status code or a result buffer it must free with
//! [`al_free`]. Every function is panic safe, because a panic that crosses the
//! FFI boundary is undefined behaviour.
//!
//! Memory ownership:
//!
//! - A returned buffer is owned by the caller and released with [`al_free`].
//! - An input buffer is borrowed for the duration of the call only.

use std::ffi::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::slice;

use arrowlane::select::{Decision, Policy, Selection, Topology, Workload};

/// Status codes. Zero is success.
pub const AL_OK: i32 = 0;
/// A null pointer or a length of zero where one is required.
pub const AL_ERR_INVALID: i32 = -1;
/// The frame could not be decoded.
pub const AL_ERR_FRAME: i32 = -2;
/// The call panicked, which is a bug in arrowlane.
pub const AL_ERR_PANIC: i32 = -3;
/// The output buffer was too small.
pub const AL_ERR_BUFFER: i32 = -4;

/// An owned byte buffer handed back across the ABI.
#[repr(C)]
pub struct AlBuffer {
    /// Pointer to the bytes. May be null when `len` is zero.
    pub data: *mut u8,
    /// Length in bytes.
    pub len: usize,
    /// Capacity in bytes, for the matching free.
    pub cap: usize,
}

impl AlBuffer {
    fn from_vec(mut bytes: Vec<u8>) -> Self {
        bytes.shrink_to_fit();
        let cap = bytes.capacity();
        let len = bytes.len();
        let data = bytes.as_mut_ptr();
        std::mem::forget(bytes);
        Self { data, len, cap }
    }

    fn empty() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

/// A selection result, flattened for a C caller.
#[repr(C)]
pub struct AlDecision {
    /// Chosen lane id.
    pub lane_id: u8,
    /// Predicted milliseconds.
    pub predicted_ms: f64,
    /// Predicted wire bytes.
    pub predicted_bytes: u64,
    /// Reason code, as a NUL terminated string owned by the library.
    pub reason: *const c_char,
}

/// Free a buffer returned by this library.
///
/// # Safety
///
/// `buffer` must have come from this library and must not be freed twice.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn al_free(buffer: AlBuffer) {
    if !buffer.data.is_null() && buffer.cap > 0 {
        // SAFETY: the pointer came from a `Vec<u8>` in this library with the
        // same capacity, and the caller has not freed it before.
        unsafe {
            drop(Vec::from_raw_parts(buffer.data, buffer.len, buffer.cap));
        }
    }
}

/// Return the library version as a static NUL terminated string.
#[unsafe(no_mangle)]
pub extern "C" fn al_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// Choose a lane for a workload.
///
/// Returns [`AL_OK`] and writes to `out` on success.
///
/// # Safety
///
/// `out` must be a valid, writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn al_select(
    rows: u64,
    arrow_bytes: u64,
    topology: u8,
    policy: u8,
    shm_ready: bool,
    out: *mut AlDecision,
) -> i32 {
    if out.is_null() {
        return AL_ERR_INVALID;
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        let topology = match topology {
            0 => Topology::SameHost,
            1 => Topology::SameNetwork,
            2 => Topology::WideArea,
            _ => return Err(AL_ERR_INVALID),
        };
        let policy = match policy {
            0 => Policy::Latency,
            1 => Policy::Throughput,
            2 => Policy::Cpu,
            _ => return Err(AL_ERR_INVALID),
        };
        let workload = Workload::new(rows, arrow_bytes).map_err(|_| AL_ERR_INVALID)?;
        let mut selection = Selection::new(workload, topology);
        selection.policy = policy;
        selection.shm_ready = shm_ready;
        let decision = arrowlane::select_plan(arrowlane::costmodel::load_default(), &selection)
            .map_err(|_| AL_ERR_FRAME)?;
        Ok(decision)
    }));
    match result {
        Ok(Ok(decision)) => {
            // SAFETY: the caller promised `out` is writable.
            unsafe {
                out.write(AlDecision {
                    lane_id: lane_id_for(&decision),
                    predicted_ms: decision.predicted_ms,
                    predicted_bytes: decision.predicted_bytes,
                    reason: leak_reason(&decision),
                });
            }
            AL_OK
        }
        Ok(Err(code)) => code,
        Err(_) => AL_ERR_PANIC,
    }
}

fn lane_id_for(decision: &Decision) -> u8 {
    arrowlane::frame::lane_id(&decision.lane).unwrap_or(0)
}

fn leak_reason(decision: &Decision) -> *const c_char {
    let mut text = decision.reason.clone().into_bytes();
    text.push(0);
    let boxed = text.into_boxed_slice();
    Box::into_raw(boxed) as *const c_char
}

/// Free a reason string returned in [`AlDecision`].
///
/// # Safety
///
/// `reason` must be a pointer returned by [`al_select`] and not freed twice.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn al_free_reason(reason: *const c_char) {
    if reason.is_null() {
        return;
    }
    let len = unsafe { std::ffi::CStr::from_ptr(reason) }.to_bytes().len();
    // SAFETY: the pointer came from a `Box<[u8]>` of the same length plus the NUL.
    unsafe {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            reason as *mut u8,
            len + 1,
        )));
    }
}

/// Decode a frame and return a JSON summary as a buffer.
///
/// # Safety
///
/// `data` must point to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn al_inspect(data: *const u8, len: usize) -> AlBuffer {
    if data.is_null() || len == 0 {
        return AlBuffer::empty();
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller promised `len` readable bytes at `data`.
        let bytes = unsafe { slice::from_raw_parts(data, len) };
        let frame = arrowlane::decode_frame(
            bytes,
            arrowlane::MAX_PAYLOAD_BYTES,
            arrowlane::MAX_UNCOMPRESSED_BYTES,
        )?;
        let batch = arrowlane::unpack(
            bytes,
            None,
            None,
            arrowlane::MAX_PAYLOAD_BYTES,
            arrowlane::MAX_UNCOMPRESSED_BYTES,
        )?;
        let summary = serde_json::json!({
            "lane": frame.lane(),
            "rows": batch.num_rows(),
            "columns": batch.num_columns(),
            "wire_bytes": len,
            "uncompressed_bytes": frame.uncompressed_len,
        });
        Ok::<_, arrowlane::ArrowLaneError>(serde_json::to_vec(&summary).unwrap_or_default())
    }));
    match result {
        Ok(Ok(bytes)) => AlBuffer::from_vec(bytes),
        _ => AlBuffer::empty(),
    }
}

/// Re-encode a frame on one lane, returning the new wire bytes.
///
/// `lane_id` follows the wire ids: 1 json, 2 ipc, 3 ipc-zstd.
///
/// # Safety
///
/// `data` must point to `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn al_repack(data: *const u8, len: usize, lane_id: u8) -> AlBuffer {
    if data.is_null() || len == 0 {
        return AlBuffer::empty();
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the caller promised `len` readable bytes at `data`.
        let bytes = unsafe { slice::from_raw_parts(data, len) };
        let batch = arrowlane::unpack(
            bytes,
            None,
            None,
            arrowlane::MAX_PAYLOAD_BYTES,
            arrowlane::MAX_UNCOMPRESSED_BYTES,
        )?;
        let lane = arrowlane::frame::lane_name(lane_id)
            .ok_or_else(|| arrowlane::ArrowLaneError::Config("unknown lane id".to_string()))?;
        let wire = arrowlane::pack(&batch, lane, None, None, None, arrowlane::MAX_PAYLOAD_BYTES)?;
        Ok::<_, arrowlane::ArrowLaneError>(wire)
    }));
    match result {
        Ok(Ok(bytes)) => AlBuffer::from_vec(bytes),
        _ => AlBuffer::empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_non_empty() {
        let pointer = al_version();
        let text = unsafe { std::ffi::CStr::from_ptr(pointer) }
            .to_str()
            .expect("utf8");
        assert!(!text.is_empty());
    }

    #[test]
    fn select_writes_a_decision() {
        let mut out = AlDecision {
            lane_id: 0,
            predicted_ms: 0.0,
            predicted_bytes: 0,
            reason: std::ptr::null(),
        };
        let code = unsafe { al_select(500_000, 13_000_000, 1, 0, false, &mut out) };
        assert_eq!(code, AL_OK);
        assert_ne!(out.lane_id, 0);
        assert!(out.predicted_ms > 0.0);
        unsafe { al_free_reason(out.reason) };
    }

    #[test]
    fn select_rejects_a_null_out_pointer() {
        let code = unsafe { al_select(1, 1, 0, 0, false, std::ptr::null_mut()) };
        assert_eq!(code, AL_ERR_INVALID);
    }

    #[test]
    fn inspect_rejects_a_null_pointer() {
        let buffer = unsafe { al_inspect(std::ptr::null(), 0) };
        assert!(buffer.data.is_null());
    }

    #[test]
    fn repack_rejects_an_unknown_lane() {
        let buffer = unsafe { al_repack(std::ptr::null(), 0, 99) };
        assert!(buffer.data.is_null());
    }
}
