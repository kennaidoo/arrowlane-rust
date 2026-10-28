//! POSIX shared memory segments for the `shm` lane.
//!
//! The descriptor is small and travels in the frame. The payload stays in a file
//! under a shared directory, usually `/dev/shm` or a memory backed volume. The
//! reader maps it, verifies the CRC, unlinks the name, and returns a buffer that
//! owns the mapping.
//!
//! Linux and macOS allow unlink while a mapping is open. Windows does not, so
//! [`is_ready`] reports false there.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::error::{ArrowLaneError, FrameError, Result};

/// Segment name prefix.
pub const NAME_PREFIX: &str = "al-";
/// Default sweep TTL in seconds.
pub const DEFAULT_TTL_S: f64 = 60.0;
/// Segment size cap.
pub const MAX_SEGMENT_BYTES: usize = 536_870_912;

/// Where the payload is and how to prove it arrived intact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descriptor {
    /// Segment file name.
    pub name: String,
    /// Payload length within the segment.
    pub length: u64,
    /// CRC32 of the payload.
    pub crc32: u32,
    /// Shared token.
    pub token: String,
    /// Offset of the payload within the segment.
    pub offset: u64,
}

impl Descriptor {
    /// Serialise the descriptor for the frame payload.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut map: BTreeMap<&str, Value> = BTreeMap::new();
        map.insert("name", Value::String(self.name.clone()));
        map.insert("length", Value::from(self.length));
        map.insert("crc32", Value::from(self.crc32));
        map.insert("token", Value::String(self.token.clone()));
        map.insert("offset", Value::from(self.offset));
        serde_json::to_vec(&map).unwrap_or_default()
    }

    /// Parse and validate a descriptor. Every field is untrusted.
    pub fn from_bytes(raw: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(raw).map_err(|err| {
            ArrowLaneError::Segment(format!("descriptor is not UTF-8 JSON: {err}"))
        })?;
        let map = value
            .as_object()
            .ok_or_else(|| ArrowLaneError::Segment("descriptor is not an object".to_string()))?;
        for key in map.keys() {
            if !["name", "length", "crc32", "token", "offset"].contains(&key.as_str()) {
                return Err(ArrowLaneError::Segment(
                    "descriptor has unknown keys".to_string(),
                ));
            }
        }
        let name = map
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| {
                name.starts_with(NAME_PREFIX) && !name.contains('/') && !name.contains('\\')
            })
            .ok_or_else(|| ArrowLaneError::Segment("descriptor name is invalid".to_string()))?;
        let length = map
            .get("length")
            .and_then(Value::as_u64)
            .filter(|length| *length > 0 && *length <= MAX_SEGMENT_BYTES as u64)
            .ok_or_else(|| ArrowLaneError::Segment("descriptor length is invalid".to_string()))?;
        let offset = map.get("offset").and_then(Value::as_u64).unwrap_or(0);
        let crc32 = map
            .get("crc32")
            .and_then(Value::as_u64)
            .filter(|crc| *crc <= u64::from(u32::MAX))
            .ok_or_else(|| ArrowLaneError::Segment("descriptor crc32 is invalid".to_string()))?;
        let token = map
            .get("token")
            .and_then(Value::as_str)
            .filter(|token| token.len() >= 16)
            .ok_or_else(|| ArrowLaneError::Segment("descriptor token is invalid".to_string()))?;
        Ok(Self {
            name: name.to_string(),
            length,
            crc32: crc32 as u32,
            token: token.to_string(),
            offset,
        })
    }
}

/// Directory that holds the segments.
pub fn segment_dir() -> PathBuf {
    if let Some(override_dir) = std::env::var_os("ARROWLAND_SHM_DIR") {
        return PathBuf::from(override_dir);
    }
    let dev_shm = Path::new("/dev/shm");
    if dev_shm.is_dir() {
        return dev_shm.to_path_buf();
    }
    std::env::temp_dir()
}

fn memory_dir_available() -> bool {
    Path::new("/dev/shm").is_dir() || std::env::var_os("ARROWLAND_SHM_DIR").is_some()
}

/// True when this platform and directory can serve the `shm` lane.
pub fn is_ready(directory: Option<&Path>) -> bool {
    if !cfg!(unix) {
        return false;
    }
    let target = match directory {
        None => {
            if !memory_dir_available() {
                return false;
            }
            segment_dir()
        }
        Some(path) => path.to_path_buf(),
    };
    if !target.is_dir() {
        return false;
    }
    let probe = target.join(format!(
        "{NAME_PREFIX}probe-{}",
        uuid::Uuid::now_v7().simple()
    ));
    match std::fs::write(&probe, b"x") {
        Ok(()) => {
            let _ = std::fs::remove_file(&probe);
            true
        }
        Err(_) => false,
    }
}

/// Write data to a new segment with mode 0600 and return its descriptor.
pub fn write_segment(
    data: &[u8],
    directory: Option<&Path>,
    token: Option<&str>,
) -> Result<Descriptor> {
    if data.is_empty() {
        return Err(ArrowLaneError::Segment(
            "refusing to write an empty segment".to_string(),
        ));
    }
    if data.len() > MAX_SEGMENT_BYTES {
        return Err(ArrowLaneError::Segment(format!(
            "segment {} exceeds {MAX_SEGMENT_BYTES}",
            data.len()
        )));
    }
    let target = directory.map(Path::to_path_buf).unwrap_or_else(segment_dir);
    std::fs::create_dir_all(&target)?;
    let name = format!("{NAME_PREFIX}{}", uuid::Uuid::now_v7().simple());
    let path = target.join(&name);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    if let Err(err) = file.write_all(data) {
        let _ = std::fs::remove_file(&path);
        return Err(err.into());
    }
    drop(file);
    Ok(Descriptor {
        name,
        length: data.len() as u64,
        crc32: crc32fast::hash(data),
        token: token.map(str::to_string).unwrap_or_else(random_token),
        offset: 0,
    })
}

fn random_token() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

/// A mapped segment. The mapping owns the bytes the payload points into, so it
/// must outlive any buffer taken from it.
#[derive(Debug)]
pub struct Segment {
    /// The mapping.
    pub mapping: memmap2::Mmap,
    /// Payload offset within the mapping.
    pub offset: usize,
    /// Payload length.
    pub length: usize,
}

/// Map a segment, verify it, and unlink the name.
pub fn read_segment(
    descriptor: &Descriptor,
    directory: Option<&Path>,
    token: &str,
    verify: bool,
) -> Result<Segment> {
    if !constant_time_eq(descriptor.token.as_bytes(), token.as_bytes()) {
        return Err(ArrowLaneError::Auth(
            "segment token does not match".to_string(),
        ));
    }
    let target = directory.map(Path::to_path_buf).unwrap_or_else(segment_dir);
    let path = target.join(&descriptor.name);
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(ArrowLaneError::Segment(format!(
                "segment {} is gone",
                descriptor.name
            )));
        }
        Err(err) => return Err(err.into()),
    };
    let size = file.metadata()?.len();
    let end = descriptor
        .offset
        .checked_add(descriptor.length)
        .ok_or_else(|| ArrowLaneError::Segment("descriptor range overflows a u64".to_string()))?;
    if end != size {
        return Err(ArrowLaneError::Segment(format!(
            "segment is {size} bytes, descriptor claims offset {} length {}",
            descriptor.offset, descriptor.length
        )));
    }
    let offset = usize::try_from(descriptor.offset)
        .map_err(|_| ArrowLaneError::Segment("descriptor offset is too large".to_string()))?;
    let length = usize::try_from(descriptor.length)
        .map_err(|_| ArrowLaneError::Segment("descriptor length is too large".to_string()))?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| ArrowLaneError::Segment("descriptor range overflows a usize".to_string()))?;
    // SAFETY: the segment is created by this crate with mode 0600 and an
    // unguessable name, and the caller has already proved it holds the token.
    let mapping = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|err| ArrowLaneError::Segment(format!("segment cannot be mapped: {err}")))?;
    drop(file);
    let payload = mapping.get(offset..end).ok_or_else(|| {
        ArrowLaneError::Segment("descriptor range is outside the segment".to_string())
    })?;
    let matched = crc32fast::hash(payload) == descriptor.crc32;
    if verify && !matched {
        let _ = std::fs::remove_file(&path);
        return Err(ArrowLaneError::Frame(FrameError::ChecksumMismatch));
    }
    let _ = std::fs::remove_file(&path);
    Ok(Segment {
        mapping,
        offset,
        length,
    })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Remove a segment that was written but never sent.
pub fn discard(descriptor: &Descriptor, directory: Option<&Path>) {
    let target = directory.map(Path::to_path_buf).unwrap_or_else(segment_dir);
    let _ = std::fs::remove_file(target.join(&descriptor.name));
}

/// Remove segments older than `ttl_s`. Returns the count removed.
pub fn sweep(directory: Option<&Path>, ttl_s: f64, now: Option<f64>) -> usize {
    let target = directory.map(Path::to_path_buf).unwrap_or_else(segment_dir);
    if !target.is_dir() {
        return 0;
    }
    let now = now.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    });
    let cutoff = now - ttl_s;
    let mut removed = 0;
    let Ok(entries) = std::fs::read_dir(&target) else {
        return 0;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with(NAME_PREFIX) {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        let modified = metadata
            .modified()
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs_f64())
            .unwrap_or(f64::INFINITY);
        if modified < cutoff && std::fs::remove_file(entry.path()).is_ok() {
            removed += 1;
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_roundtrip() {
        let descriptor = Descriptor {
            name: format!("{NAME_PREFIX}abc"),
            length: 10,
            crc32: 7,
            token: "0123456789abcdef".to_string(),
            offset: 0,
        };
        let raw = descriptor.to_bytes();
        assert_eq!(Descriptor::from_bytes(&raw).unwrap(), descriptor);
    }

    #[test]
    fn descriptor_rejects_a_bad_name() {
        let raw = br#"{"name":"evil","length":10,"crc32":1,"token":"0123456789abcdef","offset":0}"#;
        assert!(Descriptor::from_bytes(raw).is_err());
    }

    #[test]
    fn descriptor_rejects_a_short_token() {
        let raw = br#"{"name":"al-x","length":10,"crc32":1,"token":"short","offset":0}"#;
        assert!(Descriptor::from_bytes(raw).is_err());
    }

    #[test]
    fn descriptor_rejects_unknown_keys() {
        let raw = br#"{"name":"al-x","length":10,"crc32":1,"token":"0123456789abcdef","extra":1}"#;
        assert!(Descriptor::from_bytes(raw).is_err());
    }

    #[test]
    fn descriptor_range_overflow_is_rejected() {
        let dir = std::env::temp_dir().join(format!("arrowlane-shm-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let name = format!("{NAME_PREFIX}overflow");
        std::fs::write(dir.join(&name), b"").unwrap();
        let descriptor = Descriptor {
            name,
            length: 1,
            crc32: 0,
            token: "0123456789abcdef".to_string(),
            offset: u64::MAX,
        };
        let err = read_segment(&descriptor, Some(&dir), &descriptor.token, false).unwrap_err();
        assert!(err.to_string().contains("overflows"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
