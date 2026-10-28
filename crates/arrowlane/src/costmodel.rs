//! Measured cost model for the lane selector.
//!
//! Every slope is milliseconds per megabyte of Arrow data. `ratio` is wire bytes
//! divided by Arrow bytes. The fixed term is the per-call overhead. The wire term
//! is microseconds per megabyte for a topology, multiplied by the lane ratio.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{ArrowLaneError, Result};

/// Model schema version understood by this crate.
pub const SCHEMA_VERSION: u32 = 1;
/// Topologies the model carries a wire cost for.
pub const TOPOLOGIES: [&str; 3] = ["same_host", "same_network", "wide_area"];

/// Cost coefficients for one lane.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct LaneCost {
    /// Per-call overhead in milliseconds.
    pub fixed_ms: f64,
    /// Encode cost in milliseconds per megabyte.
    pub enc_ms_per_mb: f64,
    /// Decode cost in milliseconds per megabyte.
    pub dec_ms_per_mb: f64,
    /// Wire bytes divided by Arrow bytes.
    pub ratio: f64,
}

/// A validated, versioned cost model.
#[derive(Debug, Clone)]
pub struct CostModel {
    schema_version: u32,
    provenance: Value,
    wire_us_per_mb: BTreeMap<String, f64>,
    lanes: BTreeMap<String, LaneCost>,
}

impl CostModel {
    /// Model schema version.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Provenance recorded when the model was measured.
    pub fn provenance(&self) -> &Value {
        &self.provenance
    }

    /// Wire cost per topology, in microseconds per megabyte.
    pub fn wire_us_per_mb(&self) -> &BTreeMap<String, f64> {
        &self.wire_us_per_mb
    }

    /// Coefficients for every lane the model carries.
    pub fn lanes(&self) -> &BTreeMap<String, LaneCost> {
        &self.lanes
    }

    /// Coefficients for one lane.
    pub fn lane_cost(&self, lane: &str) -> Result<LaneCost> {
        self.lanes.get(lane).copied().ok_or_else(|| {
            ArrowLaneError::Config(format!("lane {lane:?} is not in the cost model"))
        })
    }

    /// Predicted end to end milliseconds for one table on one lane.
    pub fn predict(&self, lane: &str, mb: f64, topology: &str, policy: &str) -> Result<f64> {
        let cost = self.lane_cost(lane)?;
        let mut total = cost.fixed_ms + (cost.enc_ms_per_mb + cost.dec_ms_per_mb) * mb;
        if policy != "cpu" {
            let wire = *self.wire_us_per_mb.get(topology).ok_or_else(|| {
                ArrowLaneError::Config(format!("topology {topology:?} is unknown"))
            })?;
            total += (wire / 1000.0) * mb * cost.ratio;
        }
        Ok(total)
    }

    /// Predicted wire bytes for a table of this Arrow size.
    pub fn bytes_for(&self, lane: &str, arrow_bytes: u64) -> Result<u64> {
        Ok((arrow_bytes as f64 * self.lane_cost(lane)?.ratio) as u64)
    }
}

/// Top level keys the model may carry.
const TOP_KEYS: [&str; 4] = ["schema_version", "provenance", "wire_us_per_mb", "lanes"];
/// Keys a lane entry may carry.
const LANE_KEYS: [&str; 4] = ["fixed_ms", "enc_ms_per_mb", "dec_ms_per_mb", "ratio"];

fn number(value: Option<&Value>, what: &str) -> Result<f64> {
    let value = value.ok_or_else(|| ArrowLaneError::Config(format!("{what} must be a number")))?;
    let number = match value {
        Value::Number(n) => n
            .as_f64()
            .ok_or_else(|| ArrowLaneError::Config(format!("{what} must be a number")))?,
        _ => {
            return Err(ArrowLaneError::Config(format!("{what} must be a number")));
        }
    };
    if !number.is_finite() {
        return Err(ArrowLaneError::Config(format!("{what} must be finite")));
    }
    if number < 0.0 {
        return Err(ArrowLaneError::Config(format!(
            "{what} must not be negative"
        )));
    }
    Ok(number)
}

/// Validate an already parsed cost model.
pub fn parse(data: &Value) -> Result<CostModel> {
    let object = data
        .as_object()
        .ok_or_else(|| ArrowLaneError::Config("cost model must be a JSON object".to_string()))?;
    if let Some(unknown) = object.keys().find(|key| !TOP_KEYS.contains(&key.as_str())) {
        return Err(ArrowLaneError::Config(format!("unknown key: {unknown}")));
    }
    let schema_version = object
        .get("schema_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ArrowLaneError::Config(format!("schema_version must be {SCHEMA_VERSION}"))
        })?;
    if schema_version != SCHEMA_VERSION as u64 {
        return Err(ArrowLaneError::Config(format!(
            "schema_version must be {SCHEMA_VERSION}"
        )));
    }
    let provenance = object.get("provenance").cloned().unwrap_or(Value::Null);
    if !provenance.is_object() || provenance.as_object().is_some_and(|m| m.is_empty()) {
        return Err(ArrowLaneError::Config(
            "provenance must be a non-empty object".to_string(),
        ));
    }
    let wire_raw = object
        .get("wire_us_per_mb")
        .and_then(Value::as_object)
        .ok_or_else(|| ArrowLaneError::Config("wire_us_per_mb must be an object".to_string()))?;
    let mut wire_us_per_mb = BTreeMap::new();
    for name in TOPOLOGIES {
        wire_us_per_mb.insert(
            name.to_string(),
            number(wire_raw.get(name), &format!("wire_us_per_mb.{name}"))?,
        );
    }
    let lanes_raw = object
        .get("lanes")
        .and_then(Value::as_object)
        .filter(|m| !m.is_empty())
        .ok_or_else(|| ArrowLaneError::Config("lanes must be a non-empty object".to_string()))?;
    let mut lanes = BTreeMap::new();
    for (name, entry) in lanes_raw {
        let entry = entry
            .as_object()
            .ok_or_else(|| ArrowLaneError::Config(format!("lane {name} must be an object")))?;
        if let Some(extra) = entry.keys().find(|key| !LANE_KEYS.contains(&key.as_str())) {
            return Err(ArrowLaneError::Config(format!(
                "lane {name} has unknown key {extra}"
            )));
        }
        if let Some(missing) = LANE_KEYS.iter().find(|key| !entry.contains_key(**key)) {
            return Err(ArrowLaneError::Config(format!(
                "lane {name} is missing {missing}"
            )));
        }
        lanes.insert(
            name.clone(),
            LaneCost {
                fixed_ms: number(entry.get("fixed_ms"), &format!("lanes.{name}.fixed_ms"))?,
                enc_ms_per_mb: number(
                    entry.get("enc_ms_per_mb"),
                    &format!("lanes.{name}.enc_ms_per_mb"),
                )?,
                dec_ms_per_mb: number(
                    entry.get("dec_ms_per_mb"),
                    &format!("lanes.{name}.dec_ms_per_mb"),
                )?,
                ratio: number(entry.get("ratio"), &format!("lanes.{name}.ratio"))?,
            },
        );
    }
    if !lanes.contains_key("json") {
        return Err(ArrowLaneError::Config(
            "cost model must carry the json lane as the baseline".to_string(),
        ));
    }
    Ok(CostModel {
        schema_version: schema_version as u32,
        provenance,
        wire_us_per_mb,
        lanes,
    })
}

/// The shipped cost model, measured on the same host as the Python package.
pub const DEFAULT_MODEL_JSON: &str = include_str!("costmodel.json");

static DEFAULT_MODEL: OnceLock<CostModel> = OnceLock::new();

/// Load the shipped cost model once per process.
pub fn load_default() -> &'static CostModel {
    DEFAULT_MODEL.get_or_init(|| {
        let value: Value = serde_json::from_str(DEFAULT_MODEL_JSON)
            .unwrap_or_else(|err| panic!("shipped cost model is not JSON: {err}"));
        parse(&value).unwrap_or_else(|err| panic!("shipped cost model is invalid: {err}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_model_loads() {
        let model = load_default();
        assert_eq!(model.schema_version(), SCHEMA_VERSION);
        assert!(model.lanes().contains_key("json"));
        assert!(model.lanes().contains_key("ipc-zstd"));
    }

    #[test]
    fn predict_is_monotonic_in_size() {
        let model = load_default();
        let small = model.predict("ipc", 1.0, "same_host", "latency").unwrap();
        let large = model.predict("ipc", 10.0, "same_host", "latency").unwrap();
        assert!(large > small);
    }

    #[test]
    fn cpu_policy_drops_the_wire_term() {
        let model = load_default();
        let latency = model.predict("ipc", 5.0, "wide_area", "latency").unwrap();
        let cpu = model.predict("ipc", 5.0, "wide_area", "cpu").unwrap();
        assert!(cpu < latency);
    }

    #[test]
    fn unknown_lane_is_a_config_error() {
        let model = load_default();
        assert!(model.lane_cost("nope").is_err());
    }

    #[test]
    fn missing_json_baseline_is_rejected() {
        let bad = serde_json::json!({
            "schema_version": 1,
            "provenance": {"a": 1},
            "wire_us_per_mb": {"same_host": 1.0, "same_network": 2.0, "wide_area": 3.0},
            "lanes": {"ipc": {"fixed_ms": 0.1, "enc_ms_per_mb": 0.1, "dec_ms_per_mb": 0.1, "ratio": 1.0}}
        });
        assert!(parse(&bad).is_err());
    }

    #[test]
    fn negative_number_is_rejected() {
        let bad = serde_json::json!({
            "schema_version": 1,
            "provenance": {"a": 1},
            "wire_us_per_mb": {"same_host": -1.0, "same_network": 2.0, "wide_area": 3.0},
            "lanes": {"json": {"fixed_ms": 0.1, "enc_ms_per_mb": 0.1, "dec_ms_per_mb": 0.1, "ratio": 1.0}}
        });
        assert!(parse(&bad).is_err());
    }
}
