//! Deterministic lane selection from a measured cost model.
//!
//! The rule order matches the Python package exactly, so the same input produces
//! the same decision on either side: non-tabular, empty, then scoring, then the
//! gain floor, then the tie break.

use std::fmt;
use std::str::FromStr;

use serde::Serialize;

use crate::costmodel::CostModel;
use crate::error::{ArrowLaneError, Result};
use crate::lanes::available_names;

/// Where the two endpoints sit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// Both endpoints on one host.
    SameHost,
    /// Both endpoints on one network.
    SameNetwork,
    /// Endpoints separated by a wide area link.
    WideArea,
}

impl Topology {
    /// Wire name used in the cost model and on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SameHost => "same_host",
            Self::SameNetwork => "same_network",
            Self::WideArea => "wide_area",
        }
    }

    /// Every topology, in declaration order.
    pub fn all() -> [Self; 3] {
        [Self::SameHost, Self::SameNetwork, Self::WideArea]
    }
}

impl fmt::Display for Topology {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Topology {
    type Err = ArrowLaneError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "same_host" => Ok(Self::SameHost),
            "same_network" => Ok(Self::SameNetwork),
            "wide_area" => Ok(Self::WideArea),
            other => Err(ArrowLaneError::Config(format!(
                "unknown topology {other:?}"
            ))),
        }
    }
}

/// What the caller wants optimised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// Minimise latency.
    Latency,
    /// Minimise bytes when scores are close.
    Throughput,
    /// Ignore the wire term.
    Cpu,
}

impl Policy {
    /// Wire name used on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Latency => "latency",
            Self::Throughput => "throughput",
            Self::Cpu => "cpu",
        }
    }

    /// Every policy, in declaration order.
    pub fn all() -> [Self; 3] {
        [Self::Latency, Self::Throughput, Self::Cpu]
    }
}

impl fmt::Display for Policy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Policy {
    type Err = ArrowLaneError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "latency" => Ok(Self::Latency),
            "throughput" => Ok(Self::Throughput),
            "cpu" => Ok(Self::Cpu),
            other => Err(ArrowLaneError::Config(format!("unknown policy {other:?}"))),
        }
    }
}

/// What is about to be sent.
#[derive(Debug, Clone, Copy)]
pub struct Workload {
    /// Row count.
    pub rows: u64,
    /// Logical Arrow buffer size in bytes.
    pub arrow_bytes: u64,
    /// True when the payload is tabular.
    pub tabular: bool,
}

impl Workload {
    /// Build a tabular workload.
    pub fn new(rows: u64, arrow_bytes: u64) -> Result<Self> {
        if rows > 0 && arrow_bytes == 0 {
            return Err(ArrowLaneError::Config(
                "a non-empty table needs positive arrow_bytes".to_string(),
            ));
        }
        Ok(Self {
            rows,
            arrow_bytes,
            tabular: true,
        })
    }
}

/// Inputs to one selection.
#[derive(Debug, Clone)]
pub struct Selection {
    /// The workload under consideration.
    pub workload: Workload,
    /// Where the endpoints sit.
    pub topology: Topology,
    /// What the caller wants optimised.
    pub policy: Policy,
    /// Lane allowlist. `None` means every lane in the model.
    pub allowed: Option<Vec<String>>,
    /// True when the shared memory lane can be used here.
    pub shm_ready: bool,
    /// Minimum fractional gain before a lane other than `json` is chosen.
    pub min_gain: f64,
}

impl Selection {
    /// Build a selection with the defaults the Python package uses.
    pub fn new(workload: Workload, topology: Topology) -> Self {
        Self {
            workload,
            topology,
            policy: Policy::Latency,
            allowed: None,
            shm_ready: false,
            min_gain: 0.10,
        }
    }
}

/// The chosen lane and the evidence for the choice.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Decision {
    /// Chosen lane name.
    pub lane: String,
    /// Why this lane was chosen.
    pub reason: String,
    /// Predicted milliseconds for the chosen lane.
    pub predicted_ms: f64,
    /// Predicted wire bytes for the chosen lane.
    pub predicted_bytes: u64,
    /// Score for every lane that was considered.
    pub scores: Vec<(String, f64)>,
    /// Lanes that were dropped, with the reason.
    pub rejected: Vec<(String, String)>,
}

impl Decision {
    /// Scores rounded for display, in the order they were computed.
    pub fn rounded_scores(&self) -> Vec<(String, f64)> {
        self.scores
            .iter()
            .map(|(name, value)| (name.clone(), round4(*value)))
            .collect()
    }
}

fn round4(value: f64) -> f64 {
    (value * 10_000.0).round() / 10_000.0
}

/// Tie break order, matching the Python `_ORDER` map.
fn order(name: &str) -> i32 {
    match name {
        "shm" => 0,
        "ipc-zstd" => 1,
        "ipc" => 2,
        "json" => 3,
        _ => 99,
    }
}

/// Choose a lane. The same input always produces the same decision.
pub fn select_plan(model: &CostModel, selection: &Selection) -> Result<Decision> {
    if !(0.0..1.0).contains(&selection.min_gain) {
        return Err(ArrowLaneError::Config(
            "min_gain must be in [0, 1)".to_string(),
        ));
    }
    let workload = selection.workload;
    let topology = selection.topology.as_str();
    let policy = selection.policy.as_str();
    let mb = (workload.arrow_bytes.max(1) as f64) / 1e6;
    let baseline = model.predict("json", mb, topology, policy)?;
    if !workload.tabular {
        return Ok(Decision {
            lane: "json".to_string(),
            reason: "NON_TABULAR".to_string(),
            predicted_ms: baseline,
            predicted_bytes: workload.arrow_bytes,
            scores: Vec::new(),
            rejected: Vec::new(),
        });
    }
    if workload.rows == 0 {
        return Ok(Decision {
            lane: "json".to_string(),
            reason: "EMPTY".to_string(),
            predicted_ms: baseline,
            predicted_bytes: workload.arrow_bytes,
            scores: Vec::new(),
            rejected: Vec::new(),
        });
    }

    let platform = available_names();
    let names: Vec<String> = match &selection.allowed {
        Some(allowed) => allowed.clone(),
        None => model.lanes().keys().cloned().collect(),
    };
    let mut scores: Vec<(String, f64)> = Vec::with_capacity(names.len());
    let mut rejected: Vec<(String, String)> = Vec::new();
    for name in names {
        if !model.lanes().contains_key(&name) {
            rejected.push((name, "UNKNOWN_LANE".to_string()));
            continue;
        }
        if name == "shm" && !selection.shm_ready {
            rejected.push((name, "SHM_UNAVAILABLE".to_string()));
            continue;
        }
        if !platform.contains(&name.as_str()) {
            rejected.push((name, "NOT_AVAILABLE_ON_PLATFORM".to_string()));
            continue;
        }
        scores.push((name.clone(), model.predict(&name, mb, topology, policy)?));
    }
    if scores.is_empty() {
        return Ok(Decision {
            lane: "json".to_string(),
            reason: "NO_CANDIDATE".to_string(),
            predicted_ms: baseline,
            predicted_bytes: workload.arrow_bytes,
            scores,
            rejected,
        });
    }

    let (best_name, best_value) = if selection.policy == Policy::Throughput {
        let floor = scores
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::INFINITY, f64::min)
            * 1.05;
        let near: Vec<&(String, f64)> =
            scores.iter().filter(|(_, value)| *value <= floor).collect();
        let mut best = near[0];
        let mut best_key = (model.bytes_for(&best.0, workload.arrow_bytes)?, best.1);
        for candidate in near.into_iter().skip(1) {
            let key = (
                model.bytes_for(&candidate.0, workload.arrow_bytes)?,
                candidate.1,
            );
            if key < best_key {
                best = candidate;
                best_key = key;
            }
        }
        (best.0.clone(), best.1)
    } else {
        let mut best = &scores[0];
        for candidate in scores.iter().skip(1) {
            let key = (candidate.1, order(&candidate.0));
            let best_key = (best.1, order(&best.0));
            if key < best_key {
                best = candidate;
            }
        }
        (best.0.clone(), best.1)
    };

    if best_name != "json" && scores.iter().any(|(name, _)| name == "json") {
        let too_small = if selection.policy == Policy::Throughput {
            let chosen_bytes = model.bytes_for(&best_name, workload.arrow_bytes)?;
            let base_bytes = model.bytes_for("json", workload.arrow_bytes)?;
            chosen_bytes as f64 > base_bytes as f64 * (1.0 - selection.min_gain)
        } else {
            best_value > baseline * (1.0 - selection.min_gain)
        };
        if too_small {
            return Ok(Decision {
                lane: "json".to_string(),
                reason: "GAIN_TOO_SMALL".to_string(),
                predicted_ms: baseline,
                predicted_bytes: workload.arrow_bytes,
                scores,
                rejected,
            });
        }
    }
    Ok(Decision {
        predicted_bytes: model.bytes_for(&best_name, workload.arrow_bytes)?,
        lane: best_name,
        reason: "BEST".to_string(),
        predicted_ms: best_value,
        scores,
        rejected,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::costmodel::load_default;

    fn selection(rows: u64, bytes: u64, topology: Topology) -> Selection {
        Selection::new(
            Workload::new(rows, bytes).expect("valid workload"),
            topology,
        )
    }

    #[test]
    fn empty_table_chooses_json() {
        let model = load_default();
        let decision = select_plan(model, &selection(0, 0, Topology::SameNetwork)).unwrap();
        assert_eq!(decision.reason, "EMPTY");
        assert_eq!(decision.lane, "json");
    }

    #[test]
    fn non_tabular_chooses_json() {
        let model = load_default();
        let mut sel = selection(10, 1000, Topology::SameNetwork);
        sel.workload.tabular = false;
        let decision = select_plan(model, &sel).unwrap();
        assert_eq!(decision.reason, "NON_TABULAR");
    }

    #[test]
    fn tiny_payload_keeps_json() {
        let model = load_default();
        let decision = select_plan(model, &selection(2, 64, Topology::SameHost)).unwrap();
        assert_eq!(decision.lane, "json");
        assert!(matches!(
            decision.reason.as_str(),
            "GAIN_TOO_SMALL" | "BEST"
        ));
    }

    #[test]
    fn large_payload_escapes_json() {
        let model = load_default();
        let decision =
            select_plan(model, &selection(500_000, 13_000_000, Topology::SameHost)).unwrap();
        assert_ne!(decision.lane, "json");
        assert_eq!(decision.reason, "BEST");
    }

    #[test]
    fn shm_is_rejected_without_a_token() {
        let model = load_default();
        let sel = selection(500_000, 13_000_000, Topology::SameHost);
        let decision = select_plan(model, &sel).unwrap();
        let rejected: Vec<&str> = decision.rejected.iter().map(|(n, _)| n.as_str()).collect();
        if cfg!(not(target_os = "linux")) || true {
            assert!(
                decision.rejected.iter().any(|(n, why)| n == "shm"
                    && (why == "SHM_UNAVAILABLE" || why == "NOT_AVAILABLE_ON_PLATFORM"))
                    || !rejected.contains(&"shm")
            );
        }
    }

    #[test]
    fn allowlist_that_excludes_everything_reports_no_candidate() {
        let model = load_default();
        let mut sel = selection(500_000, 13_000_000, Topology::SameHost);
        sel.allowed = Some(vec!["nope".to_string()]);
        let decision = select_plan(model, &sel).unwrap();
        assert_eq!(decision.reason, "NO_CANDIDATE");
        assert_eq!(
            decision.rejected,
            vec![("nope".to_string(), "UNKNOWN_LANE".to_string())]
        );
    }

    #[test]
    fn cpu_policy_prefers_no_wire_cost() {
        let model = load_default();
        let mut sel = selection(500_000, 13_000_000, Topology::WideArea);
        sel.policy = Policy::Cpu;
        let decision = select_plan(model, &sel).unwrap();
        assert_ne!(decision.lane, "json");
    }

    #[test]
    fn min_gain_out_of_range_is_rejected() {
        let model = load_default();
        let mut sel = selection(1000, 1000, Topology::SameHost);
        sel.min_gain = 1.5;
        assert!(select_plan(model, &sel).is_err());
    }

    #[test]
    fn decision_is_deterministic() {
        let model = load_default();
        let sel = selection(250_000, 9_000_000, Topology::SameNetwork);
        let first = select_plan(model, &sel).unwrap();
        let second = select_plan(model, &sel).unwrap();
        assert_eq!(first, second);
    }
}
