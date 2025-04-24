use serde::{Deserialize, Serialize};

/// Copied from [`prusto::Stat`], but with `root_stage`
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Stat {
    pub completed_splits: u32,
    pub cpu_time_millis: u64,
    pub elapsed_time_millis: u64,
    pub nodes: u32,
    pub peak_memory_bytes: u64,
    pub physical_input_bytes: u64,
    pub processed_bytes: u64,
    pub processed_rows: u64,
    pub progress_percentage: Option<f32>,
    pub queued_splits: u32,
    pub queued_time_millis: u64,
    pub queued: bool,
    pub root_stage: Option<serde_json::Value>,
    pub running_percentage: Option<f32>,
    pub running_splits: u32,
    pub scheduled: bool,
    pub spilled_bytes: u64,
    pub state: String,
    pub total_splits: u32,
    pub wall_time_millis: u64,
}
