use std::fmt::Display;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::TrinoQueryId;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoEvent {
    pub metadata: TrinoEventMetadata,
}

impl TrinoEvent {
    pub fn query_state(&self) -> &TrinoQueryState {
        &self.metadata.query_state
    }

    pub fn query_id(&self) -> &TrinoQueryId {
        &self.metadata.query_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrinoEventMetadata {
    pub uri: Url,
    pub query_id: TrinoQueryId,
    pub query_state: TrinoQueryState,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TrinoQueryState {
    Queued,
    Executing,
    Finished,
}

impl Display for TrinoQueryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Sticking to Trino casing for consistency
        match self {
            TrinoQueryState::Queued => write!(f, "QUEUED"),
            TrinoQueryState::Executing => write!(f, "EXECUTING"),
            TrinoQueryState::Finished => write!(f, "FINISHED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialization() {
        let query_crated: TrinoEvent = serde_json::from_str(include_str!(
            "../../tests/sample_trino_events/query_created.json"
        ))
        .expect("Failed to deserialize query created event");
        assert_eq!(query_crated.query_id(), "20250328_101456_00000_gt85c");
        assert_eq!(query_crated.query_state(), &TrinoQueryState::Queued);

        let query_finished: TrinoEvent = serde_json::from_str(include_str!(
            "../../tests/sample_trino_events/query_finished.json"
        ))
        .expect("Failed to deserialize query finished event");
        assert_eq!(query_finished.query_state(), &TrinoQueryState::Finished);
    }
}
