use serde::Deserialize;

use crate::TrinoQueryId;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OriginalTrinoEvent {
    metadata: OriginalTrinoEventMetadata,
}

impl OriginalTrinoEvent {
    pub fn is_finished(&self) -> bool {
        self.metadata.query_state == OriginalQueryState::Finished
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OriginalTrinoEventMetadata {
    pub query_id: TrinoQueryId,
    pub query_state: OriginalQueryState,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OriginalQueryState {
    Queued,
    Executing,
    Finished,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialization() {
        let query_crated: OriginalTrinoEvent =
            serde_json::from_str(include_str!("sample_trino_events/query_created.json"))
                .expect("Failed to deserialize query created event");
        assert_eq!(
            query_crated.metadata.query_id,
            "20250328_101456_00000_gt85c"
        );
        assert_eq!(
            query_crated.metadata.query_state,
            OriginalQueryState::Queued,
        );
        assert!(!query_crated.is_finished());

        let query_finished: OriginalTrinoEvent =
            serde_json::from_str(include_str!("sample_trino_events/query_finished.json"))
                .expect("Failed to deserialize query finished event");
        assert!(query_finished.is_finished());
    }
}
