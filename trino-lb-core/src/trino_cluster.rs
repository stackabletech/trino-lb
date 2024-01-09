use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Hash, Serialize, IntoStaticStr)]
pub enum ClusterState {
    Unknown,
    /// Not running at all
    Stopped,
    /// Cluster is starting up but not ready yet. No queries should be submitted
    Starting,
    /// Up and running, ready to get queries
    Ready,
    /// No new queries should be submitted. Once all running queries are finished and a certain time period has passed
    /// go to `Terminating`
    Draining {
        last_time_seen_with_queries: SystemTime,
    },
    /// In the process of shutting down, don't send new queries
    Terminating,
    /// Deactivated by a system administrator (manually or programmatically), probably for maintenance
    Deactivated,
}

impl ClusterState {
    pub fn start(&self) -> Self {
        match self {
            ClusterState::Unknown
            | ClusterState::Stopped
            | ClusterState::Starting
            | ClusterState::Terminating => ClusterState::Starting,
            ClusterState::Ready | ClusterState::Draining { .. } => ClusterState::Ready,
            ClusterState::Deactivated => ClusterState::Deactivated,
        }
    }

    pub fn can_be_started(&self) -> bool {
        match self {
            ClusterState::Stopped | ClusterState::Draining { .. } => true,
            ClusterState::Unknown
            // No, because it is already started
            | ClusterState::Starting
            | ClusterState::Ready
            | ClusterState::Terminating
            | ClusterState::Deactivated => false,
        }
    }

    pub fn ready_to_accept_queries(&self) -> bool {
        match self {
            ClusterState::Unknown
            | ClusterState::Stopped
            | ClusterState::Draining { .. }
            | ClusterState::Terminating
            | ClusterState::Starting
            | ClusterState::Deactivated => false,
            ClusterState::Ready => true,
        }
    }
}
