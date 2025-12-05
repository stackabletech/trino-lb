use std::{fmt::Display, time::SystemTime};

use serde::{Deserialize, Serialize};
use strum::IntoStaticStr;

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Hash, Serialize, IntoStaticStr)]
pub enum ClusterState {
    Unknown,
    /// Not running at all
    Stopped,
    /// Cluster is starting up but not ready yet. No queries should be submitted
    Starting,
    /// Up and running, ready to get queries
    Ready,
    /// Up, but not ready to accept queries. It should not be started or stopped, as it's healing itself.
    Unhealthy,
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

impl Display for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterState::Unknown => f.write_str("Unknown"),
            ClusterState::Stopped => f.write_str("Stopped"),
            ClusterState::Starting => f.write_str("Starting"),
            ClusterState::Ready => f.write_str("Ready"),
            ClusterState::Unhealthy => f.write_str("Unhealthy"),
            ClusterState::Draining { .. } => f.write_str("Draining"),
            ClusterState::Terminating => f.write_str("Terminating"),
            ClusterState::Deactivated => f.write_str("Deactivated"),
        }
    }
}

impl ClusterState {
    pub fn start(&self) -> Self {
        match self {
            ClusterState::Unknown
            | ClusterState::Stopped
            | ClusterState::Starting
            | ClusterState::Terminating => ClusterState::Starting,
            ClusterState::Ready | ClusterState::Draining { .. } => ClusterState::Ready,
            ClusterState::Unhealthy => ClusterState::Unhealthy,
            ClusterState::Deactivated => ClusterState::Deactivated,
        }
    }

    pub fn mark_ready_if_not_deactivated(&self) -> Self {
        match self {
            ClusterState::Deactivated => Self::Deactivated,
            _ => ClusterState::Ready,
        }
    }

    pub fn can_be_started(&self) -> bool {
        match self {
            ClusterState::Stopped | ClusterState::Draining { .. } => true,
            ClusterState::Unknown
            // No, because it is already started
            | ClusterState::Starting
            | ClusterState::Unhealthy
            | ClusterState::Ready
            | ClusterState::Terminating
            | ClusterState::Deactivated => false,
        }
    }

    pub fn ready_to_accept_queries(&self) -> bool {
        match self {
            ClusterState::Unknown
            | ClusterState::Unhealthy
            | ClusterState::Stopped
            | ClusterState::Draining { .. }
            | ClusterState::Terminating
            | ClusterState::Starting
            | ClusterState::Deactivated => false,
            ClusterState::Ready => true,
        }
    }
}
