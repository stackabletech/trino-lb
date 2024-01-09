use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::time;
use tracing::{debug, error, info, info_span, Instrument};
#[cfg(doc)]
use trino_lb_core::trino_query::QueuedQuery;
use trino_lb_persistence::{Persistence, PersistenceImplementation};

/// Internal optimization to not always update [`QueuedQuery::last_accessed`] (as this causes unended persistence
/// traffic) but only once in a while.
///
/// Needs to be aligned with [`QUEUED_QUERY_CLIENT_TIMEOUT`]!
pub const UPDATE_QUEUED_QUERY_LAST_ACCESSED_INTERVAL: Duration = Duration::from_secs(2 * 60);

/// From Trino docs on `query.client.timeout`:
/// > Configures how long the cluster runs without contact from the client application, such as the CLI, before it abandons and cancels its work
///
/// Matches the default value of Trino.
pub const QUEUED_QUERY_CLIENT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

pub struct LeftoverQueryDetector {
    persistence: Arc<PersistenceImplementation>,
}

impl LeftoverQueryDetector {
    pub fn new(persistence: Arc<PersistenceImplementation>) -> Self {
        Self { persistence }
    }

    pub fn start_loop(self) {
        tokio::spawn(async move {
            // There is no point in checking more frequent than [`QueuedQuery::last_accessed`] is updated
            let mut interval = time::interval(UPDATE_QUEUED_QUERY_LAST_ACCESSED_INTERVAL);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

            loop {
                // First tick does not sleep, so let's put it at the start of the loop.
                interval.tick().await;

                async {
                    let not_accessed_after = SystemTime::now() - QUEUED_QUERY_CLIENT_TIMEOUT;
                    match self
                        .persistence
                        .delete_queued_queries_not_accessed_after(not_accessed_after)
                        .await
                    {
                        // Verbosity level defending on wether a queued query was removed
                        Ok(0) => debug!(
                            "LeftoverQueryDetector: Successfully checked for leftover queued queries"
                        ),
                        Ok(removed) => info!(
                            removed,
                            "LeftoverQueryDetector: Successfully removed leftover queued queries"
                        ),
                        Err(error) => error!(
                            ?error,
                            "LeftoverQueryDetector: Failed to check for leftover queued queries"
                        ),
                    }
                }
                .instrument(info_span!("Checking for leftover queued queries"))
                .await;
            }
        });
    }
}
