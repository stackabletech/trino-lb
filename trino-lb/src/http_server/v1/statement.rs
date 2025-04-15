use std::{
    cmp::min,
    fmt::Debug,
    num::TryFromIntError,
    sync::Arc,
    time::{Duration, SystemTime, SystemTimeError},
};

use axum::{
    Json,
    extract::{Path, State},
    response::{IntoResponse, Response},
};
use futures::TryFutureExt;
use http::{HeaderMap, StatusCode, Uri};
use opentelemetry::KeyValue;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::time::Instant;
use tracing::{Instrument, debug, info, info_span, instrument, warn};
use trino_lb_core::{
    TrinoLbQueryId, TrinoQueryId,
    config::TrinoLbProxyMode,
    sanitization::Sanitize,
    trino_api::queries::TrinoQueryApiResponse,
    trino_query::{QueuedQuery, TrinoQuery},
};
use trino_lb_persistence::Persistence;
use url::Url;

use crate::{
    cluster_group_manager::{self, SendToTrinoResponse},
    http_server::AppState,
    maintenance::leftover_queries::UPDATE_QUEUED_QUERY_LAST_ACCESSED_INTERVAL,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to modify nextUri trino send us to point tu trino-lb"))]
    ModifyNextUri {
        source: trino_lb_core::trino_api::queries::Error,
    },

    #[snafu(display("Failed to convert queued query to trino query"))]
    ConvertQueuedQueryToTrinoQuery {
        source: trino_lb_core::trino_api::queries::Error,
    },

    #[snafu(display("Failed to store queued query in persistence"))]
    StoreQueuedQueryInPersistence { source: trino_lb_persistence::Error },

    #[snafu(display("Failed to load queued query with id {query_id:?} from persistence"))]
    LoadQueuedQueryFromPersistence {
        source: trino_lb_persistence::Error,
        query_id: TrinoLbQueryId,
    },

    #[snafu(display("Failed to delete queued query with id {query_id:?} from persistence"))]
    DeleteQueuedQueryFromPersistence {
        source: trino_lb_persistence::Error,
        query_id: TrinoLbQueryId,
    },

    #[snafu(display("Query with id {query_id:?} not found"))]
    QueryNotFound { query_id: TrinoQueryId },

    #[snafu(display("Failed to store query in persistence"))]
    StoreQueryInPersistence {
        source: trino_lb_persistence::Error,
        query_id: TrinoQueryId,
    },

    #[snafu(display("Failed to delete query from persistence"))]
    DeleteQueryFromPersistence {
        source: trino_lb_persistence::Error,
        query_id: TrinoQueryId,
    },

    #[snafu(display("Failed to load query with id {query_id:?} from persistence"))]
    LoadQueryFromPersistence {
        source: trino_lb_persistence::Error,
        query_id: TrinoQueryId,
    },

    #[snafu(display("Failed to find best cluster for cluster group {cluster_group}"))]
    FindBestClusterForClusterGroup {
        source: cluster_group_manager::Error,
        cluster_group: String,
    },

    #[snafu(display("Failed to send query to trino"))]
    SendQueryToTrino {
        source: cluster_group_manager::Error,
    },

    #[snafu(display("Failed to cancel query on trino"))]
    CancelQueryOnTrino {
        source: cluster_group_manager::Error,
    },

    #[snafu(display("Failed to ask trino for query state"))]
    AskTrinoForQueryState {
        source: cluster_group_manager::Error,
    },

    #[snafu(display(
        "Failed to decrement the query counter query trino cluster {trino_cluster:?}"
    ))]
    DecClusterQueryCounter {
        source: trino_lb_persistence::Error,
        trino_cluster: String,
    },

    #[snafu(display(
        "Failed to determine how long the query was queued. Maybe the clocks are out of sync"
    ))]
    DetermineQueuedDuration { source: SystemTimeError },

    #[snafu(display(
        "Failed to determine how long ago the query was access last. Maybe the clocks are out of sync"
    ))]
    DetermineLastAccessedDuration { source: SystemTimeError },

    #[snafu(display(
        "Failed to convert queued time {queued_duration:?} to milliseconds contained in a u64. This should not happen, as that would mean the query took forever"
    ))]
    ConvertQueuedDurationToMillis {
        source: TryFromIntError,
        queued_duration: Duration,
    },

    #[snafu(display(
        "Failed to join the path of the current request {requested_path:?} to the Trino endpoint {trino_endpoint}"
    ))]
    JoinRequestPathToTrinoEndpoint {
        source: url::ParseError,
        requested_path: String,
        trino_endpoint: Url,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        warn!(error = ?self, "Error while processing request");
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{self:?}")).into_response()
    }
}

/// This function gets a new query and decided wether to queue it or to send it to a Trino cluster directly.
#[instrument(name = "POST /v1/statement", skip(state, headers, query))]
pub async fn post_statement(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    query: String,
) -> Result<SendToTrinoResponse, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "post_statement")]);

    let cluster_group = state
        .router
        .get_target_cluster_group(&query, &headers)
        .await;

    // While we technically construct an [`QueuedQuery`] object here, this does not mean the query will be queued!
    // We just use the same code flow for queued and (non-queued) fresh queries from the initial POST.
    let queued_query = QueuedQuery::new_from(query, headers, cluster_group);

    queue_or_hand_over_query(&state, queued_query, false, 0).await
}

/// This function get's asked about the current state of a query that is queued in trino-lb.
/// It either replies with "please hold the line" or forwards the query to an Trino cluster.
#[instrument(
    name = "GET /v1/statement/queued_in_trino_lb/{queryId}/{sequenceNumber}",
    skip(state, sequence_number)
)]
pub async fn get_trino_lb_statement(
    State(state): State<Arc<AppState>>,
    Path((query_id, sequence_number)): Path<(TrinoLbQueryId, u64)>,
) -> Result<SendToTrinoResponse, Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "get_trino_lb_statement")]);

    let queued_query = state
        .persistence
        .load_queued_query(&query_id)
        .await
        .context(LoadQueuedQueryFromPersistenceSnafu {
            query_id: &query_id,
        })?;

    queue_or_hand_over_query(&state, queued_query, true, sequence_number).await
}

/// This function get's asked about the current state of a query that is already sent to an
/// Trino cluster, but is still queued on the Trino cluster.
///
/// In case the nextUri is null, the query will be stopped and removed from trino-lb.
#[instrument(
    name = "GET /v1/statement/queued/{queryId}/{slug}/{token}",
    skip(state, headers)
)]
pub async fn get_trino_queued_statement(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((query_id, _, _)): Path<(TrinoQueryId, String, u64)>,
    uri: Uri,
) -> Result<(HeaderMap, Json<TrinoQueryApiResponse>), Error> {
    state.metrics.http_counter.add(
        1,
        &[KeyValue::new("resource", "get_trino_queued_statement")],
    );

    handle_query_running_on_trino(&state, headers, query_id, uri.path()).await
}

/// This function get's asked about the current state of a query that is already sent to an
/// Trino cluster and currently running.
///
/// In case the nextUri is null, the query will be stopped and removed from trino-lb.
#[instrument(
    name = "GET /v1/statement/executing/{queryId}/{slug}/{token}",
    skip(state, headers, uri)
)]
pub async fn get_trino_executing_statement(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((query_id, _, _)): Path<(TrinoQueryId, String, u64)>,
    uri: Uri,
) -> Result<(HeaderMap, Json<TrinoQueryApiResponse>), Error> {
    state.metrics.http_counter.add(
        1,
        &[KeyValue::new("resource", "get_trino_executing_statement")],
    );

    handle_query_running_on_trino(&state, headers, query_id, uri.path()).await
}

#[instrument(skip_all)]
async fn queue_or_hand_over_query(
    state: &Arc<AppState>,
    queued_query: QueuedQuery,
    queued_query_already_stored_in_persistence: bool,
    current_sequence_number: u64,
) -> Result<SendToTrinoResponse, Error> {
    let QueuedQuery {
        id: queued_query_id,
        query,
        headers,
        creation_time,
        last_accessed,
        cluster_group,
    } = &queued_query;
    let proxy_mode = &state.config.trino_lb.proxy_mode;

    let start_of_request = Instant::now();

    let best_cluster_for_group = state
        .cluster_group_manager
        .try_find_best_cluster_for_group(cluster_group)
        .await
        .context(FindBestClusterForClusterGroupSnafu { cluster_group })?;

    if let Some(cluster) = best_cluster_for_group {
        debug!(
            cluster = cluster.name,
            "Found cluster that has sufficient space"
        );

        let has_increased = state
            .persistence
            .inc_cluster_query_count(&cluster.name.to_string(), cluster.max_running_queries)
            .await
            .context(DecClusterQueryCounterSnafu {
                trino_cluster: &cluster.name,
            })?;

        if !has_increased {
            debug!(
                cluster = cluster.name,
                "The cluster had enough space when asked for the best cluster, but inc_cluster_query_count returned None, \
                    probably because the cluster has reached its maximum query count in the meantime"
            );
        }

        // let cont = match proxy_mode {
        //     // Only continue when the increment was successful
        //     TrinoLbProxyMode::ProxyAllCalls => {
        //         let has_increased = state
        //             .persistence
        //             .inc_cluster_query_count(&cluster.name.to_string(), cluster.max_running_queries)
        //             .await
        //             .context(DecClusterQueryCounterSnafu {
        //                 trino_cluster: &cluster.name,
        //             })?;

        //         if !has_increased {
        //             debug!(
        //                 cluster = cluster.name,
        //                 "The cluster had enough space when asked for the best cluster, but inc_cluster_query_count returned None, \
        //                         probably because the cluster has reached its maximum query count in the meantime"
        //             );
        //         }

        //         has_increased
        //     }
        //     // Always continue, we don't store any query in the persistence
        //     TrinoLbProxyMode::ProxyFirstCall => true,
        // };

        if has_increased {
            let mut send_to_trino_response = state
                .cluster_group_manager
                .send_query_to_cluster(query.clone(), headers.clone(), cluster)
                .await
                .context(SendQueryToTrinoSnafu)?;

            match send_to_trino_response {
                SendToTrinoResponse::HandedOver {
                    ref mut trino_query_api_response,
                    ..
                } => {
                    let queued_duration = creation_time
                        .elapsed()
                        .context(DetermineQueuedDurationSnafu)?;
                    state.metrics.queued_time.record(
                        queued_duration
                            .as_millis()
                            .try_into()
                            .context(ConvertQueuedDurationToMillisSnafu { queued_duration })?,
                        &[],
                    );

                    if trino_query_api_response.next_uri.is_some() {
                        match state.config.trino_lb.proxy_mode {
                            TrinoLbProxyMode::ProxyAllCalls => {
                                // Only store the query and change the nextURI to trino-lb in case it should proxy all
                                // calls
                                let query = TrinoQuery::new_from(
                                    cluster.name.clone(),
                                    trino_query_api_response.id.clone(),
                                    cluster.endpoint.clone(),
                                    *creation_time,
                                    SystemTime::now(),
                                );
                                let query_id = query.id.clone();

                                state.persistence.store_query(query).await.context(
                                    StoreQueryInPersistenceSnafu {
                                        query_id: &query_id,
                                    },
                                )?;

                                trino_query_api_response
                                    .change_next_uri_to_trino_lb(
                                        &state.config.trino_lb.external_address,
                                    )
                                    .context(ModifyNextUriSnafu)?;
                            }
                            TrinoLbProxyMode::ProxyFirstCall => {
                                // In case http-server.process-forwarded is set to true, Trino might choose the original
                                // host, which is trino-lb. But maybe also not, not entirely sure. As we *don't* want to
                                // send future calls to trino-lb, we need to actively change the nextUri to the Trino
                                // cluster. Better safe than sorry!
                                trino_query_api_response
                                    .change_next_uri_to_trino(&cluster.endpoint)
                                    .context(ModifyNextUriSnafu)?;
                            }
                        }

                        info!(
                            trino_cluster_name = cluster.name,
                            "Successfully handed query over to Trino cluster"
                        );
                    } else {
                        warn!(
                            trino_cluster_name = cluster.name,
                            new_query_id = trino_query_api_response.id,
                            "Trino got our query but send no nextUri. Maybe an Syntax error or something similar?"
                        );

                        if proxy_mode == &TrinoLbProxyMode::ProxyAllCalls {
                            // The queued query will be removed from the persistence below.
                            // As the query is probably finished, lets decrement the query counter again.
                            state
                                .persistence
                                .dec_cluster_query_count(&cluster.name)
                                .await
                                .context(DecClusterQueryCounterSnafu {
                                    trino_cluster: &cluster.name,
                                })?;
                        }
                        // We don't increment the query counter in ProxyFirstCall mode, as we assume
                        // we get a query event that the query finished (even if it fails). It might
                        // be the case that this assumption is wrong, but in this case we are better
                        // safe than sorry and don't decrement the counter, we'd rather have one
                        // query to few on the cluster instead of too much. A periodic query counter
                        // sync is in place anyway.
                    }
                }
                SendToTrinoResponse::Unauthorized { .. } => {
                    // As the query was not actually started decrement the query counter again
                    // (in all proxy modes)
                    state
                        .persistence
                        .dec_cluster_query_count(&cluster.name)
                        .await
                        .context(DecClusterQueryCounterSnafu {
                            trino_cluster: &cluster.name,
                        })?;

                    // We don't need to store any information about this request in the persistence, as the client will
                    // retry the POST /v1/statement shortly with the correct `Authorization` header set.
                }
            }

            if queued_query_already_stored_in_persistence {
                state
                    .persistence
                    .remove_queued_query(&queued_query)
                    .await
                    .context(DeleteQueuedQueryFromPersistenceSnafu {
                        query_id: queued_query_id,
                    })?;
            }

            return Ok(send_to_trino_response);
        }
    }

    let trino_lb_query_api_response = TrinoQueryApiResponse::new_from_queued_query(
        &queued_query,
        current_sequence_number,
        &state.config.trino_lb.external_address,
    )
    .context(ConvertQueuedQueryToTrinoQuerySnafu)?;

    if !queued_query_already_stored_in_persistence {
        state
            .persistence
            .store_queued_query(queued_query)
            .await
            .context(StoreQueuedQueryInPersistenceSnafu)?;
    } else if last_accessed
        .elapsed()
        .context(DetermineLastAccessedDurationSnafu)?
        >= UPDATE_QUEUED_QUERY_LAST_ACCESSED_INTERVAL
    {
        let queued_query = QueuedQuery {
            last_accessed: SystemTime::now(),
            ..queued_query
        };
        state
            .persistence
            .store_queued_query(queued_query)
            .await
            .context(StoreQueuedQueryInPersistenceSnafu)?;
    }

    // We slow down here, so that clients don't flood us with status requests. We skip this for the first request,
    // so that e.g. trino-cli imminently shows the query is queued in trino-lb (at least in theory - in practice
    // trino-cli behaves a bit strange).
    if current_sequence_number > 1 {
        let delay = delay_for_sequence_number(current_sequence_number);
        tokio::time::sleep(delay.saturating_sub(start_of_request.elapsed()))
            .instrument(info_span!("Delaying response to slow down clients", ?delay))
            .await;
    }

    Ok(SendToTrinoResponse::HandedOver {
        trino_query_api_response: trino_lb_query_api_response,
        headers: HeaderMap::new(),
    })
}

#[instrument(skip(state, headers))]
async fn handle_query_running_on_trino(
    state: &Arc<AppState>,
    headers: HeaderMap,
    query_id: TrinoQueryId,
    requested_path: &str,
) -> Result<(HeaderMap, Json<TrinoQueryApiResponse>), Error> {
    let query = state
        .persistence
        .load_query(&query_id)
        .await
        .with_context(|_| LoadQueryFromPersistenceSnafu {
            query_id: query_id.clone(),
        })?
        .with_context(|| QueryNotFoundSnafu {
            query_id: query_id.clone(),
        })?;

    let (mut trino_query_api_response, trino_headers) = state
        .cluster_group_manager
        .ask_for_query_state(
            query.trino_endpoint.join(requested_path).context(
                JoinRequestPathToTrinoEndpointSnafu {
                    requested_path,
                    trino_endpoint: query.trino_endpoint.clone(),
                },
            )?,
            headers,
        )
        .await
        .context(AskTrinoForQueryStateSnafu)?;

    if trino_query_api_response.next_uri.is_some() {
        // Only change the nextURI to trino-lb in case it should proxy all calls
        if state.config.trino_lb.proxy_mode == TrinoLbProxyMode::ProxyAllCalls {
            trino_query_api_response
                .change_next_uri_to_trino_lb(&state.config.trino_lb.external_address)
                .context(ModifyNextUriSnafu)?;
        }
    } else {
        info!(%query_id, "Query completed (no next_uri send)");

        tokio::try_join!(
            state.persistence.remove_query(&query_id).map_err(|err| {
                Error::DeleteQueryFromPersistence {
                    source: err,
                    query_id: query_id.to_owned(),
                }
            }),
            state
                .persistence
                .dec_cluster_query_count(&query.trino_cluster)
                .map_err(|err| {
                    Error::DecClusterQueryCounter {
                        source: err,
                        trino_cluster: query.trino_cluster.to_owned(),
                    }
                }),
        )?;
    }

    Ok((trino_headers, Json(trino_query_api_response)))
}

/// This function get's asked to delete the queued query.
/// IMPORTANT: It does not check that the user is authorized to delete the queued query. Instead we assume that the
/// random part of the queryId trino-lb generates provides sufficient protection, as other clients can not extract
/// the queryId. Please note that this only applies to queries queued in trino-lb, not running on Trino.
#[instrument(
    name = "DELETE /v1/statement/queued_in_trino_lb/{queryId}/{sequenceNumber}",
    skip(state)
)]
pub async fn delete_trino_lb_statement(
    State(state): State<Arc<AppState>>,
    Path((query_id, _)): Path<(TrinoLbQueryId, u64)>,
) -> Result<(), Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "delete_trino_lb_statement")]);

    let queued_query = state
        .persistence
        .load_queued_query(&query_id)
        .await
        .context(LoadQueryFromPersistenceSnafu {
            query_id: &query_id,
        })?;
    state
        .persistence
        .remove_queued_query(&queued_query)
        .await
        .context(DeleteQueuedQueryFromPersistenceSnafu { query_id })?;

    Ok(())
}

/// This function get's asked to cancel a query that is already sent to an Trino cluster, but is still queued on the
/// Trino cluster.
#[instrument(
    name = "DELETE /v1/statement/queued/{queryId}/{slug}/{token}",
    skip(state),
    fields(headers = ?headers.sanitize()),
)]
pub async fn delete_trino_queued_statement(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((query_id, _, _)): Path<(TrinoQueryId, String, u64)>,
    uri: Uri,
) -> Result<(), Error> {
    state.metrics.http_counter.add(
        1,
        &[KeyValue::new("resource", "delete_trino_queued_statement")],
    );

    cancel_query_on_trino(headers, &state, query_id, uri.path()).await
}

/// This function get's asked to cancel a query that is already sent to an Trino cluster and currently running.
#[instrument(
    name = "DELETE /v1/statement/executing/{queryId}/{slug}/{token}",
    skip(state, uri),
    fields(headers = ?headers.sanitize()),
)]
pub async fn delete_trino_executing_statement(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path((query_id, _, _)): Path<(TrinoQueryId, String, u64)>,
    uri: Uri,
) -> Result<(), Error> {
    state.metrics.http_counter.add(
        1,
        &[KeyValue::new(
            "resource",
            "delete_trino_executing_statement",
        )],
    );

    cancel_query_on_trino(headers, &state, query_id, uri.path()).await
}

#[instrument(skip(state, headers))]
async fn cancel_query_on_trino(
    headers: HeaderMap,
    state: &Arc<AppState>,
    query_id: TrinoQueryId,
    requested_path: &str,
) -> Result<(), Error> {
    state
        .metrics
        .http_counter
        .add(1, &[KeyValue::new("resource", "cancel_query_on_trino")]);

    let query = state
        .persistence
        .load_query(&query_id)
        .await
        .with_context(|_| StoreQueryInPersistenceSnafu {
            query_id: query_id.clone(),
        })?
        .context(QueryNotFoundSnafu { query_id })?;

    state
        .cluster_group_manager
        .cancel_query_on_trino(headers, &query, requested_path)
        .await
        .context(CancelQueryOnTrinoSnafu)?;

    // We don't need to remove the query or decrement the query counter, as the client will continue polling the state
    // of the query. So the normal flow of the last request returning a response without a nextUri will work here as
    // well.

    Ok(())
}

/// This should be a duration most setups should be able to handle without timeouts (e.g. by Nginx or similar)
/// It's a tradeoff between query responsiveness and the load (HTTP requests/s) on trino-lb.
const MAX_POLL_DELAY: Duration = Duration::from_secs(3);

fn delay_for_sequence_number(sequence_number: u64) -> Duration {
    if sequence_number == 0 {
        return Duration::ZERO;
    }
    let Ok(sequence_number): Result<u32, _> = sequence_number.try_into() else {
        // At such high sequence numbers we can just return the max delay
        return MAX_POLL_DELAY;
    };

    let Some(millis) = 2_u64.checked_pow(sequence_number.saturating_add(7)) else {
        // At such high sequence numbers we can just return the max delay
        return MAX_POLL_DELAY;
    };

    min(Duration::from_millis(millis), MAX_POLL_DELAY)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(0, Duration::from_millis(0))]
    #[case(1, Duration::from_millis(256))]
    #[case(2, Duration::from_millis(512))]
    #[case(3, Duration::from_millis(1024))]
    #[case(4, Duration::from_millis(2048))]
    #[case(5, MAX_POLL_DELAY)]
    #[case(6, MAX_POLL_DELAY)]
    #[case(7, MAX_POLL_DELAY)]
    #[case(8, MAX_POLL_DELAY)]
    #[case(u32::MAX as u64 - 1, MAX_POLL_DELAY)]
    #[case(u32::MAX as u64, MAX_POLL_DELAY)]
    #[case(u32::MAX as u64 + 1, MAX_POLL_DELAY)]
    #[case(u64::MAX - 1, MAX_POLL_DELAY)]
    #[case(u64::MAX, MAX_POLL_DELAY)]
    fn test_delay_for_sequence_number(
        #[case] sequence_number: u64,
        #[case] expected_delay: Duration,
    ) {
        assert_eq!(delay_for_sequence_number(sequence_number), expected_delay);
    }
}
