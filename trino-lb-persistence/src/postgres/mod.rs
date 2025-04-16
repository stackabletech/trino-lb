use std::{
    num::TryFromIntError,
    time::{SystemTime, UNIX_EPOCH},
};

use http::HeaderMap;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use sqlx::{
    Pool, Postgres,
    migrate::MigrateError,
    postgres::PgPoolOptions,
    query,
    types::chrono::{DateTime, Utc},
};
use tracing::{debug, info, instrument, warn};
use trino_lb_core::{
    TrinoClusterName, TrinoLbQueryId, TrinoQueryId,
    config::PostgresConfig,
    trino_cluster::ClusterState,
    trino_query::{QueuedQuery, TrinoQuery},
};
use url::Url;

use crate::Persistence;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create Postgres connection pool"))]
    CreatePool { source: sqlx::Error },

    #[snafu(display("Failed to run database migrations"))]
    RunMigrations { source: MigrateError },

    #[snafu(display("Failed to start transaction"))]
    StartTransaction { source: sqlx::Error },

    #[snafu(display("Failed to commit transaction"))]
    CommitTransaction { source: sqlx::Error },

    #[snafu(display("Failed to rollback transaction"))]
    RollbackTransaction { source: sqlx::Error },

    #[snafu(display("Failed to store queued query"))]
    StoreQueuedQuery { source: sqlx::Error },

    #[snafu(display("Failed to load queued query"))]
    LoadQueuedQuery { source: sqlx::Error },

    #[snafu(display("Failed to delete queued query"))]
    DeleteQueuedQuery { source: sqlx::Error },

    #[snafu(display("Failed to store query"))]
    StoreQuery { source: sqlx::Error },

    #[snafu(display("Failed to load query"))]
    LoadQuery { source: sqlx::Error },

    #[snafu(display("Failed to delete query"))]
    DeleteQuery { source: sqlx::Error },

    #[snafu(display("Failed to get current queued query counter"))]
    GetCurrentQueuedQueryCounter { source: sqlx::Error },

    #[snafu(display("Failed to set current queued query counter"))]
    SetCurrentQueuedQueryCounter { source: sqlx::Error },

    #[snafu(display("Failed to get current query counter"))]
    GetCurrentQueryCounter { source: sqlx::Error },

    #[snafu(display("Failed to set current query counter"))]
    SetCurrentQueryCounter { source: sqlx::Error },

    #[snafu(display("Failed to get current cluster state"))]
    GetCurrentClusterState { source: sqlx::Error },

    #[snafu(display("Failed to set current cluster state"))]
    SetCurrentClusterState { source: sqlx::Error },

    #[snafu(display("Failed to get last query count fetcher update"))]
    GetLastQueryCountFetcherUpdate { source: sqlx::Error },

    #[snafu(display("Failed to set last query count fetcher update"))]
    SetLastQueryCountFetcherUpdate { source: sqlx::Error },

    #[snafu(display("Failed to parse headers of stored queued query"))]
    ParseHeadersOfStoredQueuedQuery { source: serde_json::Error },

    #[snafu(display("Failed to parse state of stored cluster state"))]
    ParseStateOfStoredClusterState { source: serde_json::Error },

    #[snafu(display("Failed to parse endpoint url of cluster from stored query"))]
    ParseClusterEndpointFromStoredQuery { source: url::ParseError },

    #[snafu(display("Failed to convert max query counter to u64, as it is too high"))]
    ConvertMaxAllowedQueryCounterToU64 { source: TryFromIntError },

    #[snafu(display("Failed to convert current query counter to u64, as it is too high"))]
    ConvertCurrentQueryCounterToU64 { source: TryFromIntError },

    #[snafu(display("Failed to convert current queued query counter to u64, as it is too high"))]
    ConvertCurrentQueuedQueryCounterToU64 { source: TryFromIntError },

    #[snafu(display("Failed to convert current query counter to u64, as it is too high"))]
    ConvertStoredQueryCounterToU64 { source: TryFromIntError },
}

pub struct PostgresPersistence {
    pool: Pool<Postgres>,
}

impl PostgresPersistence {
    pub async fn new(config: &PostgresConfig) -> Result<Self, Error> {
        warn!(
            "Please note that the Postgres persistence is experimental! We have seen a few queries too much being send to the Trino clusters, probably related to some transactional problems"
        );

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(config.url.as_str())
            .await
            .context(CreatePoolSnafu)?;

        sqlx::migrate!("src/postgres/migrations")
            .run(&pool)
            .await
            .context(RunMigrationsSnafu)?;

        Ok(Self { pool })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct HeaderMapWrapper {
    #[serde(flatten, with = "http_serde::header_map")]
    pub inner: HeaderMap,
}

impl Persistence for PostgresPersistence {
    #[instrument(skip(self, queued_query))]
    async fn store_queued_query(&self, queued_query: QueuedQuery) -> Result<(), super::Error> {
        query!(
            r#"INSERT INTO queued_queries (id, query, headers, creation_time, last_accessed, cluster_group)
            VALUES ($1, $2, $3, $4, $5, $6)"#,
            queued_query.id,
            queued_query.query,
            sqlx::types::Json(HeaderMapWrapper {
                inner: queued_query.headers
            }) as _,
            Into::<DateTime<Utc>>::into(queued_query.creation_time),
            Into::<DateTime<Utc>>::into(queued_query.last_accessed),
            queued_query.cluster_group,
        )
        .execute(&self.pool)
        .await
        .context(StoreQueuedQuerySnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_queued_query(
        &self,
        queued_query_id: &TrinoLbQueryId,
    ) -> Result<QueuedQuery, super::Error> {
        let result = query!(
            r#"SELECT id, query, headers, creation_time, last_accessed, cluster_group
            FROM queued_queries
            WHERE id = $1"#,
            queued_query_id,
        )
        .fetch_one(&self.pool)
        .await
        .context(LoadQueuedQuerySnafu)?;

        let headers: HeaderMapWrapper =
            serde_json::from_value(result.headers).context(ParseHeadersOfStoredQueuedQuerySnafu)?;
        let queued_query = QueuedQuery {
            id: result.id,
            query: result.query,
            headers: headers.inner,
            creation_time: result.creation_time.into(),
            last_accessed: result.last_accessed.into(),
            cluster_group: result.cluster_group,
        };

        Ok(queued_query)
    }

    #[instrument(skip(self, queued_query))]
    async fn remove_queued_query(&self, queued_query: &QueuedQuery) -> Result<(), super::Error> {
        query!(
            r#"DELETE FROM queued_queries
            WHERE id = $1"#,
            queued_query.id,
        )
        .execute(&self.pool)
        .await
        .context(DeleteQueuedQuerySnafu)?;

        Ok(())
    }

    #[instrument(skip(self, query))]
    async fn store_query(&self, query: TrinoQuery) -> Result<(), super::Error> {
        query!(
            r#"INSERT INTO queries (id, trino_cluster, trino_endpoint, creation_time, delivered_time)
            VALUES ($1, $2, $3, $4, $5)"#,
            query.id,
            query.trino_cluster,
            query.trino_endpoint.as_str(),
            Into::<DateTime<Utc>>::into(query.creation_time),
            Into::<DateTime<Utc>>::into(query.delivered_time),
        )
        .execute(&self.pool)
        .await
        .context(StoreQuerySnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn load_query(
        &self,
        query_id: &TrinoQueryId,
    ) -> Result<Option<TrinoQuery>, super::Error> {
        let result = query!(
            r#"SELECT id, trino_cluster, trino_endpoint, creation_time, delivered_time
            FROM queries
            WHERE id = $1"#,
            query_id,
        )
        .fetch_optional(&self.pool)
        .await
        .context(LoadQuerySnafu)?;

        let Some(result) = result else {
            return Ok(None);
        };

        let query = TrinoQuery {
            id: result.id,
            trino_cluster: result.trino_cluster,
            trino_endpoint: Url::parse(&result.trino_endpoint)
                .context(ParseClusterEndpointFromStoredQuerySnafu)?,
            creation_time: result.creation_time.into(),
            delivered_time: result.delivered_time.into(),
        };

        Ok(Some(query))
    }

    #[instrument(skip(self))]
    async fn remove_query(&self, query_id: &TrinoQueryId) -> Result<(), super::Error> {
        query!(
            r#"DELETE FROM queries
            WHERE id = $1"#,
            query_id,
        )
        .execute(&self.pool)
        .await
        .context(DeleteQuerySnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn inc_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        max_allowed_count: u64,
    ) -> Result<bool, super::Error> {
        let mut transaction = self.pool.begin().await.context(StartTransactionSnafu)?;

        let max_allowed_count: i64 = max_allowed_count
            .try_into()
            .context(ConvertMaxAllowedQueryCounterToU64Snafu)?;
        let current = query!(
            r#"SELECT count
            FROM cluster_query_counts
            WHERE cluster = $1"#,
            cluster_name,
        )
        .fetch_optional(&mut *transaction)
        .await
        .context(GetCurrentQueryCounterSnafu)?
        .map(|r| r.count)
        .unwrap_or_default();

        debug!(?current, "Current counter is");

        if current + 1 > max_allowed_count {
            debug!(
                current,
                max_allowed_count,
                "Rejected increasing the cluster query count, as the current count + 1 is bigger than the max allowed count"
            );
            transaction
                .rollback()
                .await
                .context(RollbackTransactionSnafu)?;
            return Ok(false);
        }

        query!(
            r#"INSERT INTO cluster_query_counts (cluster, count)
            VALUES ($1, $2)
            ON CONFLICT (cluster) DO UPDATE SET count = $2
            "#,
            cluster_name,
            current + 1,
        )
        .execute(&mut *transaction)
        .await
        .context(SetCurrentQueryCounterSnafu)?;

        transaction.commit().await.context(CommitTransactionSnafu)?;
        Ok(true)
    }

    #[instrument(skip(self))]
    async fn dec_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<(), super::Error> {
        let mut transaction = self.pool.begin().await.context(StartTransactionSnafu)?;

        let current = query!(
            r#"SELECT count
            FROM cluster_query_counts
            WHERE cluster = $1"#,
            cluster_name,
        )
        .fetch_optional(&mut *transaction)
        .await
        .context(GetCurrentQueryCounterSnafu)?
        .map(|r| r.count)
        .unwrap_or_default();

        debug!(?current, "Current counter is");

        if current <= 0 {
            debug!("Current value was already 0, nothing to do here");
            transaction
                .rollback()
                .await
                .context(RollbackTransactionSnafu)?;
            return Ok(());
        }

        query!(
            r#"INSERT INTO cluster_query_counts (cluster, count)
            VALUES ($1, $2)
            ON CONFLICT (cluster) DO UPDATE SET count = $2
            "#,
            cluster_name,
            current - 1,
        )
        .execute(&mut *transaction)
        .await
        .context(SetCurrentQueryCounterSnafu)?;

        transaction.commit().await.context(CommitTransactionSnafu)?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn set_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
        count: u64,
    ) -> Result<(), super::Error> {
        let mut transaction = self.pool.begin().await.context(StartTransactionSnafu)?;

        query!(
            r#"INSERT INTO cluster_query_counts (cluster, count)
            VALUES ($1, $2)
            ON CONFLICT (cluster) DO UPDATE SET count = $2
            "#,
            cluster_name,
            TryInto::<i64>::try_into(count).context(ConvertCurrentQueryCounterToU64Snafu)?,
        )
        .execute(&mut *transaction)
        .await
        .context(SetCurrentQueryCounterSnafu)?;

        transaction.commit().await.context(CommitTransactionSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_query_count(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<u64, super::Error> {
        let result = query!(
            r#"SELECT count
            FROM cluster_query_counts
            WHERE cluster = $1"#,
            cluster_name,
        )
        .fetch_optional(&self.pool)
        .await
        .context(GetCurrentQueryCounterSnafu)?;

        Ok(result
            .map(|r| r.count)
            // The count might not have been set yet
            .unwrap_or_default()
            .try_into()
            .context(ConvertStoredQueryCounterToU64Snafu)?)
    }

    #[instrument(skip(self))]
    async fn get_queued_query_count(&self, cluster_group: &str) -> Result<u64, super::Error> {
        Ok(query!(
            r#"SELECT count(*)
            FROM queued_queries
            WHERE cluster_group = $1"#,
            cluster_group,
        )
        .fetch_one(&self.pool)
        .await
        .context(GetCurrentQueuedQueryCounterSnafu)?
        .count
        .unwrap_or_default()
        .try_into()
        .context(ConvertCurrentQueuedQueryCounterToU64Snafu)?)
    }

    #[instrument(skip(self))]
    async fn delete_queued_queries_not_accessed_after(
        &self,
        not_accessed_after: SystemTime,
    ) -> Result<u64, super::Error> {
        let result = query!(
            r#"DELETE FROM queued_queries
            WHERE last_accessed < $1"#,
            Into::<DateTime<Utc>>::into(not_accessed_after),
        )
        .execute(&self.pool)
        .await
        .context(DeleteQuerySnafu)?;
        let removed = result.rows_affected();

        info!(
            removed,
            ?not_accessed_after,
            "Deleted all queries that were not accessed after"
        );

        Ok(removed)
    }

    #[instrument(skip(self))]
    async fn get_last_query_count_fetcher_update(&self) -> Result<SystemTime, super::Error> {
        let result = query!(
            r#"SELECT last_query_count_fetcher_update
            FROM last_query_count_fetcher_update"#
        )
        .fetch_optional(&self.pool)
        .await
        .context(GetLastQueryCountFetcherUpdateSnafu)?;

        let last_query_count_fetcher_update = result
            .map(|r| r.last_query_count_fetcher_update.into())
            // There might not have been an update so far
            .unwrap_or(UNIX_EPOCH);

        Ok(last_query_count_fetcher_update)
    }

    #[instrument(skip(self))]
    async fn set_last_query_count_fetcher_update(
        &self,
        update: SystemTime,
    ) -> Result<(), super::Error> {
        let mut transaction = self.pool.begin().await.context(StartTransactionSnafu)?;

        query!(
            r#"INSERT INTO last_query_count_fetcher_update (dummy, last_query_count_fetcher_update)
            VALUES ($1, $2)
            ON CONFLICT (dummy) DO UPDATE SET last_query_count_fetcher_update = $2
            "#,
            19971208,
            Into::<DateTime<Utc>>::into(update),
        )
        .execute(&mut *transaction)
        .await
        .context(SetLastQueryCountFetcherUpdateSnafu)?;

        transaction.commit().await.context(CommitTransactionSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn set_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
        state: ClusterState,
    ) -> Result<(), super::Error> {
        let mut transaction = self.pool.begin().await.context(StartTransactionSnafu)?;

        query!(
            r#"INSERT INTO cluster_states (id, state)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET state = $2
            "#,
            cluster_name,
            sqlx::types::Json(state) as _,
        )
        .execute(&mut *transaction)
        .await
        .context(SetCurrentClusterStateSnafu)?;

        transaction.commit().await.context(CommitTransactionSnafu)?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_cluster_state(
        &self,
        cluster_name: &TrinoClusterName,
    ) -> Result<ClusterState, super::Error> {
        let result = query!(
            r#"SELECT state
            FROM cluster_states
            WHERE id = $1"#,
            cluster_name,
        )
        .fetch_optional(&self.pool)
        .await
        .context(GetCurrentClusterStateSnafu)?;

        let cluster_state = match result {
            Some(result) => {
                serde_json::from_value(result.state).context(ParseStateOfStoredClusterStateSnafu)?
            }
            None => ClusterState::Unknown,
        };

        Ok(cluster_state)
    }
}
