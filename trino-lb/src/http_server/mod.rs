use std::{
    fmt::Debug,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    response::Redirect,
    routing::{delete, get, post},
};
use axum_server::{Handle, tls_rustls::RustlsConfig};
use futures::FutureExt;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::time::sleep;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use tracing::info;
use trino_lb_persistence::PersistenceImplementation;

use crate::{
    cluster_group_manager::ClusterGroupManager, config::Config, metrics::Metrics, routing,
};

mod admin;
mod metrics;
mod ui;
mod v1;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "Failed configure HTTP server PEM cert at {cert_pem_file:?} and PEM key at {key_pem_file:?}"
    ))]
    ConfigureServerTrustAndKeystore {
        source: std::io::Error,
        cert_pem_file: PathBuf,
        key_pem_file: PathBuf,
    },

    #[snafu(display("Failed start HTTP server"))]
    StartHttpServer { source: std::io::Error },

    #[snafu(display("Failed to start Prometheus metrics exporter"))]
    StartMetricsExporter { source: std::io::Error },

    #[snafu(display(
        "In case https is used the `tls.certPemFile` and `tls.keyPemFile` options must be set"
    ))]
    CertsMissing,
}

pub struct AppState {
    config: Config,
    persistence: Arc<PersistenceImplementation>,
    cluster_group_manager: ClusterGroupManager,
    router: routing::Router,
    metrics: Arc<Metrics>,
}

pub async fn start_http_server(
    config: Config,
    persistence: Arc<PersistenceImplementation>,
    cluster_group_manager: ClusterGroupManager,
    router: routing::Router,
    metrics: Arc<Metrics>,
) -> Result<(), Error> {
    let tls_config = config.trino_lb.tls.clone();
    let ports_config = config.trino_lb.ports.clone();
    let app_state = Arc::new(AppState {
        config,
        persistence,
        cluster_group_manager,
        router,
        metrics,
    });

    // Start Prometheus metrics exporter
    let app = Router::new()
        .route("/", get(|| async { Redirect::permanent("/metrics") }))
        .route("/metrics", get(metrics::get))
        .with_state(Arc::clone(&app_state));
    let listen_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, ports_config.metrics));
    info!(%listen_addr, "Starting metrics exporter");

    let handle = Handle::new();
    tokio::spawn(graceful_shutdown(handle.clone()));

    // The metrics exporter is run concurrently with the main server (see `try_join!` below) rather than
    // on a detached task. This way a failure of either server (e.g. failing to bind the listen address)
    // brings down the whole trino-lb server instead of being silently ignored.
    let metrics_server = axum_server::bind(listen_addr)
        .handle(handle.clone())
        .serve(app.into_make_service())
        .map(|result| result.context(StartMetricsExporterSnafu));

    // Note that get routes will also be called for HEAD requests but will have the response body
    // removed. Make sure to add explicit HEAD routes afterwards.
    // See https://docs.rs/axum/latest/axum/routing/method_routing/fn.get.html
    let app = Router::new()
        .route("/", get(|| async { Redirect::permanent("/ui/index.html") }))
        .route("/v1/statement", post(v1::statement::post_statement))
        .route(
            "/v1/statement/queued_in_trino_lb/{query_id}/{sequence_number}",
            get(v1::statement::get_trino_lb_statement),
        )
        .route(
            "/v1/statement/queued/{query_id}/{slug}/{token}",
            get(v1::statement::get_trino_queued_statement),
        )
        .route(
            "/v1/statement/executing/{query_id}/{slug}/{token}",
            get(v1::statement::get_or_head_trino_executing_statement),
        )
        .route(
            "/v1/statement/queued_in_trino_lb/{query_id}/{sequence_number}",
            delete(v1::statement::delete_trino_lb_statement),
        )
        .route(
            "/v1/statement/queued/{query_id}/{slug}/{token}",
            delete(v1::statement::delete_trino_queued_statement),
        )
        .route(
            "/v1/statement/executing/{query_id}/{slug}/{token}",
            delete(v1::statement::delete_trino_executing_statement),
        )
        .route(
            "/admin/clusters/{cluster_name}/activate",
            post(admin::post_activate_cluster),
        )
        .route(
            "/admin/clusters/{cluster_name}/deactivate",
            post(admin::post_deactivate_cluster),
        )
        .route(
            "/admin/clusters/{cluster_name}/status",
            get(admin::get_cluster_status),
        )
        .route("/admin/clusters/status", get(admin::get_all_cluster_status))
        .route("/ui/index.html", get(ui::index::get_ui_index))
        .route("/ui/query.html", get(ui::query::get_ui_query))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .with_state(app_state);

    if tls_config.enabled {
        // Start https server
        let listen_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, ports_config.https));
        info!(%listen_addr, "Starting server");

        let cert_pem_file = tls_config.cert_pem_file.context(CertsMissingSnafu)?;
        let key_pem_file = tls_config.key_pem_file.context(CertsMissingSnafu)?;
        let tls_config = RustlsConfig::from_pem_file(&cert_pem_file, &key_pem_file)
            .await
            .context(ConfigureServerTrustAndKeystoreSnafu {
                cert_pem_file,
                key_pem_file,
            })?;

        let main_server = axum_server::bind_rustls(listen_addr, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .map(|result| result.context(StartHttpServerSnafu));

        tokio::try_join!(metrics_server, main_server)?;
    } else {
        // Start http server
        let listen_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, ports_config.http));
        info!(%listen_addr, "Starting server");

        let main_server = axum_server::bind(listen_addr)
            .handle(handle)
            .serve(app.into_make_service())
            .map(|result| result.context(StartHttpServerSnafu));

        tokio::try_join!(metrics_server, main_server)?;
    }

    info!("Shut down");

    Ok(())
}

async fn graceful_shutdown(handle: Handle<SocketAddr>) {
    wait_for_shutdown_signal().await;

    info!("Shutting down gracefully");

    // Signal the server to shutdown using Handle.
    handle.graceful_shutdown(Some(Duration::from_secs(5)));
    loop {
        info!(
            connections = handle.connection_count(),
            "Waiting for all connections to close"
        );
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_shutdown_signal() {
    // Copied from kube::runtime::Controller::shutdown_on_signal
    futures::future::select(
        tokio::signal::ctrl_c().map(|_| ()).boxed(),
        #[cfg(unix)]
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .unwrap()
            .recv()
            .map(|_| ())
            .boxed(),
        // Assume that ctrl_c is enough on non-Unix platforms (such as Windows)
        #[cfg(not(unix))]
        futures::future::pending::<()>(),
    )
    .await;
}
