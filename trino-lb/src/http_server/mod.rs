use std::{
    fmt::Debug,
    net::{Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::{
    response::Redirect,
    routing::{delete, get, post},
    Router,
};
use axum_server::{tls_rustls::RustlsConfig, Handle};
use futures::FutureExt;
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::time::sleep;
use tracing::info;
use trino_lb_persistence::PersistenceImplementation;

use crate::{
    cluster_group_manager::ClusterGroupManager, config::Config, metrics::Metrics, routing,
};

mod metrics;
mod ui;
mod v1;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed configure HTTP server"))]
    ConfigureHttpServer { source: std::io::Error },

    #[snafu(display("Failed start HTTP server"))]
    StartHttpServer { source: std::io::Error },

    #[snafu(display(
        "In case https is used the `tls.certPemFile` and `tls.keyPemFile` option must be set"
    ))]
    CertsMissing {},
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
    let listen_addr = SocketAddr::from(([0, 0, 0, 0], 9090));
    info!(addr = %listen_addr, "Starting metrics exporter");

    let handle = Handle::new();
    tokio::spawn(graceful_shutdown(handle.clone()));

    // TODO: Think about shutting down the whole trino-lb server when the Prometheus metrics exporter fails.
    // This is the reason why we start the metrics exporter first on a new task, so we still fail when the main
    // server fails.
    let handle_clone = handle.clone();
    tokio::spawn(async move {
        axum_server::bind(listen_addr)
            .handle(handle_clone)
            .serve(app.into_make_service())
            .await
    });

    let app = Router::new()
        .route("/v1/statement", post(v1::statement::post_statement))
        .route(
            "/v1/statement/queued_in_trino_lb/:query_id/:sequence_number",
            get(v1::statement::get_trino_lb_statement),
        )
        .route(
            "/v1/statement/queued/:query_id/:slug/:token",
            get(v1::statement::get_trino_queued_statement),
        )
        .route(
            "/v1/statement/executing/:query_id/:slug/:token",
            get(v1::statement::get_trino_executing_statement),
        )
        .route(
            "/v1/statement/queued_in_trino_lb/:query_id/:sequence_number",
            delete(v1::statement::delete_trino_lb_statement),
        )
        .route(
            "/v1/statement/queued/:query_id/:slug/:token",
            delete(v1::statement::delete_trino_queued_statement),
        )
        .route(
            "/v1/statement/executing/:query_id/:slug/:token",
            delete(v1::statement::delete_trino_executing_statement),
        )
        .route("/ui/query.html", get(ui::query::get_ui_query))
        .with_state(app_state);

    if tls_config.enabled {
        // Start https server
        let listen_addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 8443));
        info!(addr = %listen_addr, "Starting server");
        let tls_config = RustlsConfig::from_pem_file(
            tls_config.cert_pem_file.context(CertsMissingSnafu)?,
            tls_config.key_pem_file.context(CertsMissingSnafu)?,
        )
        .await
        .context(ConfigureHttpServerSnafu)?;

        axum_server::bind_rustls(listen_addr, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context(StartHttpServerSnafu)?;
    } else {
        // Start http server
        let listen_addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 8080));
        info!(addr = %listen_addr, "Starting server");

        axum_server::bind(listen_addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context(StartHttpServerSnafu)?;
    }

    Ok(())
}

async fn graceful_shutdown(handle: Handle) {
    wait_for_shutdown_signal().await;

    info!("Sending graceful shutdown signal");

    // Signal the server to shutdown using Handle.
    handle.graceful_shutdown(Some(Duration::from_secs(5)));
    loop {
        info!(connection = handle.connection_count(), "Alive connections");
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
