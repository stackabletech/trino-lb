use clap::Parser;
use url::Url;

/// Helper tool to submit many concurrent queries to a Trino cluster
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Trino cluster endpoint
    #[arg(short, long)]
    pub endpoint: Url,

    /// Username
    #[arg(short, long)]
    pub username: String,

    /// Password
    #[arg(short, long)]
    pub password: String,

    /// The number of queries to submit
    #[arg(short, long)]
    pub queries: u64,

    /// How many queries should be send per second. Can also take floating point numbers and values less than 1.0.
    #[arg(long, default_value_t = 10.0)]
    pub queries_per_second: f32,

    /// Ignore the certificate of the Trino cluster in case HTTPS is used
    #[arg(short, long)]
    pub ignore_cert: bool,
}
