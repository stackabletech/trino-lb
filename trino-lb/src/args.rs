use std::path::PathBuf;

use clap::Parser;

/// Loadbalancer in front of Stackable Trino clusters
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Config file that contains needed information to start trino-lb.
    #[arg(short, long)]
    pub config_file: PathBuf,
}
