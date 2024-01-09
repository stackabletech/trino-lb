use std::{sync::Arc, time::Duration};

use args::Args;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar};
use prusto::{auth::Auth, ClientBuilder, Row};
use tokio::time;

mod args;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = Arc::new(
        ClientBuilder::new(&args.username, args.endpoint.host().unwrap())
            .port(args.endpoint.port_or_known_default().unwrap())
            .secure(args.endpoint.scheme() == "https")
            .no_verify(args.ignore_cert)
            .auth(Auth::Basic(
                args.username.to_owned(),
                Some(args.password.to_owned()),
            ))
            .build()
            .unwrap(),
    );

    println!(
        "[INFO] Submitting {} queries at {} queries/s",
        args.queries, args.queries_per_second
    );

    let multi_bar = MultiProgress::new();
    let started_bar = multi_bar.add(ProgressBar::new(args.queries));
    let finished_bar = Arc::new(multi_bar.add(ProgressBar::new(args.queries)));

    let mut handles = vec![];

    let wait_time = Duration::from_nanos((1E9 / args.queries_per_second) as u64);
    let mut interval = time::interval(wait_time);
    let mut count = 0;

    while count < args.queries {
        interval.tick().await;
        let client_clone = Arc::clone(&client);
        let finished_bar_clone = Arc::clone(&finished_bar);
        handles.push(tokio::spawn(async move {
            let result = client_clone
                .get_all::<Row>("select count(*) from tpch.sf2.lineitem".to_owned())
                .await;
            if let Err(err) = result {
                println!("[WARN] Query failed: {err}")
            }
            finished_bar_clone.inc(1);
        }));
        started_bar.inc(1);
        count += 1;
    }

    futures::future::join_all(handles).await;
}
