pub fn snafu_error_to_string<E: std::error::Error>(err: &E) -> String {
    let mut result = format!("{err}");
    let mut source = err.source();
    while let Some(err) = source {
        result.push_str(format!(": {err}").as_str());
        source = err.source();
    }

    result
}

#[test]
fn test_error_formatting() {
    let err = trino_lb_persistence::Error::RedisError {
        source: trino_lb_persistence::redis::Error::CreateClient {
            source: redis::RedisError::from(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "Connection to Redis was refused",
            )),
        },
    };

    assert_eq!(format!("{err}"), "Redis persistence error");
    assert_eq!(format!("{err:#}"), "Redis persistence error");
    assert_eq!(
        format!("{err:?}"),
        "RedisError { source: CreateClient { source: Connection to Redis was refused } }"
    );
    assert_eq!(
        snafu_error_to_string(&err),
        "Redis persistence error: Failed to create redis client: Connection to Redis was refused: Connection to Redis was refused"
    );
    assert_eq!(
        format!("{:#}", snafu::Report::from_error(err)),
        "Redis persistence error

Caused by these errors (recent errors listed first):
  1: Failed to create redis client
  2: Connection to Redis was refused

NOTE: Some redundant information has been removed. Set SNAFU_RAW_ERROR_MESSAGES=1 to disable this behavior.
"
    );
}
