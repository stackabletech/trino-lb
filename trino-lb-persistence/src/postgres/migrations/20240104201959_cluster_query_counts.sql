CREATE TABLE IF NOT EXISTS cluster_query_counts
(
    cluster  VARCHAR PRIMARY KEY NOT NULL,
    count    BIGINT NOT NULL
);
