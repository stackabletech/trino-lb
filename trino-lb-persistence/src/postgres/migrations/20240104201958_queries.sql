CREATE TABLE IF NOT EXISTS queries
(
    id              VARCHAR PRIMARY KEY NOT NULL,
    trino_cluster   VARCHAR NOT NULL,
    trino_endpoint  VARCHAR NOT NULL,
    creation_time   TIMESTAMP WITH TIME ZONE NOT NULL,
    delivered_time  TIMESTAMP WITH TIME ZONE NOT NULL
);
