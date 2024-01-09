CREATE TABLE IF NOT EXISTS queued_queries
(
    id             VARCHAR PRIMARY KEY NOT NULL,
    query          VARCHAR NOT NULL,
    headers        JSONB NOT NULL,
    creation_time  TIMESTAMP WITH TIME ZONE NOT NULL,
    last_accessed  TIMESTAMP WITH TIME ZONE NOT NULL,
    cluster_group  VARCHAR NOT NULL
);
