CREATE TABLE IF NOT EXISTS cluster_states
(
    id       VARCHAR PRIMARY KEY NOT NULL,
    state    JSONB NOT NULL
);
