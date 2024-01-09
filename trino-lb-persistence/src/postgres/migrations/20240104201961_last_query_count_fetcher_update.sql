CREATE TABLE IF NOT EXISTS last_query_count_fetcher_update
(
    -- Always the same constant
    dummy                           INT PRIMARY KEY NOT NULL,
    last_query_count_fetcher_update TIMESTAMP WITH TIME ZONE NOT NULL
);
