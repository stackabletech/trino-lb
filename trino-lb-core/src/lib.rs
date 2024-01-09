pub mod config;
pub mod sanitization;
pub mod trino_api;
pub mod trino_cluster;
pub mod trino_query;
pub mod trino_query_plan;

pub type TrinoQueryId = String;
pub type TrinoLbQueryId = TrinoQueryId;
pub type TrinoClusterName = String;
