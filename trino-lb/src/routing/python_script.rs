use std::{
    collections::{HashMap, HashSet},
    ffi::CString,
};

use pyo3::{
    Py, PyAny, Python,
    ffi::c_str,
    types::{IntoPyDict, PyAnyMethods, PyModule},
};
use snafu::{ResultExt, Snafu};
use tracing::{error, instrument, warn};
use trino_lb_core::{config::PythonScriptRouterConfig, sanitization::Sanitize};

use crate::routing::RouterImplementationTrait;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to convert Python script to CString"))]
    ConvertScriptToCString { source: std::ffi::NulError },

    #[snafu(display("Failed to convert HTTP headers to Python dict"))]
    ConvertHeadersToPythonDict { source: pyo3::PyErr },

    #[snafu(display("Failed to parse Python script"))]
    ParsePythonScript { source: pyo3::PyErr },

    #[snafu(display("Failed to execute Python script"))]
    ExecutePythonScript { source: pyo3::PyErr },

    #[snafu(display("Failed to find Python function {function_name:?}"))]
    FindPythonFunction {
        source: pyo3::PyErr,
        function_name: String,
    },
}

pub struct PythonScriptRouter {
    function: Py<PyAny>,
    valid_target_groups: HashSet<String>,
}

impl PythonScriptRouter {
    // Intentionally including the config here, this is only logged on startup
    #[instrument(name = "PythonScriptRouter::new")]
    pub fn new(
        config: &PythonScriptRouterConfig,
        valid_target_groups: HashSet<String>,
    ) -> Result<Self, Error> {
        let function = Python::attach(|py| {
            let script =
                CString::new(config.script.clone()).context(ConvertScriptToCStringSnafu)?;
            let function: Py<PyAny> =
                PyModule::from_code(py, script.as_c_str(), c_str!(""), c_str!(""))
                    .context(ParsePythonScriptSnafu)?
                    .getattr("targetClusterGroup")
                    .context(FindPythonFunctionSnafu {
                        function_name: "targetClusterGroup",
                    })?
                    .into();

            Ok(function)
        })?;

        Ok(Self {
            function,
            valid_target_groups,
        })
    }
}

impl RouterImplementationTrait for PythonScriptRouter {
    #[instrument(
        name = "PythonScriptRouter::route"
        skip(self),
        fields(headers = ?headers.sanitize()),
    )]
    async fn route(&self, query: &str, headers: &http::HeaderMap) -> Option<String> {
        let result = Python::attach(|py| {
            let headers_dict = header_map_to_hashmap(headers)
                .into_py_dict(py)
                .context(ConvertHeadersToPythonDictSnafu)?;
            self.function
                .call1(py, (query, headers_dict))
                .context(ExecutePythonScriptSnafu)
        });
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                error!(query, ?error, "Failed to execute Python script");
                return None;
            }
        };

        let target_group = match Python::attach(|py| result.extract::<Option<String>>(py)) {
            Ok(target_group) => target_group,
            Err(error) => {
                error!(query, ?error, "Failed to execute Python script");
                return None;
            }
        };

        if let Some(target_group) = target_group {
            if self.valid_target_groups.contains(&target_group) {
                return Some(target_group);
            } else {
                warn!(
                    target_group,
                    "The target group returned from the Python script that does not exist, skipped routing"
                );
            }
        }

        None
    }
}

#[instrument(fields(headers = ?headers.sanitize()))]
fn header_map_to_hashmap(headers: &http::HeaderMap) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for (key, value) in headers {
        let key = key.to_string();
        if let Ok(value) = value.to_str() {
            result.insert(key, value.to_string());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderName};
    use indoc::indoc;
    use rstest::rstest;

    use super::*;

    fn create_router(script: String) -> PythonScriptRouter {
        let valid_target_groups = HashSet::from([
            "s".to_string(),
            "m".to_string(),
            "l".to_string(),
            "etl".to_string(),
            "etl-special".to_string(),
            "etl-foo".to_string(),
            "etl-bar".to_string(),
        ]);

        PythonScriptRouter::new(
            &PythonScriptRouterConfig {
                script: script.to_string(),
            },
            valid_target_groups,
        )
        .expect("Failed to create PythonScriptRouter")
    }

    fn get_headers(x_trino_source: Option<&str>, x_trino_client_tags: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();

        if let Some(x_trino_source) = x_trino_source {
            headers.insert(
                HeaderName::from_static("x-trino-source"),
                x_trino_source
                    .parse()
                    .expect("Failed to create x-trino-source header"),
            );
        }
        if let Some(x_trino_client_tags) = x_trino_client_tags {
            headers.insert(
                HeaderName::from_static("x-trino-client-tags"),
                x_trino_client_tags
                    .parse()
                    .expect("Failed to create x-trino-client-tags header"),
            );
        }

        headers
    }

    #[rstest]
    #[case(None, None, None)]
    #[case(Some("airflow"), None, Some("etl"))]
    #[case(Some("airflow"), Some("label=special"), Some("etl-special"))]
    #[case(
        Some("airflow"),
        Some("foo=bar,label=special,something=else"),
        Some("etl-special")
    )]
    #[tokio::test]
    async fn test_routing_based_on_trino_source_and_client_tags(
        #[case] x_trino_source: Option<&str>,
        #[case] x_trino_client_tags: Option<&str>,
        #[case] expected: Option<&str>,
    ) {
        let script = indoc! {r#"
from typing import Optional

def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
    # If query from airflow, route to etl group
    if headers.get('x-trino-source') == 'airflow':
        # If query from airflow with special label, route to etl-special group
        if 'x-trino-client-tags' in headers and 'label=special' in headers.get('x-trino-client-tags'):
            return 'etl-special'
        else:
            return 'etl'
        "#};

        let router = create_router(script.to_string());

        assert_eq!(
            router
                .route(
                    "show catalogs",
                    &get_headers(x_trino_source, x_trino_client_tags,)
                )
                .await,
            expected.map(ToOwned::to_owned)
        );
    }

    #[rstest]
    #[case(None, None, None)]
    #[case(Some("airflow"), None, Some("etl"))]
    #[case(Some("airflow"), Some("label=foo"), Some("etl-foo"))]
    #[case(
        Some("airflow"),
        Some("foo=bar,label=foo,something=else"),
        Some("etl-foo")
    )]
    #[case(
        Some("airflow"),
        Some("foo=bar,label=bar,something=else"),
        Some("etl-bar")
    )]
    #[tokio::test]
    async fn test_routing_based_on_trino_source_and_client_tags_with_functions(
        #[case] x_trino_source: Option<&str>,
        #[case] x_trino_client_tags: Option<&str>,
        #[case] expected: Option<&str>,
    ) {
        let script = indoc! {r#"
from typing import Optional

def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
    client_tags = get_client_tags(headers)

    if get_source(headers) == "airflow":
        if client_tags.get("label") == "foo":
            return 'etl-foo'
        elif client_tags.get("label") == "bar":
            return 'etl-bar'
        else:
            return 'etl'

def get_source(headers: dict[str, str]) -> Optional[str]:
    return headers.get("x-trino-source")

def get_client_tags(headers: dict[str, str]) -> dict[str, str]:
    tags = {}
    header_value = headers.get("x-trino-client-tags")
    if header_value is not None:
        for pair in header_value.split(","):
            pair = pair.split("=", 1)
            tags[pair[0]] = pair[1]

    return tags
        "#};

        let router = create_router(script.to_string());

        assert_eq!(
            router
                .route(
                    "show catalogs",
                    &get_headers(x_trino_source, x_trino_client_tags)
                )
                .await,
            expected.map(ToOwned::to_owned)
        );
    }

    #[rstest]
    #[case("show catalogs", None)]
    #[case("ALTER TABLE foo EXECUTE OPTIMIZE", Some("l"))]
    #[case("alter table foo execute optimize", Some("l"))]
    #[case(
        "ALTER TABLE foo EXECUTE OPTIMIZE(file_size_threshold => '10MB')
    ",
        Some("l")
    )]
    #[case("ALTER TABLE foo EXECUTE OPTIMIZE WHERE partition_key = 1", Some("l"))]
    #[tokio::test]
    async fn test_routing_based_on_query_regex(
        #[case] query: String,
        #[case] expected: Option<&str>,
    ) {
        let script = indoc! {r#"
from typing import Optional
import re

def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
    # Compactions have to run on "l" clusters
    if re.search("^alter table .* execute optimize", query.lower()):
        return "l"
        "#};

        let router = create_router(script.to_string());

        assert_eq!(
            router.route(&query, &get_headers(None, None)).await,
            expected.map(ToOwned::to_owned)
        );
    }

    #[tokio::test]
    async fn test_invalid_script() {
        let script = "malformed python :)".to_string();
        let config = PythonScriptRouterConfig { script };

        let result = PythonScriptRouter::new(&config, HashSet::new());
        assert!(matches!(result, Err(Error::ParsePythonScript { .. })));
    }

    #[tokio::test]
    async fn test_missing_function() {
        let script = "foo = 42".to_string();
        let config = PythonScriptRouterConfig { script };

        let result = PythonScriptRouter::new(&config, HashSet::new());
        assert!(matches!(result, Err(Error::FindPythonFunction { .. })));
    }
}
