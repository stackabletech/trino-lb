use std::{collections::BTreeMap, fmt::Display, iter::Sum, ops::Add};

use number_prefix::NumberPrefix;
use prusto::Presto;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, Presto)]
pub struct ExplainQueryResult {
    #[presto(rename = "Query Plan")]
    pub query_plan: String,
}

#[derive(Debug, Deserialize)]
pub struct QueryPlan {
    #[serde(flatten)]
    pub items: BTreeMap<String, QueryPlanItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPlanItem {
    pub id: String,
    pub name: String,
    pub estimates: Vec<QueryPlanEstimation>,
    pub children: Vec<QueryPlanItem>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryPlanEstimation {
    #[serde(deserialize_with = "deserialize_maybe_nan")]
    pub output_row_count: f32,

    #[serde(deserialize_with = "deserialize_maybe_nan")]
    pub output_size_in_bytes: f32,

    #[serde(deserialize_with = "deserialize_maybe_nan")]
    pub cpu_cost: f32,

    #[serde(deserialize_with = "deserialize_maybe_nan")]
    pub memory_cost: f32,

    #[serde(deserialize_with = "deserialize_maybe_nan")]
    pub network_cost: f32,
}

impl Display for QueryPlanEstimation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[ {} rows", format(self.output_row_count))?;
        write!(f, ", {}B bytes", format(self.output_size_in_bytes))?;
        write!(f, ", {} cpu", format(self.cpu_cost))?;
        write!(f, ", {}B memory", format(self.memory_cost))?;
        write!(f, ", {}B network ]", format(self.network_cost))?;
        Ok(())
    }
}

fn format(value: f32) -> String {
    match NumberPrefix::decimal(value) {
        NumberPrefix::Prefixed(prefix, n) => format!("{n:.1}{prefix}"),
        NumberPrefix::Standalone(n) => format!("{n}"),
    }
}

impl Add<&QueryPlanEstimation> for QueryPlanEstimation {
    type Output = QueryPlanEstimation;

    fn add(self, rhs: &QueryPlanEstimation) -> Self::Output {
        QueryPlanEstimation {
            output_row_count: self.output_row_count + rhs.output_row_count,
            output_size_in_bytes: self.output_size_in_bytes + rhs.output_size_in_bytes,
            cpu_cost: self.cpu_cost + rhs.cpu_cost,
            memory_cost: self.memory_cost + rhs.memory_cost,
            network_cost: self.network_cost + rhs.network_cost,
        }
    }
}

impl<'a> Sum<&'a QueryPlanEstimation> for QueryPlanEstimation {
    fn sum<I: Iterator<Item = &'a QueryPlanEstimation>>(iter: I) -> Self {
        iter.fold(QueryPlanEstimation::default(), QueryPlanEstimation::add)
    }
}

impl Sum<QueryPlanEstimation> for QueryPlanEstimation {
    fn sum<I: Iterator<Item = QueryPlanEstimation>>(iter: I) -> Self {
        iter.fold(QueryPlanEstimation::default(), |acc, e| acc + &e)
    }
}

impl QueryPlanEstimation {
    pub fn smaller_in_all_measurements(&self, other: &QueryPlanEstimation) -> bool {
        self.output_row_count <= other.output_row_count
            && self.output_size_in_bytes <= other.output_size_in_bytes
            && self.cpu_cost <= other.cpu_cost
            && self.memory_cost <= other.memory_cost
            && self.network_cost <= other.network_cost
    }
}

impl QueryPlanItem {
    pub fn total_estimates(&self) -> QueryPlanEstimation {
        let estimates: QueryPlanEstimation = self.estimates.iter().sum();
        let child_estimates = &self.children.iter().map(|c| c.total_estimates()).sum();
        estimates + child_estimates
    }
}

impl QueryPlan {
    pub fn total_estimates(&self) -> QueryPlanEstimation {
        self.items.values().map(|i| i.total_estimates()).sum()
    }
}

/// Parses the number and return the default (0.0) in cases the number is `NaN` or `n/a`.
///
/// Thanks to <https://stackoverflow.com/questions/56384447/how-do-i-transform-special-values-into-optionnone-when-using-serde-to-deserial>!
fn deserialize_maybe_nan<'de, D, T: Deserialize<'de> + Default>(
    deserializer: D,
) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MaybeNA<U> {
        Value(U),
        NAString(String),
    }

    let value: MaybeNA<T> = Deserialize::deserialize(deserializer)?;
    match value {
        MaybeNA::Value(value) => Ok(value),
        MaybeNA::NAString(string) => {
            if string == "NaN" || string == "n/a" {
                Ok(T::default())
            } else {
                Err(serde::de::Error::custom("Unexpected string"))
            }
        }
    }
}
