use std::{sync::OnceLock, time::Duration};

use chrono::{DateTime, Timelike, Utc};
use regex::Regex;
use snafu::{OptionExt, Snafu};
use trino_lb_core::config::{MinClustersConfig, TrinoClusterGroupAutoscalingConfig};

static TIME_RANGE_REGEX: OnceLock<Regex> = OnceLock::new();
const MIN_DRAIN_IDLE_DURATION_BEFORE_SHUTDOWN: Duration = Duration::from_secs(10);

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "Time range {time_range:?} can not be parsed. It needs to have the format \"09:00:00 - 11:59:59\""
    ))]
    InvalidTimeRange { time_range: String },

    #[snafu(display("Any weekdays other tha \"Mon - Son\" are not supported yet"))]
    WeekdaysNotSupportedYet,

    #[snafu(display(
        "Please configure a drainIdleDurationBeforeShutdown of at least {min_duration:?}"
    ))]
    DrainIdleDurationBeforeShutdownToLow { min_duration: Duration },
}

#[derive(Clone, Debug)]
pub struct TrinoClusterGroupAutoscaling {
    pub upscale_queued_queries_threshold: u64,
    pub downscale_running_queries_percentage_threshold: u64,
    pub drain_idle_duration_before_shutdown: Duration,
    pub min_clusters: Vec<MinClusters>,
}

impl TryFrom<TrinoClusterGroupAutoscalingConfig> for TrinoClusterGroupAutoscaling {
    type Error = Error;

    fn try_from(config: TrinoClusterGroupAutoscalingConfig) -> Result<Self, Self::Error> {
        if config.drain_idle_duration_before_shutdown < MIN_DRAIN_IDLE_DURATION_BEFORE_SHUTDOWN {
            DrainIdleDurationBeforeShutdownToLowSnafu {
                min_duration: MIN_DRAIN_IDLE_DURATION_BEFORE_SHUTDOWN,
            }
            .fail()?;
        }
        Ok(Self {
            upscale_queued_queries_threshold: config.upscale_queued_queries_threshold,
            downscale_running_queries_percentage_threshold: config
                .downscale_running_queries_percentage_threshold,
            drain_idle_duration_before_shutdown: config.drain_idle_duration_before_shutdown,
            min_clusters: config
                .min_clusters
                .into_iter()
                .map(|m| m.try_into())
                .collect::<Result<Vec<_>, Error>>()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct MinClusters {
    time_start_hour: u32,
    time_start_minute: u32,
    time_start_second: u32,
    time_end_hour: u32,
    time_end_minute: u32,
    time_end_second: u32,
    pub min: u64,
}

impl TryFrom<MinClustersConfig> for MinClusters {
    type Error = Error;

    fn try_from(config: MinClustersConfig) -> Result<Self, Error> {
        let time_range_regex = TIME_RANGE_REGEX.get_or_init(|| {
            Regex::new(
                r"^([0-9][0-9]):([0-9][0-9]):([0-9][0-9]) - ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])$",
            )
            .unwrap()
        });

        let time_captures =
            time_range_regex
                .captures(&config.time_utc)
                .context(InvalidTimeRangeSnafu {
                    time_range: &config.time_utc,
                })?;

        if config.weekdays != "Mon - Son" {
            WeekdaysNotSupportedYetSnafu.fail()?;
        }

        Ok(MinClusters {
            // Safety: The array access and digit parsing can not fail as of the regex content
            time_start_hour: time_captures[1].parse().unwrap(),
            time_start_minute: time_captures[2].parse().unwrap(),
            time_start_second: time_captures[3].parse().unwrap(),
            time_end_hour: time_captures[4].parse().unwrap(),
            time_end_minute: time_captures[5].parse().unwrap(),
            time_end_second: time_captures[6].parse().unwrap(),
            min: config.min,
        })
    }
}

impl MinClusters {
    pub fn date_is_in_range(&self, date: &DateTime<Utc>) -> bool {
        let hour = date.hour();
        let minute = date.minute();
        let second = date.second();

        let date = hour * 60 * 60 + minute * 60 + second;
        date >= self.time_start_hour * 60 * 60
            + self.time_start_minute * 60
            + self.time_start_second
            && date
                <= self.time_end_hour * 60 * 60 + self.time_end_minute * 60 + self.time_end_second
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeZone;
    use rstest::rstest;

    #[rstest]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 0, 0, 0).unwrap(), true)]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 8, 0, 0).unwrap(), true)]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 8, 59, 59).unwrap(), true)]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 9, 0, 0).unwrap(), true)]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 11, 59, 59).unwrap(), true)]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 12, 0, 0).unwrap(), true)]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 23, 0, 0).unwrap(), true)]
    #[case("00:00:00 - 23:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 23, 59, 59).unwrap(), true)]
    //
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 0, 0, 0).unwrap(), false)]
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 8, 0, 0).unwrap(), true)]
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 8, 59, 59).unwrap(), true)]
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 9, 0, 0).unwrap(), false)]
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 11, 59, 59).unwrap(), false)]
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 12, 0, 0).unwrap(), false)]
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 23, 0, 0).unwrap(), false)]
    #[case("08:00:00 - 08:59:59", Utc.with_ymd_and_hms(2023, 12, 8, 23, 59, 59).unwrap(), false)]
    //
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 0, 0, 0).unwrap(), false)]
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 8, 0, 0).unwrap(), true)]
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 8, 59, 59).unwrap(), true)]
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 9, 0, 0).unwrap(), true)]
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 11, 59, 59).unwrap(), false)]
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 12, 0, 0).unwrap(), false)]
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 23, 0, 0).unwrap(), false)]
    #[case("08:00:00 - 09:00:00", Utc.with_ymd_and_hms(2023, 12, 8, 23, 59, 59).unwrap(), false)]
    fn test_date_is_in_range(
        #[case] time_utc: String,
        #[case] date: DateTime<Utc>,
        #[case] expected: bool,
    ) {
        let config = MinClustersConfig {
            time_utc: time_utc.clone(),
            weekdays: "Mon - Son".to_string(),
            min: 42,
        };
        let min_clusters: MinClusters = config.try_into().unwrap();

        assert_eq!(
            min_clusters.date_is_in_range(&date),
            expected,
            "Testing if {date} is in {time_utc}"
        );
    }
}
