use std::time::Duration;

use color_eyre::eyre::{eyre, Result};
use zakurad::components::mempool::private_pool::PrivateReleaseConfig;

pub const TEST_PRIVATE_RELEASE_EPOCH_MS: &str = "TEST_ZCASHD_COMPAT_PRIVATE_RELEASE_EPOCH_MS";
pub const TEST_PRIVATE_MINIMUM_DELAY_MS: &str =
    "TEST_ZCASHD_COMPAT_PRIVATE_RELEASE_MINIMUM_DELAY_MS";
pub const TEST_PRIVATE_MAXIMUM_DELAY_MS: &str =
    "TEST_ZCASHD_COMPAT_PRIVATE_RELEASE_MAXIMUM_DELAY_MS";

const DEFAULT_RELEASE_EPOCH: Duration = Duration::from_millis(250);
const DEFAULT_MINIMUM_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_MAXIMUM_DELAY: Duration = Duration::from_secs(6);
const DETERMINISTIC_MINIMUM_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug)]
pub struct InspectionTiming {
    release: PrivateReleaseConfig,
    minimum: Duration,
}

impl InspectionTiming {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let epoch = duration_from_lookup(
            &lookup,
            TEST_PRIVATE_RELEASE_EPOCH_MS,
            DEFAULT_RELEASE_EPOCH,
        )?;
        let minimum = duration_from_lookup(
            &lookup,
            TEST_PRIVATE_MINIMUM_DELAY_MS,
            DEFAULT_MINIMUM_DELAY,
        )?;
        let maximum = duration_from_lookup(
            &lookup,
            TEST_PRIVATE_MAXIMUM_DELAY_MS,
            DEFAULT_MAXIMUM_DELAY,
        )?;
        let release = PrivateReleaseConfig::new(epoch, minimum, maximum).map_err(|error| {
            eyre!(
                "invalid private-release inspection timing from {TEST_PRIVATE_RELEASE_EPOCH_MS}, {TEST_PRIVATE_MINIMUM_DELAY_MS}, and {TEST_PRIVATE_MAXIMUM_DELAY_MS}: {error}"
            )
        })?;
        if minimum < DETERMINISTIC_MINIMUM_DELAY {
            return Err(eyre!(
                "{TEST_PRIVATE_MINIMUM_DELAY_MS} must be at least {} milliseconds for a deterministic retry before release",
                DETERMINISTIC_MINIMUM_DELAY.as_millis()
            ));
        }
        Ok(Self { release, minimum })
    }

    pub const fn release_config(self) -> PrivateReleaseConfig {
        self.release
    }

    pub fn retry_delay(self) -> Duration {
        self.minimum / 2
    }
}

fn duration_from_lookup(
    lookup: &impl Fn(&str) -> Option<String>,
    name: &str,
    default: Duration,
) -> Result<Duration> {
    let Some(value) = lookup(name).filter(|value| !value.is_empty()) else {
        return Ok(default);
    };
    let milliseconds = value.parse::<u64>().map_err(|error| {
        eyre!("invalid {name} value {value:?}: expected u64 milliseconds: {error}")
    })?;
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use super::*;
    use zakurad::components::mempool::private_pool::PrivatePoolConfig;

    type TestResult = Result<()>;

    fn lookup(values: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let values = values
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        move |name| values.get(name).cloned()
    }

    #[test]
    fn defaults_are_used_when_overrides_are_absent_or_empty() -> TestResult {
        // Given: no test timing overrides, then explicitly empty overrides.
        let absent = lookup(&[]);
        let empty = lookup(&[
            (TEST_PRIVATE_RELEASE_EPOCH_MS, ""),
            (TEST_PRIVATE_MINIMUM_DELAY_MS, ""),
            (TEST_PRIVATE_MAXIMUM_DELAY_MS, ""),
        ]);

        // When: both mappings are parsed.
        let absent = InspectionTiming::from_lookup(absent)?;
        let empty = InspectionTiming::from_lookup(empty)?;

        // Then: both select the documented defaults and derived retry delay.
        for timing in [absent, empty] {
            let config = PrivatePoolConfig::new(1, 1, timing.release_config())?;
            assert_eq!(config.release_epoch(), Duration::from_millis(250));
            assert_eq!(config.minimum_release_delay(), Duration::from_secs(5));
            assert_eq!(config.maximum_release_delay(), Duration::from_secs(6));
            assert_eq!(timing.retry_delay(), Duration::from_millis(2_500));
        }
        Ok(())
    }

    #[test]
    fn valid_overrides_change_every_timing_value() -> TestResult {
        // Given: valid values distinct from every default.
        let values = lookup(&[
            (TEST_PRIVATE_RELEASE_EPOCH_MS, "400"),
            (TEST_PRIVATE_MINIMUM_DELAY_MS, "4000"),
            (TEST_PRIVATE_MAXIMUM_DELAY_MS, "7000"),
        ]);

        // When: the mapping is parsed.
        let timing = InspectionTiming::from_lookup(values)?;

        // Then: all values and the minimum-derived retry delay change.
        let config = PrivatePoolConfig::new(1, 1, timing.release_config())?;
        assert_eq!(config.release_epoch(), Duration::from_millis(400));
        assert_eq!(config.minimum_release_delay(), Duration::from_secs(4));
        assert_eq!(config.maximum_release_delay(), Duration::from_secs(7));
        assert_eq!(timing.retry_delay(), Duration::from_secs(2));
        Ok(())
    }

    #[test]
    fn malformed_override_reports_its_environment_name() {
        // Given: each override name paired with a malformed value.
        let names = [
            TEST_PRIVATE_RELEASE_EPOCH_MS,
            TEST_PRIVATE_MINIMUM_DELAY_MS,
            TEST_PRIVATE_MAXIMUM_DELAY_MS,
        ];

        // When / Then: parsing reports the exact offending variable.
        for name in names {
            let error = InspectionTiming::from_lookup(lookup(&[(name, "soon")]))
                .expect_err("malformed value must fail");
            assert!(error.to_string().contains(name));
        }
    }

    #[test]
    fn zero_inverted_and_too_short_values_are_rejected() {
        // Given: invalid boundary and ordering combinations.
        let cases = [
            [(TEST_PRIVATE_RELEASE_EPOCH_MS, "0"), ("", "")],
            [(TEST_PRIVATE_MINIMUM_DELAY_MS, "0"), ("", "")],
            [(TEST_PRIVATE_MAXIMUM_DELAY_MS, "0"), ("", "")],
            [
                (TEST_PRIVATE_MINIMUM_DELAY_MS, "4000"),
                (TEST_PRIVATE_MAXIMUM_DELAY_MS, "3000"),
            ],
            [(TEST_PRIVATE_MINIMUM_DELAY_MS, "1999"), ("", "")],
        ];

        // When / Then: every invalid mapping is rejected.
        for values in cases {
            let populated = values
                .into_iter()
                .filter(|(name, _)| !name.is_empty())
                .collect::<Vec<_>>();
            assert!(InspectionTiming::from_lookup(lookup(&populated)).is_err());
        }
    }
}
