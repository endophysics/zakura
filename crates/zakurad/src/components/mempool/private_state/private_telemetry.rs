use std::{num::NonZeroU64, time::Duration};

use privacy_admission_core::Timestamp;
use zakura_node_services::mempool::PrivateWindowAggregate;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PrivateTelemetryOutcome {
    Promoted,
    Recoverable,
    Terminal,
}

pub(super) struct PrivateTelemetry {
    epoch_ns: NonZeroU64,
    current_window: u64,
    current: PrivateWindowAggregate,
    completed: Option<PrivateWindowAggregate>,
}

impl PrivateTelemetry {
    pub(super) fn new(observed: Timestamp, validated_epoch: Duration) -> Self {
        let Ok(epoch_ns) = u64::try_from(validated_epoch.as_nanos()) else {
            unreachable!("release epoch was validated as a representable timestamp")
        };
        let Some(epoch_ns) = NonZeroU64::new(epoch_ns) else {
            unreachable!("release epoch was validated as nonzero")
        };
        Self {
            epoch_ns,
            current_window: observed.as_nanos() / epoch_ns.get(),
            current: PrivateWindowAggregate::default(),
            completed: None,
        }
    }

    pub(super) fn record(
        &mut self,
        observed: Timestamp,
        outcome: PrivateTelemetryOutcome,
        count: u64,
    ) {
        self.rollover(observed);
        match outcome {
            PrivateTelemetryOutcome::Promoted => {
                self.current.promoted = self.current.promoted.saturating_add(count);
            }
            PrivateTelemetryOutcome::Recoverable => {
                self.current.recoverable = self.current.recoverable.saturating_add(count);
            }
            PrivateTelemetryOutcome::Terminal => {
                self.current.terminal = self.current.terminal.saturating_add(count);
            }
        }
    }

    pub(super) fn completed_window(
        &mut self,
        observed: Timestamp,
    ) -> Option<PrivateWindowAggregate> {
        self.rollover(observed);
        self.completed
    }

    fn rollover(&mut self, observed: Timestamp) {
        let observed_window = observed.as_nanos() / self.epoch_ns.get();
        if observed_window <= self.current_window {
            return;
        }
        self.completed = Some(
            if observed_window == self.current_window.saturating_add(1) {
                self.current
            } else {
                PrivateWindowAggregate::default()
            },
        );
        self.current_window = observed_window;
        self.current = PrivateWindowAggregate::default();
    }
}

#[cfg(test)]
mod tests;
