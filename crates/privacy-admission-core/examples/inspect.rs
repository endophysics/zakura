//! Deterministic JSONL inspection of the synthetic admission lifecycle.

use std::{
    io::{self, Write},
    time::Duration,
};

use clap::Parser;
use privacy_admission_core::{
    AdmissionCore, AdmissionError, AdmissionId, AdmissionOrigin, AdmissionOutcome,
    AdmissionStateLabel, BatchId, Clock, DiagnosticSnapshot, ManualClock, ReasonCode,
    ReasonCodeError, ReleaseOutcome, ReleasePolicy, ReleasePolicyError, Timestamp, TimestampError,
    TransitionOutcome,
};
use serde::Serialize;
use thiserror::Error;

const ID_ONE: AdmissionId = AdmissionId(1);
const ID_TWO: AdmissionId = AdmissionId(2);
const ID_THREE: AdmissionId = AdmissionId(3);

#[derive(Clone, Copy, Debug, Parser)]
struct Cli {
    #[arg(long, value_parser = humantime::parse_duration)]
    epoch: Duration,
    #[arg(long, value_parser = humantime::parse_duration)]
    minimum_delay: Duration,
    #[arg(long, value_parser = humantime::parse_duration)]
    maximum_delay: Option<Duration>,
}

#[derive(Debug, Error)]
enum ExampleError {
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("new admission unexpectedly already existed: {admission_id:?}")]
    NewAdmissionWasExisting { admission_id: AdmissionId },
    #[error("retry unexpectedly accepted a new admission: {admission_id:?}")]
    RetryWasAccepted { admission_id: AdmissionId },
    #[error("terminal transition unexpectedly found an existing admission: {admission_id:?}")]
    TransitionWasExisting { admission_id: AdmissionId },
    #[error(transparent)]
    Policy(#[from] ReleasePolicyError),
    #[error(transparent)]
    Reason(#[from] ReasonCodeError),
    #[error("release unexpectedly found no due admissions")]
    ReleaseWasEmpty,
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    Timestamp(#[from] TimestampError),
}

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    Accepted {
        admission_id: AdmissionId,
        accepted_at_ns: u64,
        scheduled_release_at_ns: u64,
    },
    Embargoed {
        admission_ids: Vec<AdmissionId>,
    },
    Existing {
        admission_id: AdmissionId,
        state: AdmissionStateLabel,
    },
    Rejected {
        admission_id: AdmissionId,
        reason: ReasonCode,
    },
    Removed {
        admission_id: AdmissionId,
        reason: ReasonCode,
    },
    Advanced {
        now_ns: u64,
    },
    Due {
        admission_ids: Vec<AdmissionId>,
    },
    Eligible {
        admission_ids: Vec<AdmissionId>,
    },
    Released {
        batch_id: BatchId,
        admission_ids: Vec<AdmissionId>,
    },
    Snapshot {
        snapshot: DiagnosticSnapshot,
    },
}

fn main() -> Result<(), ExampleError> {
    execute(Cli::parse(), io::stdout().lock())
}

fn execute<W: Write>(cli: Cli, mut output: W) -> Result<(), ExampleError> {
    let maximum_delay = cli.maximum_delay.unwrap_or(cli.minimum_delay);
    let policy = ReleasePolicy::new(cli.epoch, cli.minimum_delay, maximum_delay)?;
    let mut core = AdmissionCore::new(ManualClock::new(Timestamp(0)), policy);

    let scheduled_release_at = match core.admit(ID_ONE, AdmissionOrigin::PrivateGateway)? {
        AdmissionOutcome::Accepted(view) => {
            write_event(
                &mut output,
                Event::Accepted {
                    admission_id: view.admission_id,
                    accepted_at_ns: view.state.accepted_at().as_nanos(),
                    scheduled_release_at_ns: view.state.scheduled_release_at().as_nanos(),
                },
            )?;
            view.state.scheduled_release_at()
        }
        AdmissionOutcome::Existing(_) => {
            return Err(ExampleError::NewAdmissionWasExisting {
                admission_id: ID_ONE,
            });
        }
    };
    for admission_id in [ID_TWO, ID_THREE] {
        match core.admit(admission_id, AdmissionOrigin::PrivateGateway)? {
            AdmissionOutcome::Accepted(view) => write_event(
                &mut output,
                Event::Accepted {
                    admission_id: view.admission_id,
                    accepted_at_ns: view.state.accepted_at().as_nanos(),
                    scheduled_release_at_ns: view.state.scheduled_release_at().as_nanos(),
                },
            )?,
            AdmissionOutcome::Existing(_) => {
                return Err(ExampleError::NewAdmissionWasExisting { admission_id });
            }
        }
    }
    write_event(
        &mut output,
        Event::Embargoed {
            admission_ids: core.embargoed_ids(),
        },
    )?;

    match core.admit(ID_ONE, AdmissionOrigin::PrivateGateway)? {
        AdmissionOutcome::Accepted(_) => {
            return Err(ExampleError::RetryWasAccepted {
                admission_id: ID_ONE,
            });
        }
        AdmissionOutcome::Existing(view) => write_event(
            &mut output,
            Event::Existing {
                admission_id: view.admission_id,
                state: view.state.label(),
            },
        )?,
    }

    let rejection = ReasonCode::try_from("policy_rejected")?;
    match core.reject(ID_TWO, rejection.clone())? {
        TransitionOutcome::Updated(_) => write_event(
            &mut output,
            Event::Rejected {
                admission_id: ID_TWO,
                reason: rejection,
            },
        )?,
        TransitionOutcome::Existing(_) => {
            return Err(ExampleError::TransitionWasExisting {
                admission_id: ID_TWO,
            });
        }
    }
    let removal = ReasonCode::try_from("operator_removed")?;
    match core.remove(ID_THREE, removal.clone())? {
        TransitionOutcome::Updated(_) => write_event(
            &mut output,
            Event::Removed {
                admission_id: ID_THREE,
                reason: removal,
            },
        )?,
        TransitionOutcome::Existing(_) => {
            return Err(ExampleError::TransitionWasExisting {
                admission_id: ID_THREE,
            });
        }
    }

    core.clock_mut()
        .advance(scheduled_release_at.checked_duration_since(Timestamp(0))?)?;
    write_event(
        &mut output,
        Event::Advanced {
            now_ns: core.clock().now().as_nanos(),
        },
    )?;
    write_event(
        &mut output,
        Event::Due {
            admission_ids: core.refresh()?,
        },
    )?;
    write_event(
        &mut output,
        Event::Eligible {
            admission_ids: core.eligible_ids(),
        },
    )?;
    match core.release_due()? {
        ReleaseOutcome::NoDue => return Err(ExampleError::ReleaseWasEmpty),
        ReleaseOutcome::Released {
            batch_id,
            admissions,
        } => write_event(
            &mut output,
            Event::Released {
                batch_id,
                admission_ids: admissions
                    .into_iter()
                    .map(|view| view.admission_id)
                    .collect(),
            },
        )?,
    }
    write_event(
        &mut output,
        Event::Snapshot {
            snapshot: core.snapshot(),
        },
    )
}

fn write_event<W: Write>(output: &mut W, event: Event) -> Result<(), ExampleError> {
    serde_json::to_writer(&mut *output, &event)?;
    writeln!(output)?;
    Ok(())
}

#[cfg(test)]
#[path = "inspect/tests.rs"]
mod inspect_tests;
