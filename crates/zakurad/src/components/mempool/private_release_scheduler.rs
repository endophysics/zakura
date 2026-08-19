use std::time::Duration;

use privacy_admission_core::{MonotonicClock, Timestamp};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, sleep_until, Instant},
};
use tokio_util::sync::CancellationToken;
use tower::{BoxError, Service, ServiceExt};
use tracing::warn;
use tracing_futures::Instrument;
use zakura_node_services::mempool::{PrivatePromotionOutcome, Request, Response, SchedulerState};

pub(super) const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct PrivateReleaseTiming {
    clock: MonotonicClock,
    deadlines: watch::Receiver<Option<Timestamp>>,
}

impl PrivateReleaseTiming {
    pub(crate) const fn new(
        clock: MonotonicClock,
        deadlines: watch::Receiver<Option<Timestamp>>,
    ) -> Self {
        Self { clock, deadlines }
    }

    #[cfg(test)]
    const fn clock(&self) -> &MonotonicClock {
        &self.clock
    }

    #[cfg(test)]
    pub(super) fn deadline(&self) -> Option<Timestamp> {
        *self.deadlines.borrow()
    }
}

enum WaitOutcome {
    Cancelled,
    Continue,
}

enum PromotionAttempt {
    Completed,
    Retry,
}

pub(crate) struct PrivateReleaseScheduler<Mempool> {
    mempool: Mempool,
    timing: PrivateReleaseTiming,
    state: watch::Sender<SchedulerState>,
    shutdown: CancellationToken,
}

impl<Mempool> PrivateReleaseScheduler<Mempool>
where
    Mempool: Service<Request, Response = Response, Error = BoxError> + Clone + Send + 'static,
    Mempool::Future: Send,
{
    pub(crate) fn spawn(
        mempool: Mempool,
        timing: PrivateReleaseTiming,
        state: watch::Sender<SchedulerState>,
        shutdown: CancellationToken,
    ) -> JoinHandle<Result<(), BoxError>> {
        let scheduler = Self::new(mempool, timing, state, shutdown);
        tokio::spawn(scheduler.run().in_current_span())
    }

    fn new(
        mempool: Mempool,
        timing: PrivateReleaseTiming,
        state: watch::Sender<SchedulerState>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            mempool,
            timing,
            state,
            shutdown,
        }
    }

    async fn run(mut self) -> Result<(), BoxError> {
        let shutdown = self.shutdown.clone();
        let state = self.state.clone();
        state.send_replace(SchedulerState::Running);

        loop {
            match self.wait_until_due().await {
                Ok(WaitOutcome::Cancelled) => {
                    state.send_replace(SchedulerState::Stopping);
                    return Ok(());
                }
                Ok(WaitOutcome::Continue) => {}
                Err(error) => {
                    state.send_replace(SchedulerState::Stalled);
                    return Err(error);
                }
            }

            let promotion = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    state.send_replace(SchedulerState::Stopping);
                    return Ok(());
                }
                result = self.promote_due() => result,
            };
            match promotion {
                Ok(PromotionAttempt::Completed) => {}
                Ok(PromotionAttempt::Retry) => {
                    let retry_wait = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => Ok(WaitOutcome::Cancelled),
                        changed = self.timing.deadlines.changed() => changed
                            .map(|()| WaitOutcome::Continue)
                            .map_err(BoxError::from),
                        _ = sleep(RETRY_DELAY) => Ok(WaitOutcome::Continue),
                    };
                    match retry_wait {
                        Ok(WaitOutcome::Cancelled) => {
                            state.send_replace(SchedulerState::Stopping);
                            return Ok(());
                        }
                        Ok(WaitOutcome::Continue) => {}
                        Err(error) => {
                            state.send_replace(SchedulerState::Stalled);
                            return Err(error);
                        }
                    }
                }
                Err(error) => {
                    state.send_replace(SchedulerState::Stalled);
                    return Err(error);
                }
            }
        }
    }

    async fn wait_until_due(&mut self) -> Result<WaitOutcome, BoxError> {
        loop {
            let deadline = *self.timing.deadlines.borrow_and_update();
            let Some(deadline) = deadline else {
                tokio::select! {
                    biased;
                    _ = self.shutdown.cancelled() => return Ok(WaitOutcome::Cancelled),
                    changed = self.timing.deadlines.changed() => changed.map_err(BoxError::from)?,
                }
                continue;
            };
            let deadline = self
                .timing
                .clock
                .instant_at(deadline)
                .map(Instant::from_std)
                .ok_or("private release deadline exceeds the platform monotonic clock")?;
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => return Ok(WaitOutcome::Cancelled),
                changed = self.timing.deadlines.changed() => changed.map_err(BoxError::from)?,
                _ = sleep_until(deadline) => return Ok(WaitOutcome::Continue),
            }
        }
    }

    async fn promote_due(&mut self) -> Result<PromotionAttempt, BoxError> {
        let mut mempool = match self.mempool.clone().ready_oneshot().await {
            Ok(mempool) => mempool,
            Err(_error) => {
                warn!("private release scheduler readiness failed; retrying");
                return Ok(PromotionAttempt::Retry);
            }
        };
        let response = match mempool.call(Request::PromotePrivateDue).await {
            Ok(response) => response,
            Err(_error) => {
                warn!("private release scheduler call failed; retrying");
                return Ok(PromotionAttempt::Retry);
            }
        };
        match response {
            Response::PrivatePromoted(
                PrivatePromotionOutcome::Promoted { .. } | PrivatePromotionOutcome::Terminal { .. },
            ) => Ok(PromotionAttempt::Completed),
            Response::PrivatePromoted(
                PrivatePromotionOutcome::NoDue | PrivatePromotionOutcome::Recoverable { .. },
            ) => Ok(PromotionAttempt::Retry),
            _ => Err(
                "mempool returned an unexpected response to the private release scheduler".into(),
            ),
        }
    }
}

#[cfg(test)]
mod tests;
