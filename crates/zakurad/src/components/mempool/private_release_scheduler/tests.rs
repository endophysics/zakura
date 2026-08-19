use std::{
    collections::VecDeque,
    future::{pending, Future},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use privacy_admission_core::{
    AdmissionCore, AdmissionId, AdmissionOrigin, AdmissionOutcome, Clock, MonotonicClock,
    ReleasePolicy, Timestamp,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tower::{BoxError, Service};
use zakura_node_services::mempool::{PrivatePromotionOutcome, Request, Response, SchedulerState};

use super::{PrivateReleaseScheduler, PrivateReleaseTiming, RETRY_DELAY};

mod deadlines;
mod retries;

#[derive(Clone, Copy)]
enum BlockAt {
    Never,
    Readiness,
    Call,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FailOnce {
    Readiness,
    Call,
}

#[derive(Clone)]
struct RecordingMempool {
    calls: Arc<Mutex<Vec<Request>>>,
    responses: Arc<Mutex<VecDeque<PrivatePromotionOutcome>>>,
    block_at: BlockAt,
    fail_once: Arc<Mutex<Option<FailOnce>>>,
    deadlines: Option<watch::Sender<Option<Timestamp>>>,
}

impl RecordingMempool {
    fn responding(responses: impl IntoIterator<Item = PrivatePromotionOutcome>) -> Self {
        Self {
            calls: Arc::default(),
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            block_at: BlockAt::Never,
            fail_once: Arc::default(),
            deadlines: None,
        }
    }

    fn blocked(block_at: BlockAt) -> Self {
        Self {
            block_at,
            ..Self::responding([PrivatePromotionOutcome::NoDue])
        }
    }

    fn failing_once(failure: FailOnce) -> Self {
        Self {
            fail_once: Arc::new(Mutex::new(Some(failure))),
            ..Self::responding([PrivatePromotionOutcome::Promoted { count: 1 }])
        }
    }

    fn clearing_deadline(mut self, deadlines: watch::Sender<Option<Timestamp>>) -> Self {
        self.deadlines = Some(deadlines);
        self
    }

    fn call_count(&self) -> usize {
        self.calls
            .lock()
            .expect("test call lock is available")
            .len()
    }

    fn take_failure(&self, failure: FailOnce) -> bool {
        let mut next = self
            .fail_once
            .lock()
            .expect("test failure lock is available");
        if *next == Some(failure) {
            *next = None;
            true
        } else {
            false
        }
    }
}

impl Service<Request> for RecordingMempool {
    type Response = Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.take_failure(FailOnce::Readiness) {
            return Poll::Ready(Err("readiness failed".into()));
        }
        match self.block_at {
            BlockAt::Readiness => Poll::Pending,
            BlockAt::Never | BlockAt::Call => Poll::Ready(Ok(())),
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.calls
            .lock()
            .expect("test call lock is available")
            .push(request);
        if matches!(self.block_at, BlockAt::Call) {
            return Box::pin(pending());
        }
        if self.take_failure(FailOnce::Call) {
            return Box::pin(async { Err("call failed".into()) });
        }
        let outcome = self
            .responses
            .lock()
            .expect("test response lock is available")
            .pop_front()
            .unwrap_or(PrivatePromotionOutcome::NoDue);
        if matches!(
            outcome,
            PrivatePromotionOutcome::Promoted { .. } | PrivatePromotionOutcome::Terminal { .. }
        ) {
            if let Some(deadlines) = &self.deadlines {
                deadlines.send_replace(None);
            }
        }
        Box::pin(async move { Ok(Response::PrivatePromoted(outcome)) })
    }
}

fn timing_after(
    delay: Duration,
) -> (
    PrivateReleaseTiming,
    watch::Sender<Option<Timestamp>>,
    tokio::time::Instant,
) {
    let clock = MonotonicClock::new();
    let deadline = clock
        .now()
        .checked_add(delay)
        .expect("test deadline is representable");
    let deadline_instant = tokio::time::Instant::from_std(
        clock
            .instant_at(deadline)
            .expect("test deadline fits the platform monotonic clock"),
    );
    let (sender, receiver) = watch::channel(Some(deadline));
    (
        PrivateReleaseTiming::new(clock, receiver),
        sender,
        deadline_instant,
    )
}

fn scheduler(
    mempool: RecordingMempool,
    timing: PrivateReleaseTiming,
    shutdown: CancellationToken,
) -> (
    PrivateReleaseScheduler<RecordingMempool>,
    watch::Receiver<SchedulerState>,
) {
    let (state_sender, state_receiver) = watch::channel(SchedulerState::Idle);
    (
        PrivateReleaseScheduler::new(mempool, timing, state_sender, shutdown),
        state_receiver,
    )
}
