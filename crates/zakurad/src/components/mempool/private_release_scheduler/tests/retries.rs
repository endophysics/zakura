use super::*;

#[tokio::test(start_paused = true)]
async fn cancellation_while_readiness_is_blocked_stops_promptly() {
    cancellation_while_service_is_blocked(BlockAt::Readiness).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_while_call_is_blocked_stops_promptly() {
    cancellation_while_service_is_blocked(BlockAt::Call).await;
}

#[tokio::test(start_paused = true)]
async fn one_shot_readiness_failure_retries_without_stalling() {
    service_failure_retries(FailOnce::Readiness, 1).await;
}

#[tokio::test(start_paused = true)]
async fn one_shot_call_failure_retries_without_stalling() {
    service_failure_retries(FailOnce::Call, 2).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_retry_wait_stops_promptly() {
    let (timing, deadlines, deadline) = timing_after(Duration::from_secs(1));
    let mempool = RecordingMempool::failing_once(FailOnce::Call).clearing_deadline(deadlines);
    let observer = mempool.clone();
    let shutdown = CancellationToken::new();
    let (scheduler, mut state) = scheduler(mempool, timing, shutdown.clone());
    let task = tokio::spawn(scheduler.run());
    tokio::task::yield_now().await;
    tokio::time::advance(
        deadline.saturating_duration_since(tokio::time::Instant::now()) + Duration::from_millis(1),
    )
    .await;
    tokio::task::yield_now().await;
    assert_eq!(observer.call_count(), 1);

    shutdown.cancel();

    assert!(task.await.expect("scheduler task does not panic").is_ok());
    assert_eq!(*state.borrow_and_update(), SchedulerState::Stopping);
    assert_eq!(observer.call_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn deadline_update_interrupts_retry_wait() {
    // Given: a due promotion that reports one recoverable result before succeeding.
    let (timing, deadlines, deadline) = timing_after(Duration::from_secs(1));
    let clock = timing.clock().clone();
    let mempool = RecordingMempool::responding([
        PrivatePromotionOutcome::Recoverable { count: 1 },
        PrivatePromotionOutcome::Promoted { count: 1 },
    ])
    .clearing_deadline(deadlines.clone());
    let observer = mempool.clone();
    let shutdown = CancellationToken::new();
    let (scheduler, _state) = scheduler(mempool, timing, shutdown.clone());
    let task = tokio::spawn(scheduler.run());
    tokio::task::yield_now().await;
    tokio::time::advance(
        deadline.saturating_duration_since(tokio::time::Instant::now()) + Duration::from_millis(1),
    )
    .await;
    tokio::task::yield_now().await;
    assert_eq!(observer.call_count(), 1);

    // When: authoritative private state publishes a new deadline that is already due.
    deadlines.send_replace(Some(clock.now()));
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;

    // Then: the scheduler retries before the one-second fallback delay.
    assert_eq!(observer.call_count(), 2);
    shutdown.cancel();
    assert!(task.await.expect("scheduler task does not panic").is_ok());
}

async fn cancellation_while_service_is_blocked(block_at: BlockAt) {
    let mempool = RecordingMempool::blocked(block_at);
    let observer = mempool.clone();
    let shutdown = CancellationToken::new();
    let (timing, _deadlines, deadline) = timing_after(Duration::from_secs(1));
    let (scheduler, mut state) = scheduler(mempool, timing, shutdown.clone());
    let task = tokio::spawn(scheduler.run());
    tokio::task::yield_now().await;
    tokio::time::advance(
        deadline.saturating_duration_since(tokio::time::Instant::now()) + Duration::from_millis(1),
    )
    .await;
    tokio::task::yield_now().await;

    shutdown.cancel();

    assert!(task.await.expect("scheduler task does not panic").is_ok());
    assert!(observer.call_count() <= 1);
    assert_eq!(*state.borrow_and_update(), SchedulerState::Stopping);
}

async fn service_failure_retries(failure: FailOnce, expected_calls: usize) {
    let (timing, deadlines, deadline) = timing_after(Duration::from_secs(1));
    let mempool = RecordingMempool::failing_once(failure).clearing_deadline(deadlines);
    let observer = mempool.clone();
    let shutdown = CancellationToken::new();
    let (scheduler, mut state) = scheduler(mempool, timing, shutdown.clone());
    let task = tokio::spawn(scheduler.run());
    tokio::task::yield_now().await;
    tokio::time::advance(
        deadline.saturating_duration_since(tokio::time::Instant::now()) + Duration::from_millis(1),
    )
    .await;
    tokio::task::yield_now().await;
    assert_eq!(*state.borrow_and_update(), SchedulerState::Running);
    assert!(!task.is_finished());

    tokio::time::advance(RETRY_DELAY + Duration::from_millis(1)).await;
    tokio::task::yield_now().await;

    assert_eq!(observer.call_count(), expected_calls);
    assert_eq!(*state.borrow_and_update(), SchedulerState::Running);
    assert!(!task.is_finished());
    shutdown.cancel();
    assert!(task.await.expect("scheduler task does not panic").is_ok());
}
