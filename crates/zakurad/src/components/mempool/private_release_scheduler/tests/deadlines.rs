use super::*;

#[tokio::test(start_paused = true)]
async fn promotion_runs_at_the_non_epoch_aligned_maximum_deadline() {
    // Given: a 60-second epoch whose first admission is capped at exactly 65 seconds.
    let clock = MonotonicClock::new();
    let policy = ReleasePolicy::new(
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(65),
    )
    .expect("test release policy is valid");
    let mut core = AdmissionCore::new(clock.clone(), policy);
    let AdmissionOutcome::Accepted(admission) = core
        .admit(AdmissionId(1), AdmissionOrigin::PrivateGateway)
        .expect("test admission succeeds")
    else {
        panic!("new admission is accepted");
    };
    let scheduled = admission.state.scheduled_release_at();
    assert_eq!(
        scheduled
            .checked_duration_since(admission.state.accepted_at())
            .expect("release follows acceptance"),
        Duration::from_secs(65)
    );
    let deadline = tokio::time::Instant::from_std(
        clock
            .instant_at(scheduled)
            .expect("test deadline fits the platform monotonic clock"),
    );
    let (deadlines, deadline_receiver) = watch::channel(core.earliest_release_at());
    let timing = PrivateReleaseTiming::new(clock, deadline_receiver);
    let mempool = RecordingMempool::responding([PrivatePromotionOutcome::Promoted { count: 1 }])
        .clearing_deadline(deadlines);
    let observer = mempool.clone();
    let shutdown = CancellationToken::new();
    let (scheduler, _state) = scheduler(mempool, timing, shutdown.clone());
    let task = tokio::spawn(scheduler.run());
    tokio::task::yield_now().await;

    let until_deadline = deadline.saturating_duration_since(tokio::time::Instant::now());
    tokio::time::advance(until_deadline - Duration::from_nanos(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(observer.call_count(), 0);
    tokio::time::advance(Duration::from_millis(1) + Duration::from_nanos(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(observer.call_count(), 1);

    shutdown.cancel();
    assert!(task.await.expect("scheduler task does not panic").is_ok());
}

#[tokio::test(start_paused = true)]
async fn earlier_deadline_update_wakes_the_scheduler() {
    let (timing, deadlines, _) = timing_after(Duration::from_secs(120));
    let earlier = timing
        .clock()
        .now()
        .checked_add(Duration::from_secs(10))
        .expect("test deadline is representable");
    let mempool = RecordingMempool::responding([
        PrivatePromotionOutcome::Recoverable { count: 2 },
        PrivatePromotionOutcome::Promoted { count: 1 },
    ])
    .clearing_deadline(deadlines.clone());
    let observer = mempool.clone();
    let shutdown = CancellationToken::new();
    let (scheduler, _state) = scheduler(mempool, timing.clone(), shutdown.clone());
    let task = tokio::spawn(scheduler.run());
    tokio::task::yield_now().await;

    deadlines.send_replace(Some(earlier));
    let earlier = tokio::time::Instant::from_std(
        timing
            .clock()
            .instant_at(earlier)
            .expect("test deadline fits the platform monotonic clock"),
    );
    tokio::time::advance(
        earlier.saturating_duration_since(tokio::time::Instant::now()) + Duration::from_millis(1),
    )
    .await;
    tokio::task::yield_now().await;
    assert_eq!(observer.call_count(), 1);
    tokio::time::advance(RETRY_DELAY + Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(observer.call_count(), 2);
    assert!(!task.is_finished());

    shutdown.cancel();
    assert!(task.await.expect("scheduler task does not panic").is_ok());
}

#[tokio::test(start_paused = true)]
async fn cancellation_before_first_tick_stops_without_promotion() {
    let mempool = RecordingMempool::responding([PrivatePromotionOutcome::NoDue]);
    let observer = mempool.clone();
    let shutdown = CancellationToken::new();
    let (timing, _deadlines, _) = timing_after(Duration::from_secs(60));
    let (scheduler, mut state) = scheduler(mempool, timing, shutdown.clone());
    let task = tokio::spawn(scheduler.run());
    tokio::task::yield_now().await;
    assert_eq!(*state.borrow_and_update(), SchedulerState::Running);

    shutdown.cancel();

    assert!(task.await.expect("scheduler task does not panic").is_ok());
    assert_eq!(observer.call_count(), 0);
    assert_eq!(*state.borrow_and_update(), SchedulerState::Stopping);
}
