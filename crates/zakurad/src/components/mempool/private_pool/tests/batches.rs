use super::*;

#[test]
fn snapshot_preserves_the_requested_admission_order() {
    // Given: records inserted in an order different from a release batch.
    let mut transactions = transactions(3).into_iter();
    let mut pool = PrivateVerifiedPool::new(config(3, LARGE_LIMIT));
    for admission_id in [3, 1, 2] {
        pool.insert(record(
            admission_id,
            transactions.next().expect("transaction for admission"),
        ))
        .expect("record fits");
    }

    // When: a caller snapshots an explicitly ordered complete batch.
    let batch = pool
        .snapshot_batch(&[AdmissionId(2), AdmissionId(3), AdmissionId(1)])
        .expect("all requested records exist");

    // Then: the immutable snapshot retains the caller's deterministic order.
    assert_eq!(
        batch.admission_ids().collect::<Vec<_>>(),
        [AdmissionId(2), AdmissionId(3), AdmissionId(1)]
    );
}

#[test]
fn remove_batch_is_all_or_nothing() {
    // Given: two retained records and a request containing one absent identity.
    let mut transactions = transactions(2).into_iter();
    let mut pool = PrivateVerifiedPool::new(config(2, LARGE_LIMIT));
    for admission_id in [20, 21] {
        pool.insert(record(
            admission_id,
            transactions.next().expect("transaction for admission"),
        ))
        .expect("record fits");
    }
    let before = pool.stats();

    // When: exact removal cannot resolve the complete requested batch.
    let incomplete = pool.remove_batch(&[AdmissionId(20), AdmissionId(99)]);

    // Then: no private record is removed.
    assert_eq!(incomplete, Err(PrivatePoolError::IncompleteBatch));
    assert_eq!(pool.stats(), before);

    // When: the exact complete batch is removed.
    let removed = pool
        .remove_batch(&[AdmissionId(21), AdmissionId(20)])
        .expect("complete batch removes atomically");

    // Then: all records are returned in requested order and capacity is released.
    assert_eq!(
        removed.admission_ids().collect::<Vec<_>>(),
        [AdmissionId(21), AdmissionId(20)]
    );
    assert_eq!(pool.stats().transaction_count, 0);
    assert_eq!(pool.stats().serialized_bytes, 0);
}

#[test]
fn duplicate_ids_from_an_internal_batch_caller_are_rejected_without_removal() {
    // Given: one retained record and malformed input from an internal batch caller.
    // The RPC adapter assigns IDs, and AdmissionCore uniquely keys them before batching.
    let transaction = transactions(1).pop().expect("transaction");
    let mut pool = PrivateVerifiedPool::new(config(1, LARGE_LIMIT));
    pool.insert(record(30, transaction)).expect("record fits");
    let before = pool.stats();

    // When: removal repeats an admission identity in the same batch.
    let result = pool.remove_batch(&[AdmissionId(30), AdmissionId(30)]);

    // Then: the malformed batch is rejected before mutation.
    assert_eq!(result, Err(PrivatePoolError::DuplicateBatchAdmission));
    assert_eq!(pool.stats(), before);
}

#[test]
fn revalidation_snapshot_selects_stale_records_and_can_force_current_tip() {
    // Given: one retained record verified at the current tip and one at an older tip.
    let mut transactions = transactions(2).into_iter();
    let current_tip = VerificationTip::new(Some((
        zakura_chain::block::Hash([2; 32]),
        zakura_chain::block::Height(101),
    )));
    let mut current = record(40, transactions.next().expect("current-tip transaction"));
    current = PrivateRecord::new(PrivateRecordFields {
        transaction: current.transaction().clone(),
        spent_mempool_outpoints: current.spent_mempool_outpoints().to_vec(),
        context: current.context(),
        verification_tip: current_tip,
    });
    let stale = record(41, transactions.next().expect("stale transaction"));
    let mut pool = PrivateVerifiedPool::new(config(2, LARGE_LIMIT));
    pool.insert(current).expect("current record fits");
    pool.insert(stale).expect("stale record fits");

    // When: Grow-style and Reset-style snapshots are requested.
    let grow = pool.snapshot_revalidation_requests(current_tip, false);
    let reset = pool.snapshot_revalidation_requests(current_tip, true);

    // Then: Grow selects only stale records, while Reset also selects same-tip records.
    assert_eq!(
        grow.iter()
            .map(PrivateRecord::admission_id)
            .collect::<Vec<_>>(),
        [AdmissionId(41)]
    );
    assert_eq!(
        reset
            .iter()
            .map(PrivateRecord::admission_id)
            .collect::<Vec<_>>(),
        [AdmissionId(40), AdmissionId(41)]
    );
    assert_eq!(pool.stats().transaction_count, 2);
}

#[test]
fn replace_updates_same_record_without_releasing_capacity() {
    // Given: one retained record and a newly verified replacement at another tip.
    let transaction = transactions(1).pop().expect("transaction");
    let original = record(50, transaction.clone());
    let replacement_tip = VerificationTip::new(Some((
        zakura_chain::block::Hash([3; 32]),
        zakura_chain::block::Height(102),
    )));
    let replacement = PrivateRecord::new(PrivateRecordFields {
        transaction,
        spent_mempool_outpoints: Vec::new(),
        context: original.context(),
        verification_tip: replacement_tip,
    });
    let mut pool = PrivateVerifiedPool::new(config(1, LARGE_LIMIT));
    pool.insert(original).expect("original record fits");
    let before = pool.stats();

    // When: revalidation atomically replaces the same admission and transaction.
    pool.replace(replacement)
        .expect("same record can be replaced");

    // Then: the new verification metadata is visible and capacity ownership is unchanged.
    assert_eq!(pool.stats(), before);
    assert_eq!(
        pool.record(AdmissionId(50))
            .expect("replacement remains retained")
            .verification_tip(),
        replacement_tip
    );
}

#[test]
fn remove_one_record_releases_only_its_capacity() {
    // Given: two retained records.
    let mut transactions = transactions(2).into_iter();
    let first = record(60, transactions.next().expect("first transaction"));
    let first_bytes = first.serialized_bytes();
    let second = record(61, transactions.next().expect("second transaction"));
    let second_bytes = second.serialized_bytes();
    let mut pool = PrivateVerifiedPool::new(config(2, LARGE_LIMIT));
    pool.insert(first).expect("first record fits");
    pool.insert(second).expect("second record fits");

    // When: one exact admission is removed.
    let removed = pool.remove(AdmissionId(60)).expect("record exists");

    // Then: only that record's capacity is released.
    assert_eq!(removed.admission_id(), AdmissionId(60));
    assert_eq!(removed.serialized_bytes(), first_bytes);
    assert_eq!(pool.stats().transaction_count, 1);
    assert_eq!(pool.stats().serialized_bytes, second_bytes);
    assert!(pool.record(AdmissionId(60)).is_none());
    assert!(pool.record(AdmissionId(61)).is_some());
}
