use super::*;

#[test]
fn insert_rejects_transaction_when_count_limit_is_full() {
    // Given: a pool at its independent transaction-count limit.
    let mut transactions = transactions(2).into_iter();
    let mut pool = PrivateVerifiedPool::new(config(1, LARGE_LIMIT));
    pool.insert(record(1, transactions.next().expect("first transaction")))
        .expect("first transaction fits");

    // When: another distinct private record is inserted.
    let result = pool.insert(record(2, transactions.next().expect("second transaction")));

    // Then: the private count bound rejects it without changing the pool.
    assert_eq!(result, Err(PrivatePoolError::TransactionCountLimit));
    assert_eq!(pool.stats().transaction_count, 1);
}

#[test]
fn insert_rejects_transaction_when_byte_limit_would_be_exceeded() {
    // Given: a byte limit one byte below the size needed by two records.
    let mut transactions = transactions(2).into_iter();
    let first = transactions.next().expect("first transaction");
    let second = transactions.next().expect("second transaction");
    let byte_limit = first.transaction.size() + second.transaction.size() - 1;
    let mut pool = PrivateVerifiedPool::new(config(2, byte_limit));
    pool.insert(record(1, first))
        .expect("first transaction fits");

    // When: the second record would cross the private byte bound.
    let result = pool.insert(record(2, second));

    // Then: byte pressure rejects it without changing retained bytes.
    assert_eq!(result, Err(PrivatePoolError::SerializedByteLimit));
    assert!(pool.stats().serialized_bytes <= byte_limit);
    assert_eq!(pool.stats().transaction_count, 1);
}

#[test]
fn insert_is_idempotent_for_the_exact_same_record() {
    // Given: one retained verified private record.
    let transaction = transactions(1).pop().expect("transaction");
    let expected_bytes = transaction.transaction.size();
    let candidate = record(7, transaction);
    let mut pool = PrivateVerifiedPool::new(config(1, expected_bytes));
    assert_eq!(pool.insert(candidate.clone()), Ok(InsertOutcome::Inserted));

    // When: the exact same identity, transaction, and context is retried.
    let result = pool.insert(candidate);

    // Then: insertion is idempotent and consumes no additional capacity.
    assert_eq!(result, Ok(InsertOutcome::Existing));
    assert_eq!(pool.stats().transaction_count, 1);
    assert_eq!(pool.stats().serialized_bytes, expected_bytes);
}

#[test]
fn insert_rejects_same_admission_id_for_a_different_transaction() {
    // Given: one admission identity already owns a transaction.
    let mut transactions = transactions(2).into_iter();
    let mut pool = PrivateVerifiedPool::new(config(2, LARGE_LIMIT));
    pool.insert(record(9, transactions.next().expect("first transaction")))
        .expect("first transaction fits");

    // When: that admission identity is reused for another transaction.
    let result = pool.insert(record(9, transactions.next().expect("second transaction")));

    // Then: a typed admission-ID conflict is returned.
    assert_eq!(result, Err(PrivatePoolError::AdmissionIdConflict));
}

#[test]
fn insert_rejects_same_transaction_under_a_different_context() {
    // Given: one transaction already belongs to an admission context.
    let transaction = transactions(1).pop().expect("transaction");
    let mut pool = PrivateVerifiedPool::new(config(2, LARGE_LIMIT));
    pool.insert(record(10, transaction.clone()))
        .expect("first context fits");

    // When: the same exact transaction is associated with another admission.
    let result = pool.insert(record(11, transaction));

    // Then: a typed transaction-context conflict is returned.
    assert_eq!(result, Err(PrivatePoolError::TransactionContextConflict));
}

#[test]
fn insert_rejects_dependency_on_an_existing_private_parent() {
    // Given: a retained private transaction with a transparent output.
    let parent = transaction_with_output();
    let parent_outpoint = OutPoint::from_usize(parent.transaction.id().mined_id(), 0);
    let child = transactions(2)
        .into_iter()
        .find(|candidate| candidate.transaction.id() != parent.transaction.id())
        .expect("a distinct child transaction");
    let mut pool = PrivateVerifiedPool::new(config(2, LARGE_LIMIT));
    pool.insert(record(12, parent)).expect("parent fits");

    // When: a candidate names an output created by that private parent.
    let result = pool.insert(record_with_spends(13, child, vec![parent_outpoint]));

    // Then: the dependency is rejected without creating a private graph.
    assert_eq!(result, Err(PrivatePoolError::PrivateParent));
    assert_eq!(pool.stats().transaction_count, 1);
}
