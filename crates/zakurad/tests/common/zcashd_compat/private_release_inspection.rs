#![allow(clippy::print_stdout)]

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use color_eyre::eyre::{eyre, Result};
use tokio::time::{sleep, Instant};
use zakura_chain::parameters::NetworkKind;
use zakura_node_services::mempool::{
    PrivateAdmissionStatus, PrivatePoolDiagnostics, SchedulerState,
};
use zakurad::components::mempool::{
    operator_policy::OperatorPrivacyPolicy, private_pool::PrivatePoolConfig,
};

use super::{
    config::read_test_network_kind,
    launch::{spawn_zakurad_with_zcashd_compat_config, ZcashdCompatSetup},
    private_release::InspectionTiming,
    private_release_transcript::{
        completion_record, format_policy_records, format_timeline_record, p2p_observer_record,
        MempoolCounts, PolicyRecord,
    },
    tx_flow::{import_miner_key, signed_transparent_transaction},
    wait_for_zcashd_height, TEST_ZCASHD_COMPAT,
};
use crate::common::regtest::MiningRpcMethods;

const PRIVATE_MEMPOOL_POLL_INTERVAL: Duration = Duration::from_millis(100);
const ZCASHD_RELAY_POLL_ATTEMPTS: u32 = 30;

pub async fn inspect_private_release() -> Result<()> {
    if std::env::var_os(TEST_ZCASHD_COMPAT).is_none() {
        return Err(eyre!(
            "managed private-release inspection requires TEST_ZCASHD_COMPAT=1"
        ));
    }
    if read_test_network_kind()? != NetworkKind::Regtest {
        return Err(eyre!(
            "managed private-release inspection requires regtest; unset TEST_ZCASHD_COMPAT_NETWORK"
        ));
    }

    let timing = InspectionTiming::from_env()?;
    let captured_policy = Arc::new(OnceLock::new());
    let spawn_policy = Arc::clone(&captured_policy);
    let setup = spawn_zakurad_with_zcashd_compat_config(move |config| {
        config.mempool.private_pool =
            PrivatePoolConfig::new(10, 1_000_000, timing.release_config())
                .expect("inspection private-pool limits are nonzero");
        let policy = OperatorPrivacyPolicy::new(
            config.mempool.private_pool,
            config.network.max_connections_per_ip,
            config.network.peerset_initial_target_size,
        )
        .expect("final inspection config fits the canonical policy representation");
        spawn_policy
            .set(policy)
            .expect("the spawn config closure runs exactly once");
    })
    .await
    .map_err(|error| {
        eyre!("failed to start the managed Zakura and zcashd regtest nodes: {error}")
    })?;
    let policy = *captured_policy
        .get()
        .expect("the completed spawn applied the inspection config closure");
    let policy_hash = policy.hash().to_hex();
    for record in format_policy_records(PolicyRecord {
        version: policy.version(),
        hash: &policy_hash,
        release_timing: policy.release_timing(),
        epoch: policy.release_epoch(),
        minimum: policy.minimum_release_delay(),
        maximum: policy.maximum_release_delay(),
    }) {
        println!("{record}");
    }
    let started_mempools = query_inspection_mempools(&setup).await?;
    started_mempools.print_timeline("managed_nodes_started");

    setup.zakura_client.generate(110).await?;
    wait_for_zcashd_height(&setup.zcashd_client, 110).await?;
    import_miner_key(&setup).await?;
    let (raw_transaction, transaction_id) = signed_transparent_transaction(&setup)
        .await
        .map_err(|_| eyre!("failed to create the signed inspection transaction"))?;

    let private_params = serde_json::json!([&raw_transaction]).to_string();
    let accepted: PrivateAdmissionStatus = setup
        .zakura_client
        .json_result_from_call("sendprivatetransaction", &private_params)
        .await
        .map_err(|_| eyre!("private transaction admission failed"))?;
    assert_eq!(
        accepted,
        PrivateAdmissionStatus::Accepted,
        "first private submission must be accepted"
    );
    let admission_completed = Instant::now();
    let admitted: PrivatePoolDiagnostics = setup
        .zakura_client
        .json_result_from_call("getprivatepoolinfo", "[]")
        .await
        .map_err(|_| eyre!("private-pool diagnostics failed after admission"))?;
    assert!(admitted.completed_window.is_none());
    assert_eq!(admitted.transaction_count, 1);
    assert!(admitted.serialized_bytes > 0);
    assert_eq!(admitted.embargoed_count, 1);
    assert_eq!(admitted.eligible_count, 0);
    assert_eq!(admitted.releasing_count, 0);
    assert_eq!(admitted.scheduler_state, SchedulerState::Running);
    let admitted_mempools = query_inspection_mempools(&setup).await?;
    assert_eq!(admitted_mempools.private, admitted);
    admitted_mempools.assert_public_omits(&transaction_id);
    admitted_mempools.print_timeline("private_admission_accepted");

    sleep(timing.retry_delay()).await;
    let existing: PrivateAdmissionStatus = setup
        .zakura_client
        .json_result_from_call("sendprivatetransaction", &private_params)
        .await
        .map_err(|_| eyre!("exact private transaction retry failed"))?;
    assert_eq!(
        existing,
        PrivateAdmissionStatus::Existing,
        "exact private retry must return Existing"
    );
    let after_retry: PrivatePoolDiagnostics = setup
        .zakura_client
        .json_result_from_call("getprivatepoolinfo", "[]")
        .await
        .map_err(|_| eyre!("private-pool diagnostics failed after retry"))?;
    assert_eq!(
        after_retry, admitted,
        "exact retry must not change aggregate private state"
    );
    let retry_mempools = query_inspection_mempools(&setup).await?;
    assert_eq!(retry_mempools.private, after_retry);
    retry_mempools.assert_public_omits(&transaction_id);
    assert!(
        Instant::now() < admission_completed + policy.minimum_release_delay(),
        "the idempotent retry must complete before the configured minimum release delay"
    );
    retry_mempools.print_timeline("private_retry_existing");

    wait_for_zakura_mempool_tx_before(
        &setup,
        &transaction_id,
        admission_completed + policy.maximum_release_delay() + PRIVATE_MEMPOOL_POLL_INTERVAL,
    )
    .await?;
    let zakura_release_mempools = query_inspection_mempools(&setup).await?;
    assert!(zakura_release_mempools.zakura.contains(&transaction_id));
    zakura_release_mempools.print_timeline("zakura_public_release");

    wait_for_zcashd_mempool_tx(&setup, &transaction_id).await?;
    let observer_release_mempools = query_inspection_mempools(&setup).await?;
    assert!(observer_release_mempools.zakura.contains(&transaction_id));
    assert!(observer_release_mempools.observer.contains(&transaction_id));
    let released = &observer_release_mempools.private;
    assert_eq!(released.transaction_count, 0);
    assert_eq!(released.serialized_bytes, 0);
    assert_eq!(released.embargoed_count, 0);
    assert_eq!(released.eligible_count, 0);
    assert_eq!(released.releasing_count, 0);
    observer_release_mempools.print_timeline("observer_public_release");
    println!("{}", p2p_observer_record());

    setup.teardown()?;
    println!("{}", completion_record());
    Ok(())
}

struct InspectionMempools {
    private: PrivatePoolDiagnostics,
    zakura: Vec<String>,
    observer: Vec<String>,
}

impl InspectionMempools {
    fn print_timeline(&self, event: &str) {
        let counts = MempoolCounts {
            private: self.private.transaction_count,
            zakura_public: self.zakura.len(),
            observer_public: self.observer.len(),
        };
        println!("{}", format_timeline_record(event, counts));
    }

    fn assert_public_omits(&self, transaction_id: &str) {
        assert!(self.zakura.iter().all(|entry| entry != transaction_id));
        assert!(self.observer.iter().all(|entry| entry != transaction_id));
    }
}

async fn query_inspection_mempools(setup: &ZcashdCompatSetup) -> Result<InspectionMempools> {
    let private = setup
        .zakura_client
        .json_result_from_call("getprivatepoolinfo", "[]")
        .await
        .map_err(|error| eyre!("private-pool diagnostics query failed: {error}"))?;
    let zakura = setup
        .zakura_client
        .json_result_from_call("getrawmempool", "[]")
        .await
        .map_err(|error| eyre!("Zakura public mempool query failed: {error}"))?;
    let observer = setup
        .zcashd_client
        .json_result_from_call("getrawmempool", "[]")
        .await
        .map_err(|error| eyre!("zcashd public mempool query failed: {error}"))?;
    Ok(InspectionMempools {
        private,
        zakura,
        observer,
    })
}

async fn wait_for_zakura_mempool_tx_before(
    setup: &ZcashdCompatSetup,
    transaction_id: &str,
    deadline: Instant,
) -> Result<()> {
    loop {
        if Instant::now() >= deadline {
            return Err(eyre!(
                "transaction was not observed before a retry-derived release deadline"
            ));
        }
        let mempool: Vec<String> = setup
            .zakura_client
            .json_result_from_call("getrawmempool", "[]")
            .await
            .map_err(|_| eyre!("Zakura public mempool query failed while awaiting release"))?;
        if mempool.iter().any(|entry| entry == transaction_id) && Instant::now() < deadline {
            return Ok(());
        }
        sleep(PRIVATE_MEMPOOL_POLL_INTERVAL).await;
    }
}

async fn wait_for_zcashd_mempool_tx(setup: &ZcashdCompatSetup, transaction_id: &str) -> Result<()> {
    for attempt in 1..=ZCASHD_RELAY_POLL_ATTEMPTS {
        let mempool: Vec<String> = setup
            .zcashd_client
            .json_result_from_call("getrawmempool", "[]")
            .await
            .map_err(|error| eyre!("zcashd getrawmempool after public release: {error}"))?;
        if mempool.iter().any(|entry| entry == transaction_id) {
            return Ok(());
        }
        if attempt < ZCASHD_RELAY_POLL_ATTEMPTS {
            sleep(Duration::from_secs(1)).await;
        }
    }
    Err(eyre!(
        "public release did not relay to zcashd within {ZCASHD_RELAY_POLL_ATTEMPTS} seconds"
    ))
}
