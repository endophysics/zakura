use std::{collections::BTreeSet, time::Duration};

use clap::Parser;
use serde_json::Value;

use super::{execute, Cli, ExampleError};

#[test]
fn parser_accepts_required_invocation() {
    // Given: the documented deterministic invocation.
    let arguments = ["inspect", "--epoch", "5s", "--minimum-delay", "5s"];

    // When: clap parses its typed duration arguments.
    let parsed = Cli::try_parse_from(arguments);

    // Then: each required duration becomes the expected value.
    assert!(matches!(
        parsed,
        Ok(Cli {
            epoch,
            minimum_delay,
            maximum_delay: None,
        }) if epoch == Duration::from_secs(5) && minimum_delay == Duration::from_secs(5)
    ));
}

#[test]
fn parser_rejects_invalid_duration() {
    // Given: a non-duration epoch.
    let arguments = ["inspect", "--epoch", "invalid", "--minimum-delay", "5s"];

    // When: clap parses the arguments.
    let parsed = Cli::try_parse_from(arguments);

    // Then: parsing fails at the command boundary.
    assert!(parsed.is_err());
}

#[test]
fn maximum_delay_defaults_to_minimum_and_accepts_override() -> Result<(), ExampleError> {
    // Given: a minimum delay whose epoch rounding is capped differently by an override.
    let defaults = Cli {
        epoch: Duration::from_secs(5),
        minimum_delay: Duration::from_secs(6),
        maximum_delay: None,
    };
    let override_delay = Cli {
        maximum_delay: Some(Duration::from_secs(7)),
        ..defaults
    };
    let mut default_output = Vec::new();
    let mut override_output = Vec::new();

    // When: each policy drives the deterministic scenario.
    execute(defaults, &mut default_output)?;
    execute(override_delay, &mut override_output)?;

    // Then: absent maximum uses the minimum, while an override controls the cap.
    let default_events = json_lines(&default_output)?;
    let override_events = json_lines(&override_output)?;
    assert_eq!(
        default_events[0]["scheduled_release_at_ns"],
        6_000_000_000_u64
    );
    assert_eq!(
        override_events[0]["scheduled_release_at_ns"],
        7_000_000_000_u64
    );
    Ok(())
}

#[test]
fn scenario_emits_compact_plaintext_free_jsonl_contract() -> Result<(), ExampleError> {
    // Given: the documented deterministic policy.
    let cli = Cli {
        epoch: Duration::from_secs(5),
        minimum_delay: Duration::from_secs(5),
        maximum_delay: None,
    };
    let mut output = Vec::new();

    // When: the complete synthetic lifecycle runs.
    execute(cli, &mut output)?;

    // Then: each observable event is JSON with stable machine fields and ordering.
    let events = json_lines(&output)?;
    assert_eq!(
        events
            .iter()
            .map(|event| &event["event"])
            .collect::<Vec<_>>(),
        [
            "accepted",
            "accepted",
            "accepted",
            "embargoed",
            "existing",
            "rejected",
            "removed",
            "advanced",
            "due",
            "eligible",
            "prepared",
            "released",
            "snapshot",
        ]
    );
    assert_eq!(events[0]["admission_id"], 1_u64);
    assert_eq!(events[0]["accepted_at_ns"], 0_u64);
    assert_eq!(events[0]["scheduled_release_at_ns"], 5_000_000_000_u64);
    assert_eq!(events[3]["admission_ids"], serde_json::json!([1, 2, 3]));
    assert_eq!(events[5]["reason"], "policy_rejected");
    assert_eq!(events[6]["reason"], "operator_removed");
    assert_eq!(events[7]["now_ns"], 5_000_000_000_u64);
    assert_eq!(events[8]["admission_ids"], serde_json::json!([1]));
    assert_eq!(events[9]["admission_ids"], serde_json::json!([1]));
    assert_eq!(events[10]["batch_id"], 0_u64);
    assert_eq!(events[10]["admission_ids"], serde_json::json!([1]));
    assert_eq!(events[11]["batch_id"], 0_u64);
    assert_eq!(events[11]["admission_ids"], serde_json::json!([1]));
    assert_eq!(
        events[12]["snapshot"]
            .as_object()
            .map(|snapshot| snapshot.keys().cloned().collect::<BTreeSet<_>>()),
        Some(
            ["admissions".to_owned(), "schema_version".to_owned()]
                .into_iter()
                .collect()
        )
    );
    assert_eq!(events[12]["snapshot"]["schema_version"], 1_u64);
    assert_eq!(
        events[12]["snapshot"]["admissions"][0]["admission_id"],
        1_u64
    );
    assert_eq!(events[12]["snapshot"]["admissions"][0]["batch_id"], 0_u64);
    assert_eq!(events[12]["snapshot"]["admissions"][1]["state"], "rejected");
    assert_eq!(events[12]["snapshot"]["admissions"][2]["state"], "removed");
    Ok(())
}

fn json_lines(output: &[u8]) -> Result<Vec<Value>, ExampleError> {
    output
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(serde_json::from_slice)
        .collect::<Result<Vec<_>, _>>()
        .map_err(ExampleError::from)
}
