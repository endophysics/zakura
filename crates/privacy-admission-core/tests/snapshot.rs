//! Versioned diagnostic snapshot behavior.

mod common;

use std::collections::BTreeSet;

use common::{policy, CountingClock};
use privacy_admission_core::{
    AdmissionCore, AdmissionId, AdmissionOrigin, AdmissionStateLabel, DiagnosticSnapshot,
    ReasonCode, Timestamp, DIAGNOSTIC_SNAPSHOT_SCHEMA_VERSION,
};

#[test]
fn diagnostic_snapshot_is_versioned_ordered_and_plaintext_free() {
    // Given: records inserted out of identifier order with one terminal reason.
    let clock = CountingClock::new(Timestamp(12));
    let mut core = AdmissionCore::new(clock.clone(), policy());
    core.admit(AdmissionId(9), AdmissionOrigin::Development)
        .expect("admission succeeds");
    core.admit(AdmissionId(3), AdmissionOrigin::PrivateGateway)
        .expect("admission succeeds");
    clock.set(Timestamp(15));
    core.reject(
        AdmissionId(9),
        ReasonCode::try_from("policy").expect("reason is valid"),
    )
    .expect("rejection succeeds");

    // When: diagnostics are projected and serialized.
    let snapshot = core.snapshot();
    let json = serde_json::to_value(&snapshot).expect("snapshot serializes");
    let encoded = serde_json::to_string(&snapshot).expect("snapshot serializes");
    let decoded: DiagnosticSnapshot =
        serde_json::from_str(&encoded).expect("snapshot deserializes");

    // Then: schema one exposes only the documented opaque fields in ID order.
    assert_eq!(snapshot.schema_version, DIAGNOSTIC_SNAPSHOT_SCHEMA_VERSION);
    assert_eq!(decoded, snapshot);
    assert_eq!(
        snapshot
            .admissions
            .iter()
            .map(|record| record.admission_id)
            .collect::<Vec<_>>(),
        vec![AdmissionId(3), AdmissionId(9)]
    );
    assert_eq!(snapshot.admissions[0].state, AdmissionStateLabel::Embargoed);
    assert_eq!(snapshot.admissions[1].state, AdmissionStateLabel::Rejected);
    assert_eq!(clock.calls(), 3);

    let root = json.as_object().expect("diagnostic snapshot is an object");
    assert_eq!(
        root.keys().cloned().collect::<BTreeSet<_>>(),
        ["admissions", "schema_version"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );

    let admission = json["admissions"][0]
        .as_object()
        .expect("diagnostic admission is an object");
    let keys = admission.keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        [
            "accepted_at_ns",
            "admission_id",
            "batch_id",
            "origin",
            "reason",
            "scheduled_release_at_ns",
            "state",
            "terminal_at_ns",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
}
