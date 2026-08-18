use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable identifier for an accepted admission.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct AdmissionId(pub u64);

/// Stable identifier for a release batch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BatchId(pub u64);

/// Source of an admission request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionOrigin {
    /// Request received through the private gateway.
    PrivateGateway,
    /// Request created by a development-only flow.
    Development,
}

/// A validated machine-readable reason for an admission decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReasonCode(String);

impl ReasonCode {
    /// Borrow the validated reason-code text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ReasonCode {
    type Error = ReasonCodeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if !(1..=32).contains(&value.len()) {
            return Err(ReasonCodeError::Length);
        }

        if !value.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '_' | '-')
        }) {
            return Err(ReasonCodeError::Characters);
        }

        Ok(Self(value))
    }
}

impl TryFrom<&str> for ReasonCode {
    type Error = ReasonCodeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_owned())
    }
}

impl From<ReasonCode> for String {
    fn from(value: ReasonCode) -> Self {
        value.0
    }
}

/// Reason-code validation failed.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ReasonCodeError {
    /// The code length is outside the inclusive 1 through 32 byte range.
    #[error("reason code must be 1 through 32 bytes long")]
    Length,
    /// The code contains a character outside lowercase ASCII, digits, `_`, and `-`.
    #[error("reason code contains an unsupported character")]
    Characters,
}

#[cfg(test)]
mod tests {
    use crate::{AdmissionId, AdmissionOrigin, BatchId, ReasonCode};

    #[test]
    fn identifiers_and_origin_serialize_as_stable_domain_values() {
        // Given: domain identifiers and a private-gateway origin.
        let admission_id = AdmissionId(7);
        let batch_id = BatchId(9);
        let origin = AdmissionOrigin::PrivateGateway;

        // When: each value crosses the JSON boundary.
        let admission_json = serde_json::to_string(&admission_id).expect("identifier serializes");
        let batch_json = serde_json::to_string(&batch_id).expect("identifier serializes");
        let origin_json = serde_json::to_string(&origin).expect("origin serializes");

        // Then: its stable representation is preserved.
        assert_eq!(admission_json, "7");
        assert_eq!(batch_json, "9");
        assert_eq!(origin_json, "\"private_gateway\"");
    }

    #[test]
    fn reason_code_accepts_bounded_ascii_lowercase_code() {
        // Given: a reason code at the documented boundary.
        let reason = "a".repeat(32);

        // When: it is parsed at the domain boundary.
        let parsed = ReasonCode::try_from(reason.as_str()).expect("valid reason code");

        // Then: the validated value is retained.
        assert_eq!(parsed.as_str(), reason);
    }

    #[test]
    fn reason_code_rejects_empty_overlong_or_non_ascii_values() {
        // Given: invalid boundary inputs.
        let too_long = "a".repeat(33);
        let invalid = [
            "",
            too_long.as_str(),
            "Uppercase",
            "reason code",
            "caf\u{00e9}",
        ];

        // When / Then: parsing rejects each invalid input.
        for value in invalid {
            assert!(ReasonCode::try_from(value).is_err());
        }
    }
}
