//! Isolated in-memory ownership for verified private transactions.

mod config;
mod error;
mod record;
mod stats;
mod store;

pub use super::VerificationTip;
pub use config::{PrivatePoolConfig, PrivatePoolConfigError, PrivateReleaseConfig};
pub use error::{InsertOutcome, PrivatePoolError};
pub use record::{PrivateBatch, PrivateRecord, PrivateRecordFields};
pub use stats::{PrivatePoolStateTotals, PrivatePoolStats};
pub(crate) use store::PrivatePoolMatch;
pub use store::PrivateVerifiedPool;

#[cfg(test)]
mod tests;
