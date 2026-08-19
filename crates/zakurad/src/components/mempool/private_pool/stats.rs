use serde::Serialize;

/// Coarse private-record lifecycle totals.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PrivatePoolStateTotals {
    /// Records retained after verification.
    pub verified: usize,
}

/// Aggregate-only private-pool diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct PrivatePoolStats {
    /// Number of retained transactions.
    pub transaction_count: usize,
    /// Sum of retained transactions' serialized sizes.
    pub serialized_bytes: usize,
    /// Configured transaction-count limit.
    pub max_transactions: usize,
    /// Configured serialized-byte limit.
    pub max_serialized_bytes: usize,
    /// Coarse lifecycle totals.
    pub state_totals: PrivatePoolStateTotals,
}
