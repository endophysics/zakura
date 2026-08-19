use tokio::sync::oneshot;

use zakura_node_services::mempool::{AdmissionOrigin, Gossip, QueueSource};

/// A marker struct for the oneshot channels which cancel a pending download and verify.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct CancelDownloadAndVerify;

#[derive(Debug)]
pub(in crate::components::mempool) struct PendingTransaction {
    pub(super) cancel_sender: Option<oneshot::Sender<CancelDownloadAndVerify>>,
    pub(super) gossip: Gossip,
    pub(super) origin: AdmissionOrigin,
}

impl PendingTransaction {
    pub(in crate::components::mempool) fn new(gossip: Gossip, origin: AdmissionOrigin) -> Self {
        Self {
            cancel_sender: None,
            gossip,
            origin,
        }
    }

    pub(in crate::components::mempool) fn retry(&self) -> Self {
        Self::for_retry(self.gossip.clone(), &self.origin)
    }

    pub(in crate::components::mempool) fn for_retry(
        gossip: impl Into<Gossip>,
        origin: &AdmissionOrigin,
    ) -> Self {
        let origin = match origin {
            #[cfg(feature = "privacy-admission")]
            AdmissionOrigin::PrivateLocal(context) => AdmissionOrigin::PrivateLocal(*context),
            AdmissionOrigin::Peer(_) | AdmissionOrigin::Crawler | AdmissionOrigin::LegacyLocal => {
                AdmissionOrigin::LegacyLocal
            }
        };

        Self::new(gossip.into(), origin)
    }

    #[cfg(all(test, feature = "privacy-admission"))]
    pub(in crate::components::mempool) fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    #[cfg(all(test, feature = "privacy-admission"))]
    pub(in crate::components::mempool) fn origin(&self) -> &AdmissionOrigin {
        &self.origin
    }

    #[cfg(feature = "privacy-admission")]
    pub(in crate::components::mempool) fn conflicts_with(&self, incoming: &Self) -> bool {
        match &self.origin {
            #[cfg(feature = "privacy-admission")]
            AdmissionOrigin::PrivateLocal(existing) => match &incoming.origin {
                AdmissionOrigin::PrivateLocal(incoming) => existing != incoming,
                AdmissionOrigin::Peer(_)
                | AdmissionOrigin::Crawler
                | AdmissionOrigin::LegacyLocal => false,
            },
            AdmissionOrigin::Peer(_) | AdmissionOrigin::Crawler | AdmissionOrigin::LegacyLocal => {
                false
            }
        }
    }

    #[cfg(not(feature = "privacy-admission"))]
    pub(in crate::components::mempool) fn conflicts_with(&self, _incoming: &Self) -> bool {
        false
    }

    pub(in crate::components::mempool) fn peer_source(&self) -> Option<&QueueSource> {
        match &self.origin {
            AdmissionOrigin::Peer(source) => Some(source),
            AdmissionOrigin::Crawler | AdmissionOrigin::LegacyLocal => None,
            #[cfg(feature = "privacy-admission")]
            AdmissionOrigin::PrivateLocal(_) => None,
        }
    }
}
