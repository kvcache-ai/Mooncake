use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{AllocationRouteIdentity, AllocationRouteReference, ClientRuntimeId, ObjectKey};

/// Immutable authority provenance for one reserve/write/publication attempt.
/// Route-version retries retain this identity; a retired attempt must reserve
/// and write a new allocation before it may publish again.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PublicationAttempt(Arc<PublicationAttemptFields>);

/// Field values retain the original serialized publication identity.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename = "PublicationAttempt")]
pub struct PublicationAttemptFields {
    pub publisher: ClientRuntimeId,
    pub sequence: u64,
    pub key: ObjectKey,
    pub primary: ClientRuntimeId,
    pub receivers: Vec<ClientRuntimeId>,
    pub permits: Vec<PublicationReceiverPermit>,
}

impl Deref for PublicationAttempt {
    type Target = PublicationAttemptFields;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl DerefMut for PublicationAttempt {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

impl Serialize for PublicationAttempt {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PublicationAttempt {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        PublicationAttemptFields::deserialize(deserializer).map(Self::new)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct PublicationReceiverPermit {
    pub receiver: ClientRuntimeId,
    pub enrollment: u64,
}

impl PublicationAttempt {
    #[inline]
    pub fn new(fields: PublicationAttemptFields) -> Self {
        Self(Arc::new(fields))
    }

    pub fn accepts_receiver(&self, receiver: &ClientRuntimeId) -> bool {
        self.sequence != 0
            && self.receivers.contains(&self.primary)
            && self.receivers.contains(receiver)
            && self
                .permits
                .iter()
                .any(|permit| &permit.receiver == receiver && permit.enrollment != 0)
    }
}

/// Failure-path operation, serialized with publication at each receiver.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicationRevokeRequest {
    pub attempt: PublicationAttempt,
    pub allocation: AllocationRouteIdentity,
    #[serde(default)]
    pub closed_receivers: Vec<ClientRuntimeId>,
    #[serde(default)]
    pub registration_generation: u64,
    #[serde(default)]
    pub receiver_permits: Vec<PublicationReceiverPermit>,
    /// Strict bound from a newer route or the pinned primary's deletion proof.
    #[serde(default)]
    pub superseded_before: Option<crate::RouteVersion>,
}

/// The primary closes receiver registration before exposing its final set.
/// An absent reference alone is insufficient permission to release memory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicationRevokeReply {
    pub reference: AllocationRouteReference,
    pub registered_receivers: Vec<ClientRuntimeId>,
    pub registration_closed: bool,
    pub registration_generation: u64,
    pub receiver_permits: Vec<PublicationReceiverPermit>,
}

/// Non-hot-path permission to import an allocation at another incarnation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicationReceiverRegistration {
    pub attempt: PublicationAttempt,
    pub receiver: ClientRuntimeId,
    pub permit: PublicationReceiverPermit,
    pub allocations: Vec<AllocationRouteIdentity>,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublicationControl {
    PruneExisting {
        expected: Box<crate::ObjectRoute>,
        next: Option<Box<crate::ObjectRoute>>,
    },
    Enroll {
        publisher: ClientRuntimeId,
    },
    Revoke(PublicationRevokeRequest),
    MarkerProof {
        request: PublicationRevokeRequest,
        expected: crate::RouteVersion,
    },
    RegisterReceiver(PublicationReceiverRegistration),
    AdmitReceiver(PublicationReceiverRegistration),
    Finalize {
        request: PublicationRevokeRequest,
        released: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublicationControlReply {
    Pruned(Box<crate::CasResult>),
    Enrolled(PublicationReceiverPermit),
    Revoked(PublicationRevokeReply),
    MarkerProof(Option<crate::RouteVersion>),
    Registered(PublicationReceiverRegistration),
    Admitted,
    Finalized,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientEpoch;

    fn identity_fields() -> PublicationAttemptFields {
        let primary = ClientRuntimeId::new("primary", ClientEpoch(7));
        PublicationAttemptFields {
            publisher: ClientRuntimeId::new("writer", ClientEpoch(3)),
            sequence: 1,
            key: ObjectKey::new("default::key"),
            primary: primary.clone(),
            receivers: vec![primary.clone()],
            permits: vec![PublicationReceiverPermit {
                receiver: primary,
                enrollment: 1,
            }],
        }
    }

    #[test]
    fn publication_clone_mutation_preserves_captured_identity_and_map_keys() {
        use std::collections::{BTreeMap, HashMap};

        let original = PublicationAttempt::new(identity_fields());
        let captured = original.clone();
        assert!(Arc::ptr_eq(&original.0, &captured.0));
        let ordered = BTreeMap::from([(original.clone(), "captured")]);
        let hashed = HashMap::from([(original.clone(), "captured")]);
        let mut changed = original.clone();
        changed.sequence += 1;
        changed.publisher.epoch = crate::ClientEpoch(9);
        changed.receivers.clear();
        changed.permits[0].enrollment += 1;
        assert!(!Arc::ptr_eq(&original.0, &changed.0));
        assert_eq!(original, captured);
        assert_eq!(original.sequence, 1);
        assert_eq!(original.publisher.epoch, crate::ClientEpoch(3));
        assert_eq!(original.receivers.len(), 1);
        assert_eq!(original.permits[0].enrollment, 1);
        assert_eq!(ordered.get(&captured), Some(&"captured"));
        assert_eq!(hashed.get(&captured), Some(&"captured"));
        assert!(!ordered.contains_key(&changed));
        assert!(!hashed.contains_key(&changed));
        let independently_allocated = PublicationAttempt::new(identity_fields());
        assert!(!Arc::ptr_eq(&original.0, &independently_allocated.0));
        assert_eq!(ordered.get(&independently_allocated), Some(&"captured"));
        assert_eq!(hashed.get(&independently_allocated), Some(&"captured"));
    }

    #[test]
    fn publication_shared_value_preserves_original_json_bytes() {
        #[derive(Serialize)]
        #[serde(rename = "PublicationAttempt")]
        struct LegacyAttempt<'a> {
            publisher: &'a ClientRuntimeId,
            sequence: u64,
            key: &'a ObjectKey,
            primary: &'a ClientRuntimeId,
            receivers: &'a [ClientRuntimeId],
            permits: &'a [PublicationReceiverPermit],
        }

        for sequence in [0, 1, 127, 128, u64::MAX] {
            for empty in [false, true] {
                let mut fields = identity_fields();
                fields.sequence = sequence;
                fields.key = ObjectKey::new("tenant::δ🚀\0");
                if empty {
                    fields.receivers.clear();
                    fields.permits.clear();
                }
                let original_bytes = serde_json::to_vec(&LegacyAttempt {
                    publisher: &fields.publisher,
                    sequence: fields.sequence,
                    key: &fields.key,
                    primary: &fields.primary,
                    receivers: &fields.receivers,
                    permits: &fields.permits,
                })
                .unwrap();
                let shared = PublicationAttempt::new(fields);
                assert_eq!(serde_json::to_vec(&shared).unwrap(), original_bytes);
                let restored: PublicationAttempt = serde_json::from_slice(&original_bytes).unwrap();
                assert_eq!(restored, shared);
                assert_eq!(serde_json::to_vec(&restored).unwrap(), original_bytes);
            }
        }
    }

    #[test]
    fn publication_receivers_bind_exact_incarnations() {
        let primary = ClientRuntimeId::new("primary", ClientEpoch(7));
        let attempt = PublicationAttempt::new(crate::publication::PublicationAttemptFields {
            publisher: ClientRuntimeId::new("writer", ClientEpoch(3)),
            sequence: 1,
            key: ObjectKey::new("default::key"),
            primary: primary.clone(),
            receivers: vec![primary.clone()],
            permits: vec![PublicationReceiverPermit {
                receiver: primary.clone(),
                enrollment: 1,
            }],
        });
        assert!(attempt.accepts_receiver(&primary));
        assert!(!attempt.accepts_receiver(&ClientRuntimeId::new("primary", ClientEpoch(8))));
        let mut invalid = attempt.clone();
        invalid.sequence = 0;
        assert!(!invalid.accepts_receiver(&primary));
        invalid = attempt;
        invalid.receivers.clear();
        assert!(!invalid.accepts_receiver(&primary));
    }
}
