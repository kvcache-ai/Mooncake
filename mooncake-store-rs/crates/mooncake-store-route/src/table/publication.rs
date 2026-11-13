use std::collections::{BTreeMap, BTreeSet, HashMap};

use mooncake_store_core::{
    AllocationRouteIdentity, ClientRuntimeId, ObjectRoute, PublicationAttempt,
    PublicationReceiverPermit, PublicationReceiverRegistration, Result, StoreError,
};

const MAX_PENDING_ENROLLMENTS: usize = 4096;
const MAX_PENDING_ATTEMPTS_PER_ENROLLMENT: usize = 1024;

#[derive(Default)]
pub(super) struct PublicationLedger {
    issued_enrollments: u64,
    publishers: HashMap<ClientRuntimeId, u64>,
    enrollments: BTreeMap<u64, PublisherEnrollment>,
}

struct PublisherEnrollment {
    publisher: ClientRuntimeId,
    retired_sequence: u64,
    attempts: BTreeMap<u64, PublicationRecord>,
    pending_attempts: usize,
}

pub(super) struct PublicationRecord {
    pub attempt: PublicationAttempt,
    pub receivers: BTreeSet<ClientRuntimeId>,
    pub allocations: BTreeSet<AllocationRouteIdentity>,
    pub revoked: bool,
    finalized: BTreeSet<AllocationRouteIdentity>,
    released: BTreeSet<AllocationRouteIdentity>,
    registration_generation: u64,
    closed_registration_generation: u64,
    import_generation: u64,
    permits: BTreeMap<ClientRuntimeId, PublicationReceiverPermit>,
}

impl PublicationLedger {
    pub fn validate_marker_proof(
        &mut self,
        receiver: &ClientRuntimeId,
        attempt: &PublicationAttempt,
        allocation: &AllocationRouteIdentity,
    ) -> Result<()> {
        if receiver != &attempt.primary || !attempt.accepts_receiver(receiver) {
            return Err(StoreError::PublicationRetired(
                "marker proof primary incarnation changed".to_string(),
            ));
        }
        let state = self.enrollment_mut(receiver, attempt)?;
        let record = state.attempts.get(&attempt.sequence).ok_or_else(|| {
            StoreError::PublicationRetired("marker proof attempt has retired".to_string())
        })?;
        if record.attempt != *attempt || !record.allocations.contains(allocation) {
            return Err(StoreError::PublicationRetired(
                "marker proof allocation changed".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn retained_attempts(&self) -> usize {
        self.enrollments
            .values()
            .map(|state| state.attempts.len())
            .sum()
    }

    fn retired_enrollment(
        &self,
        receiver: &ClientRuntimeId,
        attempt: &PublicationAttempt,
        supplied: Option<&PublicationReceiverPermit>,
    ) -> bool {
        supplied
            .filter(|permit| &permit.receiver == receiver)
            .or_else(|| {
                attempt
                    .permits
                    .iter()
                    .find(|permit| &permit.receiver == receiver)
            })
            .is_some_and(|permit| {
                permit.enrollment != 0
                    && permit.enrollment <= self.issued_enrollments
                    && !self.enrollments.contains_key(&permit.enrollment)
            })
    }

    pub fn enroll(
        &mut self,
        receiver: &ClientRuntimeId,
        publisher: &ClientRuntimeId,
    ) -> Result<PublicationReceiverPermit> {
        if let Some(enrollment) = self.publishers.get(publisher) {
            return Ok(PublicationReceiverPermit {
                receiver: receiver.clone(),
                enrollment: *enrollment,
            });
        }
        if self.enrollments.len() >= MAX_PENDING_ENROLLMENTS {
            let disposable = self
                .enrollments
                .iter()
                .find_map(|(ordinal, state)| state.attempts.is_empty().then_some(*ordinal));
            if let Some(ordinal) = disposable {
                let previous = self
                    .enrollments
                    .remove(&ordinal)
                    .expect("enrollment exists");
                self.publishers.remove(&previous.publisher);
            } else {
                return Err(StoreError::Backpressure(
                    "publication enrollment capacity exhausted".to_string(),
                ));
            }
        }
        let ordinal = self.issued_enrollments.checked_add(1).ok_or_else(|| {
            StoreError::InvalidState("publication enrollment generation exhausted".to_string())
        })?;
        self.issued_enrollments = ordinal;
        self.publishers.insert(publisher.clone(), ordinal);
        self.enrollments.insert(
            ordinal,
            PublisherEnrollment {
                publisher: publisher.clone(),
                retired_sequence: 0,
                attempts: BTreeMap::new(),
                pending_attempts: 0,
            },
        );
        Ok(PublicationReceiverPermit {
            receiver: receiver.clone(),
            enrollment: ordinal,
        })
    }

    fn enrollment_mut(
        &mut self,
        receiver: &ClientRuntimeId,
        attempt: &PublicationAttempt,
    ) -> Result<&mut PublisherEnrollment> {
        self.enrollment_mut_with_permit(receiver, attempt, None)
    }

    fn enrollment_mut_with_permit(
        &mut self,
        receiver: &ClientRuntimeId,
        attempt: &PublicationAttempt,
        supplied: Option<&PublicationReceiverPermit>,
    ) -> Result<&mut PublisherEnrollment> {
        let ordinal = supplied
            .filter(|permit| &permit.receiver == receiver)
            .map(|permit| permit.enrollment)
            .or_else(|| {
                attempt
                    .permits
                    .iter()
                    .find(|permit| &permit.receiver == receiver)
                    .map(|permit| permit.enrollment)
            })
            .or_else(|| self.publishers.get(&attempt.publisher).copied())
            .ok_or_else(|| {
                StoreError::PublicationRetired("receiver incarnation is not enrolled".to_string())
            })?;
        let state = self.enrollments.get_mut(&ordinal).ok_or_else(|| {
            StoreError::PublicationRetired("publisher enrollment has retired".to_string())
        })?;
        if state.publisher != attempt.publisher || attempt.sequence == 0 {
            return Err(StoreError::PublicationRetired(
                "publisher enrollment mismatch".to_string(),
            ));
        }
        Ok(state)
    }

    pub fn admit(
        &mut self,
        receiver: &ClientRuntimeId,
        attempt: &PublicationAttempt,
        route: &ObjectRoute,
        imported: bool,
    ) -> Result<()> {
        if route.key != attempt.key || (!imported && !attempt.accepts_receiver(receiver)) {
            return Err(StoreError::PublicationRetired(
                "publication receiver or key changed".to_string(),
            ));
        }
        let state = self.enrollment_mut(receiver, attempt)?;
        let allocations: BTreeSet<_> = route
            .replicas
            .iter()
            .filter(|replica| replica.publication.as_ref() == Some(attempt))
            .filter_map(|replica| {
                replica
                    .allocation_id
                    .map(|allocation_id| AllocationRouteIdentity {
                        owner: replica.owner.clone(),
                        segment_name: replica.segment_name.clone(),
                        offset_bytes: replica.segment_offset,
                        length_bytes: replica.length,
                        allocation_id,
                    })
            })
            .collect();
        if allocations.len() != 1 {
            return Err(StoreError::PublicationRetired(
                "publication attempt must identify exactly one allocation".to_string(),
            ));
        }
        if let Some(record) = state.attempts.get(&attempt.sequence) {
            if (record.revoked
                && !(imported && record.import_generation > record.closed_registration_generation))
                || record.attempt != *attempt
                || !record.receivers.contains(receiver)
                || allocations != record.allocations
            {
                return Err(StoreError::PublicationRetired(
                    "publication attempt is closed".to_string(),
                ));
            }
            return Ok(());
        }
        if imported && !attempt.accepts_receiver(receiver) {
            return Err(StoreError::PublicationRetired(
                "import receiver has no registered permit".to_string(),
            ));
        }
        if !imported && receiver != &attempt.primary {
            return Err(StoreError::PublicationRetired(
                "initial publication requires its pinned primary".to_string(),
            ));
        }
        if attempt.sequence <= state.retired_sequence {
            return Err(StoreError::PublicationRetired(
                "publication sequence has retired".to_string(),
            ));
        }
        if state.pending_attempts >= MAX_PENDING_ATTEMPTS_PER_ENROLLMENT {
            return Err(StoreError::Backpressure(
                "publication replay window exhausted".to_string(),
            ));
        }
        state.attempts.insert(
            attempt.sequence,
            PublicationRecord {
                attempt: attempt.clone(),
                receivers: attempt.receivers.iter().cloned().collect(),
                allocations,
                revoked: false,
                finalized: BTreeSet::new(),
                released: BTreeSet::new(),
                registration_generation: 0,
                closed_registration_generation: 0,
                import_generation: 0,
                permits: attempt
                    .permits
                    .iter()
                    .cloned()
                    .map(|permit| (permit.receiver.clone(), permit))
                    .collect(),
            },
        );
        state.pending_attempts += 1;
        Ok(())
    }

    pub fn revoke(
        &mut self,
        receiver: &ClientRuntimeId,
        attempt: &PublicationAttempt,
        allocation: &AllocationRouteIdentity,
        generation: u64,
        permits: &[PublicationReceiverPermit],
    ) -> Result<(Vec<ClientRuntimeId>, u64, Vec<PublicationReceiverPermit>)> {
        let supplied = permits.iter().find(|permit| &permit.receiver == receiver);
        if self.retired_enrollment(receiver, attempt, supplied) {
            return Ok((attempt.receivers.clone(), generation, permits.to_vec()));
        }
        let state = self.enrollment_mut_with_permit(receiver, attempt, supplied)?;
        if let Some(record) = state.attempts.get_mut(&attempt.sequence) {
            if record.attempt != *attempt {
                return Err(StoreError::PublicationRetired(
                    "publication identity mismatch".to_string(),
                ));
            }
            record.revoked = true;
            record.closed_registration_generation = record
                .closed_registration_generation
                .max(generation)
                .max(record.registration_generation);
            return Ok((
                record.receivers.iter().cloned().collect(),
                record.closed_registration_generation,
                record.permits.values().cloned().collect(),
            ));
        }
        if attempt.sequence <= state.retired_sequence {
            return Ok((
                attempt.receivers.clone(),
                generation,
                attempt.permits.clone(),
            ));
        }
        if state.pending_attempts >= MAX_PENDING_ATTEMPTS_PER_ENROLLMENT {
            return Err(StoreError::Backpressure(
                "publication replay window exhausted".to_string(),
            ));
        }
        state.attempts.insert(
            attempt.sequence,
            PublicationRecord {
                attempt: attempt.clone(),
                receivers: attempt.receivers.iter().cloned().collect(),
                allocations: BTreeSet::from([allocation.clone()]),
                revoked: true,
                finalized: BTreeSet::new(),
                released: BTreeSet::new(),
                registration_generation: generation,
                closed_registration_generation: generation,
                import_generation: 0,
                permits: attempt
                    .permits
                    .iter()
                    .cloned()
                    .chain(supplied.cloned())
                    .map(|permit| (permit.receiver.clone(), permit))
                    .collect(),
            },
        );
        state.pending_attempts += 1;
        Ok((
            attempt.receivers.clone(),
            generation,
            attempt.permits.clone(),
        ))
    }

    pub fn finalize(
        &mut self,
        receiver: &ClientRuntimeId,
        attempt: &PublicationAttempt,
        allocation: &AllocationRouteIdentity,
        released: bool,
        generation: u64,
        permits: &[PublicationReceiverPermit],
    ) -> Result<()> {
        let supplied = permits.iter().find(|permit| &permit.receiver == receiver);
        if released && self.retired_enrollment(receiver, attempt, supplied) {
            return Ok(());
        }
        let state = self.enrollment_mut_with_permit(receiver, attempt, supplied)?;
        if let Some(record) = state.attempts.get_mut(&attempt.sequence) {
            if record.attempt != *attempt {
                return Err(StoreError::PublicationRetired(
                    "publication identity mismatch".to_string(),
                ));
            }
            if !record.allocations.contains(allocation) {
                return Err(StoreError::InvalidState(
                    "finalization allocation is outside the publication".to_string(),
                ));
            }
            if released
                && (!record.revoked
                    || generation < record.registration_generation
                    || generation < record.import_generation
                    || generation < record.closed_registration_generation)
            {
                return Err(StoreError::PublicationRetired(
                    "release proof predates receiver registration".to_string(),
                ));
            }
            let was_pending = record.finalized.len() < record.allocations.len();
            record.finalized.insert(allocation.clone());
            if was_pending && record.finalized.len() >= record.allocations.len() {
                state.pending_attempts = state.pending_attempts.saturating_sub(1);
            }
            if released {
                record.released.insert(allocation.clone());
            }
            if record.revoked && record.released == record.allocations {
                state.attempts.remove(&attempt.sequence);
                state.retired_sequence = state.retired_sequence.max(attempt.sequence);
            }
        } else if !released && attempt.sequence > state.retired_sequence {
            let allocations = BTreeSet::from([allocation.clone()]);
            state.attempts.insert(
                attempt.sequence,
                PublicationRecord {
                    attempt: attempt.clone(),
                    receivers: attempt.receivers.iter().cloned().collect(),
                    finalized: allocations.clone(),
                    allocations,
                    revoked: false,
                    released: BTreeSet::new(),
                    registration_generation: 0,
                    closed_registration_generation: 0,
                    import_generation: 0,
                    permits: attempt
                        .permits
                        .iter()
                        .cloned()
                        .map(|permit| (permit.receiver.clone(), permit))
                        .collect(),
                },
            );
        }
        Ok(())
    }

    pub fn register_receiver(
        &mut self,
        receiver: &ClientRuntimeId,
        registration: &PublicationReceiverRegistration,
    ) -> Result<PublicationReceiverRegistration> {
        let attempt = &registration.attempt;
        if receiver != &attempt.primary || registration.receiver != registration.permit.receiver {
            return Err(StoreError::PublicationRetired(
                "receiver registration has wrong primary or permit".to_string(),
            ));
        }
        let state = self.enrollment_mut(receiver, attempt)?;
        let record = state.attempts.get_mut(&attempt.sequence).ok_or_else(|| {
            StoreError::PublicationRetired("publication has no registered provenance".to_string())
        })?;
        let allocations: BTreeSet<_> = registration.allocations.iter().cloned().collect();
        if record.attempt != *attempt || record.allocations != allocations {
            return Err(StoreError::PublicationRetired(
                "publication registration is closed".to_string(),
            ));
        }
        if record.receivers.len() >= 64 && !record.receivers.contains(&registration.receiver) {
            return Err(StoreError::Backpressure(
                "publication receiver capacity exhausted".to_string(),
            ));
        }
        record.receivers.insert(registration.receiver.clone());
        record
            .permits
            .insert(registration.receiver.clone(), registration.permit.clone());
        record.registration_generation =
            record
                .registration_generation
                .checked_add(1)
                .ok_or_else(|| {
                    StoreError::InvalidState("publication import generation exhausted".to_string())
                })?;
        let mut issued = registration.clone();
        issued.generation = record.registration_generation;
        Ok(issued)
    }

    pub fn admit_receiver(
        &mut self,
        receiver: &ClientRuntimeId,
        registration: &PublicationReceiverRegistration,
    ) -> Result<()> {
        if receiver != &registration.receiver || receiver != &registration.permit.receiver {
            return Err(StoreError::PublicationRetired(
                "import receiver incarnation changed".to_string(),
            ));
        }
        let state = self
            .enrollments
            .get_mut(&registration.permit.enrollment)
            .ok_or_else(|| {
                StoreError::PublicationRetired("import enrollment has retired".to_string())
            })?;
        let attempt = &registration.attempt;
        if state.publisher != attempt.publisher {
            return Err(StoreError::PublicationRetired(
                "import publisher changed".to_string(),
            ));
        }
        if let Some(record) = state.attempts.get_mut(&attempt.sequence) {
            if record.attempt != *attempt
                || registration.generation <= record.closed_registration_generation
            {
                return Err(StoreError::PublicationRetired(
                    "import attempt is closed".to_string(),
                ));
            }
            record.import_generation = record.import_generation.max(registration.generation);
            record.receivers.insert(receiver.clone());
            record
                .permits
                .insert(receiver.clone(), registration.permit.clone());
            return Ok(());
        }
        if attempt.sequence <= state.retired_sequence {
            return Err(StoreError::PublicationRetired(
                "import sequence has retired".to_string(),
            ));
        }
        if state.pending_attempts >= MAX_PENDING_ATTEMPTS_PER_ENROLLMENT {
            return Err(StoreError::Backpressure(
                "publication replay window exhausted".to_string(),
            ));
        }
        let mut receivers: BTreeSet<_> = attempt.receivers.iter().cloned().collect();
        receivers.insert(receiver.clone());
        let allocations: BTreeSet<_> = registration.allocations.iter().cloned().collect();
        state.attempts.insert(
            attempt.sequence,
            PublicationRecord {
                attempt: attempt.clone(),
                receivers,
                finalized: allocations.clone(),
                allocations,
                revoked: false,
                released: BTreeSet::new(),
                registration_generation: registration.generation,
                closed_registration_generation: 0,
                import_generation: registration.generation,
                permits: attempt
                    .permits
                    .iter()
                    .cloned()
                    .chain(std::iter::once(registration.permit.clone()))
                    .map(|permit| (permit.receiver.clone(), permit))
                    .collect(),
            },
        );
        Ok(())
    }
}
