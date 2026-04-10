struct LocalAuthorityAdapter;

impl AuthorityService for LocalAuthorityAdapter {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        authority_get(namespace, authority, key)
    }

    fn list_routes_by_replica_owner(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        authority_list_routes_by_replica_owner(namespace, authority, owner)
    }

    fn compare_and_swap_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        authority_compare_and_swap(namespace, authority, key, expected, next)
    }

    fn replace_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()> {
        authority_replace(namespace, authority, key, next)
    }

    fn batch_get_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<Option<ObjectRoute>>> {
        match authority_get_many(namespace, authority, keys) {
            Ok(routes) => routes.into_iter().map(Ok).collect(),
            Err(error) => keys
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }

    fn batch_compare_and_swap_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<CasResult>> {
        match authority_compare_and_swap_many(namespace, authority, requests) {
            Ok(results) => results.into_iter().map(Ok).collect(),
            Err(error) => requests
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }

    fn batch_replace_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<()>> {
        match authority_replace_many(namespace, authority, requests) {
            Ok(()) => requests.iter().map(|_| Ok(())).collect(),
            Err(error) => requests
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }
}

struct LocalAllocatorAdapter {
    runtime: ClientRuntimeId,
    allocator: Arc<Mutex<LocalAllocatorState>>,
    storage_owner: Arc<StorageOwnerState>,
}

impl LocalAllocatorAdapter {
    fn reserve_any_with_eviction(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let mut last_error = None;
        for _ in 0..=32usize {
            match self.allocator.lock().reserve_any(owner, length_bytes) {
                Ok(reservation) => return Ok(reservation),
                Err(StoreError::Allocator(message)) => {
                    last_error = Some(message);
                }
                Err(error) => return Err(error),
            }
            if !self.storage_owner.evict_one(None)? {
                break;
            }
        }
        Err(StoreError::Allocator(last_error.unwrap_or_else(|| {
            format!("no writable active segment available for {}", owner)
        })))
    }

    fn reserve_specific_with_eviction(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let mut last_error = None;
        for _ in 0..=32usize {
            match self
                .allocator
                .lock()
                .reserve_specific(owner, segment_name, length_bytes)
            {
                Ok(reservation) => return Ok(reservation),
                Err(StoreError::Allocator(message)) => {
                    last_error = Some(message);
                }
                Err(error) => return Err(error),
            }
            if !self.storage_owner.evict_one(Some(segment_name))? {
                break;
            }
        }
        Err(StoreError::Allocator(last_error.unwrap_or_else(|| {
            format!(
                "segment capacity exhausted for {}:{} requested={}",
                owner, segment_name.0, length_bytes
            )
        })))
    }
}

impl AllocatorService for LocalAllocatorAdapter {
    fn reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.reserve_any_with_eviction(owner, length_bytes)
    }

    fn reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.reserve_specific_with_eviction(owner, segment_name, length_bytes)
    }

    fn release(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.allocator
            .lock()
            .release(owner, segment_name, offset_bytes, length_bytes)
    }

    fn batch_reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: &[u64],
    ) -> Vec<Result<mooncake_store_core::SegmentReservation>> {
        if *owner != self.runtime {
            return length_bytes
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        length_bytes
            .iter()
            .map(|length_bytes| self.reserve_any_with_eviction(owner, *length_bytes))
            .collect()
    }

    fn batch_reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        requests: &[ReserveSpecificOp],
    ) -> Vec<Result<mooncake_store_core::SegmentReservation>> {
        if *owner != self.runtime {
            return requests
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        requests
            .iter()
            .map(|request| {
                self.reserve_specific_with_eviction(owner, &request.segment_name, request.length_bytes)
            })
            .collect()
    }

    fn batch_release(&self, owner: &ClientRuntimeId, requests: &[ReleaseOp]) -> Vec<Result<()>> {
        if *owner != self.runtime {
            return requests
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        let mut allocator = self.allocator.lock();
        requests
            .iter()
            .map(|request| {
                allocator.release(
                    owner,
                    &request.segment_name,
                    request.offset_bytes,
                    request.length_bytes,
                )
            })
            .collect()
    }
}

impl EvictionService for LocalAllocatorAdapter {
    fn batch_report_route_hits(&self, keys: &[ObjectKey]) -> Result<usize> {
        Ok(self.storage_owner.report_route_hits(keys))
    }

    fn batch_track_routes(&self, routes: &[ObjectRoute]) -> Result<usize> {
        Ok(self.storage_owner.track_routes(routes))
    }
}
