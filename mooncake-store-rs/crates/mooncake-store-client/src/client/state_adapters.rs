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
        self.allocator.lock().reserve_any(owner, length_bytes)
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
        self.allocator
            .lock()
            .reserve_specific(owner, segment_name, length_bytes)
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
        let mut allocator = self.allocator.lock();
        length_bytes
            .iter()
            .map(|length_bytes| allocator.reserve_any(owner, *length_bytes))
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
        let mut allocator = self.allocator.lock();
        requests
            .iter()
            .map(|request| {
                allocator.reserve_specific(owner, &request.segment_name, request.length_bytes)
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

