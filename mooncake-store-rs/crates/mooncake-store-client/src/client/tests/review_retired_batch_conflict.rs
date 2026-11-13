use super::*;

struct RetireBatchThenPublishWinner {
    inner: Arc<dyn RouteDirectory>,
    winner: StoreClient,
    raced: AtomicBool,
    winner_route: Mutex<Option<ObjectRoute>>,
}

impl RouteDirectory for RetireBatchThenPublishWinner {
    delegate_publication_setup!();
    delegate_publication_revoke!();

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }

    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.inner.get_object_route(observer, key)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[mooncake_store_core::RouteCasRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<mooncake_store_core::CasResult>>>
    {
        if !self.raced.swap(true, Ordering::SeqCst) {
            assert_eq!(requests.len(), 1);
            let route = requests[0]
                .next
                .as_ref()
                .expect("initial batch publishes a route");
            let replica = &route.replicas[0];
            let replies = self.inner.revoke_publications(
                observer,
                &[mooncake_store_core::PublicationRevokeRequest {
                    attempt: replica.publication.clone().expect("batch has provenance"),
                    allocation: mooncake_store_core::AllocationRouteIdentity {
                        owner: replica.owner.clone(),
                        segment_name: replica.segment_name.clone(),
                        offset_bytes: replica.segment_offset,
                        length_bytes: replica.length,
                        allocation_id: replica.allocation_id.expect("batch has allocation id"),
                    },
                    closed_receivers: Vec::new(),
                    registration_generation: 0,
                    receiver_permits: Vec::new(),
                    superseded_before: None,
                }],
            )?;
            assert_eq!(replies.len(), 1);
            assert!(replies[0].registration_closed);
            assert_eq!(
                replies[0].reference.state,
                mooncake_store_core::AllocationRouteReferenceState::Absent
            );
            // A real writer wins while the first batch is between data transfer
            // and route publication. The batch must keep its conflict policy.
            let winner = self.winner.put("retired-race", b"winner-payload")?;
            assert_eq!(self.winner.get("retired-race")?, b"winner-payload");
            *self.winner_route.lock() = Some(winner);
        }
        self.inner
            .compare_and_swap_object_routes(observer, requests)
    }
}

fn retired_batch_preserves_concurrent_winner(accept_existing: bool) {
    let name = if accept_existing {
        "review-retired-batch-accept"
    } else {
        "review-retired-batch-strict"
    };
    let (winner, mut writer, _transport) = super::batch_put_status_tests::routed_status_writer(
        Arc::new(InMemoryMetadataBackend::new()),
        name,
    );
    let directory = Arc::new(RetireBatchThenPublishWinner {
        inner: writer.route_directory.clone(),
        winner,
        raced: AtomicBool::new(false),
        winner_route: Mutex::new(None),
    });
    writer.route_directory = directory.clone();
    let mut value = b"losing-payload".to_vec();
    let results = if accept_existing {
        writer
            .register_buffer(value.as_mut_ptr().cast(), value.len())
            .expect("registered batch source");
        writer.batch_put_from_statuses(&[PutFromRequest::new(
            "retired-race",
            value.as_ptr().cast(),
            value.len(),
        )])
    } else {
        vec![writer
            .batch_put(&[PutRequest::new("retired-race", b"losing-payload")])
            .map(|mut routes| routes.remove(0))]
    };
    if accept_existing {
        writer
            .unregister_buffer(value.as_mut_ptr().cast(), value.len())
            .expect("unregister batch source");
    }
    assert!(
        directory.raced.load(Ordering::SeqCst),
        "batch reached publication: {results:?}"
    );
    let winner = directory
        .winner_route
        .lock()
        .clone()
        .expect("winner published");
    assert_eq!(
        directory
            .winner
            .get("retired-race")
            .expect("winner remains readable"),
        b"winner-payload",
        "retired publication retry must not overwrite the concurrent winner"
    );
    assert_eq!(results.len(), 1);
    if accept_existing {
        assert_eq!(
            results[0].as_ref().expect("accept existing succeeds"),
            &winner
        );
    } else {
        assert!(
            matches!(&results[0], Err(StoreError::Conflict(_))),
            "strict batch must report its publication conflict: {:?}",
            results[0]
        );
    }
}

#[test]
fn retired_batch_accept_existing_preserves_concurrent_winner() {
    retired_batch_preserves_concurrent_winner(true);
}

#[test]
fn retired_batch_strict_preserves_concurrent_winner() {
    retired_batch_preserves_concurrent_winner(false);
}
