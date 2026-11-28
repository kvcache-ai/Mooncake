use std::sync::Arc;

use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientLease, ClientRuntimeId, ClientStableId, ObjectKey, Result, RouteCasRequest, StoreError,
};

use crate::shim::{
    RouteAuthorityClient, RouteAuthorityPresence, RouteAuthorityRecord, RouteAuthorityService,
    RouteOwnerPage, RouteOwnerPageQuery,
};

#[derive(Clone, Debug)]
pub enum RouteControlRequest {
    BatchGet {
        namespace: String,
        authority: ClientStableId,
        keys: Vec<ObjectKey>,
    },
    BatchContains {
        namespace: String,
        authority: ClientStableId,
        keys: Vec<ObjectKey>,
        include_presence: bool,
    },
    BatchCompareAndSwap {
        namespace: String,
        authority: ClientStableId,
        requests: Vec<RouteCasRequest>,
    },
    BatchReplace {
        namespace: String,
        authority: ClientStableId,
        requests: Vec<RouteCasRequest>,
    },
    ListByReplicaOwner {
        namespace: String,
        authority: ClientStableId,
        owner: ClientRuntimeId,
        cursor: Option<String>,
        snapshot_epoch: Option<u64>,
        limit: usize,
    },
    GetAllocationReferences {
        namespace: String,
        authority: ClientStableId,
        identities: Vec<AllocationRouteIdentity>,
        include_route_key: bool,
    },
}

impl RouteControlRequest {
    pub fn operation(&self) -> &'static str {
        match self {
            Self::BatchGet { .. } => "control_route_batch_get",
            Self::BatchContains { .. } => "control_route_batch_contains",
            Self::BatchCompareAndSwap { .. } => "control_route_batch_cas",
            Self::BatchReplace { .. } => "control_route_batch_replace",
            Self::ListByReplicaOwner { .. } => "control_route_list_by_replica_owner",
            Self::GetAllocationReferences {
                include_route_key: true,
                ..
            } => "control_route_get_allocation_reference_keys",
            Self::GetAllocationReferences { .. } => "control_route_get_allocation_references",
        }
    }

    pub fn item_count(&self) -> usize {
        match self {
            Self::BatchGet { keys, .. } | Self::BatchContains { keys, .. } => keys.len(),
            Self::BatchCompareAndSwap { requests, .. } | Self::BatchReplace { requests, .. } => {
                requests.len()
            }
            Self::ListByReplicaOwner { .. } => 1,
            Self::GetAllocationReferences { identities, .. } => identities.len(),
        }
    }

    pub fn empty_response(&self) -> Option<RouteControlResponse> {
        match self {
            Self::BatchGet { keys, .. } if keys.is_empty() => {
                Some(RouteControlResponse::BatchGet(Vec::new()))
            }
            Self::BatchContains {
                keys,
                include_presence,
                ..
            } if keys.is_empty() => Some(if *include_presence {
                RouteControlResponse::BatchGetPresence(Vec::new())
            } else {
                RouteControlResponse::BatchContains(Vec::new())
            }),
            Self::BatchCompareAndSwap { requests, .. } if requests.is_empty() => {
                Some(RouteControlResponse::BatchCompareAndSwap(Vec::new()))
            }
            Self::BatchReplace { requests, .. } if requests.is_empty() => {
                Some(RouteControlResponse::BatchReplace(Vec::new()))
            }
            Self::GetAllocationReferences {
                identities,
                include_route_key,
                ..
            } if identities.is_empty() => Some(if *include_route_key {
                RouteControlResponse::AllocationReferenceKeys(Vec::new())
            } else {
                RouteControlResponse::AllocationReferences(Vec::new())
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum RouteControlResponse {
    BatchGet(Vec<Result<RouteAuthorityRecord>>),
    BatchContains(Vec<Result<bool>>),
    BatchGetPresence(Vec<Result<RouteAuthorityPresence>>),
    BatchCompareAndSwap(Vec<Result<CasResult>>),
    BatchReplace(Vec<Result<()>>),
    ListByReplicaOwner(RouteOwnerPage),
    AllocationReferences(Vec<AllocationRouteReferenceState>),
    AllocationReferenceKeys(Vec<AllocationRouteReference>),
}

pub trait RouteControlTransport: Send + Sync {
    fn send_route_control_batch(
        &self,
        requests: Vec<(ClientLease, RouteControlRequest)>,
    ) -> Vec<Result<RouteControlResponse>> {
        requests
            .into_iter()
            .map(|(lease, request)| self.send_route_control(&lease, request))
            .collect()
    }

    fn send_route_control(
        &self,
        lease: &ClientLease,
        request: RouteControlRequest,
    ) -> Result<RouteControlResponse>;
}

pub fn serve_route_control_request(
    service: &dyn RouteAuthorityService,
    request: RouteControlRequest,
) -> Result<RouteControlResponse> {
    match request {
        RouteControlRequest::BatchGet {
            namespace,
            authority,
            keys,
        } => Ok(RouteControlResponse::BatchGet(
            service.batch_get_route_records(&namespace, &authority, &keys),
        )),
        RouteControlRequest::BatchContains {
            namespace,
            authority,
            keys,
            include_presence,
        } => {
            if include_presence {
                Ok(RouteControlResponse::BatchGetPresence(
                    service.batch_get_route_presence(&namespace, &authority, &keys),
                ))
            } else {
                Ok(RouteControlResponse::BatchContains(
                    service.batch_contains_routes(&namespace, &authority, &keys),
                ))
            }
        }
        RouteControlRequest::BatchCompareAndSwap {
            namespace,
            authority,
            requests,
        } => Ok(RouteControlResponse::BatchCompareAndSwap(
            service.batch_compare_and_swap_routes(&namespace, &authority, &requests),
        )),
        RouteControlRequest::BatchReplace {
            namespace,
            authority,
            requests,
        } => Ok(RouteControlResponse::BatchReplace(
            service.batch_replace_routes(&namespace, &authority, &requests),
        )),
        RouteControlRequest::ListByReplicaOwner {
            namespace,
            authority,
            owner,
            cursor,
            snapshot_epoch,
            limit,
        } => service
            .list_routes_by_replica_owner_page(
                &namespace,
                &authority,
                &owner,
                cursor.as_deref(),
                snapshot_epoch,
                limit,
            )
            .map(RouteControlResponse::ListByReplicaOwner),
        RouteControlRequest::GetAllocationReferences {
            namespace,
            authority,
            identities,
            include_route_key,
        } => {
            if include_route_key {
                service
                    .get_allocation_route_reference_keys(&namespace, &authority, &identities)
                    .map(RouteControlResponse::AllocationReferenceKeys)
            } else {
                service
                    .get_allocation_route_references(&namespace, &authority, &identities)
                    .map(RouteControlResponse::AllocationReferences)
            }
        }
    }
}

pub(crate) struct RouteControlAuthorityClient {
    transport: Arc<dyn RouteControlTransport>,
}

impl RouteControlAuthorityClient {
    pub(crate) fn new(transport: Arc<dyn RouteControlTransport>) -> Self {
        Self { transport }
    }

    fn send_batch(
        &self,
        requests: Vec<(ClientLease, RouteControlRequest)>,
    ) -> Vec<Result<RouteControlResponse>> {
        if requests.is_empty() {
            return Vec::new();
        }
        let expected = requests.len();
        let replies = self.transport.send_route_control_batch(requests);
        if replies.len() != expected {
            return (0..expected)
                .map(|_| {
                    Err(StoreError::Transport(format!(
                        "route control batch reply length mismatch: expected={expected} actual={}",
                        replies.len()
                    )))
                })
                .collect();
        }
        replies
    }
}

impl RouteAuthorityClient for RouteControlAuthorityClient {
    fn batch_get_routes_many(
        &self,
        namespace: &str,
        batches: &[(&ClientLease, &[ObjectKey])],
    ) -> Vec<Result<Vec<Result<RouteAuthorityRecord>>>> {
        let requests = batches
            .iter()
            .map(|(lease, keys)| {
                (
                    (*lease).clone(),
                    RouteControlRequest::BatchGet {
                        namespace: namespace.to_string(),
                        authority: lease.runtime.stable_id.clone(),
                        keys: keys.to_vec(),
                    },
                )
            })
            .collect();
        self.send_batch(requests)
            .into_iter()
            .zip(batches)
            .map(|(reply, (_, keys))| {
                let RouteControlResponse::BatchGet(results) = reply? else {
                    return unexpected_response("batch_get_routes");
                };
                ensure_batch_len("batch_get_routes", keys.len(), results.len())?;
                Ok(results)
            })
            .collect()
    }

    fn batch_get_route_presence_many(
        &self,
        namespace: &str,
        batches: &[(&ClientLease, &[ObjectKey])],
    ) -> Vec<Result<Vec<Result<RouteAuthorityPresence>>>> {
        let requests = batches
            .iter()
            .map(|(lease, keys)| {
                (
                    (*lease).clone(),
                    RouteControlRequest::BatchContains {
                        namespace: namespace.to_string(),
                        authority: lease.runtime.stable_id.clone(),
                        keys: keys.to_vec(),
                        include_presence: true,
                    },
                )
            })
            .collect();
        self.send_batch(requests)
            .into_iter()
            .zip(batches)
            .map(|(reply, (_, keys))| {
                let RouteControlResponse::BatchGetPresence(results) = reply? else {
                    return unexpected_response("batch_get_route_presence");
                };
                ensure_batch_len("batch_get_route_presence", keys.len(), results.len())?;
                Ok(results)
            })
            .collect()
    }

    fn batch_contains_routes_many(
        &self,
        namespace: &str,
        batches: &[(&ClientLease, &[ObjectKey])],
    ) -> Vec<Result<Vec<Result<bool>>>> {
        let requests = batches
            .iter()
            .map(|(lease, keys)| {
                (
                    (*lease).clone(),
                    RouteControlRequest::BatchContains {
                        namespace: namespace.to_string(),
                        authority: lease.runtime.stable_id.clone(),
                        keys: keys.to_vec(),
                        include_presence: false,
                    },
                )
            })
            .collect();
        self.send_batch(requests)
            .into_iter()
            .zip(batches)
            .map(|(reply, (_, keys))| {
                let RouteControlResponse::BatchContains(results) = reply? else {
                    return unexpected_response("batch_contains_routes");
                };
                ensure_batch_len("batch_contains_routes", keys.len(), results.len())?;
                Ok(results)
            })
            .collect()
    }

    fn get_allocation_route_references(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReferenceState>> {
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::GetAllocationReferences {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                identities: identities.to_vec(),
                include_route_key: false,
            },
        )?;
        let RouteControlResponse::AllocationReferences(references) = response else {
            return unexpected_response("get_allocation_route_references");
        };
        ensure_batch_len(
            "get_allocation_route_references",
            identities.len(),
            references.len(),
        )?;
        Ok(references)
    }

    fn get_allocation_route_reference_keys(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReference>> {
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::GetAllocationReferences {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                identities: identities.to_vec(),
                include_route_key: true,
            },
        )?;
        let RouteControlResponse::AllocationReferenceKeys(references) = response else {
            return unexpected_response("get_allocation_route_reference_keys");
        };
        ensure_batch_len(
            "get_allocation_route_reference_keys",
            identities.len(),
            references.len(),
        )?;
        Ok(references)
    }

    fn batch_get_route_presence(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<RouteAuthorityPresence>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::BatchContains {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                keys: keys.to_vec(),
                include_presence: true,
            },
        )?;
        let RouteControlResponse::BatchGetPresence(replies) = response else {
            return unexpected_response("batch_get_route_presence");
        };
        ensure_batch_len("batch_get_route_presence", keys.len(), replies.len())?;
        Ok(replies)
    }

    fn batch_contains_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<bool>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::BatchContains {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                keys: keys.to_vec(),
                include_presence: false,
            },
        )?;
        let RouteControlResponse::BatchContains(replies) = response else {
            return unexpected_response("batch_contains_routes");
        };
        ensure_batch_len("batch_contains_routes", keys.len(), replies.len())?;
        Ok(replies)
    }

    fn batch_get_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<RouteAuthorityRecord>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::BatchGet {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                keys: keys.to_vec(),
            },
        )?;
        let RouteControlResponse::BatchGet(replies) = response else {
            return unexpected_response("batch_get_routes");
        };
        ensure_batch_len("batch_get_routes", keys.len(), replies.len())?;
        Ok(replies)
    }

    fn batch_compare_and_swap_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::BatchCompareAndSwap {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                requests: requests.to_vec(),
            },
        )?;
        let RouteControlResponse::BatchCompareAndSwap(replies) = response else {
            return unexpected_response("batch_compare_and_swap_routes");
        };
        ensure_batch_len(
            "batch_compare_and_swap_routes",
            requests.len(),
            replies.len(),
        )?;
        Ok(replies)
    }

    fn batch_replace_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<()>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::BatchReplace {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                requests: requests.to_vec(),
            },
        )?;
        let RouteControlResponse::BatchReplace(replies) = response else {
            return unexpected_response("batch_replace_routes");
        };
        ensure_batch_len("batch_replace_routes", requests.len(), replies.len())?;
        Ok(replies)
    }

    fn list_routes_by_replica_owner_page(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        query: &RouteOwnerPageQuery,
    ) -> Result<RouteOwnerPage> {
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::ListByReplicaOwner {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                owner: query.owner.clone(),
                cursor: query.cursor.clone(),
                snapshot_epoch: query.snapshot_epoch,
                limit: query.limit,
            },
        )?;
        let RouteControlResponse::ListByReplicaOwner(page) = response else {
            return unexpected_response("list_routes_by_replica_owner");
        };
        Ok(page)
    }
}

fn ensure_batch_len(operation: &str, expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        return Ok(());
    }
    Err(StoreError::Transport(format!(
        "route control {operation} reply length mismatch: expected={expected} actual={actual}"
    )))
}

fn unexpected_response<T>(operation: &str) -> Result<T> {
    Err(StoreError::Transport(format!(
        "route control {operation} reply type mismatch"
    )))
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex};

    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLifecycleState, CompatibilityDescriptor,
    };

    use super::*;

    struct BatchTransport {
        replies: Mutex<VecDeque<Result<RouteControlResponse>>>,
        requests: Mutex<Vec<RouteControlRequest>>,
        malformed_batch: bool,
    }

    impl RouteControlTransport for BatchTransport {
        fn send_route_control(
            &self,
            _lease: &ClientLease,
            request: RouteControlRequest,
        ) -> Result<RouteControlResponse> {
            self.requests.lock().unwrap().push(request);
            self.replies.lock().unwrap().pop_front().unwrap()
        }

        fn send_route_control_batch(
            &self,
            requests: Vec<(ClientLease, RouteControlRequest)>,
        ) -> Vec<Result<RouteControlResponse>> {
            if self.malformed_batch {
                return Vec::new();
            }
            requests
                .into_iter()
                .map(|(lease, request)| self.send_route_control(&lease, request))
                .collect()
        }
    }

    fn lease(id: &str) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(id, ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            expires_at_ms: u64::MAX,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            endpoints: ClientEndpointSet::default(),
        }
    }

    #[test]
    fn multi_authority_contains_preserves_order_duplicates_and_item_errors() {
        let transport = Arc::new(BatchTransport {
            replies: Mutex::new(VecDeque::from([
                Ok(RouteControlResponse::BatchContains(vec![
                    Ok(true),
                    Ok(false),
                ])),
                Ok(RouteControlResponse::BatchContains(vec![Err(
                    StoreError::Unsupported("item".to_string()),
                )])),
            ])),
            requests: Mutex::new(Vec::new()),
            malformed_batch: false,
        });
        let client = RouteControlAuthorityClient::new(transport.clone());
        let first = lease("b");
        let second = lease("a");
        let duplicate = vec![ObjectKey::new("same"), ObjectKey::new("same")];
        let keys = vec![ObjectKey::new("other")];
        let mut replies = client
            .batch_contains_routes_many("ns", &[(&first, &duplicate), (&second, &keys)])
            .into_iter();
        let first_reply = replies.next().unwrap().unwrap();
        assert!(matches!(first_reply.as_slice(), [Ok(true), Ok(false)]));
        assert!(matches!(
            replies.next().unwrap().unwrap().as_slice(),
            [Err(StoreError::Unsupported(_))]
        ));
        let requests = transport.requests.lock().unwrap();
        assert!(
            matches!(&requests[0], RouteControlRequest::BatchContains { authority, keys, .. }
            if authority == &first.runtime.stable_id && keys == &duplicate)
        );
        assert!(
            matches!(&requests[1], RouteControlRequest::BatchContains { authority, .. }
            if authority == &second.runtime.stable_id)
        );
    }

    #[test]
    fn multi_authority_presence_validates_each_reply() {
        let transport = Arc::new(BatchTransport {
            replies: Mutex::new(VecDeque::from([
                Ok(RouteControlResponse::BatchContains(vec![Ok(true)])),
                Ok(RouteControlResponse::BatchGetPresence(Vec::new())),
                Err(StoreError::Unsupported("legacy".to_string())),
            ])),
            requests: Mutex::new(Vec::new()),
            malformed_batch: false,
        });
        let client = RouteControlAuthorityClient::new(transport);
        let authority = lease("a");
        let keys = vec![ObjectKey::new("key")];
        let replies = client.batch_get_route_presence_many(
            "ns",
            &[
                (&authority, &keys),
                (&authority, &keys),
                (&authority, &keys),
            ],
        );
        assert!(
            matches!(&replies[0], Err(StoreError::Transport(message)) if message.contains("type mismatch"))
        );
        assert!(
            matches!(&replies[1], Err(StoreError::Transport(message)) if message.contains("length mismatch"))
        );
        assert!(matches!(&replies[2], Err(StoreError::Unsupported(_))));
    }

    #[test]
    fn multi_authority_reads_reject_missing_transport_replies() {
        let transport = Arc::new(BatchTransport {
            replies: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
            malformed_batch: true,
        });
        let client = RouteControlAuthorityClient::new(transport);
        let authority = lease("a");
        let keys = vec![ObjectKey::new("key")];
        let replies = client.batch_get_routes_many("ns", &[(&authority, &keys)]);
        assert_eq!(replies.len(), 1);
        assert!(
            matches!(&replies[0], Err(StoreError::Transport(message)) if message.contains("length mismatch"))
        );
    }
}
