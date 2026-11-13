use std::sync::Arc;

use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientLease, ClientRuntimeId, ClientStableId, ObjectKey, Result, RouteCasRequest, StoreError,
};

use crate::shim::{
    RouteAuthorityClient, RouteAuthorityRecord, RouteAuthorityService, RouteOwnerPage,
    RouteOwnerPageQuery,
};

#[derive(Clone, Debug)]
pub enum RouteControlRequest {
    Publication {
        namespace: String,
        authority: ClientStableId,
        operations: Vec<mooncake_store_core::PublicationControl>,
    },
    BatchGet {
        namespace: String,
        authority: ClientStableId,
        keys: Vec<ObjectKey>,
    },
    BatchContains {
        namespace: String,
        authority: ClientStableId,
        keys: Vec<ObjectKey>,
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
            Self::Publication { .. } => "control_route_publication",
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
            Self::Publication { operations, .. } => operations.len(),
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
            Self::Publication { operations, .. } if operations.is_empty() => {
                Some(RouteControlResponse::Publication(Vec::new()))
            }
            Self::BatchGet { keys, .. } if keys.is_empty() => {
                Some(RouteControlResponse::BatchGet(Vec::new()))
            }
            Self::BatchContains { keys, .. } if keys.is_empty() => {
                Some(RouteControlResponse::BatchContains(Vec::new()))
            }
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
    Publication(Vec<Result<mooncake_store_core::PublicationControlReply>>),
    BatchGet(Vec<Result<RouteAuthorityRecord>>),
    BatchContains(Vec<Result<bool>>),
    BatchCompareAndSwap(Vec<Result<CasResult>>),
    BatchReplace(Vec<Result<()>>),
    ListByReplicaOwner(RouteOwnerPage),
    AllocationReferences(Vec<AllocationRouteReferenceState>),
    AllocationReferenceKeys(Vec<AllocationRouteReference>),
}

pub trait RouteControlTransport: Send + Sync {
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
        RouteControlRequest::Publication {
            namespace,
            authority,
            operations,
        } => service
            .publication_control(&namespace, &authority, &operations)
            .map(RouteControlResponse::Publication),
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
        } => Ok(RouteControlResponse::BatchContains(
            service.batch_contains_routes(&namespace, &authority, &keys),
        )),
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
}

impl RouteAuthorityClient for RouteControlAuthorityClient {
    fn publication_control(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        operations: &[mooncake_store_core::PublicationControl],
    ) -> Result<Vec<Result<mooncake_store_core::PublicationControlReply>>> {
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::Publication {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                operations: operations.to_vec(),
            },
        )?;
        let RouteControlResponse::Publication(replies) = response else {
            return unexpected_response("publication_control");
        };
        ensure_batch_len("publication_control", operations.len(), replies.len())?;
        Ok(replies)
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
