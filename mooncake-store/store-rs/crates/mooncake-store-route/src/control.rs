use std::sync::Arc;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, ClientStableId, ObjectKey, ObjectRoute, Result,
    RouteCasRequest, StoreError,
};

use crate::shim::{RouteAuthorityClient, RouteAuthorityService};

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
        }
    }

    pub fn item_count(&self) -> usize {
        match self {
            Self::BatchGet { keys, .. } | Self::BatchContains { keys, .. } => keys.len(),
            Self::BatchCompareAndSwap { requests, .. } | Self::BatchReplace { requests, .. } => {
                requests.len()
            }
            Self::ListByReplicaOwner { .. } => 1,
        }
    }

    pub fn empty_response(&self) -> Option<RouteControlResponse> {
        match self {
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
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum RouteControlResponse {
    BatchGet(Vec<Result<Option<ObjectRoute>>>),
    BatchContains(Vec<Result<bool>>),
    BatchCompareAndSwap(Vec<Result<CasResult>>),
    BatchReplace(Vec<Result<()>>),
    ListByReplicaOwner(Vec<ObjectRoute>),
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
        RouteControlRequest::BatchGet {
            namespace,
            authority,
            keys,
        } => Ok(RouteControlResponse::BatchGet(
            service.batch_get_routes(&namespace, &authority, &keys),
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
        } => service
            .list_routes_by_replica_owner(&namespace, &authority, &owner)
            .map(RouteControlResponse::ListByReplicaOwner),
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
    ) -> Result<Vec<Result<Option<ObjectRoute>>>> {
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

    fn list_routes_by_replica_owner(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        let response = self.transport.send_route_control(
            lease,
            RouteControlRequest::ListByReplicaOwner {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                owner: owner.clone(),
            },
        )?;
        let RouteControlResponse::ListByReplicaOwner(routes) = response else {
            return unexpected_response("list_routes_by_replica_owner");
        };
        Ok(routes)
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
