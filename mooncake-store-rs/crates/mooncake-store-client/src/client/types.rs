#[derive(Clone, Copy, Debug)]
pub struct ObjectRef<'a> {
    pub tenant: Option<&'a str>,
    pub domain: Option<&'a str>,
    pub object_set: Option<&'a str>,
    pub qos_tier: Option<&'a str>,
    pub key: &'a str,
}

impl<'a> ObjectRef<'a> {
    pub fn new(key: &'a str) -> Self {
        Self {
            tenant: None,
            domain: None,
            object_set: None,
            qos_tier: None,
            key,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn domain(mut self, domain: &'a str) -> Self {
        self.domain = Some(domain);
        self
    }

    pub fn object_set(mut self, object_set: &'a str) -> Self {
        self.object_set = Some(object_set);
        self
    }

    pub fn qos_tier(mut self, qos_tier: &'a str) -> Self {
        self.qos_tier = Some(qos_tier);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceQuota {
    pub max_bytes: Option<u64>,
    pub max_objects: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFairness {
    pub max_remote_batch_items_per_tenant: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BandwidthShaping {
    pub max_remote_batch_bytes: Option<usize>,
    pub max_remote_batch_burst_items: Option<usize>,
    pub max_inflight_bytes_per_batch: Option<u64>,
}

impl BandwidthShaping {
    pub fn new() -> Self {
        Self {
            max_remote_batch_bytes: None,
            max_remote_batch_burst_items: None,
            max_inflight_bytes_per_batch: None,
        }
    }

    pub fn max_remote_batch_bytes(mut self, max_remote_batch_bytes: usize) -> Self {
        self.max_remote_batch_bytes = Some(max_remote_batch_bytes.max(1));
        self
    }

    pub fn max_remote_batch_burst_items(mut self, max_remote_batch_burst_items: usize) -> Self {
        self.max_remote_batch_burst_items = Some(max_remote_batch_burst_items.max(1));
        self
    }

    pub fn max_inflight_bytes_per_batch(mut self, max_inflight_bytes_per_batch: u64) -> Self {
        self.max_inflight_bytes_per_batch = Some(max_inflight_bytes_per_batch.max(1));
        self
    }
}

impl ExecutionFairness {
    pub fn new() -> Self {
        Self {
            max_remote_batch_items_per_tenant: None,
        }
    }

    pub fn max_remote_batch_items_per_tenant(
        mut self,
        max_remote_batch_items_per_tenant: usize,
    ) -> Self {
        self.max_remote_batch_items_per_tenant = Some(max_remote_batch_items_per_tenant.max(1));
        self
    }
}

impl NamespaceQuota {
    pub fn new() -> Self {
        Self {
            max_bytes: None,
            max_objects: None,
        }
    }

    pub fn max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    pub fn max_objects(mut self, max_objects: usize) -> Self {
        self.max_objects = Some(max_objects);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationPolicy {
    pub replica_count: Option<usize>,
    pub with_soft_pin: bool,
    pub preferred_segments: Vec<SegmentName>,
    pub preferred_storage_owners: Vec<String>,
    pub prefer_alloc_in_same_node: bool,
    pub prefer_local: bool,
}

impl Default for ReplicationPolicy {
    fn default() -> Self {
        Self {
            replica_count: None,
            with_soft_pin: false,
            preferred_segments: Vec::new(),
            preferred_storage_owners: Vec::new(),
            prefer_alloc_in_same_node: false,
            prefer_local: true,
        }
    }
}

impl ReplicationPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replica_count(mut self, replica_count: usize) -> Self {
        self.replica_count = Some(replica_count);
        self
    }

    pub fn with_soft_pin(mut self, with_soft_pin: bool) -> Self {
        self.with_soft_pin = with_soft_pin;
        self
    }

    pub fn prefer_alloc_in_same_node(mut self, prefer_alloc_in_same_node: bool) -> Self {
        self.prefer_alloc_in_same_node = prefer_alloc_in_same_node;
        self
    }

    pub fn prefer_local(mut self, prefer_local: bool) -> Self {
        self.prefer_local = prefer_local;
        self
    }

    pub fn preferred_segment(mut self, segment: impl Into<String>) -> Self {
        self.preferred_segments.push(SegmentName::new(segment));
        self
    }

    pub fn preferred_segments<I, S>(mut self, segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.preferred_segments = segments
            .into_iter()
            .map(SegmentName::new)
            .collect::<Vec<_>>();
        self
    }

    pub fn preferred_storage_owner(mut self, owner: impl Into<String>) -> Self {
        self.preferred_storage_owners.push(owner.into());
        self
    }

    pub fn preferred_storage_owners<I, S>(mut self, owners: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.preferred_storage_owners = owners.into_iter().map(Into::into).collect::<Vec<_>>();
        self
    }
}

#[derive(Clone, Debug)]
pub struct PutRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub value: &'a [u8],
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> PutRequest<'a> {
    pub fn new(key: &'a str, value: &'a [u8]) -> Self {
        Self {
            tenant: None,
            key,
            value,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

#[derive(Clone, Debug)]
pub struct PutFromRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffer: *const c_void,
    pub size: usize,
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> PutFromRequest<'a> {
    pub fn new(key: &'a str, buffer: *const c_void, size: usize) -> Self {
        Self {
            tenant: None,
            key,
            buffer,
            size,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

#[derive(Debug)]
pub struct GetRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffer: &'a mut [u8],
}

pub struct MultiBufferPutRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffers: &'a [&'a [u8]],
    pub policy: Option<ReplicationPolicy>,
}

impl<'a> MultiBufferPutRequest<'a> {
    pub fn new(key: &'a str, buffers: &'a [&'a [u8]]) -> Self {
        Self {
            tenant: None,
            key,
            buffers,
            policy: None,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }

    pub fn replication(mut self, policy: ReplicationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

pub struct MultiBufferGetRequest<'a> {
    pub tenant: Option<&'a str>,
    pub key: &'a str,
    pub buffers: &'a mut [&'a mut [u8]],
}

impl<'a> MultiBufferGetRequest<'a> {
    pub fn new(key: &'a str, buffers: &'a mut [&'a mut [u8]]) -> Self {
        Self {
            tenant: None,
            key,
            buffers,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }
}

impl<'a> GetRequest<'a> {
    pub fn new(key: &'a str, buffer: &'a mut [u8]) -> Self {
        Self {
            tenant: None,
            key,
            buffer,
        }
    }

    pub fn tenant(mut self, tenant: &'a str) -> Self {
        self.tenant = Some(tenant);
        self
    }
}
