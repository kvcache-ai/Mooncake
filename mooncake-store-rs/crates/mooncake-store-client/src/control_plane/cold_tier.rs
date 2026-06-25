use super::codec::decode_error;
use super::*;
use crate::observability::OperationTracker;

#[derive(Clone, Debug)]
pub(crate) struct ColdTierFreeResult {
    pub attempted_victims: usize,
    pub freed_backings: usize,
    pub collected_backings: usize,
    pub skipped_backings: usize,
    pub reached_low_watermark: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ColdTierProbeResult {
    pub device_id: String,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub schedulable: bool,
    pub state: String,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ColdReadResult {
    pub segment_name: String,
    pub segment_offset: u64,
    pub length: u64,
    pub checksum: Option<u64>,
    pub target_chunks: Vec<SegmentTargetChunk>,
    pub transport_endpoint: Option<String>,
    pub transport_segment_descriptor: Option<String>,
    pub admission_wait_us: u64,
    pub ssd_read_us: u64,
    pub memcpy_us: u64,
    pub route_update_us: u64,
    pub total_promote_us: u64,
}

pub(crate) struct ColdReadResponse {
    pub result: Result<ColdReadResult>,
    pub deferred_promote: Option<Box<dyn FnOnce() + Send>>,
}

pub(crate) struct ColdReadTarget {
    pub tenant: String,
    pub key: String,
    pub domain: String,
    pub object_set: String,
}

pub(crate) trait ColdTierControlService: Send + Sync {
    fn trigger_offload(&self, max_tasks: usize) -> Result<usize>;

    fn manual_gc(&self, device_id: &str, max_backings: usize) -> Result<usize>;

    fn manual_free(&self, device_id: &str, max_victims: usize) -> Result<ColdTierFreeResult>;

    fn probe_device(&self, device_id: &str) -> Result<ColdTierProbeResult>;

    fn read_from_cold(
        &self,
        namespace: &str,
        authority: &str,
        tenant: &str,
        key: &str,
        domain: &str,
        object_set: &str,
    ) -> ColdReadResponse;

    fn batch_read_from_cold(
        &self,
        namespace: &str,
        authority: &str,
        targets: Vec<ColdReadTarget>,
    ) -> Vec<ColdReadResponse> {
        targets
            .into_iter()
            .map(|target| {
                self.read_from_cold(
                    namespace,
                    authority,
                    &target.tenant,
                    &target.key,
                    &target.domain,
                    &target.object_set,
                )
            })
            .collect()
    }

    fn ack_cold_read_complete(&self, slots: &[(SegmentName, u64)]);

    fn pin_for_read(&self, _slots: &[(SegmentName, u64)]) -> u64 {
        0
    }
}

pub(crate) struct UnsupportedColdTierControlService;

impl ColdTierControlService for UnsupportedColdTierControlService {
    fn trigger_offload(&self, _max_tasks: usize) -> Result<usize> {
        Err(StoreError::Unsupported(
            "cold tier offload trigger is not wired yet".to_string(),
        ))
    }

    fn manual_gc(&self, _device_id: &str, _max_backings: usize) -> Result<usize> {
        Err(StoreError::Unsupported(
            "cold tier manual GC is not wired yet".to_string(),
        ))
    }

    fn manual_free(&self, _device_id: &str, _max_victims: usize) -> Result<ColdTierFreeResult> {
        Err(StoreError::Unsupported(
            "cold tier manual free is not wired yet".to_string(),
        ))
    }

    fn probe_device(&self, _device_id: &str) -> Result<ColdTierProbeResult> {
        Err(StoreError::Unsupported(
            "cold tier device probe is not wired yet".to_string(),
        ))
    }

    fn read_from_cold(
        &self,
        _namespace: &str,
        _authority: &str,
        _tenant: &str,
        _key: &str,
        _domain: &str,
        _object_set: &str,
    ) -> ColdReadResponse {
        ColdReadResponse {
            result: Err(StoreError::Unsupported(
                "cold tier read control is not wired yet".to_string(),
            )),
            deferred_promote: None,
        }
    }

    fn ack_cold_read_complete(&self, _slots: &[(SegmentName, u64)]) {}
}

impl ControlPlaneClient {
    #[allow(dead_code)]
    pub(crate) fn trigger_cold_tier_offload(
        &self,
        lease: &ClientLease,
        max_tasks: u64,
    ) -> Result<u64> {
        let tracker = OperationTracker::new("control_cold_tier_offload_trigger");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            let request = pb::TriggerColdTierOffloadRequest { max_tasks };
            let reply = self.rpc(
                |mut client| async move {
                    client
                        .trigger_cold_tier_offload(Request::new(request))
                        .await
                },
                channel,
            )?;
            decode_error(reply.error)?;
            Ok(reply.materialized)
        })();
        tracker.finish(&result, result.as_ref().copied().unwrap_or_default());
        result
    }

    #[allow(dead_code)]
    pub(crate) fn manual_cold_tier_gc(
        &self,
        lease: &ClientLease,
        device_id: &str,
        max_backings: u64,
    ) -> Result<u64> {
        let tracker = OperationTracker::new("control_cold_tier_manual_gc");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            let request = pb::ManualColdTierGcRequest {
                device_id: device_id.to_string(),
                max_backings,
            };
            let reply = self.rpc(
                |mut client| async move { client.manual_cold_tier_gc(Request::new(request)).await },
                channel,
            )?;
            decode_error(reply.error)?;
            Ok(reply.collected)
        })();
        tracker.finish(&result, result.as_ref().copied().unwrap_or_default());
        result
    }

    #[allow(dead_code)]
    pub(crate) fn manual_cold_tier_free(
        &self,
        lease: &ClientLease,
        device_id: &str,
        max_victims: u64,
    ) -> Result<pb::ManualColdTierFreeReply> {
        let tracker = OperationTracker::new("control_cold_tier_manual_free");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            let request = pb::ManualColdTierFreeRequest {
                device_id: device_id.to_string(),
                max_victims,
            };
            let reply =
                self.rpc(
                    |mut client| async move {
                        client.manual_cold_tier_free(Request::new(request)).await
                    },
                    channel,
                )?;
            decode_error(reply.error.clone())?;
            Ok(reply)
        })();
        tracker.finish(
            &result,
            result
                .as_ref()
                .map(|reply| reply.freed_backings)
                .unwrap_or_default(),
        );
        result
    }

    #[allow(dead_code)]
    pub(crate) fn probe_cold_tier_device(
        &self,
        lease: &ClientLease,
        device_id: &str,
    ) -> Result<pb::ProbeColdTierDeviceReply> {
        let tracker = OperationTracker::new("control_cold_tier_probe_device");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            let request = pb::ProbeColdTierDeviceRequest {
                device_id: device_id.to_string(),
            };
            let reply =
                self.rpc(
                    |mut client| async move {
                        client.probe_cold_tier_device(Request::new(request)).await
                    },
                    channel,
                )?;
            decode_error(reply.error.clone())?;
            Ok(reply)
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn read_from_cold(
        &self,
        lease: &ClientLease,
        request: pb::ReadFromColdRequest,
    ) -> Result<pb::ReadFromColdReply> {
        let tracker = OperationTracker::new("control_read_from_cold");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move { client.read_from_cold(Request::new(request)).await },
                channel,
            )?;
            if reply.backpressure {
                return Err(StoreError::Backpressure(
                    "cold tier owner reported restore backpressure".to_string(),
                ));
            }
            decode_error(reply.error.clone())?;
            Ok(reply)
        })();
        tracker.finish(&result, 0);
        result
    }

    #[allow(dead_code)]
    pub(crate) fn batch_read_from_cold(
        &self,
        lease: &ClientLease,
        request: pb::BatchReadFromColdRequest,
    ) -> Result<pb::BatchReadFromColdReply> {
        let tracker = OperationTracker::new("control_batch_read_from_cold");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            self.rpc(
                |mut client| async move {
                    client.batch_read_from_cold(Request::new(request)).await
                },
                channel,
            )
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn ack_cold_read_complete(
        &self,
        lease: &ClientLease,
        segment_names: Vec<String>,
        segment_offsets: Vec<u64>,
    ) {
        let Ok(channel) = self.channel_for(lease) else {
            return;
        };
        let request = pb::AckColdReadCompleteRequest {
            segment_names,
            segment_offsets,
        };
        let request_timeout = self.request_timeout;
        self.with_runtime(|runtime| {
            runtime.spawn(async move {
                let mut client =
                    pb::control_plane_service_client::ControlPlaneServiceClient::new(channel);
                let _ = tokio::time::timeout(
                    request_timeout,
                    client.ack_cold_read_complete(Request::new(request)),
                )
                .await;
            });
        });
    }

    #[allow(dead_code)]
    pub(crate) fn pin_for_read(
        &self,
        lease: &ClientLease,
        segment_names: Vec<String>,
        segment_offsets: Vec<u64>,
    ) -> Result<u64> {
        let channel = self.channel_for(lease)?;
        let request = pb::PinForReadRequest {
            segment_names,
            segment_offsets,
        };
        let reply = self.rpc(
            |mut client| async move { client.pin_for_read(Request::new(request)).await },
            channel,
        )?;
        Ok(reply.pinned)
    }
}
