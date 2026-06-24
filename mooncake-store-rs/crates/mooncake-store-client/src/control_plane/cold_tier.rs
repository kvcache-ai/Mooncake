use super::codec::decode_error;
use super::*;
use crate::observability::OperationTracker;

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
