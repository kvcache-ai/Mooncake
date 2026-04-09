use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_store_client::{
    LocalMemoryConfig, PlacementPlanner, RouteControlMode, StoreClient, StoreClientBuilder,
    TentTransportFactory,
};
use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, Result,
};
use mooncake_transport::TentEngine;

pub use crate::config::CompatSetupArgs;

pub struct CompatRuntime {
    pub client: StoreClient,
    pub stable_id: String,
    pub segment_name: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct CompatRuntimeArgs {
    pub setup: CompatSetupArgs,
    pub local_segment_name: Option<String>,
    pub route_control: RouteControlMode,
}

impl CompatRuntimeArgs {
    pub fn build(self) -> Result<CompatRuntime> {
        let plan = self.setup.build()?;
        let planner_metadata = plan.routed_writes.then(|| plan.metadata.clone());
        let stable_id = plan.stable_id.clone();
        let expires_at_ms = plan.expires_at_ms;
        let segment_name = self
            .local_segment_name
            .unwrap_or_else(|| default_segment_name(&stable_id));
        let engine = Arc::new(TentEngine::new(
            &plan
                .tent_config
                .clone()
                .set("local_segment_name", &segment_name),
        )?);
        let factory = Arc::new(TentTransportFactory::new(plan.tent_config));
        let mut builder = StoreClientBuilder::new(plan.metadata, stable_id.clone())
            .epoch(ClientEpoch(1))
            .state(ClientLifecycleState::Active)
            .compatibility(CompatibilityDescriptor::default())
            .tenant(plan.tenant)
            .local_memory(
                LocalMemoryConfig::new()
                    .storage_bytes(plan.storage_bytes)
                    .scratch_bytes(plan.scratch_bytes)
                    .location("cpu:0"),
            )
            .with_tent(engine)
            .transport_factory(factory)
            .route_control(self.route_control);

        for (key, value) in plan.labels {
            builder = builder.label(key, value);
        }
        if let Some(metadata) = planner_metadata {
            builder = builder.routed_writes(
                PlacementPlanner::new(metadata).require_label("storage", "true"),
                plan.replica_count,
            );
        }

        Ok(CompatRuntime {
            client: builder.build(expires_at_ms)?,
            stable_id,
            segment_name,
            expires_at_ms,
        })
    }
}

fn default_segment_name(stable_id: &str) -> String {
    format!("{}-segment-{}", stable_id.replace(':', "-"), now_ms())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_millis() as u64
}
