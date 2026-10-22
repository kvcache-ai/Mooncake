// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mooncake_conductor::{
    ClientConfig, ConductorClient, ConductorError, HashProfileConfig, HealthStatus, PublisherKind,
    QueryRequest, ServiceConfig,
};

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn should_run_integration() -> bool {
    matches!(
        std::env::var("MC_RUST_CONDUCTOR_RUN_INTEGRATION").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

fn unique_instance_id() -> String {
    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("rust-smoke-{}-{timestamp_ns}", std::process::id())
}

#[test]
fn calls_before_setup_fail_without_a_server() -> Result<(), ConductorError> {
    let client = ConductorClient::new()?;
    assert_eq!(client.health_check(), HealthStatus::NotInitialized);

    let tokens = [1, 2, 3];
    let request = QueryRequest::new("model", 16, &tokens);
    assert!(matches!(
        client.query(&request),
        Err(ConductorError::Unavailable)
    ));
    assert!(matches!(
        client.list_services(),
        Err(ConductorError::Unavailable)
    ));
    assert!(matches!(
        client.setup(&ClientConfig::new("no-port")),
        Err(ConductorError::InvalidParams)
    ));
    assert!(matches!(
        client.unregister("bad\0id", "", 0),
        Err(ConductorError::InvalidString(_))
    ));
    Ok(())
}

#[test]
fn setup_against_dead_server_fails() -> Result<(), ConductorError> {
    let client = ConductorClient::new()?;
    let mut config = ClientConfig::new("127.0.0.1:1");
    config.connect_timeout = Duration::from_millis(200);
    assert!(matches!(
        client.setup(&config),
        Err(ConductorError::RpcFail)
    ));
    assert_eq!(client.health_check(), HealthStatus::NotInitialized);
    Ok(())
}

#[test]
fn smoke_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    if !should_run_integration() {
        eprintln!(
            "skipping Mooncake Conductor Rust smoke test; set MC_RUST_CONDUCTOR_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }

    let conductor_addr = env_or_default("MC_RUST_CONDUCTOR_ADDR", "127.0.0.1:13334");
    let client = ConductorClient::new()?;
    client.setup(&ClientConfig::new(conductor_addr))?;
    assert_eq!(client.health_check(), HealthStatus::Healthy);

    let instance_id = unique_instance_id();
    let model_name = "rust-smoke-model";
    let hash_profile = HashProfileConfig {
        strategy: "vllm_v1".to_string(),
        algorithm: "sha256_cbor".to_string(),
        python_hash_seed: "0".to_string(),
        index_projection: "low64_be".to_string(),
    };
    // The endpoint only has to parse; nothing publishes on it. The conductor
    // only supports cache group 0, which still exercises the optional field.
    let mut service = ServiceConfig::new(
        &instance_id,
        "tcp://127.0.0.1:29432",
        model_name,
        16,
        hash_profile,
    );
    service.cache_group = Some(0);

    assert!(client.register(&service)?.is_new);
    assert!(!client.register(&service)?.is_new);

    let services = client.list_services()?;
    let info = services
        .iter()
        .find(|info| info.instance_id == instance_id)
        .ok_or("registered service missing from list_services")?;
    assert_eq!(info.endpoint, service.endpoint);
    assert_eq!(info.publisher_kind, PublisherKind::Vllm);
    assert_eq!(info.model_name, model_name);
    assert_eq!(info.tenant_id, "default");
    assert_eq!(info.block_size, 16);
    assert_eq!(info.cache_group, Some(0));
    assert_eq!(info.hash_profile.strategy, "vllm_v1");
    assert!(!info.hash_profile.root_digest.is_empty());

    let view = client.get_global_view()?;
    let context = view
        .contexts
        .iter()
        .find(|context| context.instances.contains_key(&instance_id))
        .ok_or("registered service missing from get_global_view")?;
    assert_eq!(context.model_name, model_name);
    assert_eq!(
        context.hash_profile.root_digest,
        info.hash_profile.root_digest
    );

    // Nothing was published, so the selected instance reports a zero-length
    // match (the conductor returns an entry for every selected instance).
    let tokens: Vec<i32> = (1..=48).collect();
    let request = QueryRequest::new(model_name, 16, &tokens).with_instance_filter(&instance_id);
    let result = client.query(&request)?;
    assert_eq!(result.hits.len(), 1);
    let hit = &result.hits[&instance_id];
    assert_eq!(hit.longest_matched, 0);
    assert_eq!(
        (hit.device, hit.cpu_local, hit.cpu_share, hit.disk),
        (0, 0, 0, 0)
    );

    client.unregister(&instance_id, "", 0)?;
    assert!(matches!(
        client.unregister(&instance_id, "", 0),
        Err(ConductorError::ServiceNotFound)
    ));

    client.close();
    assert_eq!(client.health_check(), HealthStatus::Unreachable);
    Ok(())
}
