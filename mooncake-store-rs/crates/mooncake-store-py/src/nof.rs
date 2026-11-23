//! Optional SPDK targets for the Python compatibility runtime.

use std::collections::BTreeSet;

use mooncake_store_client::NofTargetConfig;
use mooncake_store_core::{Result, StoreError};
use serde::Deserialize;

const TARGETS_ENV: &str = "MC_STORE_RS_NOF_TARGETS";
const REPLICAS_ENV: &str = "MC_STORE_RS_NOF_REPLICA_COUNT";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetSpec {
    target_id: String,
    #[serde(default = "default_transport")]
    transport: String,
    traddr: String,
    port: String,
    subnqn: String,
    #[serde(default = "default_nsid")]
    nsid: u32,
    device_bytes: u64,
    #[serde(default)]
    start_offset: u64,
    #[serde(default)]
    hostnqn: Option<String>,
}

fn default_transport() -> String {
    "tcp".to_string()
}

fn default_nsid() -> u32 {
    1
}

fn invalid(message: impl Into<String>) -> StoreError {
    StoreError::InvalidState(message.into())
}

fn parse_targets(json: &str, replicas: usize) -> Result<Vec<TargetSpec>> {
    let specs: Vec<TargetSpec> = serde_json::from_str(json)
        .map_err(|error| invalid(format!("invalid {TARGETS_ENV}: {error}")))?;
    if !(1..=8).contains(&replicas) || replicas > specs.len() {
        return Err(invalid(
            "NoF replica count must be in 1..=8 and cannot exceed target count",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut namespaces = BTreeSet::new();
    for spec in &specs {
        if [&spec.target_id, &spec.traddr, &spec.port, &spec.subnqn]
            .iter()
            .any(|value| value.trim().is_empty() || value.contains('\0'))
            || !matches!(spec.transport.as_str(), "tcp" | "rdma")
            || spec.nsid == 0
            || !spec.port.parse::<u16>().is_ok_and(|port| port > 0)
            || spec.start_offset >= spec.device_bytes
            || !spec.start_offset.is_multiple_of(4096)
            || !spec.device_bytes.is_multiple_of(4096)
            || spec
                .hostnqn
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.contains('\0'))
        {
            return Err(invalid(format!("invalid NoF target {}", spec.target_id)));
        }
        if !ids.insert(&spec.target_id)
            || !namespaces.insert((
                &spec.transport,
                &spec.traddr,
                spec.port.parse::<u16>().expect("validated port"),
                &spec.subnqn,
                spec.nsid,
            ))
        {
            return Err(invalid("NoF target IDs and namespaces must be unique"));
        }
    }
    Ok(specs)
}

pub(crate) fn targets_from_env() -> Result<(Vec<NofTargetConfig>, usize)> {
    let json = match std::env::var(TARGETS_ENV) {
        Ok(json) => json,
        Err(std::env::VarError::NotPresent) => return Ok((Vec::new(), 1)),
        Err(error) => return Err(invalid(format!("invalid {TARGETS_ENV}: {error}"))),
    };
    if !std::env::var("MC_STORE_RS_ENABLE_COLD_TIER").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }) {
        return Err(invalid(
            "NoF targets require MC_STORE_RS_ENABLE_COLD_TIER=1",
        ));
    }
    let replicas = match std::env::var(REPLICAS_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "1".to_string(),
        Err(error) => return Err(invalid(format!("invalid {REPLICAS_ENV}: {error}"))),
    }
    .parse::<usize>()
    .map_err(|error| invalid(format!("invalid {REPLICAS_ENV}: {error}")))?;
    let specs = parse_targets(&json, replicas)?;
    Ok((connect_targets(specs)?, replicas))
}

#[cfg(not(feature = "nof-spdk"))]
fn connect_targets(_specs: Vec<TargetSpec>) -> Result<Vec<NofTargetConfig>> {
    Err(invalid(
        "Store-RS Python extension was built without the nof-spdk feature",
    ))
}

#[cfg(feature = "nof-spdk")]
fn connect_targets(specs: Vec<TargetSpec>) -> Result<Vec<NofTargetConfig>> {
    use mooncake_store_client::NofBackend;
    use mooncake_store_client::{
        ExtentStoreExecutor, ExtentStoreExecutorConfig, SpdkNofBlockDevice,
        SpdkNofBlockDeviceConfig,
    };
    use std::sync::Arc;

    specs
        .into_iter()
        .map(|spec| {
            let mut config = match spec.transport.as_str() {
                "tcp" => {
                    SpdkNofBlockDeviceConfig::tcp(&spec.traddr, &spec.port, &spec.subnqn, spec.nsid)
                }
                _ => SpdkNofBlockDeviceConfig::rdma(
                    &spec.traddr,
                    &spec.port,
                    &spec.subnqn,
                    spec.nsid,
                ),
            };
            config.hostnqn = spec.hostnqn;
            let device = Arc::new(SpdkNofBlockDevice::connect(config)?);
            let executor = Arc::new(ExtentStoreExecutor::new(
                device,
                ExtentStoreExecutorConfig::new(spec.start_offset, spec.device_bytes),
            )?);
            NofTargetConfig::new(spec.target_id, NofBackend::new(executor)?)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: &str = r#"{"target_id":"a","traddr":"127.0.0.1","port":"4420","subnqn":"nqn.test","device_bytes":67108864}"#;

    #[test]
    fn target_validation_precedes_device_access() {
        let specs = parse_targets(&format!("[{TARGET}]"), 1).unwrap();
        assert_eq!(specs[0].transport, "tcp");
        assert_eq!(specs[0].nsid, 1);
        for replicas in [0, 2, 9] {
            assert!(parse_targets(&format!("[{TARGET}]"), replicas).is_err());
        }
        assert!(parse_targets(&format!("[{TARGET},{TARGET}]"), 1).is_err());
        let alias = TARGET.replace("\"a\"", "\"b\"");
        assert!(parse_targets(&format!("[{TARGET},{alias}]"), 1).is_err());
        let alias = alias.replace("4420", "04420");
        assert!(parse_targets(&format!("[{TARGET},{alias}]"), 1).is_err());
        for (from, to) in [("4420", "0"), ("67108864", "4097"), ("127.0.0.1", "")] {
            assert!(parse_targets(&format!("[{}]", TARGET.replace(from, to)), 1).is_err());
        }
    }
}
