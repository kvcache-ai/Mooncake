use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use _store_rs::config::build_metadata_backend;
use clap::{Parser, Subcommand, ValueEnum};
use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{init_tracing, RouteControlMode};
use mooncake_store_core::{
    ClientEpoch, ClientRuntimeId, MetadataBackend, RoutePolicy, RoutePolicyDomain,
};
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "mooncake-store-admin")]
#[command(about = "Run explicit Mooncake store metadata maintenance tasks")]
struct Args {
    #[arg(long)]
    metadata_url: String,
    #[arg(long)]
    keyspace: Option<String>,
    #[arg(long)]
    trace_filter: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    CleanupStaleSegments,
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
}

#[derive(Subcommand, Debug)]
enum PolicyCommand {
    Get {
        #[arg(long)]
        tenant: Option<String>,
    },
    Set {
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        route_topk: u32,
        #[arg(long, value_enum)]
        route_control: RouteControlArg,
        #[arg(long, default_value = "admin")]
        created_by: String,
    },
    Delete {
        #[arg(long)]
        tenant: String,
    },
    List,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum RouteControlArg {
    EmbeddedWrh,
    MetadataOnly,
}

impl From<RouteControlArg> for RouteControlMode {
    fn from(value: RouteControlArg) -> Self {
        match value {
            RouteControlArg::EmbeddedWrh => RouteControlMode::EmbeddedWrh,
            RouteControlArg::MetadataOnly => RouteControlMode::MetadataOnly,
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    init_tracing(args.trace_filter.as_deref())?;

    match &args.command {
        Command::CleanupStaleSegments => cleanup_stale_segments(&args),
        Command::Policy { command } => run_policy_command(&args, command),
    }
}

fn run_policy_command(args: &Args, command: &PolicyCommand) -> Result<(), Box<dyn Error>> {
    let backend = metadata_backend(args)?;
    match command {
        PolicyCommand::Get { tenant } => get_policy(backend.as_ref(), args, tenant.as_deref()),
        PolicyCommand::Set {
            tenant,
            route_topk,
            route_control,
            created_by,
        } => set_policy(
            backend.as_ref(),
            args,
            tenant,
            *route_topk,
            (*route_control).into(),
            created_by,
        ),
        PolicyCommand::Delete { tenant } => delete_policy(backend.as_ref(), args, tenant),
        PolicyCommand::List => list_policies(backend.as_ref(), args),
    }
}

fn metadata_backend(args: &Args) -> Result<std::sync::Arc<dyn MetadataBackend>, Box<dyn Error>> {
    let keyspace = args
        .keyspace
        .clone()
        .map(MetadataKeyspace::new)
        .unwrap_or_default();
    let (backend, _) = build_metadata_backend(&args.metadata_url, None, keyspace)?;
    Ok(backend)
}

fn route_policy_domain(tenant: Option<&str>) -> RoutePolicyDomain {
    tenant
        .map(|tenant| RoutePolicyDomain::Tenant(tenant.to_string()))
        .unwrap_or(RoutePolicyDomain::Default)
}

fn format_route_policy_domain(domain: &RoutePolicyDomain) -> String {
    match domain {
        RoutePolicyDomain::Default => "default".to_string(),
        RoutePolicyDomain::Tenant(tenant) => format!("tenant:{tenant}"),
    }
}

fn get_policy(
    backend: &dyn MetadataBackend,
    args: &Args,
    tenant: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let domain = route_policy_domain(tenant);
    let policy = backend.get_route_policy(&domain)?;
    println!("route policy:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  domain: {}", format_route_policy_domain(&domain));
    match policy {
        Some(policy) => print_policy(&policy),
        None => println!("  status: not found"),
    }
    Ok(())
}

fn set_policy(
    backend: &dyn MetadataBackend,
    args: &Args,
    tenant: &str,
    route_topk: u32,
    route_control: RouteControlMode,
    created_by: &str,
) -> Result<(), Box<dyn Error>> {
    if route_topk < 2 {
        return Err("route_topk must be greater than or equal to 2".into());
    }
    let domain = RoutePolicyDomain::Tenant(tenant.to_string());
    let policy = RoutePolicy {
        route_topk,
        route_control,
        created_by: ClientRuntimeId::new(created_by, ClientEpoch(0)),
        created_at_ms: now_ms(),
    };
    backend.put_route_policy(&domain, &policy)?;
    println!("route policy updated:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  domain: {}", format_route_policy_domain(&domain));
    print_policy(&policy);
    Ok(())
}

fn delete_policy(
    backend: &dyn MetadataBackend,
    args: &Args,
    tenant: &str,
) -> Result<(), Box<dyn Error>> {
    let domain = RoutePolicyDomain::Tenant(tenant.to_string());
    let removed = backend.delete_route_policy(&domain)?;
    println!("route policy delete:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  domain: {}", format_route_policy_domain(&domain));
    println!("  removed: {}", removed);
    Ok(())
}

fn list_policies(backend: &dyn MetadataBackend, args: &Args) -> Result<(), Box<dyn Error>> {
    let policies = backend.list_route_policies()?;
    println!("route policies:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  count: {}", policies.len());
    for (domain, policy) in policies {
        println!("  - domain: {}", format_route_policy_domain(&domain));
        println!("    route_control: {:?}", policy.route_control);
        println!("    route_topk: {}", policy.route_topk);
        println!("    created_by: {}", policy.created_by);
        println!("    created_at_ms: {}", policy.created_at_ms);
    }
    Ok(())
}

fn print_policy(policy: &RoutePolicy) {
    println!("  route_control: {:?}", policy.route_control);
    println!("  route_topk: {}", policy.route_topk);
    println!("  created_by: {}", policy.created_by);
    println!("  created_at_ms: {}", policy.created_at_ms);
}

fn current_keyspace(args: &Args) -> MetadataKeyspace {
    args.keyspace
        .clone()
        .map(MetadataKeyspace::new)
        .unwrap_or_default()
}

fn cleanup_stale_segments(args: &Args) -> Result<(), Box<dyn Error>> {
    if !args.metadata_url.starts_with("redis://") {
        return Err("cleanup-stale-segments currently supports redis:// metadata only".into());
    }

    let keyspace = current_keyspace(args);
    let backend = RedisMetadataBackend::new(
        RedisMetadataConfig::new(args.metadata_url.clone()).keyspace(keyspace.clone()),
    )?;
    let report = backend.cleanup_stale_segments()?;

    println!("cleanup stale segments:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", keyspace.prefix());
    println!("  live_clients: {}", report.live_clients);
    println!(
        "  inspected_segment_keys: {}",
        report.inspected_segment_keys
    );
    println!("  removed_segment_keys: {}", report.removed_segment_keys);
    println!(
        "  removed_segment_index_entries: {}",
        report.removed_segment_index_entries
    );
    println!(
        "  removed_owner_segment_index_entries: {}",
        report.removed_owner_segment_index_entries
    );
    println!(
        "  stale_missing_segment_index_entries: {}",
        report.stale_missing_segment_index_entries
    );
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn redact_redis_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.to_string()
        }
        Err(_) => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn route_control_arg_maps_to_runtime_mode() {
        assert_eq!(
            RouteControlMode::from(RouteControlArg::EmbeddedWrh),
            RouteControlMode::EmbeddedWrh
        );
        assert_eq!(
            RouteControlMode::from(RouteControlArg::MetadataOnly),
            RouteControlMode::MetadataOnly
        );
    }

    #[test]
    fn policy_set_parses_expected_flags() {
        let args = Args::parse_from([
            "mooncake-store-admin",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "policy",
            "set",
            "--tenant",
            "tenant-a",
            "--route-topk",
            "3",
            "--route-control",
            "embedded-wrh",
        ]);
        match args.command {
            Command::Policy {
                command:
                    PolicyCommand::Set {
                        tenant,
                        route_topk,
                        route_control,
                        ..
                    },
            } => {
                assert_eq!(tenant, "tenant-a");
                assert_eq!(route_topk, 3);
                assert_eq!(route_control, RouteControlArg::EmbeddedWrh);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }
}
