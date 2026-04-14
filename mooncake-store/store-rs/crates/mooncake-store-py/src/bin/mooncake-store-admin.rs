use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use _store_rs::config::build_metadata_backend;
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{init_tracing, RouteControlMode};
use mooncake_store_core::{
    ClientEpoch, ClientRuntimeId, MetadataBackend, NamespaceScope, RoutePolicy,
    RoutePolicyDomain, TenantBandwidthShapingPolicy, TenantExecutionFairnessPolicy,
    TenantPlacementPolicy, TenantPolicy, TenantPolicyScope, TenantPolicySpec, TenantQuotaPolicy,
    TenantRoutePolicy, DEFAULT_DOMAIN, DEFAULT_OBJECT_SET,
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
        #[command(flatten)]
        scope: OptionalPolicyScopeArgs,
        #[arg(long, default_value_t = false)]
        effective: bool,
    },
    Set {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[command(flatten)]
        values: PolicyValueArgs,
        #[arg(long)]
        expected_version: Option<u64>,
        #[arg(long, default_value = "admin")]
        updated_by: String,
    },
    Delete {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[arg(long)]
        expected_version: Option<u64>,
    },
    List {
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(ClapArgs, Clone, Debug)]
struct OptionalPolicyScopeArgs {
    #[arg(long)]
    tenant: Option<String>,
    #[arg(long)]
    domain: Option<String>,
    #[arg(long)]
    object_set: Option<String>,
}

#[derive(ClapArgs, Clone, Debug)]
struct PolicyScopeArgs {
    #[arg(long)]
    tenant: String,
    #[arg(long)]
    domain: Option<String>,
    #[arg(long)]
    object_set: Option<String>,
}

#[derive(ClapArgs, Clone, Debug, Default)]
struct PolicyValueArgs {
    #[arg(long)]
    route_topk: Option<u32>,
    #[arg(long, value_enum)]
    route_control: Option<RouteControlArg>,
    #[arg(long)]
    max_bytes: Option<u64>,
    #[arg(long)]
    max_objects: Option<usize>,
    #[arg(long)]
    max_remote_batch_items_per_tenant: Option<usize>,
    #[arg(long)]
    max_remote_batch_bytes: Option<usize>,
    #[arg(long)]
    max_remote_batch_burst_items: Option<usize>,
    #[arg(long)]
    max_inflight_bytes_per_batch: Option<u64>,
    #[arg(long)]
    default_replica_count: Option<usize>,
    #[arg(long)]
    prefer_local: Option<bool>,
    #[arg(long)]
    prefer_alloc_in_same_node: Option<bool>,
    #[arg(long, value_delimiter = ',')]
    preferred_storage_owners: Option<Vec<String>>,
    #[arg(long, value_delimiter = ',')]
    preferred_segments: Option<Vec<String>>,
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
        PolicyCommand::Get { scope, effective } => {
            get_policy(backend.as_ref(), args, scope, *effective)
        }
        PolicyCommand::Set {
            scope,
            values,
            expected_version,
            updated_by,
        } => set_policy(
            backend.as_ref(),
            args,
            scope,
            values,
            *expected_version,
            updated_by,
        ),
        PolicyCommand::Delete {
            scope,
            expected_version,
        } => delete_policy(backend.as_ref(), args, scope, *expected_version),
        PolicyCommand::List { tenant } => list_policies(backend.as_ref(), args, tenant.as_deref()),
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
    scope: &OptionalPolicyScopeArgs,
    effective: bool,
) -> Result<(), Box<dyn Error>> {
    if scope.tenant.is_none() {
        if scope.domain.is_some() || scope.object_set.is_some() || effective {
            return Err("--domain/--object-set/--effective require --tenant".into());
        }
        return get_legacy_route_policy(backend, args, None);
    }

    let tenant = scope.tenant.as_deref().expect("checked above");
    let policy_scope = tenant_policy_scope(tenant, scope.domain.as_deref(), scope.object_set.as_deref())?;
    println!("tenant policy:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&policy_scope));

    if effective {
        let policies = backend.list_tenant_policies()?;
        let namespace = NamespaceScope::with_defaults(
            Some(tenant),
            scope.domain.as_deref(),
            scope.object_set.as_deref(),
        );
        let matching = policies
            .iter()
            .filter(|policy| policy.scope.matches_namespace(&namespace))
            .collect::<Vec<_>>();
        if matching.is_empty() {
            println!("  status: not found");
            return Ok(());
        }
        let resolved = TenantPolicySpec::resolve_for_scope(matching, &namespace);
        println!("  effective: true");
        print_policy_spec(&resolved, 2);
        return Ok(());
    }

    println!("  effective: false");
    match backend.get_tenant_policy(&policy_scope)? {
        Some(policy) => print_tenant_policy(&policy),
        None => println!("  status: not found"),
    }
    Ok(())
}

fn set_policy(
    backend: &dyn MetadataBackend,
    args: &Args,
    scope: &PolicyScopeArgs,
    values: &PolicyValueArgs,
    expected_version: Option<u64>,
    updated_by: &str,
) -> Result<(), Box<dyn Error>> {
    let scope = tenant_policy_scope(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
    )?;
    let patch = tenant_policy_patch(values)?;
    if policy_patch_is_empty(&patch) {
        return Err("at least one policy flag must be provided".into());
    }

    let current = backend.get_tenant_policy(&scope)?;
    let policy = merge_tenant_policy(current.as_ref(), scope.clone(), patch, updated_by);
    let expected = expected_version.or_else(|| current.as_ref().map(|policy| policy.version));
    let stored = backend.put_tenant_policy(&policy, expected)?;
    sync_legacy_route_policy(backend, &stored)?;

    println!("tenant policy updated:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&scope));
    print_tenant_policy(&stored);
    Ok(())
}

fn delete_policy(
    backend: &dyn MetadataBackend,
    args: &Args,
    scope: &PolicyScopeArgs,
    expected_version: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let scope = tenant_policy_scope(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
    )?;
    let removed = backend.delete_tenant_policy(&scope, expected_version)?;
    if removed && is_root_tenant_scope(&scope) {
        backend.delete_route_policy(&RoutePolicyDomain::Tenant(scope.tenant.clone()))?;
    }

    println!("tenant policy delete:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&scope));
    println!("  removed: {}", removed);
    Ok(())
}

fn list_policies(
    backend: &dyn MetadataBackend,
    args: &Args,
    tenant: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let mut policies = backend.list_tenant_policies()?;
    if let Some(tenant) = tenant {
        policies.retain(|policy| policy.scope.tenant == tenant);
    }
    policies.sort_by(|left, right| left.scope.cmp(&right.scope));

    println!("tenant policies:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  count: {}", policies.len());
    for policy in policies {
        println!("  - scope: {}", format_policy_scope(&policy.scope));
        println!("    version: {}", policy.version);
        println!("    updated_at_ms: {}", policy.updated_at_ms);
        println!("    updated_by: {}", policy.updated_by);
        print_policy_spec(&policy.spec, 4);
    }
    Ok(())
}

fn get_legacy_route_policy(
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
        Some(policy) => print_route_policy(&policy, 2),
        None => println!("  status: not found"),
    }
    Ok(())
}

fn tenant_policy_scope(
    tenant: &str,
    domain: Option<&str>,
    object_set: Option<&str>,
) -> Result<TenantPolicyScope, Box<dyn Error>> {
    let scope = TenantPolicyScope::new(tenant, domain, object_set);
    scope.validate()?;
    Ok(scope)
}

fn tenant_policy_patch(values: &PolicyValueArgs) -> Result<TenantPolicySpec, Box<dyn Error>> {
    if let Some(route_topk) = values.route_topk {
        if route_topk < 2 {
            return Err("route_topk must be greater than or equal to 2".into());
        }
    }

    Ok(TenantPolicySpec {
        routing: Some(TenantRoutePolicy {
            route_topk: values.route_topk,
            route_control: values.route_control.map(Into::into),
        })
        .filter(|policy| policy.route_topk.is_some() || policy.route_control.is_some()),
        quota: Some(TenantQuotaPolicy {
            max_bytes: values.max_bytes,
            max_objects: values.max_objects,
        })
        .filter(|policy| policy.max_bytes.is_some() || policy.max_objects.is_some()),
        fairness: Some(TenantExecutionFairnessPolicy {
            max_remote_batch_items_per_tenant: values.max_remote_batch_items_per_tenant,
        })
        .filter(|policy| policy.max_remote_batch_items_per_tenant.is_some()),
        shaping: Some(TenantBandwidthShapingPolicy {
            max_remote_batch_bytes: values.max_remote_batch_bytes,
            max_remote_batch_burst_items: values.max_remote_batch_burst_items,
            max_inflight_bytes_per_batch: values.max_inflight_bytes_per_batch,
        })
        .filter(|policy| {
            policy.max_remote_batch_bytes.is_some()
                || policy.max_remote_batch_burst_items.is_some()
                || policy.max_inflight_bytes_per_batch.is_some()
        }),
        placement: Some(TenantPlacementPolicy {
            default_replica_count: values.default_replica_count,
            prefer_local: values.prefer_local,
            prefer_alloc_in_same_node: values.prefer_alloc_in_same_node,
            preferred_storage_owners: values.preferred_storage_owners.clone(),
            preferred_segments: values.preferred_segments.clone(),
        })
        .filter(|policy| {
            policy.default_replica_count.is_some()
                || policy.prefer_local.is_some()
                || policy.prefer_alloc_in_same_node.is_some()
                || policy.preferred_storage_owners.is_some()
                || policy.preferred_segments.is_some()
        }),
    })
}

fn policy_patch_is_empty(spec: &TenantPolicySpec) -> bool {
    spec.routing.is_none()
        && spec.quota.is_none()
        && spec.fairness.is_none()
        && spec.shaping.is_none()
        && spec.placement.is_none()
}

fn merge_tenant_policy(
    current: Option<&TenantPolicy>,
    scope: TenantPolicyScope,
    patch: TenantPolicySpec,
    updated_by: &str,
) -> TenantPolicy {
    let spec = current
        .map(|policy| policy.spec.merged_with(&patch))
        .unwrap_or(patch);
    TenantPolicy {
        scope,
        spec,
        version: current.map(|policy| policy.version.saturating_add(1)).unwrap_or(1),
        updated_at_ms: now_ms(),
        updated_by: updated_by.to_string(),
    }
}

fn sync_legacy_route_policy(
    backend: &dyn MetadataBackend,
    policy: &TenantPolicy,
) -> Result<(), Box<dyn Error>> {
    if !is_root_tenant_scope(&policy.scope) {
        return Ok(());
    }
    let domain = RoutePolicyDomain::Tenant(policy.scope.tenant.clone());
    if let Some(routing) = policy.spec.routing.as_ref() {
        if let Some(route_policy) = route_policy_from_tenant_policy(policy, routing) {
            backend.put_route_policy(&domain, &route_policy)?;
            return Ok(());
        }
    }
    backend.delete_route_policy(&domain)?;
    Ok(())
}

fn route_policy_from_tenant_policy(
    policy: &TenantPolicy,
    routing: &TenantRoutePolicy,
) -> Option<RoutePolicy> {
    Some(RoutePolicy {
        route_topk: routing.route_topk?,
        route_control: routing.route_control?,
        created_by: ClientRuntimeId::new(policy.updated_by.clone(), ClientEpoch(0)),
        created_at_ms: policy.updated_at_ms,
    })
}

fn is_root_tenant_scope(scope: &TenantPolicyScope) -> bool {
    scope.domain.is_none() && scope.object_set.is_none()
}

fn format_policy_scope(scope: &TenantPolicyScope) -> String {
    let mut formatted = format!("tenant={}", scope.tenant);
    if let Some(domain) = scope.domain.as_deref() {
        formatted.push_str(&format!(", domain={domain}"));
    }
    if let Some(object_set) = scope.object_set.as_deref() {
        formatted.push_str(&format!(", object_set={object_set}"));
    }
    formatted
}

fn print_tenant_policy(policy: &TenantPolicy) {
    println!("  version: {}", policy.version);
    println!("  updated_at_ms: {}", policy.updated_at_ms);
    println!("  updated_by: {}", policy.updated_by);
    print_policy_spec(&policy.spec, 2);
}

fn print_policy_spec(spec: &TenantPolicySpec, indent: usize) {
    let pad = " ".repeat(indent);
    if let Some(routing) = spec.routing.as_ref() {
        println!("{pad}routing:");
        if let Some(route_control) = routing.route_control {
            println!("{pad}  route_control: {:?}", route_control);
        }
        if let Some(route_topk) = routing.route_topk {
            println!("{pad}  route_topk: {}", route_topk);
        }
    }
    if let Some(quota) = spec.quota.as_ref() {
        println!("{pad}quota:");
        if let Some(max_bytes) = quota.max_bytes {
            println!("{pad}  max_bytes: {}", max_bytes);
        }
        if let Some(max_objects) = quota.max_objects {
            println!("{pad}  max_objects: {}", max_objects);
        }
    }
    if let Some(fairness) = spec.fairness.as_ref() {
        println!("{pad}fairness:");
        if let Some(limit) = fairness.max_remote_batch_items_per_tenant {
            println!("{pad}  max_remote_batch_items_per_tenant: {}", limit);
        }
    }
    if let Some(shaping) = spec.shaping.as_ref() {
        println!("{pad}shaping:");
        if let Some(max_bytes) = shaping.max_remote_batch_bytes {
            println!("{pad}  max_remote_batch_bytes: {}", max_bytes);
        }
        if let Some(max_items) = shaping.max_remote_batch_burst_items {
            println!("{pad}  max_remote_batch_burst_items: {}", max_items);
        }
        if let Some(max_bytes) = shaping.max_inflight_bytes_per_batch {
            println!("{pad}  max_inflight_bytes_per_batch: {}", max_bytes);
        }
    }
    if let Some(placement) = spec.placement.as_ref() {
        println!("{pad}placement:");
        if let Some(replica_count) = placement.default_replica_count {
            println!("{pad}  default_replica_count: {}", replica_count);
        }
        if let Some(prefer_local) = placement.prefer_local {
            println!("{pad}  prefer_local: {}", prefer_local);
        }
        if let Some(prefer_same_node) = placement.prefer_alloc_in_same_node {
            println!("{pad}  prefer_alloc_in_same_node: {}", prefer_same_node);
        }
        if let Some(owners) = placement.preferred_storage_owners.as_ref() {
            println!("{pad}  preferred_storage_owners: {}", owners.join(","));
        }
        if let Some(segments) = placement.preferred_segments.as_ref() {
            println!("{pad}  preferred_segments: {}", segments.join(","));
        }
    }
}

fn print_route_policy(policy: &RoutePolicy, indent: usize) {
    let pad = " ".repeat(indent);
    println!("{pad}route_control: {:?}", policy.route_control);
    println!("{pad}route_topk: {}", policy.route_topk);
    println!("{pad}created_by: {}", policy.created_by);
    println!("{pad}created_at_ms: {}", policy.created_at_ms);
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
    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{MetadataBackend, TenantQuotaPolicy};

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
            "--domain",
            "domain-a",
            "--route-topk",
            "3",
            "--route-control",
            "embedded-wrh",
            "--max-bytes",
            "1024",
            "--prefer-local",
            "true",
        ]);
        match args.command {
            Command::Policy {
                command:
                    PolicyCommand::Set {
                        scope,
                        values,
                        updated_by,
                        ..
                    },
            } => {
                assert_eq!(scope.tenant, "tenant-a");
                assert_eq!(scope.domain.as_deref(), Some("domain-a"));
                assert_eq!(values.route_topk, Some(3));
                assert_eq!(values.route_control, Some(RouteControlArg::EmbeddedWrh));
                assert_eq!(values.max_bytes, Some(1024));
                assert_eq!(values.prefer_local, Some(true));
                assert_eq!(updated_by, "admin");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn merge_tenant_policy_preserves_unset_sections() {
        let current = TenantPolicy {
            scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
            spec: TenantPolicySpec {
                routing: Some(TenantRoutePolicy {
                    route_topk: Some(4),
                    route_control: Some(RouteControlMode::EmbeddedWrh),
                }),
                quota: Some(TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(8),
                }),
                ..TenantPolicySpec::default()
            },
            version: 7,
            updated_at_ms: 1,
            updated_by: "old-admin".to_string(),
        };
        let patch = TenantPolicySpec {
            quota: Some(TenantQuotaPolicy {
                max_bytes: Some(128),
                max_objects: None,
            }),
            ..TenantPolicySpec::default()
        };

        let merged = merge_tenant_policy(
            Some(&current),
            current.scope.clone(),
            patch,
            "new-admin",
        );
        assert_eq!(merged.version, 8);
        assert_eq!(merged.updated_by, "new-admin");
        assert_eq!(
            merged.spec.routing,
            Some(TenantRoutePolicy {
                route_topk: Some(4),
                route_control: Some(RouteControlMode::EmbeddedWrh),
            })
        );
        assert_eq!(
            merged.spec.quota,
            Some(TenantQuotaPolicy {
                max_bytes: Some(128),
                max_objects: Some(8),
            })
        );
    }

    #[test]
    fn sync_legacy_route_policy_mirrors_root_tenant_routing() {
        let backend = InMemoryMetadataBackend::new();
        let policy = TenantPolicy {
            scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
            spec: TenantPolicySpec {
                routing: Some(TenantRoutePolicy {
                    route_topk: Some(5),
                    route_control: Some(RouteControlMode::MetadataOnly),
                }),
                ..TenantPolicySpec::default()
            },
            version: 1,
            updated_at_ms: 42,
            updated_by: "admin".to_string(),
        };

        sync_legacy_route_policy(&backend, &policy).expect("legacy sync should succeed");
        let mirrored = backend
            .get_route_policy(&RoutePolicyDomain::Tenant("tenant-a".to_string()))
            .expect("mirrored route policy read should succeed")
            .expect("mirrored route policy should exist");
        assert_eq!(mirrored.route_topk, 5);
        assert_eq!(mirrored.route_control, RouteControlMode::MetadataOnly);
    }

    #[test]
    fn effective_scope_uses_default_domain_and_object_set() {
        let namespace = NamespaceScope::with_defaults(Some("tenant-a"), None, None);
        assert_eq!(namespace.domain, DEFAULT_DOMAIN);
        assert_eq!(namespace.object_set, DEFAULT_OBJECT_SET);
    }
}
