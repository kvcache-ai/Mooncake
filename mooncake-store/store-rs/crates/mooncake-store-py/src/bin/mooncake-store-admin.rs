use std::error::Error;
use std::sync::mpsc;

use _store_rs::admin::{
    format_policy_scope, format_route_policy_domain, format_tenant_object_accounting_state,
    format_tenant_quota_reservation_state, redact_redis_url, route_policy_domain,
    AdminHttpServerHandle, AdminService, PolicyPatchInput, TenantQuotaAbortRequest,
    TenantQuotaReconcileRequest,
};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use mooncake_metadata::MetadataKeyspace;
use mooncake_store_client::{init_tracing, RouteControlMode};
use mooncake_store_core::{
    RoutePolicy, TenantPolicy, TenantPolicySpec, TenantQuotaReservationState,
};
use tracing::info;
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
    #[command(alias = "serve")]
    Server(ServerArgs),
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    Quota {
        #[command(subcommand)]
        command: QuotaCommand,
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

#[derive(Subcommand, Debug)]
enum QuotaCommand {
    State {
        #[command(flatten)]
        scope: PolicyScopeArgs,
    },
    Object {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[arg(long)]
        key: String,
    },
    Reservations {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[arg(long, value_enum)]
        state: Option<ReservationStateArg>,
    },
    Abort {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[arg(long)]
        reservation_id: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    Reconcile {
        #[command(flatten)]
        scope: PolicyScopeArgs,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(ClapArgs, Clone, Debug)]
struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    bind_addr: String,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ReservationStateArg {
    Pending,
    Finalized,
    Aborted,
}

impl From<RouteControlArg> for RouteControlMode {
    fn from(value: RouteControlArg) -> Self {
        match value {
            RouteControlArg::EmbeddedWrh => RouteControlMode::EmbeddedWrh,
            RouteControlArg::MetadataOnly => RouteControlMode::MetadataOnly,
        }
    }
}

impl From<&PolicyValueArgs> for PolicyPatchInput {
    fn from(values: &PolicyValueArgs) -> Self {
        Self {
            route_topk: values.route_topk,
            route_control: values.route_control.map(Into::into),
            max_bytes: values.max_bytes,
            max_objects: values.max_objects,
            max_remote_batch_items_per_tenant: values.max_remote_batch_items_per_tenant,
            max_remote_batch_bytes: values.max_remote_batch_bytes,
            max_remote_batch_burst_items: values.max_remote_batch_burst_items,
            max_inflight_bytes_per_batch: values.max_inflight_bytes_per_batch,
            default_replica_count: values.default_replica_count,
            prefer_local: values.prefer_local,
            prefer_alloc_in_same_node: values.prefer_alloc_in_same_node,
            preferred_storage_owners: values.preferred_storage_owners.clone(),
            preferred_segments: values.preferred_segments.clone(),
        }
    }
}

impl From<ReservationStateArg> for TenantQuotaReservationState {
    fn from(value: ReservationStateArg) -> Self {
        match value {
            ReservationStateArg::Pending => TenantQuotaReservationState::Pending,
            ReservationStateArg::Finalized => TenantQuotaReservationState::Finalized,
            ReservationStateArg::Aborted => TenantQuotaReservationState::Aborted,
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    validate_args(&args)?;
    init_tracing(args.trace_filter.as_deref())?;

    match &args.command {
        Command::CleanupStaleSegments => {
            let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
            cleanup_stale_segments(&service)
        }
        Command::Server(server_args) => run_server_command(&args, server_args),
        Command::Policy { command } => {
            let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
            run_policy_command(&service, &args, command)
        }
        Command::Quota { command } => {
            let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
            run_quota_command(&service, &args, command)
        }
    }
}

fn validate_args(args: &Args) -> Result<(), Box<dyn Error>> {
    Url::parse(&args.metadata_url)?;
    Ok(())
}

fn run_server_command(args: &Args, server_args: &ServerArgs) -> Result<(), Box<dyn Error>> {
    let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
    let mut server = AdminHttpServerHandle::start(&server_args.bind_addr, service)?;
    info!(address = %server.address(), "admin http server listening");

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = shutdown_tx.send(());
    })?;
    let _ = shutdown_rx.recv();

    server.shutdown()?;
    Ok(())
}

fn run_policy_command(
    service: &AdminService,
    args: &Args,
    command: &PolicyCommand,
) -> Result<(), Box<dyn Error>> {
    match command {
        PolicyCommand::Get { scope, effective } => get_policy(service, args, scope, *effective),
        PolicyCommand::Set {
            scope,
            values,
            expected_version,
            updated_by,
        } => set_policy(service, args, scope, values, *expected_version, updated_by),
        PolicyCommand::Delete {
            scope,
            expected_version,
        } => delete_policy(service, args, scope, *expected_version),
        PolicyCommand::List { tenant } => list_policies(service, args, tenant.as_deref()),
    }
}

fn run_quota_command(
    service: &AdminService,
    args: &Args,
    command: &QuotaCommand,
) -> Result<(), Box<dyn Error>> {
    match command {
        QuotaCommand::State { scope } => quota_state(service, args, scope),
        QuotaCommand::Object { scope, key } => quota_object(service, args, scope, key),
        QuotaCommand::Reservations { scope, state } => {
            quota_reservations(service, args, scope, state.map(Into::into))
        }
        QuotaCommand::Abort {
            scope,
            reservation_id,
            dry_run,
        } => quota_abort(
            service,
            args,
            scope,
            reservation_id,
            TenantQuotaAbortRequest { dry_run: *dry_run },
        ),
        QuotaCommand::Reconcile { scope, dry_run } => quota_reconcile(
            service,
            args,
            scope,
            TenantQuotaReconcileRequest { dry_run: *dry_run },
        ),
    }
}

fn get_policy(
    service: &AdminService,
    args: &Args,
    scope: &OptionalPolicyScopeArgs,
    effective: bool,
) -> Result<(), Box<dyn Error>> {
    if scope.tenant.is_none() {
        if scope.domain.is_some() || scope.object_set.is_some() || effective {
            return Err("--domain/--object-set/--effective require --tenant".into());
        }
        return get_legacy_route_policy(service, args, None);
    }

    let tenant = scope.tenant.as_deref().expect("checked above");
    let response = service.get_tenant_policy(
        tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        effective,
    )?;
    println!("tenant policy:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&response.scope));
    println!("  effective: {}", response.effective);
    if response.effective {
        match response.effective_spec.as_ref() {
            Some(spec) => print_policy_spec(spec, 2),
            None => println!("  status: not found"),
        }
    } else {
        match response.policy.as_ref() {
            Some(policy) => print_tenant_policy(policy),
            None => println!("  status: not found"),
        }
    }
    Ok(())
}

fn set_policy(
    service: &AdminService,
    args: &Args,
    scope: &PolicyScopeArgs,
    values: &PolicyValueArgs,
    expected_version: Option<u64>,
    updated_by: &str,
) -> Result<(), Box<dyn Error>> {
    let stored = service.set_tenant_policy(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        values.into(),
        expected_version,
        updated_by,
    )?;

    println!("tenant policy updated:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&stored.scope));
    print_tenant_policy(&stored);
    Ok(())
}

fn delete_policy(
    service: &AdminService,
    args: &Args,
    scope: &PolicyScopeArgs,
    expected_version: Option<u64>,
) -> Result<(), Box<dyn Error>> {
    let removed = service.delete_tenant_policy(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        expected_version,
    )?;

    println!("tenant policy delete:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&removed.scope));
    println!("  removed: {}", removed.removed);
    Ok(())
}

fn list_policies(
    service: &AdminService,
    args: &Args,
    tenant: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let policies = service.list_tenant_policies(tenant)?;
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

fn quota_state(
    service: &AdminService,
    args: &Args,
    scope: &PolicyScopeArgs,
) -> Result<(), Box<dyn Error>> {
    let response = service.get_tenant_quota_state(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
    )?;
    println!("tenant quota state:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&response.scope));
    if let Some(state) = response.state.as_ref() {
        println!("  version: {}", state.version);
        println!("  used_bytes: {}", state.used_bytes);
        println!("  used_objects: {}", state.used_objects);
        println!("  pending_reserved_bytes: {}", state.pending_reserved_bytes);
        println!(
            "  pending_reserved_objects: {}",
            state.pending_reserved_objects
        );
        println!("  updated_at_ms: {}", state.updated_at_ms);
        println!("  updated_by: {}", state.updated_by);
    } else {
        println!("  status: not found");
    }
    Ok(())
}

fn quota_object(
    service: &AdminService,
    args: &Args,
    scope: &PolicyScopeArgs,
    key: &str,
) -> Result<(), Box<dyn Error>> {
    let response = service.get_tenant_object_accounting(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        key,
    )?;
    println!("tenant object accounting:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&response.scope));
    println!("  key: {}", response.key);
    if let Some(accounting) = response.accounting.as_ref() {
        println!("  version: {}", accounting.version);
        println!("  committed_length: {}", accounting.committed_length);
        println!(
            "  route_version: {}",
            accounting
                .route_version
                .map(|version| version.0.to_string())
                .unwrap_or_else(|| "none".to_string())
        );
        println!(
            "  state: {}",
            format_tenant_object_accounting_state(accounting.state)
        );
        println!("  last_writer: {}", accounting.last_writer);
        println!("  updated_at_ms: {}", accounting.updated_at_ms);
    } else {
        println!("  status: not found");
    }
    Ok(())
}

fn quota_reservations(
    service: &AdminService,
    args: &Args,
    scope: &PolicyScopeArgs,
    state: Option<TenantQuotaReservationState>,
) -> Result<(), Box<dyn Error>> {
    let response = service.list_tenant_quota_reservations(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        state,
    )?;
    println!("tenant quota reservations:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&response.scope));
    println!("  count: {}", response.count);
    for reservation in response.reservations {
        println!("  - reservation_id: {}", reservation.reservation_id);
        println!("    key: {}", reservation.key.0);
        println!("    version: {}", reservation.version);
        println!(
            "    expected_object_version: {}",
            reservation
                .expected_object_version
                .map(|version| version.to_string())
                .unwrap_or_else(|| "none".to_string())
        );
        println!("    delta_bytes: {}", reservation.delta_bytes);
        println!("    delta_objects: {}", reservation.delta_objects);
        println!(
            "    state: {}",
            format_tenant_quota_reservation_state(reservation.state)
        );
        println!("    expires_at_ms: {}", reservation.expires_at_ms);
        println!("    created_at_ms: {}", reservation.created_at_ms);
        println!("    writer_runtime: {}", reservation.writer_runtime);
    }
    Ok(())
}

fn quota_abort(
    service: &AdminService,
    args: &Args,
    scope: &PolicyScopeArgs,
    reservation_id: &str,
    request: TenantQuotaAbortRequest,
) -> Result<(), Box<dyn Error>> {
    let response = service.abort_tenant_quota_reservation(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        reservation_id,
        request.dry_run,
    )?;
    println!("tenant quota abort:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&response.scope));
    println!("  reservation_id: {}", response.reservation_id);
    println!("  dry_run: {}", response.dry_run);
    println!("  aborted: {}", response.aborted);
    Ok(())
}

fn quota_reconcile(
    service: &AdminService,
    args: &Args,
    scope: &PolicyScopeArgs,
    request: TenantQuotaReconcileRequest,
) -> Result<(), Box<dyn Error>> {
    let report = service.reconcile_tenant_quota_reservations(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        request.dry_run,
    )?;
    println!("tenant quota reconcile:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!("  scope: {}", format_policy_scope(&report.scope));
    println!("  dry_run: {}", report.dry_run);
    println!("  inspected: {}", report.inspected);
    println!("  finalized: {}", report.finalized);
    println!("  aborted: {}", report.aborted);
    println!("  skipped: {}", report.skipped);
    for action in report.actions {
        println!("  - reservation_id: {}", action.reservation_id);
        println!("    key: {}", action.key);
        println!("    action: {}", action.action);
        println!("    reason: {}", action.reason);
    }
    Ok(())
}

fn get_legacy_route_policy(
    service: &AdminService,
    args: &Args,
    tenant: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let response = service.get_route_policy(tenant)?;
    println!("route policy:");
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    println!(
        "  domain: {}",
        format_route_policy_domain(&route_policy_domain(tenant))
    );
    match response.policy.as_ref() {
        Some(policy) => print_route_policy(policy, 2),
        None => println!("  status: not found"),
    }
    Ok(())
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

fn cleanup_stale_segments(service: &AdminService) -> Result<(), Box<dyn Error>> {
    let report = service.cleanup_stale_segments()?;
    println!("cleanup stale segments:");
    println!("  metadata_url: {}", service.redacted_metadata_url());
    println!("  keyspace: {}", service.keyspace().prefix());
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use clap::Parser;
    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{MetadataBackend, TenantQuotaPolicy, TenantRoutePolicy};

    use super::*;

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
    fn server_subcommand_parses_bind_addr() {
        let args = Args::parse_from([
            "mooncake-store-admin",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "server",
            "--bind-addr",
            "127.0.0.1:8080",
        ]);
        match args.command {
            Command::Server(server_args) => {
                assert_eq!(server_args.bind_addr, "127.0.0.1:8080");
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn serve_alias_parses_bind_addr() {
        let args = Args::parse_from([
            "mooncake-store-admin",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "serve",
            "--bind-addr",
            "127.0.0.1:0",
        ]);
        match args.command {
            Command::Server(server_args) => {
                assert_eq!(server_args.bind_addr, "127.0.0.1:0");
            }
            other => panic!("unexpected command: {other:?}"),
        }
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
    fn policy_value_args_convert_to_patch_input() {
        let values = PolicyValueArgs {
            route_topk: Some(3),
            route_control: Some(RouteControlArg::MetadataOnly),
            max_bytes: Some(9),
            ..PolicyValueArgs::default()
        };
        let patch: PolicyPatchInput = (&values).into();
        assert_eq!(patch.route_topk, Some(3));
        assert_eq!(patch.route_control, Some(RouteControlMode::MetadataOnly));
        assert_eq!(patch.max_bytes, Some(9));
    }

    #[test]
    fn quota_reservations_parses_state_filter() {
        let args = Args::parse_from([
            "mooncake-store-admin",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "quota",
            "reservations",
            "--tenant",
            "tenant-a",
            "--state",
            "pending",
        ]);
        match args.command {
            Command::Quota {
                command:
                    QuotaCommand::Reservations {
                        scope,
                        state: Some(state),
                    },
            } => {
                assert_eq!(scope.tenant, "tenant-a");
                assert_eq!(state, ReservationStateArg::Pending);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn quota_abort_parses_reservation_id_and_dry_run() {
        let args = Args::parse_from([
            "mooncake-store-admin",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "quota",
            "abort",
            "--tenant",
            "tenant-a",
            "--reservation-id",
            "res-a",
            "--dry-run",
        ]);
        match args.command {
            Command::Quota {
                command:
                    QuotaCommand::Abort {
                        scope,
                        reservation_id,
                        dry_run,
                    },
            } => {
                assert_eq!(scope.tenant, "tenant-a");
                assert_eq!(reservation_id, "res-a");
                assert!(dry_run);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn quota_reconcile_parses_dry_run() {
        let args = Args::parse_from([
            "mooncake-store-admin",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "quota",
            "reconcile",
            "--tenant",
            "tenant-a",
            "--dry-run",
        ]);
        match args.command {
            Command::Quota {
                command: QuotaCommand::Reconcile { scope, dry_run },
            } => {
                assert_eq!(scope.tenant, "tenant-a");
                assert!(dry_run);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn reservation_state_arg_maps_to_core_state() {
        assert_eq!(
            TenantQuotaReservationState::from(ReservationStateArg::Pending),
            TenantQuotaReservationState::Pending
        );
        assert_eq!(
            TenantQuotaReservationState::from(ReservationStateArg::Finalized),
            TenantQuotaReservationState::Finalized
        );
        assert_eq!(
            TenantQuotaReservationState::from(ReservationStateArg::Aborted),
            TenantQuotaReservationState::Aborted
        );
    }

    #[test]
    fn redact_redis_url_strips_credentials() {
        assert_eq!(
            redact_redis_url("redis://user:pass@127.0.0.1:6379/0"),
            "redis://127.0.0.1:6379/0"
        );
    }

    #[test]
    fn admin_service_round_trips_policy_and_route_mirror() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        let service = AdminService::new(
            backend.clone(),
            "memory://test",
            MetadataKeyspace::default(),
        );
        let stored = service
            .set_tenant_policy(
                "tenant-a",
                None,
                None,
                PolicyPatchInput {
                    route_topk: Some(5),
                    route_control: Some(RouteControlMode::MetadataOnly),
                    max_bytes: Some(64),
                    ..PolicyPatchInput::default()
                },
                None,
                "admin",
            )
            .expect("policy write should succeed");
        assert_eq!(stored.version, 1);
        assert_eq!(
            stored.spec.quota,
            Some(TenantQuotaPolicy {
                max_bytes: Some(64),
                max_objects: None,
            })
        );
        let mirrored = service
            .get_route_policy(Some("tenant-a"))
            .expect("route policy read should succeed");
        assert_eq!(
            mirrored.policy.expect("mirrored route policy").route_topk,
            5
        );
        let effective = service
            .get_tenant_policy("tenant-a", None, None, true)
            .expect("effective policy read should succeed");
        assert!(effective.found);
        assert_eq!(
            effective.effective_spec.expect("effective spec").routing,
            Some(TenantRoutePolicy {
                route_topk: Some(5),
                route_control: Some(RouteControlMode::MetadataOnly),
            })
        );
    }

    #[test]
    fn cleanup_stale_segments_rejects_non_redis_metadata() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        let service = AdminService::new(backend, "memory://test", MetadataKeyspace::default());
        let error = service
            .cleanup_stale_segments()
            .expect_err("non-redis cleanup should fail");
        assert!(matches!(
            error,
            mooncake_store_core::StoreError::Unsupported(_)
        ));
    }
}
