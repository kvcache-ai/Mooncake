use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use _store_rs::admin::{
    format_policy_scope, format_route_policy_domain, format_tenant_object_accounting_state,
    format_tenant_quota_reservation_state, redact_redis_url, route_policy_domain,
    AdminHttpServerHandle, AdminService, ErrorResponse, PolicyPatchInput, RouteMigrationMode,
    RouteMigrationTaskListResponse, RouteMigrationTaskState, RouteMigrationTaskStatusResponse,
    RouteMigrationTaskSubmitRequest, TenantQuotaAbortRequest, TenantQuotaReconcileRequest,
};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use mooncake_metadata::MetadataKeyspace;
use mooncake_store_client::{init_tracing, RouteControlMode};
use mooncake_store_core::{
    RoutePolicy, TenantPolicy, TenantPolicySpec, TenantQuotaReservationState,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::{info, warn};
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "mooncake-store-admin")]
#[command(about = "Run explicit Mooncake store metadata maintenance tasks")]
struct Args {
    #[arg(long)]
    metadata_url: String,
    #[arg(long)]
    admin_url: Option<String>,
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
    Migrate {
        #[command(subcommand)]
        command: MigrateCommand,
    },
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
    },
    Quota {
        #[command(subcommand)]
        command: QuotaCommand,
    },
}

#[derive(ClapArgs, Clone, Debug)]
struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    bind_addr: String,
    #[arg(long, default_value_t = 5_000)]
    cleanup_interval_ms: u64,
    #[arg(long, default_value_t = 128)]
    cleanup_batch_size: usize,
    #[arg(long, default_value_t = 0)]
    quota_reconcile_interval_ms: u64,
    #[arg(long = "quota-reconcile-tenant")]
    quota_reconcile_tenants: Vec<String>,
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

#[derive(Subcommand, Debug)]
enum MigrateCommand {
    Copy {
        #[command(flatten)]
        common: RouteMigrationArgs,
        #[arg(long = "target-segment", required = true)]
        target_segments: Vec<String>,
    },
    Move {
        #[command(flatten)]
        common: RouteMigrationArgs,
        #[arg(long = "target-segment")]
        target_segment: String,
    },
    Task {
        #[command(subcommand)]
        command: MigrateTaskCommand,
    },
}

#[derive(Subcommand, Debug)]
enum MigrateTaskCommand {
    List,
    Get {
        #[arg(long)]
        task_id: String,
    },
}

#[derive(ClapArgs, Clone, Debug)]
struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1:0")]
    bind_addr: String,
    #[arg(long, default_value_t = 5_000)]
    cleanup_interval_ms: u64,
    #[arg(long, default_value_t = 128)]
    cleanup_batch_size: usize,
    #[arg(long, default_value_t = 0)]
    quota_reconcile_interval_ms: u64,
    #[arg(long = "quota-reconcile-tenant")]
    quota_reconcile_tenants: Vec<String>,
}

#[derive(ClapArgs, Clone, Debug)]
struct RouteMigrationArgs {
    #[arg(long)]
    authority: String,
    #[arg(long)]
    tenant: String,
    #[arg(long)]
    domain: Option<String>,
    #[arg(long = "object-set")]
    object_set: Option<String>,
    #[arg(long)]
    key: String,
    #[arg(long = "source-segment")]
    source_segment: String,
    #[arg(long = "task-executor")]
    task_executor: String,
    #[arg(long)]
    max_retries: Option<u32>,
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
    init_tracing(args.trace_filter.as_deref())?;

    match &args.command {
        Command::CleanupStaleSegments => {
            let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
            cleanup_stale_segments(&service)
        }
        Command::Server(server_args) => run_server_command(&args, server_args),
        Command::Migrate { command } => run_migrate_command(&args, command),
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

fn run_server_command(args: &Args, server_args: &ServerArgs) -> Result<(), Box<dyn Error>> {
    let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
    let maintenance_shutdown = Arc::new(AtomicBool::new(false));
    let quota_thread =
        spawn_quota_reconcile_worker(service.clone(), server_args, maintenance_shutdown.clone());
    let mut server = AdminHttpServerHandle::start(&server_args.bind_addr, service)?;
    info!(address = %server.address(), "admin http server listening");

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = shutdown_tx.send(());
    })?;
    let _ = shutdown_rx.recv();

    maintenance_shutdown.store(true, Ordering::Relaxed);
    server.shutdown()?;
    if let Some(thread) = quota_thread {
        thread
            .join()
            .map_err(|_| std::io::Error::other("admin quota reconcile worker panicked"))?;
    }
    Ok(())
}

fn spawn_quota_reconcile_worker(
    service: AdminService,
    server_args: &ServerArgs,
    shutdown: Arc<AtomicBool>,
) -> Option<JoinHandle<()>> {
    if server_args.quota_reconcile_interval_ms == 0 {
        info!("admin tenant quota reconcile disabled");
        return None;
    }
    if server_args.quota_reconcile_tenants.is_empty() {
        info!("admin tenant quota reconcile disabled because no tenants were configured");
        return None;
    }
    let interval = Duration::from_millis(server_args.quota_reconcile_interval_ms);
    let tenants = server_args.quota_reconcile_tenants.clone();
    Some(
        thread::Builder::new()
            .name("mooncake-store-admin-quota-reconcile".to_string())
            .spawn(move || run_quota_reconcile_loop(service, interval, tenants, shutdown))
            .expect("admin quota reconcile worker thread should spawn"),
    )
}

fn run_quota_reconcile_loop(
    service: AdminService,
    interval: Duration,
    tenants: Vec<String>,
    shutdown: Arc<AtomicBool>,
) {
    info!(
        interval_ms = interval.as_millis() as u64,
        tenants = %tenants.join(","),
        "admin tenant quota reconcile worker started"
    );
    while !shutdown.load(Ordering::Relaxed) {
        for tenant in &tenants {
            match service.reconcile_tenant_quota_reservations(tenant, None, None, false) {
                Ok(report) => {
                    if report.inspected > 0
                        && (report.finalized > 0 || report.aborted > 0 || report.skipped > 0)
                    {
                        info!(
                            tenant,
                            inspected = report.inspected,
                            finalized = report.finalized,
                            aborted = report.aborted,
                            skipped = report.skipped,
                            "admin tenant quota reconcile batch finished"
                        );
                    }
                }
                Err(error) => {
                    warn!(tenant, error = %error, "admin tenant quota reconcile batch failed");
                }
            }
        }
        thread::sleep(interval);
    }
    info!("admin tenant quota reconcile worker stopped");
}

fn run_migrate_command(args: &Args, command: &MigrateCommand) -> Result<(), Box<dyn Error>> {
    match command {
        MigrateCommand::Copy {
            common,
            target_segments,
        } => submit_route_migration(
            args,
            RouteMigrationMode::Copy,
            RouteMigrationTaskSubmitRequest {
                authority: common.authority.clone(),
                tenant: common.tenant.clone(),
                domain: common.domain.clone(),
                object_set: common.object_set.clone(),
                key: common.key.clone(),
                source_segment: common.source_segment.clone(),
                target_segments: target_segments.clone(),
                task_executor: common.task_executor.clone(),
                max_retries: common.max_retries,
            },
        ),
        MigrateCommand::Move {
            common,
            target_segment,
        } => submit_route_migration(
            args,
            RouteMigrationMode::Move,
            RouteMigrationTaskSubmitRequest {
                authority: common.authority.clone(),
                tenant: common.tenant.clone(),
                domain: common.domain.clone(),
                object_set: common.object_set.clone(),
                key: common.key.clone(),
                source_segment: common.source_segment.clone(),
                target_segments: vec![target_segment.clone()],
                task_executor: common.task_executor.clone(),
                max_retries: common.max_retries,
            },
        ),
        MigrateCommand::Task { command } => run_migrate_task_command(args, command),
    }
}

fn run_server_command(args: &Args, server_args: &ServerArgs) -> Result<(), Box<dyn Error>> {
    let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
    let maintenance_shutdown = Arc::new(AtomicBool::new(false));
    let maintenance_thread = spawn_stale_segment_maintenance_worker(
        service.clone(),
        server_args,
        maintenance_shutdown.clone(),
    );
    let quota_thread =
        spawn_quota_reconcile_worker(service.clone(), server_args, maintenance_shutdown.clone());
    let mut server = AdminHttpServerHandle::start(&server_args.bind_addr, service)?;
    info!(address = %server.address(), "admin http server listening");

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = shutdown_tx.send(());
    })?;
    let _ = shutdown_rx.recv();

    maintenance_shutdown.store(true, Ordering::Relaxed);
    server.shutdown()?;
    if let Some(thread) = maintenance_thread {
        thread
            .join()
            .map_err(|_| std::io::Error::other("admin maintenance worker panicked"))?;
    }
    if let Some(thread) = quota_thread {
        thread
            .join()
            .map_err(|_| std::io::Error::other("admin quota reconcile worker panicked"))?;
    }
    Ok(())
}

fn run_migrate_task_command(
    args: &Args,
    command: &MigrateTaskCommand,
) -> Result<(), Box<dyn Error>> {
    match command {
        MigrateTaskCommand::List => {
            let response: RouteMigrationTaskListResponse =
                admin_http_get_json(args, "/v1/route-migrations")?;
            println!("route migration tasks:");
            println!("  admin_url: {}", admin_base_url(args)?);
            println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
            println!("  keyspace: {}", current_keyspace(args).prefix());
            println!("  count: {}", response.count);
            for task in response.tasks {
                print_route_migration_task(&task, 2);
            }
            Ok(())
        }
        MigrateTaskCommand::Get { task_id } => {
            let response: RouteMigrationTaskStatusResponse =
                admin_http_get_json(args, &format!("/v1/route-migrations/{task_id}"))?;
            println!("route migration task:");
            println!("  admin_url: {}", admin_base_url(args)?);
            println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
            println!("  keyspace: {}", current_keyspace(args).prefix());
            print_route_migration_task(&response, 2);
            Ok(())
        }
    }
}

fn spawn_stale_segment_maintenance_worker(
    service: AdminService,
    server_args: &ServerArgs,
    shutdown: Arc<AtomicBool>,
) -> Option<JoinHandle<()>> {
    if !service.supports_stale_segment_maintenance() {
        info!("admin stale segment maintenance disabled for unsupported metadata backend");
        return None;
    }
    if server_args.cleanup_interval_ms == 0 {
        info!("admin stale segment maintenance disabled");
        return None;
    }
    let interval = Duration::from_millis(server_args.cleanup_interval_ms);
    let batch_size = server_args.cleanup_batch_size.max(1);
    Some(
        thread::Builder::new()
            .name("mooncake-store-admin-stale-maintenance".to_string())
            .spawn(move || run_maintenance_loop(service, interval, batch_size, shutdown))
            .expect("admin maintenance worker thread should spawn"),
    )
}

fn spawn_quota_reconcile_worker(
    service: AdminService,
    server_args: &ServerArgs,
    shutdown: Arc<AtomicBool>,
) -> Option<JoinHandle<()>> {
    if server_args.quota_reconcile_interval_ms == 0 {
        info!("admin tenant quota reconcile disabled");
        return None;
    }
    if server_args.quota_reconcile_tenants.is_empty() {
        info!("admin tenant quota reconcile disabled because no tenants were configured");
        return None;
    }
    let interval = Duration::from_millis(server_args.quota_reconcile_interval_ms);
    let tenants = server_args.quota_reconcile_tenants.clone();
    Some(
        thread::Builder::new()
            .name("mooncake-store-admin-quota-reconcile".to_string())
            .spawn(move || run_quota_reconcile_loop(service, interval, tenants, shutdown))
            .expect("admin quota reconcile worker thread should spawn"),
    )
}

fn run_maintenance_loop(
    service: AdminService,
    interval: Duration,
    batch_size: usize,
    shutdown: Arc<AtomicBool>,
) {
    info!(
        interval_ms = interval.as_millis() as u64,
        batch_size, "admin stale segment maintenance worker started"
    );
    while !shutdown.load(Ordering::Relaxed) {
        match service.reconcile_due_stale_segments(batch_size) {
            Ok(report) => {
                if report.due_entries > 0
                    || report.invalid_entries > 0
                    || report.cleaned_missing_lease > 0
                    || report.cleaned_expired_lease > 0
                {
                    info!(
                        due_entries = report.due_entries,
                        invalid_entries = report.invalid_entries,
                        cleaned_missing_lease = report.cleaned_missing_lease,
                        cleaned_expired_lease = report.cleaned_expired_lease,
                        skipped_live = report.skipped_live,
                        removed_segment_keys = report.removed_segment_keys,
                        removed_segment_index_entries = report.removed_segment_index_entries,
                        removed_owner_segment_index_entries =
                            report.removed_owner_segment_index_entries,
                        stale_missing_segment_index_entries =
                            report.stale_missing_segment_index_entries,
                        "admin stale segment maintenance batch finished"
                    );
                }
            }
            Err(error) => {
                warn!(error = %error, "admin stale segment maintenance batch failed");
            }
        }
        thread::sleep(interval);
    }
    info!("admin stale segment maintenance worker stopped");
}

fn run_quota_reconcile_loop(
    service: AdminService,
    interval: Duration,
    tenants: Vec<String>,
    shutdown: Arc<AtomicBool>,
) {
    info!(
        interval_ms = interval.as_millis() as u64,
        tenants = %tenants.join(","),
        "admin tenant quota reconcile worker started"
    );
    while !shutdown.load(Ordering::Relaxed) {
        for tenant in &tenants {
            match service.reconcile_tenant_quota_reservations(tenant, None, None, false) {
                Ok(report) => {
                    if report.inspected > 0
                        && (report.finalized > 0 || report.aborted > 0 || report.skipped > 0)
                    {
                        info!(
                            tenant,
                            inspected = report.inspected,
                            finalized = report.finalized,
                            aborted = report.aborted,
                            skipped = report.skipped,
                            "admin tenant quota reconcile batch finished"
                        );
                    }
                }
                Err(error) => {
                    warn!(tenant, error = %error, "admin tenant quota reconcile batch failed");
                }
            }
        }
        thread::sleep(interval);
    }
    info!("admin tenant quota reconcile worker stopped");
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

fn submit_route_migration(
    args: &Args,
    mode: RouteMigrationMode,
    request: RouteMigrationTaskSubmitRequest,
) -> Result<(), Box<dyn Error>> {
    let path = route_migration_submit_path(mode);
    let response: RouteMigrationTaskStatusResponse = admin_http_post_json(args, path, &request)?;
    println!("route migration task submitted:");
    println!("  admin_url: {}", admin_base_url(args)?);
    println!("  metadata_url: {}", redact_redis_url(&args.metadata_url));
    println!("  keyspace: {}", current_keyspace(args).prefix());
    print_route_migration_task(&response, 2);
    Ok(())
}

fn route_migration_submit_path(mode: RouteMigrationMode) -> &'static str {
    match mode {
        RouteMigrationMode::Copy => "/v1/route-migrations/copy",
        RouteMigrationMode::Move => "/v1/route-migrations/move",
    }
}

fn admin_base_url(args: &Args) -> Result<Url, Box<dyn Error>> {
    let raw = args
        .admin_url
        .as_ref()
        .ok_or("--admin-url is required for migrate commands because route migration tasks live in the long-lived admin server")?;
    let url = Url::parse(raw)?;
    match url.scheme() {
        "http" => {}
        other => {
            return Err(
                format!("unsupported admin_url scheme {other:?}; only http is supported").into(),
            )
        }
    }
    if url.host_str().is_none() || url.port_or_known_default().is_none() {
        return Err(format!("admin_url {raw:?} is missing host or port").into());
    }
    Ok(url)
}

fn admin_http_get_json<T>(args: &Args, path: &str) -> Result<T, Box<dyn Error>>
where
    T: DeserializeOwned,
{
    admin_http_json_request::<(), T>(args, "GET", path, None)
}

fn admin_http_post_json<P, T>(args: &Args, path: &str, payload: &P) -> Result<T, Box<dyn Error>>
where
    P: Serialize,
    T: DeserializeOwned,
{
    admin_http_json_request(args, "POST", path, Some(payload))
}

fn admin_http_json_request<P, T>(
    args: &Args,
    method: &str,
    path: &str,
    payload: Option<&P>,
) -> Result<T, Box<dyn Error>>
where
    P: Serialize,
    T: DeserializeOwned,
{
    let base = admin_base_url(args)?;
    let host = base
        .host_str()
        .ok_or("admin_url is missing host")?
        .to_string();
    let port = base
        .port_or_known_default()
        .ok_or("admin_url is missing port")?;
    let base_path = base.path().trim_end_matches('/');
    let full_path = if base_path.is_empty() || base_path == "/" {
        path.to_string()
    } else {
        format!("{base_path}{path}")
    };
    let body = payload
        .map(serde_json::to_vec)
        .transpose()?
        .unwrap_or_default();
    let mut request = format!(
        "{method} {full_path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\nAccept: application/json\r\n"
    );
    if !body.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");

    let mut stream = TcpStream::connect((host.as_str(), port))?;
    stream.write_all(request.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(&body)?;
    }
    stream.flush()?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (status_line, response_body) = split_http_response(&response)?;
    if status_line.contains(" 200 ") {
        return Ok(serde_json::from_str(response_body)?);
    }

    let message = serde_json::from_str::<ErrorResponse>(response_body)
        .map(|error| error.error)
        .unwrap_or_else(|_| response_body.trim().to_string());
    Err(format!("admin http request failed: {status_line}: {message}").into())
}

fn split_http_response(response: &str) -> Result<(&str, &str), Box<dyn Error>> {
    let mut lines = response.lines();
    let status_line = lines
        .next()
        .ok_or("admin http response is missing status line")?;
    let separator = response
        .find("\r\n\r\n")
        .ok_or("admin http response is missing header terminator")?;
    Ok((status_line, &response[separator + 4..]))
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

fn print_route_migration_task(task: &RouteMigrationTaskStatusResponse, indent: usize) {
    let pad = " ".repeat(indent);
    println!("{pad}task_id: {}", task.task_id);
    println!("{pad}namespace: {}", task.namespace);
    println!("{pad}authority: {}", task.authority);
    println!("{pad}tenant: {}", task.tenant);
    println!(
        "{pad}domain: {}",
        task.domain.as_deref().unwrap_or("default")
    );
    println!(
        "{pad}object_set: {}",
        task.object_set.as_deref().unwrap_or("default")
    );
    println!("{pad}key: {}", task.key);
    println!(
        "{pad}mode: {}",
        match task.mode {
            RouteMigrationMode::Copy => "copy",
            RouteMigrationMode::Move => "move",
        }
    );
    println!("{pad}source_segment: {}", task.source_segment);
    println!("{pad}target_segments: {}", task.target_segments.join(","));
    println!("{pad}task_executor: {}", task.task_executor);
    println!(
        "{pad}state: {}",
        match task.state {
            RouteMigrationTaskState::Pending => "pending",
            RouteMigrationTaskState::Dispatching => "dispatching",
            RouteMigrationTaskState::Running => "running",
            RouteMigrationTaskState::RetryWait => "retry_wait",
            RouteMigrationTaskState::Succeeded => "succeeded",
            RouteMigrationTaskState::Failed => "failed",
            RouteMigrationTaskState::Cancelled => "cancelled",
        }
    );
    println!("{pad}attempts: {}", task.attempts);
    println!("{pad}max_retries: {}", task.max_retries);
    println!(
        "{pad}execution_id: {}",
        task.execution_id.as_deref().unwrap_or("none")
    );
    println!(
        "{pad}next_retry_at_ms: {}",
        task.next_retry_at_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    );
    println!(
        "{pad}last_error: {}",
        if task.last_error.is_empty() {
            "none"
        } else {
            task.last_error.as_str()
        }
    );
    println!("{pad}created_at_ms: {}", task.created_at_ms);
    println!("{pad}updated_at_ms: {}", task.updated_at_ms);
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

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
                assert_eq!(server_args.cleanup_interval_ms, 5_000);
                assert_eq!(server_args.cleanup_batch_size, 128);
                assert_eq!(server_args.quota_reconcile_interval_ms, 0);
                assert!(server_args.quota_reconcile_tenants.is_empty());
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
                assert_eq!(server_args.cleanup_interval_ms, 5_000);
                assert_eq!(server_args.cleanup_batch_size, 128);
                assert_eq!(server_args.quota_reconcile_interval_ms, 0);
                assert!(server_args.quota_reconcile_tenants.is_empty());
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn server_subcommand_parses_cleanup_flags() {
        let args = Args::parse_from([
            "mooncake-store-admin",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "server",
            "--cleanup-interval-ms",
            "2000",
            "--cleanup-batch-size",
            "64",
            "--quota-reconcile-interval-ms",
            "3000",
            "--quota-reconcile-tenant",
            "tenant-a",
            "--quota-reconcile-tenant",
            "tenant-b",
        ]);
        match args.command {
            Command::Server(server_args) => {
                assert_eq!(server_args.cleanup_interval_ms, 2_000);
                assert_eq!(server_args.cleanup_batch_size, 64);
                assert_eq!(server_args.quota_reconcile_interval_ms, 3_000);
                assert_eq!(
                    server_args.quota_reconcile_tenants,
                    vec!["tenant-a".to_string(), "tenant-b".to_string()]
                );
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

    fn sample_cli_args_with_admin_url(admin_url: &str) -> Args {
        Args {
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            admin_url: Some(admin_url.to_string()),
            keyspace: None,
            trace_filter: None,
            command: Command::CleanupStaleSegments,
        }
    }

    fn serve_single_response(
        response: String,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should expose address");
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("http client should connect");
            stream
                .set_read_timeout(Some(Duration::from_millis(250)))
                .expect("read timeout should set");
            let mut buffer = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let read = stream.read(&mut chunk).expect("request should read");
                if read == 0 {
                    break;
                }
                buffer.extend_from_slice(&chunk[..read]);
                let request = String::from_utf8_lossy(&buffer);
                let Some(header_end) = request.find("\r\n\r\n") else {
                    continue;
                };
                let body = &buffer[header_end + 4..];
                let content_length = request
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("Content-Length: ")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if body.len() >= content_length {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&buffer).into_owned();
            tx.send(request).expect("request should send");
            stream
                .write_all(response.as_bytes())
                .expect("response should write");
            stream.flush().expect("response should flush");
        });
        (format!("http://{address}"), rx, handle)
    }

    #[test]
    fn route_migration_http_client_submits_task_to_admin_server() {
        let response = serde_json::to_string(&RouteMigrationTaskStatusResponse {
            task_id: "task-1".to_string(),
            namespace: "mooncake/routes".to_string(),
            authority: "authority-a".to_string(),
            tenant: "tenant-a".to_string(),
            domain: None,
            object_set: None,
            key: "object-a".to_string(),
            mode: RouteMigrationMode::Copy,
            source_segment: "segment-a".to_string(),
            target_segments: vec!["segment-b".to_string()],
            task_executor: "executor-a".to_string(),
            state: RouteMigrationTaskState::Pending,
            attempts: 0,
            max_retries: 5,
            execution_id: None,
            next_retry_at_ms: None,
            last_error: String::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .expect("response json should serialize");
        let (admin_url, requests, handle) = serve_single_response(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        ));
        let args = sample_cli_args_with_admin_url(&admin_url);

        let task = admin_http_post_json::<_, RouteMigrationTaskStatusResponse>(
            &args,
            "/v1/route-migrations/copy",
            &RouteMigrationTaskSubmitRequest {
                authority: "authority-a".to_string(),
                tenant: "tenant-a".to_string(),
                domain: None,
                object_set: None,
                key: "object-a".to_string(),
                source_segment: "segment-a".to_string(),
                target_segments: vec!["segment-b".to_string()],
                task_executor: "executor-a".to_string(),
                max_retries: Some(5),
            },
        )
        .expect("http submit should succeed");
        assert_eq!(task.task_id, "task-1");
        let request = requests.recv().expect("request should capture");
        assert!(request.starts_with("POST /v1/route-migrations/copy HTTP/1.1\r\n"));
        assert!(request.contains("\"task_executor\":\"executor-a\""));
        assert!(!request.contains("\"mode\""));
        handle.join().expect("server thread should join");
    }

    #[test]
    fn route_migration_http_client_submits_move_task_to_admin_server() {
        let response = serde_json::to_string(&RouteMigrationTaskStatusResponse {
            task_id: "task-move-1".to_string(),
            namespace: "mooncake/routes".to_string(),
            authority: "authority-a".to_string(),
            tenant: "tenant-a".to_string(),
            domain: None,
            object_set: None,
            key: "object-a".to_string(),
            mode: RouteMigrationMode::Move,
            source_segment: "segment-a".to_string(),
            target_segments: vec!["segment-b".to_string()],
            task_executor: "executor-a".to_string(),
            state: RouteMigrationTaskState::Pending,
            attempts: 0,
            max_retries: 5,
            execution_id: None,
            next_retry_at_ms: None,
            last_error: String::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
        })
        .expect("response json should serialize");
        let (admin_url, requests, handle) = serve_single_response(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        ));
        let args = sample_cli_args_with_admin_url(&admin_url);

        let task = admin_http_post_json::<_, RouteMigrationTaskStatusResponse>(
            &args,
            route_migration_submit_path(RouteMigrationMode::Move),
            &RouteMigrationTaskSubmitRequest {
                authority: "authority-a".to_string(),
                tenant: "tenant-a".to_string(),
                domain: None,
                object_set: None,
                key: "object-a".to_string(),
                source_segment: "segment-a".to_string(),
                target_segments: vec!["segment-b".to_string()],
                task_executor: "executor-a".to_string(),
                max_retries: Some(5),
            },
        )
        .expect("http submit should succeed");
        assert_eq!(task.task_id, "task-move-1");
        let request = requests.recv().expect("request should capture");
        assert!(request.starts_with("POST /v1/route-migrations/move HTTP/1.1\r\n"));
        assert!(!request.contains("\"mode\""));
        handle.join().expect("server thread should join");
    }

    #[test]
    fn route_migration_submit_path_matches_mode() {
        assert_eq!(
            route_migration_submit_path(RouteMigrationMode::Copy),
            "/v1/route-migrations/copy"
        );
        assert_eq!(
            route_migration_submit_path(RouteMigrationMode::Move),
            "/v1/route-migrations/move"
        );
    }

    #[test]
    fn route_migration_http_client_lists_tasks_from_admin_server() {
        let response = serde_json::to_string(&RouteMigrationTaskListResponse {
            count: 1,
            tasks: vec![RouteMigrationTaskStatusResponse {
                task_id: "task-2".to_string(),
                namespace: "mooncake/routes".to_string(),
                authority: "authority-a".to_string(),
                tenant: "tenant-a".to_string(),
                domain: Some("domain-a".to_string()),
                object_set: Some("set-a".to_string()),
                key: "object-a".to_string(),
                mode: RouteMigrationMode::Move,
                source_segment: "segment-a".to_string(),
                target_segments: vec!["segment-b".to_string()],
                task_executor: "executor-a".to_string(),
                state: RouteMigrationTaskState::Succeeded,
                attempts: 1,
                max_retries: 5,
                execution_id: Some("execution-1".to_string()),
                next_retry_at_ms: None,
                last_error: String::new(),
                created_at_ms: 1,
                updated_at_ms: 2,
            }],
        })
        .expect("response json should serialize");
        let (admin_url, requests, handle) = serve_single_response(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        ));
        let args = sample_cli_args_with_admin_url(&admin_url);

        let listed: RouteMigrationTaskListResponse =
            admin_http_get_json(&args, "/v1/route-migrations").expect("http list should succeed");
        assert_eq!(listed.count, 1);
        assert_eq!(listed.tasks[0].domain.as_deref(), Some("domain-a"));
        let request = requests.recv().expect("request should capture");
        assert!(request.starts_with("GET /v1/route-migrations HTTP/1.1\r\n"));
        handle.join().expect("server thread should join");
    }

    #[test]
    fn route_migration_http_client_gets_task_from_admin_server() {
        let response = serde_json::to_string(&RouteMigrationTaskStatusResponse {
            task_id: "task-3".to_string(),
            namespace: "mooncake/routes".to_string(),
            authority: "authority-a".to_string(),
            tenant: "tenant-a".to_string(),
            domain: None,
            object_set: None,
            key: "object-a".to_string(),
            mode: RouteMigrationMode::Move,
            source_segment: "segment-a".to_string(),
            target_segments: vec!["segment-b".to_string()],
            task_executor: "executor-a".to_string(),
            state: RouteMigrationTaskState::Succeeded,
            attempts: 1,
            max_retries: 5,
            execution_id: Some("execution-1".to_string()),
            next_retry_at_ms: None,
            last_error: String::new(),
            created_at_ms: 1,
            updated_at_ms: 2,
        })
        .expect("response json should serialize");
        let (admin_url, requests, handle) = serve_single_response(format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response.len(),
            response
        ));
        let args = sample_cli_args_with_admin_url(&admin_url);

        let task: RouteMigrationTaskStatusResponse =
            admin_http_get_json(&args, "/v1/route-migrations/task-3")
                .expect("http get should succeed");
        assert_eq!(task.task_id, "task-3");
        assert_eq!(task.state, RouteMigrationTaskState::Succeeded);
        let request = requests.recv().expect("request should capture");
        assert!(request.starts_with("GET /v1/route-migrations/task-3 HTTP/1.1\r\n"));
        handle.join().expect("server thread should join");
    }

    #[test]
    fn migrate_commands_require_admin_url() {
        let args = Args {
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            admin_url: None,
            keyspace: None,
            trace_filter: None,
            command: Command::CleanupStaleSegments,
        };
        let error = admin_base_url(&args).expect_err("missing admin_url should fail");
        assert!(error.to_string().contains("--admin-url"));
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
