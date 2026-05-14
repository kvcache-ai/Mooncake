use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mooncake_store_core::{Result, StoreError};
use opentelemetry::trace::{Span as _, Status, TraceContextExt, Tracer};
use opentelemetry::{global, Context, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use serde::Serialize;
use sha2::{Digest, Sha256};

const DEFAULT_SERVICE_NAME: &str = "mooncake-store-rs";
const DEFAULT_TRACER_NAME: &str = "mooncake-store-rs";
const ENABLE_ENV: &str = "MC_STORE_RS_OTLP_TRACE";
const ENDPOINT_ENV: &str = "MC_STORE_RS_OTLP_ENDPOINT";
const STANDARD_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const STANDARD_TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const SERVICE_NAME_ENV: &str = "MC_STORE_RS_OTLP_SERVICE_NAME";
const STANDARD_SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";
const SERVICE_INSTANCE_ENV: &str = "MC_STORE_RS_OTLP_SERVICE_INSTANCE_ID";
const EXPORT_TIMEOUT_ENV: &str = "MC_STORE_RS_OTLP_TIMEOUT_MS";
const SAMPLE_RATIO_ENV: &str = "MC_STORE_RS_OTLP_SAMPLE_RATIO";
const JSONL_FILE_ENV: &str = "MC_STORE_RS_TRACE_JSONL_FILE";
const ITEM_METADATA_ENV: &str = "MC_STORE_RS_TRACE_ITEM_METADATA";
const KEY_MODE_ENV: &str = "MC_STORE_RS_TRACE_KEY_MODE";
const DEFAULT_EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

static PROFILING_CONTROL: OnceLock<ProfilingControl> = OnceLock::new();
static NEXT_PROFILING_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static PROFILING_CONTEXT_STACK: RefCell<Vec<ProfilingFrame>> = const {
        RefCell::new(Vec::new())
    };
}

pub(crate) struct ProfilingSpan {
    context: Option<Context>,
    stack_depth: Option<usize>,
    local: Option<LocalProfilingSpan>,
    request_id: Option<u64>,
}

#[derive(Clone)]
struct ProfilingFrame {
    context: Context,
    request_id: u64,
    root_operation: &'static str,
    flow: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OperationSemantics {
    span_name: &'static str,
    phase: &'static str,
    flow: &'static str,
    role: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProfilingSnapshot {
    pub enabled: bool,
    pub configured: bool,
    pub provider_ready: bool,
    pub endpoint: Option<String>,
    pub file: Option<String>,
    pub service_name: String,
    pub service_instance_id: Option<String>,
    pub sample_ratio: f64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
struct ProfilingConfig {
    endpoint: Option<String>,
    file: Option<PathBuf>,
    service_name: String,
    service_instance_id: Option<String>,
    sample_ratio: f64,
    item_metadata: bool,
    key_mode: TraceKeyMode,
    export_timeout: Duration,
    last_error: Option<String>,
}

struct ProfilingInner {
    config: ProfilingConfig,
    provider: Option<SdkTracerProvider>,
    file: Option<File>,
}

struct ProfilingControl {
    enabled: AtomicBool,
    provider_ready: AtomicBool,
    inner: Mutex<ProfilingInner>,
}

struct LocalProfilingSpan {
    span_name: String,
    phase: &'static str,
    flow: &'static str,
    role: &'static str,
    request_id: u64,
    root_operation: &'static str,
    started: Instant,
    started_unix_ms: u128,
    attributes: BTreeMap<&'static str, serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TraceKeyMode {
    Hash,
    Full,
}

pub(crate) struct ApiItemsTraceRecord<'a> {
    pub trace_request_id: u64,
    pub operation: &'static str,
    pub tenant: &'a str,
    pub domain: &'a str,
    pub object_set: &'a str,
    pub runtime_id: &'a str,
    pub batch_bytes: u64,
    pub items: Vec<ApiItemTrace<'a>>,
}

pub(crate) struct ApiItemTrace<'a> {
    pub index: usize,
    pub key: &'a str,
    pub tenant: &'a str,
    pub domain: &'a str,
    pub object_set: &'a str,
    pub size: Option<u64>,
    pub read_len: Option<u64>,
    pub status: &'static str,
}

#[derive(Serialize)]
struct LocalApiItemsRecord<'a> {
    record_type: &'static str,
    timestamp_unix_ms: u128,
    trace_request_id: u64,
    operation: &'static str,
    tenant: &'a str,
    domain: &'a str,
    object_set: &'a str,
    runtime_id: &'a str,
    item_count: u64,
    batch_bytes: u64,
    items: Vec<LocalApiItemRecord<'a>>,
}

#[derive(Serialize)]
struct LocalApiItemRecord<'a> {
    index: usize,
    key_hash: String,
    key_prefix: String,
    key: Option<&'a str>,
    tenant: &'a str,
    domain: &'a str,
    object_set: &'a str,
    tp_rank: Option<u64>,
    kv_kind: Option<&'static str>,
    size: Option<u64>,
    read_len: Option<u64>,
    status: &'static str,
}

#[derive(Serialize)]
struct LocalSpanRecord<'a> {
    timestamp_unix_ms: u128,
    duration_us: u64,
    span_name: &'a str,
    phase: &'static str,
    flow: &'static str,
    role: &'static str,
    request_id: u64,
    root_operation: &'static str,
    result: &'static str,
    bytes_out: u64,
    attributes: &'a BTreeMap<&'static str, serde_json::Value>,
}

impl ProfilingSpan {
    pub(crate) fn start(operation: &'static str) -> Self {
        let semantics = operation_semantics(operation);
        let mut span = Self::start_named(|| Cow::Borrowed(semantics.span_name), semantics);
        span.set_str("mooncake.operation", operation);
        span
    }

    pub(crate) fn start_metadata(backend: &'static str, operation: &'static str) -> Self {
        let semantics = metadata_semantics();
        let mut span = Self::start_named(|| Cow::Owned(format!("metadata.{operation}")), semantics);
        span.set_str("mooncake.metadata.backend", backend);
        span.set_str("mooncake.metadata.operation", operation);
        span
    }

    pub(crate) fn set_str(&mut self, key: &'static str, value: &str) {
        if let Some(context) = &self.context {
            context
                .span()
                .set_attribute(KeyValue::new(key, value.to_string()));
        }
        if let Some(local) = &mut self.local {
            local
                .attributes
                .insert(key, serde_json::Value::String(value.to_string()));
        }
    }

    pub(crate) fn set_u64(&mut self, key: &'static str, value: u64) {
        if let Some(context) = &self.context {
            context
                .span()
                .set_attribute(KeyValue::new(key, clamp_u64_to_i64(value)));
        }
        if let Some(local) = &mut self.local {
            local.attributes.insert(
                key,
                serde_json::Value::Number(serde_json::Number::from(value)),
            );
        }
    }

    pub(crate) fn request_id(&self) -> Option<u64> {
        self.request_id
    }

    pub(crate) fn finish(mut self, result: &'static str, bytes_out: u64) {
        self.set_str("mooncake.result", result);
        self.set_u64("mooncake.bytes_out", bytes_out);
        if let Some(context) = self.context.take() {
            if result != "ok" {
                context.span().set_status(Status::error(result.to_string()));
            }
            context.span().end();
        }
        if let Some(local) = self.local.take() {
            profiling_control().write_local_span(local, result, bytes_out);
        }
        self.pop_stack_frame();
    }

    fn start_named(
        name: impl FnOnce() -> Cow<'static, str>,
        semantics: OperationSemantics,
    ) -> Self {
        let control = profiling_control();
        if !control.enabled.load(Ordering::Relaxed) {
            return Self::disabled();
        }
        if let Err(error) = control.ensure_sinks() {
            control.disable_after_error(error.to_string());
            return Self::disabled();
        }

        let span_name = name();
        let parent = current_profiling_frame();
        let request_id = parent
            .as_ref()
            .map(|frame| frame.request_id)
            .unwrap_or_else(next_request_id);
        let root_operation = parent
            .as_ref()
            .map(|frame| frame.root_operation)
            .unwrap_or(semantics.span_name);
        let flow = effective_flow(semantics.flow, parent.as_ref());
        let local = control.local_span(
            span_name.as_ref(),
            semantics,
            request_id,
            root_operation,
            flow,
        );
        let context = if control.provider_ready.load(Ordering::Acquire) {
            let tracer = global::tracer(DEFAULT_TRACER_NAME);
            let mut span = if let Some(parent) = parent.as_ref() {
                tracer.start_with_context(span_name.clone(), &parent.context)
            } else {
                tracer.start(span_name.clone())
            };
            span.set_attribute(KeyValue::new("mooncake.component", "store-rs"));
            span.set_attribute(KeyValue::new("mooncake.phase", semantics.phase));
            span.set_attribute(KeyValue::new("mooncake.flow", flow));
            span.set_attribute(KeyValue::new("mooncake.span_role", semantics.role));
            span.set_attribute(KeyValue::new(
                "mooncake.request_id",
                clamp_u64_to_i64(request_id),
            ));
            span.set_attribute(KeyValue::new("mooncake.root_operation", root_operation));
            Some(if let Some(parent) = parent.as_ref() {
                parent.context.with_span(span)
            } else {
                Context::current_with_span(span)
            })
        } else {
            local.as_ref().map(|_| Context::new())
        };
        let stack_depth = context.as_ref().map(|context| {
            push_profiling_frame(ProfilingFrame {
                context: context.clone(),
                request_id,
                root_operation,
                flow,
            })
        });
        Self {
            context,
            stack_depth,
            local,
            request_id: Some(request_id),
        }
    }

    fn disabled() -> Self {
        Self {
            context: None,
            stack_depth: None,
            local: None,
            request_id: None,
        }
    }

    fn pop_stack_frame(&mut self) {
        let Some(depth) = self.stack_depth.take() else {
            return;
        };
        PROFILING_CONTEXT_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.len() >= depth {
                stack.truncate(depth.saturating_sub(1));
            } else {
                stack.clear();
            }
        });
    }
}

impl Drop for ProfilingSpan {
    fn drop(&mut self) {
        if let Some(context) = self.context.take() {
            context.span().end();
        }
        if let Some(local) = self.local.take() {
            profiling_control().write_local_span(local, "dropped", 0);
        }
        self.pop_stack_frame();
    }
}

impl ProfilingControl {
    fn from_env() -> Self {
        let config = ProfilingConfig::from_env();
        let enabled = env_truthy(ENABLE_ENV);
        Self {
            enabled: AtomicBool::new(enabled),
            provider_ready: AtomicBool::new(false),
            inner: Mutex::new(ProfilingInner {
                config,
                provider: None,
                file: None,
            }),
        }
    }

    fn ensure_sinks(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("profiling control lock poisoned");
        if inner.config.endpoint.is_none() && inner.config.file.is_none() {
            return Err(StoreError::InvalidState(format!(
                "tracing sink is not configured; set {ENDPOINT_ENV}, \
                 {STANDARD_TRACES_ENDPOINT_ENV}, or {JSONL_FILE_ENV}"
            )));
        }

        if inner.config.file.is_some() && inner.file.is_none() {
            let path = inner.config.file.clone().expect("file path checked above");
            inner.file = Some(open_jsonl_file(&path)?);
        }

        if inner.config.endpoint.is_some()
            && inner.provider.is_none()
            && !self.provider_ready.load(Ordering::Acquire)
        {
            let endpoint = inner
                .config
                .endpoint
                .clone()
                .expect("endpoint checked above");
            let provider = build_provider(&inner.config, &endpoint)?;
            global::set_tracer_provider(provider.clone());
            inner.provider = Some(provider);
            self.provider_ready.store(true, Ordering::Release);
        }
        inner.config.last_error = None;
        Ok(())
    }

    fn disable_after_error(&self, message: String) {
        self.enabled.store(false, Ordering::Release);
        let mut inner = self.inner.lock().expect("profiling control lock poisoned");
        inner.config.last_error = Some(message);
    }

    fn snapshot(&self) -> ProfilingSnapshot {
        let inner = self.inner.lock().expect("profiling control lock poisoned");
        ProfilingSnapshot {
            enabled: self.enabled.load(Ordering::Acquire),
            configured: inner.config.endpoint.is_some() || inner.config.file.is_some(),
            provider_ready: self.provider_ready.load(Ordering::Acquire),
            endpoint: inner.config.endpoint.clone(),
            file: inner
                .config
                .file
                .as_ref()
                .map(|path| path.display().to_string()),
            service_name: inner.config.service_name.clone(),
            service_instance_id: inner.config.service_instance_id.clone(),
            sample_ratio: inner.config.sample_ratio,
            last_error: inner.config.last_error.clone(),
        }
    }

    fn update(
        &self,
        endpoint: Option<String>,
        service_name: Option<String>,
        service_instance_id: Option<String>,
        sample_ratio: Option<f64>,
        file: Option<Option<String>>,
        enabled: Option<bool>,
    ) -> Result<ProfilingSnapshot> {
        {
            let mut inner = self.inner.lock().expect("profiling control lock poisoned");
            if let Some(endpoint) = endpoint {
                let endpoint = normalize_http_trace_endpoint(&endpoint);
                if inner.provider.is_some() && inner.config.endpoint.as_ref() != Some(&endpoint) {
                    return Err(StoreError::InvalidState(
                        "OTLP endpoint cannot be changed after the exporter is initialized"
                            .to_string(),
                    ));
                }
                inner.config.endpoint = Some(endpoint);
                inner.config.last_error = None;
            }
            if let Some(service_name) = non_empty(service_name.as_deref()) {
                if inner.provider.is_some() && inner.config.service_name != service_name {
                    return Err(StoreError::InvalidState(
                        "OTLP service name cannot be changed after the exporter is initialized"
                            .to_string(),
                    ));
                }
                inner.config.service_name = service_name.to_string();
            }
            if let Some(instance) = service_instance_id {
                if inner.provider.is_some()
                    && inner.config.service_instance_id.as_deref() != non_empty(Some(&instance))
                {
                    return Err(StoreError::InvalidState(
                        "OTLP service instance id cannot be changed after the exporter is initialized"
                            .to_string(),
                    ));
                }
                inner.config.service_instance_id = non_empty(Some(&instance)).map(str::to_string);
            }
            if let Some(sample_ratio) = sample_ratio {
                if inner.provider.is_some() && inner.config.sample_ratio != sample_ratio {
                    return Err(StoreError::InvalidState(
                        "OTLP sample ratio cannot be changed after the exporter is initialized"
                            .to_string(),
                    ));
                }
                inner.config.sample_ratio = sample_ratio;
            }
            if let Some(file) = file {
                let next_file = file.and_then(|value| non_empty(Some(&value)).map(PathBuf::from));
                if inner.config.file != next_file {
                    inner.file = None;
                    inner.config.file = next_file;
                    inner.config.last_error = None;
                }
            }
        }

        if let Some(enabled) = enabled {
            if enabled {
                self.ensure_sinks()?;
            }
            self.enabled.store(enabled, Ordering::Release);
        }
        Ok(self.snapshot())
    }

    fn force_flush(&self) -> Result<()> {
        let provider = {
            let mut inner = self.inner.lock().expect("profiling control lock poisoned");
            if let Some(file) = &mut inner.file {
                file.flush().map_err(|error| {
                    StoreError::InvalidState(format!("local tracing flush failed: {error}"))
                })?;
            }
            inner.provider.clone()
        };
        if let Some(provider) = provider {
            provider.force_flush().map_err(|error| {
                StoreError::InvalidState(format!("OTLP tracing flush failed: {error}"))
            })?;
        }
        Ok(())
    }

    fn local_span(
        &self,
        span_name: &str,
        semantics: OperationSemantics,
        request_id: u64,
        root_operation: &'static str,
        flow: &'static str,
    ) -> Option<LocalProfilingSpan> {
        let has_file = self
            .inner
            .lock()
            .expect("profiling control lock poisoned")
            .file
            .is_some();
        if !has_file {
            return None;
        }
        let mut attributes = BTreeMap::new();
        attributes.insert(
            "mooncake.component",
            serde_json::Value::String("store-rs".to_string()),
        );
        attributes.insert(
            "mooncake.phase",
            serde_json::Value::String(semantics.phase.to_string()),
        );
        attributes.insert("mooncake.flow", serde_json::Value::String(flow.to_string()));
        attributes.insert(
            "mooncake.span_role",
            serde_json::Value::String(semantics.role.to_string()),
        );
        attributes.insert(
            "mooncake.request_id",
            serde_json::Value::Number(serde_json::Number::from(request_id)),
        );
        attributes.insert(
            "mooncake.root_operation",
            serde_json::Value::String(root_operation.to_string()),
        );
        Some(LocalProfilingSpan {
            span_name: span_name.to_string(),
            phase: semantics.phase,
            flow,
            role: semantics.role,
            request_id,
            root_operation,
            started: Instant::now(),
            started_unix_ms: unix_now_ms(),
            attributes,
        })
    }

    fn write_local_span(&self, span: LocalProfilingSpan, result: &'static str, bytes_out: u64) {
        let record = LocalSpanRecord {
            timestamp_unix_ms: span.started_unix_ms,
            duration_us: span.started.elapsed().as_micros() as u64,
            span_name: &span.span_name,
            phase: span.phase,
            flow: span.flow,
            role: span.role,
            request_id: span.request_id,
            root_operation: span.root_operation,
            result,
            bytes_out,
            attributes: &span.attributes,
        };
        let Ok(line) = serde_json::to_string(&record) else {
            return;
        };
        let mut inner = self.inner.lock().expect("profiling control lock poisoned");
        if let Some(file) = &mut inner.file {
            let _ = writeln!(file, "{line}");
        }
    }

    fn write_api_items(&self, record: ApiItemsTraceRecord<'_>) {
        let mut inner = self.inner.lock().expect("profiling control lock poisoned");
        if !inner.config.item_metadata {
            return;
        }
        let key_mode = inner.config.key_mode;
        if inner.file.is_none() {
            return;
        }
        let items = record
            .items
            .into_iter()
            .map(|item| {
                let (tp_rank, kv_kind) = parse_sglang_key_suffix(item.key);
                LocalApiItemRecord {
                    index: item.index,
                    key_hash: hash_key(item.key),
                    key_prefix: key_prefix(item.key),
                    key: (key_mode == TraceKeyMode::Full).then_some(item.key),
                    tenant: item.tenant,
                    domain: item.domain,
                    object_set: item.object_set,
                    tp_rank,
                    kv_kind,
                    size: item.size,
                    read_len: item.read_len,
                    status: item.status,
                }
            })
            .collect::<Vec<_>>();
        let local = LocalApiItemsRecord {
            record_type: "store.api_items.v1",
            timestamp_unix_ms: unix_now_ms(),
            trace_request_id: record.trace_request_id,
            operation: record.operation,
            tenant: record.tenant,
            domain: record.domain,
            object_set: record.object_set,
            runtime_id: record.runtime_id,
            item_count: items.len() as u64,
            batch_bytes: record.batch_bytes,
            items,
        };
        let Ok(line) = serde_json::to_string(&local) else {
            return;
        };
        if let Some(file) = &mut inner.file {
            let _ = writeln!(file, "{line}");
        }
    }
}

impl ProfilingConfig {
    fn from_env() -> Self {
        Self {
            endpoint: endpoint_from_env(),
            file: env_value(JSONL_FILE_ENV).map(PathBuf::from),
            service_name: service_name_from_env(),
            service_instance_id: service_instance_from_env(),
            sample_ratio: sample_ratio_from_env(),
            item_metadata: env_truthy(ITEM_METADATA_ENV),
            key_mode: key_mode_from_env(),
            export_timeout: export_timeout_from_env(),
            last_error: None,
        }
    }
}

pub(crate) fn record_api_items(record: ApiItemsTraceRecord<'_>) {
    let control = profiling_control();
    if !control.enabled.load(Ordering::Relaxed) {
        return;
    }
    control.write_api_items(record);
}

pub(crate) fn handle_tracing_http_path(path: &str) -> Result<String> {
    let (route, query) = split_query(path);
    let update = match route {
        "/tracing" | "/trace" => parse_update_query(query)?,
        "/tracing/on" | "/trace/on" => ProfilingUpdate {
            enabled: Some(true),
            ..ProfilingUpdate::default()
        },
        "/tracing/off" | "/trace/off" => ProfilingUpdate {
            enabled: Some(false),
            ..ProfilingUpdate::default()
        },
        "/tracing/flush" | "/trace/flush" => {
            profiling_control().force_flush()?;
            ProfilingUpdate::default()
        }
        _ => {
            return Err(StoreError::InvalidState(format!(
                "unknown tracing control path {route}"
            )));
        }
    };
    let snapshot = profiling_control().update(
        update.endpoint,
        update.service_name,
        update.service_instance_id,
        update.sample_ratio,
        update.file,
        update.enabled,
    )?;
    serde_json::to_string(&snapshot)
        .map(|json| format!("{json}\n"))
        .map_err(|error| {
            StoreError::InvalidState(format!("serialize tracing status failed: {error}"))
        })
}

fn profiling_control() -> &'static ProfilingControl {
    PROFILING_CONTROL.get_or_init(ProfilingControl::from_env)
}

fn build_provider(config: &ProfilingConfig, endpoint: &str) -> Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint.to_string())
        .with_protocol(Protocol::HttpBinary)
        .with_timeout(config.export_timeout)
        .build()
        .map_err(|error| {
            StoreError::InvalidState(format!("OTLP span exporter init failed: {error}"))
        })?;

    let mut resource = Resource::builder_empty()
        .with_attribute(KeyValue::new("service.name", config.service_name.clone()));
    if let Some(instance) = &config.service_instance_id {
        resource = resource.with_attribute(KeyValue::new("service.instance.id", instance.clone()));
    }

    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler(config.sample_ratio))
        .with_resource(resource.build())
        .build())
}

#[derive(Debug, Default)]
struct ProfilingUpdate {
    endpoint: Option<String>,
    service_name: Option<String>,
    service_instance_id: Option<String>,
    sample_ratio: Option<f64>,
    file: Option<Option<String>>,
    enabled: Option<bool>,
}

fn parse_update_query(query: Option<&str>) -> Result<ProfilingUpdate> {
    let mut update = ProfilingUpdate::default();
    let Some(query) = query else {
        return Ok(update);
    };
    for pair in query.split('&').filter(|entry| !entry.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        match key.as_str() {
            "enabled" | "enable" => update.enabled = Some(parse_bool(&value)?),
            "endpoint" => update.endpoint = Some(value),
            "service_name" => update.service_name = Some(value),
            "service_instance_id" => update.service_instance_id = Some(value),
            "sample_ratio" | "sampling_ratio" => update.sample_ratio = Some(parse_ratio(&value)?),
            "file" | "jsonl_file" => update.file = Some(Some(value)),
            "clear_file" => {
                if parse_bool(&value)? {
                    update.file = Some(None);
                }
            }
            unknown => {
                return Err(StoreError::InvalidState(format!(
                    "unknown tracing control query key {unknown:?}"
                )));
            }
        }
    }
    Ok(update)
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(StoreError::InvalidState(format!(
            "invalid tracing enabled value {other:?}"
        ))),
    }
}

fn parse_ratio(value: &str) -> Result<f64> {
    let ratio = value.trim().parse::<f64>().map_err(|error| {
        StoreError::InvalidState(format!("invalid tracing sample ratio {value:?}: {error}"))
    })?;
    if ratio.is_finite() && (0.0..=1.0).contains(&ratio) {
        return Ok(ratio);
    }
    Err(StoreError::InvalidState(format!(
        "invalid tracing sample ratio {value:?}; expected 0.0..=1.0"
    )))
}

fn split_query(path: &str) -> (&str, Option<&str>) {
    path.split_once('?')
        .map(|(route, query)| (route, Some(query)))
        .unwrap_or((path, None))
}

fn endpoint_from_env() -> Option<String> {
    env_value(STANDARD_TRACES_ENDPOINT_ENV)
        .or_else(|| {
            env_value(ENDPOINT_ENV).map(|endpoint| normalize_http_trace_endpoint(&endpoint))
        })
        .or_else(|| {
            env_value(STANDARD_ENDPOINT_ENV)
                .map(|endpoint| normalize_http_trace_endpoint(&endpoint))
        })
}

fn service_name_from_env() -> String {
    env_value(SERVICE_NAME_ENV)
        .or_else(|| env_value(STANDARD_SERVICE_NAME_ENV))
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_string())
}

fn service_instance_from_env() -> Option<String> {
    env_value(SERVICE_INSTANCE_ENV).or_else(|| env_value("HOSTNAME"))
}

fn export_timeout_from_env() -> Duration {
    env_value(EXPORT_TIMEOUT_ENV)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_EXPORT_TIMEOUT)
}

fn sample_ratio_from_env() -> f64 {
    env_value(SAMPLE_RATIO_ENV)
        .and_then(|value| parse_ratio(&value).ok())
        .unwrap_or(1.0)
}

fn key_mode_from_env() -> TraceKeyMode {
    match env_value(KEY_MODE_ENV)
        .unwrap_or_else(|| "hash".to_string())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "full" => TraceKeyMode::Full,
        _ => TraceKeyMode::Hash,
    }
}

fn hash_key(key: &str) -> String {
    let digest = Sha256::digest(key.as_bytes());
    let mut output = String::with_capacity("sha256:".len() + 32);
    output.push_str("sha256:");
    for byte in digest.iter().take(16) {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn key_prefix(key: &str) -> String {
    key.chars().take(16).collect()
}

fn parse_sglang_key_suffix(key: &str) -> (Option<u64>, Option<&'static str>) {
    let mut parts = key.rsplitn(3, '_');
    let Some(kv_kind) = parts.next() else {
        return (None, None);
    };
    let Some(tp_rank) = parts.next() else {
        return (None, None);
    };
    let Some(_) = parts.next() else {
        return (None, None);
    };
    let kv_kind = match kv_kind {
        "k" => Some("k"),
        "v" => Some("v"),
        _ => None,
    };
    let tp_rank = tp_rank.parse::<u64>().ok();
    (tp_rank, kv_kind)
}

fn sampler(sample_ratio: f64) -> Sampler {
    match sample_ratio {
        ratio if ratio <= 0.0 => Sampler::AlwaysOff,
        ratio if ratio >= 1.0 => Sampler::AlwaysOn,
        ratio => Sampler::TraceIdRatioBased(ratio),
    }
}

fn operation_semantics(operation: &'static str) -> OperationSemantics {
    match operation {
        "put" => api_semantics("store.put", "put"),
        "put_from" => api_semantics("store.put_from", "put"),
        "batch_put" => api_semantics("store.batch_put", "put"),
        "batch_put_from" => api_semantics("store.batch_put_from", "put"),
        "batch_is_exist" => api_semantics("store.batch_is_exist", "get"),
        "get_size" => api_semantics("store.get_size", "get"),
        "query_route" => api_semantics("store.query_route", "control"),
        "batch_put_from_multi_buffers" => {
            api_semantics("store.batch_put_from_multi_buffers", "put")
        }
        "get" => api_semantics("store.get", "get"),
        "get_into" => api_semantics("store.get_into", "get"),
        "batch_get" => api_semantics("store.batch_get", "get"),
        "batch_get_into" => api_semantics("store.batch_get_into", "get"),
        "batch_get_into_multi_buffers" => {
            api_semantics("store.batch_get_into_multi_buffers", "get")
        }
        "route_lookup_many" | "put_stage_load_route" | "batch_put_stage_load_routes" => {
            stage_semantics("control.route_lookup", "control", infer_flow(operation))
        }
        "readable_replica_select" => {
            stage_semantics("control.readable_replica_select", "control", "get")
        }
        "compat_dispatcher_bridge" => stage_semantics("python.bridge", "python", "inherit"),
        "py_batch_put_from_fanout" => {
            stage_semantics("python.batch_put_from_fanout", "python", "put")
        }
        "batch_put_stage_rank" => stage_semantics("control.placement_rank", "control", "put"),
        "put_stage_reserve" | "batch_put_stage_reserve" => {
            stage_semantics("control.allocate", "control", "put")
        }
        "put_stage_write" | "batch_put_stage_write" | "put_remote_batch_write" => {
            stage_semantics("data.transfer_write", "data", "put")
        }
        "put_local_copy" => stage_semantics("data.local_write", "data", "put"),
        "put_stage_route_cas" | "batch_put_stage_route_cas" => {
            stage_semantics("control.route_publish", "control", "put")
        }
        "control_route_batch_get" => stage_semantics("control.route_lookup", "control", "get"),
        "control_route_batch_cas" | "control_route_batch_replace" => {
            stage_semantics("control.route_publish", "control", "put")
        }
        "control_allocator_batch_reserve_any" => {
            stage_semantics("control.allocate", "control", "put")
        }
        "control_eviction_batch_track_replica_routes" => {
            stage_semantics("control.replica_track", "control", "put")
        }
        "get_remote_batch_chunk" | "get_remote_batch_direct" | "get_remote_direct" => {
            stage_semantics("data.transfer_read", "data", "get")
        }
        "remote_batch_get_fallback" | "remote_direct_get_fallback" => {
            stage_semantics("data.transfer_read_fallback", "data", "get")
        }
        "get_local_copy" => stage_semantics("data.local_read", "data", "get"),
        "register_local_memory" | "register_buffer" | "unregister_buffer" => {
            stage_semantics("data.registration", "data", "maintenance")
        }
        "heartbeat" | "live_client_snapshot_refresh" | "tenant_policy_cache_refresh" => {
            stage_semantics("control.membership_refresh", "control", "background")
        }
        "storage_owner_background_eviction" | "storage_owner_evict_one" => {
            stage_semantics("data.eviction", "data", "background")
        }
        "storage_owner_rebuild_clock" => {
            stage_semantics("data.eviction_rebuild", "data", "background")
        }
        _ if operation.starts_with("control_") => {
            stage_semantics("control.rpc", "control", infer_flow(operation))
        }
        _ if operation.contains("metadata") => {
            stage_semantics("metadata.operation", "metadata", "inherit")
        }
        _ if operation.contains("remote") || operation.contains("transfer") => {
            stage_semantics("data.operation", "data", infer_flow(operation))
        }
        _ if operation.contains("route") || operation.contains("alloc") => {
            stage_semantics("control.operation", "control", infer_flow(operation))
        }
        _ if operation.contains("background")
            || operation.contains("evict")
            || operation.contains("rebuild") =>
        {
            stage_semantics("background.operation", "background", "background")
        }
        _ => stage_semantics("store.operation", "api", infer_flow(operation)),
    }
}

fn api_semantics(span_name: &'static str, flow: &'static str) -> OperationSemantics {
    OperationSemantics {
        span_name,
        phase: "api",
        flow,
        role: "root",
    }
}

fn stage_semantics(
    span_name: &'static str,
    phase: &'static str,
    flow: &'static str,
) -> OperationSemantics {
    OperationSemantics {
        span_name,
        phase,
        flow,
        role: "stage",
    }
}

fn metadata_semantics() -> OperationSemantics {
    stage_semantics("metadata.operation", "metadata", "inherit")
}

fn infer_flow(operation: &str) -> &'static str {
    if operation.contains("put") || operation.contains("write") || operation.contains("reserve") {
        return "put";
    }
    if operation.contains("get") || operation.contains("read") || operation.contains("lookup") {
        return "get";
    }
    if operation.contains("remove") || operation.contains("delete") || operation.contains("reclaim")
    {
        return "remove";
    }
    "unknown"
}

fn effective_flow(flow: &'static str, parent: Option<&ProfilingFrame>) -> &'static str {
    match (flow, parent) {
        ("inherit" | "unknown", Some(parent)) => parent.flow,
        ("inherit", None) => "metadata",
        _ => flow,
    }
}

fn next_request_id() -> u64 {
    NEXT_PROFILING_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

fn current_profiling_frame() -> Option<ProfilingFrame> {
    PROFILING_CONTEXT_STACK.with(|stack| stack.borrow().last().cloned())
}

fn push_profiling_frame(frame: ProfilingFrame) -> usize {
    PROFILING_CONTEXT_STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        stack.push(frame);
        stack.len()
    })
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .and_then(|value| non_empty(Some(&value)).map(str::to_string))
}

fn env_truthy(name: &str) -> bool {
    env::var(name).ok().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn normalize_http_trace_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.ends_with("/v1/traces") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/traces")
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn open_jsonl_file(path: &Path) -> Result<File> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| {
            StoreError::InvalidState(format!(
                "failed to create tracing jsonl directory {}: {error}",
                parent.display()
            ))
        })?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            StoreError::InvalidState(format!(
                "failed to open tracing jsonl file {}: {error}",
                path.display()
            ))
        })
}

fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn percent_decode(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                output.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = from_hex(bytes[index + 1])?;
                let low = from_hex(bytes[index + 2])?;
                output.push((high << 4) | low);
                index += 3;
            }
            b'%' => {
                return Err(StoreError::InvalidState(
                    "tracing control query has truncated percent escape".to_string(),
                ));
            }
            byte => {
                output.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(output).map_err(|error| {
        StoreError::InvalidState(format!("tracing control query is not valid utf-8: {error}"))
    })
}

fn from_hex(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(StoreError::InvalidState(
            "tracing control query has invalid percent escape".to_string(),
        )),
    }
}

fn clamp_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Mutex;

    #[test]
    fn normalizes_http_trace_endpoint() {
        assert_eq!(
            normalize_http_trace_endpoint("http://jaeger:4318"),
            "http://jaeger:4318/v1/traces"
        );
        assert_eq!(
            normalize_http_trace_endpoint("http://jaeger:4318/v1/traces"),
            "http://jaeger:4318/v1/traces"
        );
    }

    #[test]
    fn parse_update_query_decodes_values() {
        let update = parse_update_query(Some(
            "enabled=on&endpoint=http%3A%2F%2Fjaeger%3A4318&service_name=store+rs&sample_ratio=0.125&file=%2Ftmp%2Fstore.jsonl",
        ))
        .expect("query should parse");
        assert_eq!(update.enabled, Some(true));
        assert_eq!(update.endpoint.as_deref(), Some("http://jaeger:4318"));
        assert_eq!(update.service_name.as_deref(), Some("store rs"));
        assert_eq!(update.sample_ratio, Some(0.125));
        assert_eq!(
            update.file.as_ref().and_then(|file| file.as_deref()),
            Some("/tmp/store.jsonl")
        );
    }

    #[test]
    fn local_jsonl_sink_writes_span_records() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mooncake-store-jsonl-{}-{}",
            std::process::id(),
            next_request_id()
        ));
        let path = temp_dir.join("trace.jsonl");
        let control = ProfilingControl {
            enabled: AtomicBool::new(true),
            provider_ready: AtomicBool::new(false),
            inner: Mutex::new(ProfilingInner {
                config: ProfilingConfig {
                    endpoint: None,
                    file: Some(path.clone()),
                    service_name: DEFAULT_SERVICE_NAME.to_string(),
                    service_instance_id: None,
                    sample_ratio: 1.0,
                    item_metadata: false,
                    key_mode: TraceKeyMode::Hash,
                    export_timeout: DEFAULT_EXPORT_TIMEOUT,
                    last_error: None,
                },
                provider: None,
                file: None,
            }),
        };
        control
            .ensure_sinks()
            .expect("file-only tracing sink should initialize");
        let mut span = control
            .local_span(
                "store.batch_put_from",
                api_semantics("store.batch_put_from", "put"),
                42,
                "store.batch_put_from",
                "put",
            )
            .expect("local span should be created");
        span.attributes.insert(
            "mooncake.operation",
            serde_json::Value::String("batch_put_from".to_string()),
        );
        control.write_local_span(span, "ok", 128);
        control.force_flush().expect("file flush should succeed");

        let contents = std::fs::read_to_string(&path).expect("jsonl file should be readable");
        let line = contents
            .lines()
            .next()
            .expect("jsonl should have one record");
        let value: serde_json::Value =
            serde_json::from_str(line).expect("jsonl record should be valid json");
        assert_eq!(value["span_name"], "store.batch_put_from");
        assert_eq!(value["phase"], "api");
        assert_eq!(value["result"], "ok");
        assert_eq!(value["bytes_out"], 128_u64);

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn local_jsonl_sink_writes_api_item_metadata_when_enabled() {
        let temp_dir = std::env::temp_dir().join(format!(
            "mooncake-store-items-jsonl-{}-{}",
            std::process::id(),
            next_request_id()
        ));
        let path = temp_dir.join("trace.jsonl");
        let control = ProfilingControl {
            enabled: AtomicBool::new(true),
            provider_ready: AtomicBool::new(false),
            inner: Mutex::new(ProfilingInner {
                config: ProfilingConfig {
                    endpoint: None,
                    file: Some(path.clone()),
                    service_name: DEFAULT_SERVICE_NAME.to_string(),
                    service_instance_id: None,
                    sample_ratio: 1.0,
                    item_metadata: true,
                    key_mode: TraceKeyMode::Hash,
                    export_timeout: DEFAULT_EXPORT_TIMEOUT,
                    last_error: None,
                },
                provider: None,
                file: None,
            }),
        };
        control
            .ensure_sinks()
            .expect("file-only tracing sink should initialize");

        control.write_api_items(ApiItemsTraceRecord {
            trace_request_id: 32574,
            operation: "batch_put_from",
            tenant: "tenant-a",
            domain: "domain-a",
            object_set: "set-a",
            runtime_id: "py-store-1:2",
            batch_bytes: 1540096,
            items: vec![ApiItemTrace {
                index: 0,
                key: "8544a78269348681_2_k",
                tenant: "tenant-a",
                domain: "domain-a",
                object_set: "set-a",
                size: Some(1540096),
                read_len: None,
                status: "ok",
            }],
        });
        control.force_flush().expect("file flush should succeed");

        let contents = std::fs::read_to_string(&path).expect("jsonl file should be readable");
        let value: serde_json::Value =
            serde_json::from_str(contents.trim()).expect("jsonl record should be valid json");
        assert_eq!(value["record_type"], "store.api_items.v1");
        assert_eq!(value["trace_request_id"], 32574_u64);
        assert_eq!(value["operation"], "batch_put_from");
        assert_eq!(value["tenant"], "tenant-a");
        assert_eq!(value["domain"], "domain-a");
        assert_eq!(value["object_set"], "set-a");
        assert_eq!(value["runtime_id"], "py-store-1:2");
        assert_eq!(value["item_count"], 1_u64);
        assert_eq!(value["batch_bytes"], 1540096_u64);
        assert_eq!(value["items"][0]["key_prefix"], "8544a78269348681");
        assert_eq!(value["items"][0]["tp_rank"], 2_u64);
        assert_eq!(value["items"][0]["kv_kind"], "k");
        assert_eq!(value["items"][0]["size"], 1540096_u64);
        assert_eq!(value["items"][0]["status"], "ok");
        assert!(value["items"][0]["key"].is_null());
        assert!(value["items"][0]["key_hash"]
            .as_str()
            .expect("key hash should be a string")
            .starts_with("sha256:"));

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn parse_update_query_rejects_invalid_sample_ratio() {
        let error = parse_update_query(Some("sample_ratio=2")).expect_err("ratio must fail");
        assert!(error.to_string().contains("expected 0.0..=1.0"));
    }

    #[test]
    fn operation_semantics_make_put_get_waterfalls_readable() {
        let put = operation_semantics("put");
        assert_eq!(put.span_name, "store.put");
        assert_eq!(put.phase, "api");
        assert_eq!(put.flow, "put");
        assert_eq!(put.role, "root");

        let reserve = operation_semantics("batch_put_stage_reserve");
        assert_eq!(reserve.span_name, "control.allocate");
        assert_eq!(reserve.phase, "control");
        assert_eq!(reserve.flow, "put");
        assert_eq!(reserve.role, "stage");

        let write = operation_semantics("batch_put_stage_write");
        assert_eq!(write.span_name, "data.transfer_write");
        assert_eq!(write.phase, "data");
        assert_eq!(write.flow, "put");
        assert_eq!(write.role, "stage");

        let lookup = operation_semantics("route_lookup_many");
        assert_eq!(lookup.span_name, "control.route_lookup");
        assert_eq!(lookup.phase, "control");
        assert_eq!(lookup.flow, "get");
        assert_eq!(lookup.role, "stage");

        let read = operation_semantics("get_remote_batch_chunk");
        assert_eq!(read.span_name, "data.transfer_read");
        assert_eq!(read.phase, "data");
        assert_eq!(read.flow, "get");
        assert_eq!(read.role, "stage");
    }
}
