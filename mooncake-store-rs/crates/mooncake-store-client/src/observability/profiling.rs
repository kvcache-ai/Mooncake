use std::borrow::Cow;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use mooncake_store_core::{Result, StoreError};
use opentelemetry::global::BoxedSpan;
use opentelemetry::trace::{Span as _, Status, Tracer};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use serde::Serialize;

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
const DEFAULT_EXPORT_TIMEOUT: Duration = Duration::from_secs(3);

static PROFILING_CONTROL: OnceLock<ProfilingControl> = OnceLock::new();

pub(crate) struct ProfilingSpan {
    span: Option<BoxedSpan>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ProfilingSnapshot {
    pub enabled: bool,
    pub configured: bool,
    pub provider_ready: bool,
    pub endpoint: Option<String>,
    pub service_name: String,
    pub service_instance_id: Option<String>,
    pub sample_ratio: f64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
struct ProfilingConfig {
    endpoint: Option<String>,
    service_name: String,
    service_instance_id: Option<String>,
    sample_ratio: f64,
    export_timeout: Duration,
    last_error: Option<String>,
}

struct ProfilingInner {
    config: ProfilingConfig,
    provider: Option<SdkTracerProvider>,
}

struct ProfilingControl {
    enabled: AtomicBool,
    provider_ready: AtomicBool,
    inner: Mutex<ProfilingInner>,
}

impl ProfilingSpan {
    pub(crate) fn start(name: &'static str) -> Self {
        Self::start_named(|| Cow::Borrowed(name))
    }

    pub(crate) fn start_metadata(backend: &'static str, operation: &'static str) -> Self {
        let mut span = Self::start_named(|| Cow::Owned(format!("metadata.{operation}")));
        span.set_str("mooncake.metadata.backend", backend);
        span.set_str("mooncake.metadata.operation", operation);
        span
    }

    pub(crate) fn set_str(&mut self, key: &'static str, value: &str) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new(key, value.to_string()));
        }
    }

    pub(crate) fn set_u64(&mut self, key: &'static str, value: u64) {
        if let Some(span) = &mut self.span {
            span.set_attribute(KeyValue::new(key, clamp_u64_to_i64(value)));
        }
    }

    pub(crate) fn finish(mut self, result: &'static str, bytes_out: u64) {
        self.set_str("mooncake.result", result);
        self.set_u64("mooncake.bytes_out", bytes_out);
        if let Some(mut span) = self.span.take() {
            if result != "ok" {
                span.set_status(Status::error(result.to_string()));
            }
            span.end();
        }
    }

    fn start_named(name: impl FnOnce() -> Cow<'static, str>) -> Self {
        let control = profiling_control();
        if !control.enabled.load(Ordering::Relaxed) {
            return Self::disabled();
        }
        if let Err(error) = control.ensure_provider() {
            control.disable_after_error(error.to_string());
            return Self::disabled();
        }

        let tracer = global::tracer(DEFAULT_TRACER_NAME);
        let mut span = tracer.start(name());
        span.set_attribute(KeyValue::new("mooncake.component", "store-rs"));
        Self { span: Some(span) }
    }

    fn disabled() -> Self {
        Self { span: None }
    }
}

impl Drop for ProfilingSpan {
    fn drop(&mut self) {
        if let Some(mut span) = self.span.take() {
            span.end();
        }
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
            }),
        }
    }

    fn ensure_provider(&self) -> Result<()> {
        if self.provider_ready.load(Ordering::Acquire) {
            return Ok(());
        }

        let mut inner = self.inner.lock().expect("profiling control lock poisoned");
        if inner.provider.is_some() {
            self.provider_ready.store(true, Ordering::Release);
            return Ok(());
        }
        let endpoint = inner.config.endpoint.clone().ok_or_else(|| {
            StoreError::InvalidState(format!(
                "OTLP tracing endpoint is not configured; set {ENDPOINT_ENV} \
                 or {STANDARD_TRACES_ENDPOINT_ENV}"
            ))
        })?;
        let provider = build_provider(&inner.config, &endpoint)?;
        global::set_tracer_provider(provider.clone());
        inner.provider = Some(provider);
        inner.config.last_error = None;
        self.provider_ready.store(true, Ordering::Release);
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
            configured: inner.config.endpoint.is_some(),
            provider_ready: self.provider_ready.load(Ordering::Acquire),
            endpoint: inner.config.endpoint.clone(),
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
        }

        if let Some(enabled) = enabled {
            if enabled {
                self.ensure_provider()?;
            }
            self.enabled.store(enabled, Ordering::Release);
        }
        Ok(self.snapshot())
    }

    fn force_flush(&self) -> Result<()> {
        let provider = self
            .inner
            .lock()
            .expect("profiling control lock poisoned")
            .provider
            .clone();
        if let Some(provider) = provider {
            provider.force_flush().map_err(|error| {
                StoreError::InvalidState(format!("OTLP tracing flush failed: {error}"))
            })?;
        }
        Ok(())
    }
}

impl ProfilingConfig {
    fn from_env() -> Self {
        Self {
            endpoint: endpoint_from_env(),
            service_name: service_name_from_env(),
            service_instance_id: service_instance_from_env(),
            sample_ratio: sample_ratio_from_env(),
            export_timeout: export_timeout_from_env(),
            last_error: None,
        }
    }
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

fn sampler(sample_ratio: f64) -> Sampler {
    match sample_ratio {
        ratio if ratio <= 0.0 => Sampler::AlwaysOff,
        ratio if ratio >= 1.0 => Sampler::AlwaysOn,
        ratio => Sampler::TraceIdRatioBased(ratio),
    }
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
            "enabled=on&endpoint=http%3A%2F%2Fjaeger%3A4318&service_name=store+rs&sample_ratio=0.125",
        ))
        .expect("query should parse");
        assert_eq!(update.enabled, Some(true));
        assert_eq!(update.endpoint.as_deref(), Some("http://jaeger:4318"));
        assert_eq!(update.service_name.as_deref(), Some("store rs"));
        assert_eq!(update.sample_ratio, Some(0.125));
    }

    #[test]
    fn parse_update_query_rejects_invalid_sample_ratio() {
        let error = parse_update_query(Some("sample_ratio=2")).expect_err("ratio must fail");
        assert!(error.to_string().contains("expected 0.0..=1.0"));
    }
}
