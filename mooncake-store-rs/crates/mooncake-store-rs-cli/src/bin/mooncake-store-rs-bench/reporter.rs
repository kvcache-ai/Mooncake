use std::time::Duration;

use tracing::info;

use crate::cli::OutputFormat;
use crate::latency::LatencyRecorder;

fn format_duration(d: Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{us}us")
    } else if us < 1_000_000 {
        format!("{:.2}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

fn format_throughput(mib_s: f64) -> String {
    if mib_s < 1.0 {
        format!("{:.1} KiB/s", mib_s * 1024.0)
    } else if mib_s < 1024.0 {
        format!("{:.1} MiB/s", mib_s)
    } else {
        format!("{:.2} GiB/s", mib_s / 1024.0)
    }
}

pub struct BenchReport {
    pub mode: String,
    pub write_label: String,
    pub read_label: String,
    pub elapsed: Duration,
    pub concurrency: usize,
    pub value_size: usize,
    pub batch_size: usize,
    pub put_stats: Option<LatencyRecorder>,
    pub get_stats: Option<LatencyRecorder>,
}

impl BenchReport {
    pub fn print(&mut self, format: &OutputFormat) {
        match format {
            OutputFormat::Text => self.print_text(),
            OutputFormat::Json => self.print_json(),
            OutputFormat::Csv => self.print_csv(),
        }
    }

    fn print_text(&mut self) {
        let mut lines = Vec::new();
        lines.push("=== Benchmark Results ===".to_string());
        lines.push(format!(
            "Mode: {} | Write: {} | Read: {} | Duration: {:.2}s | Workers: {} | Value size: {} B | Batch size: {}",
            self.mode,
            self.write_label,
            self.read_label,
            self.elapsed.as_secs_f64(),
            self.concurrency,
            self.value_size,
            self.batch_size,
        ));
        lines.push(String::new());

        let has_put = self.put_stats.is_some();
        let has_get = self.get_stats.is_some();

        if has_put && has_get {
            lines.push(format!(
                "  {:<16} {:<16} {:<16}",
                "Metric", self.write_label, self.read_label
            ));
            lines.push(format!("  {:<16} {:<16} {:<16}", "------", "---", "---"));
        } else if has_put {
            lines.push(format!("  {:<16} {:<16}", "Metric", self.write_label));
            lines.push(format!("  {:<16} {:<16}", "------", "---"));
        } else if has_get {
            lines.push(format!("  {:<16} {:<16}", "Metric", self.read_label));
            lines.push(format!("  {:<16} {:<16}", "------", "---"));
        }

        let format_row = |label: &str, put_val: Option<String>, get_val: Option<String>| match (
            put_val, get_val,
        ) {
            (Some(p), Some(g)) => Some(format!("  {:<16} {:<16} {:<16}", label, p, g)),
            (Some(p), None) => Some(format!("  {:<16} {:<16}", label, p)),
            (None, Some(g)) => Some(format!("  {:<16} {:<16}", label, g)),
            (None, None) => None,
        };

        if let Some(line) = format_row(
            "Total ops",
            self.put_stats.as_ref().map(|s| s.total_ops().to_string()),
            self.get_stats.as_ref().map(|s| s.total_ops().to_string()),
        ) {
            lines.push(line);
        }

        if let Some(line) = format_row(
            "QPS",
            self.put_stats.as_ref().map(|s| format!("{:.1}", s.qps())),
            self.get_stats.as_ref().map(|s| format!("{:.1}", s.qps())),
        ) {
            lines.push(line);
        }

        if let Some(line) = format_row(
            "Throughput",
            self.put_stats
                .as_ref()
                .map(|s| format_throughput(s.throughput_mib_s())),
            self.get_stats
                .as_ref()
                .map(|s| format_throughput(s.throughput_mib_s())),
        ) {
            lines.push(line);
        }

        for (label, pct) in [
            ("Latency p50", 50.0),
            ("Latency p90", 90.0),
            ("Latency p99", 99.0),
            ("Latency p999", 99.9),
        ] {
            if let Some(line) = format_row(
                label,
                self.put_stats
                    .as_mut()
                    .map(|s| format_duration(s.percentile(pct))),
                self.get_stats
                    .as_mut()
                    .map(|s| format_duration(s.percentile(pct))),
            ) {
                lines.push(line);
            }
        }

        if let Some(line) = format_row(
            "Latency max",
            self.put_stats
                .as_mut()
                .map(|s| format_duration(s.max_latency())),
            self.get_stats
                .as_mut()
                .map(|s| format_duration(s.max_latency())),
        ) {
            lines.push(line);
        }

        if let Some(line) = format_row(
            "Errors",
            self.put_stats.as_ref().map(|s| s.error_count().to_string()),
            self.get_stats.as_ref().map(|s| s.error_count().to_string()),
        ) {
            lines.push(line);
        }

        for line in lines {
            info!("{line}");
        }
    }

    fn print_json(&mut self) {
        let mut obj = serde_json::Map::new();
        obj.insert("mode".into(), serde_json::Value::String(self.mode.clone()));
        obj.insert(
            "write_interface".into(),
            serde_json::Value::String(self.write_label.clone()),
        );
        obj.insert(
            "read_interface".into(),
            serde_json::Value::String(self.read_label.clone()),
        );
        obj.insert(
            "elapsed_s".into(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(self.elapsed.as_secs_f64()).unwrap(),
            ),
        );
        obj.insert("concurrency".into(), serde_json::json!(self.concurrency));
        obj.insert("value_size".into(), serde_json::json!(self.value_size));
        obj.insert("batch_size".into(), serde_json::json!(self.batch_size));

        if let Some(ref mut stats) = self.put_stats {
            obj.insert(self.write_label.clone(), stats_to_json(stats));
        }
        if let Some(ref mut stats) = self.get_stats {
            obj.insert(self.read_label.clone(), stats_to_json(stats));
        }

        info!(
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Object(obj)).unwrap()
        );
    }

    fn print_csv(&mut self) {
        info!("operation,metric,value");
        if let Some(ref mut stats) = self.put_stats {
            print_csv_rows(&self.write_label, stats);
        }
        if let Some(ref mut stats) = self.get_stats {
            print_csv_rows(&self.read_label, stats);
        }
    }
}

fn stats_to_json(stats: &mut LatencyRecorder) -> serde_json::Value {
    serde_json::json!({
        "total_ops": stats.total_ops(),
        "total_bytes": stats.total_bytes(),
        "qps": stats.qps(),
        "throughput_mib_s": stats.throughput_mib_s(),
        "errors": stats.error_count(),
        "latency_p50_us": stats.percentile(50.0).as_micros(),
        "latency_p90_us": stats.percentile(90.0).as_micros(),
        "latency_p99_us": stats.percentile(99.0).as_micros(),
        "latency_p999_us": stats.percentile(99.9).as_micros(),
        "latency_max_us": stats.max_latency().as_micros(),
        "latency_mean_us": stats.mean_latency().as_micros(),
    })
}

fn print_csv_rows(op: &str, stats: &mut LatencyRecorder) {
    info!("{op},total_ops,{}", stats.total_ops());
    info!("{op},total_bytes,{}", stats.total_bytes());
    info!("{op},qps,{:.2}", stats.qps());
    info!("{op},throughput_mib_s,{:.4}", stats.throughput_mib_s());
    info!("{op},errors,{}", stats.error_count());
    info!("{op},latency_p50_us,{}", stats.percentile(50.0).as_micros());
    info!("{op},latency_p90_us,{}", stats.percentile(90.0).as_micros());
    info!("{op},latency_p99_us,{}", stats.percentile(99.0).as_micros());
    info!(
        "{op},latency_p999_us,{}",
        stats.percentile(99.9).as_micros()
    );
    info!("{op},latency_max_us,{}", stats.max_latency().as_micros());
    info!("{op},latency_mean_us,{}", stats.mean_latency().as_micros());
}

pub fn print_progress_detailed(
    elapsed_secs: u64,
    write_label: &str,
    read_label: &str,
    put_stats: &mut Option<LatencyRecorder>,
    get_stats: &mut Option<LatencyRecorder>,
) {
    let mut parts = Vec::new();
    if let Some(ref mut s) = put_stats {
        parts.push(format!(
            "{write_label}: {:.0} qps p50={} p99={}",
            s.qps(),
            format_duration(s.percentile(50.0)),
            format_duration(s.percentile(99.0)),
        ));
    }
    if let Some(ref mut s) = get_stats {
        parts.push(format!(
            "{read_label}: {:.0} qps p50={} p99={}",
            s.qps(),
            format_duration(s.percentile(50.0)),
            format_duration(s.percentile(99.0)),
        ));
    }
    let total_errors = put_stats.as_ref().map(|s| s.error_count()).unwrap_or(0)
        + get_stats.as_ref().map(|s| s.error_count()).unwrap_or(0);
    info!(
        "[{elapsed_secs}s] {} | errors: {total_errors}",
        parts.join(" | ")
    );
}
