use std::error::Error;

use clap::{Parser, Subcommand};
use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::init_tracing;
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
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    init_tracing(args.trace_filter.as_deref())?;

    match args.command {
        Command::CleanupStaleSegments => cleanup_stale_segments(&args),
    }
}

fn cleanup_stale_segments(args: &Args) -> Result<(), Box<dyn Error>> {
    if !args.metadata_url.starts_with("redis://") {
        return Err("cleanup-stale-segments currently supports redis:// metadata only".into());
    }

    let keyspace = args
        .keyspace
        .clone()
        .map(MetadataKeyspace::new)
        .unwrap_or_default();
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
