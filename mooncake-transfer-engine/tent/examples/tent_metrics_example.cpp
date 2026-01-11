/**
 * @file tent_metrics_example.cpp
 * @brief Example demonstrating TENT metrics usage with HTTP server
 *
 * This example shows how to:
 * 1. Initialize the TENT metrics system with HTTP server
 * 2. Record transfer metrics with latency tracking
 * 3. Use ScopedLatencyRecorder for automatic latency measurement
 * 4. Use runtime enable/disable switch for dynamic control
 * 5. Access metrics via HTTP endpoints:
 *    - GET /metrics - Prometheus format
 *    - GET /metrics/summary - Human readable summary
 *    - GET /metrics/json - JSON format
 *    - GET /health - Health check
 *
 * Performance optimization features:
 * - Compile-time disable: Build with -DTENT_METRICS_ENABLED=OFF for zero
 * overhead
 * - Runtime disable: Use TentMetrics::setEnabled(false) for minimal overhead
 * - Pre-allocated buffers: Reduced memory allocation in hot paths
 */

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

#include "tent/metrics/tent_metrics.h"
#include "tent/metrics/config_loader.h"

using namespace mooncake::tent;

void printUsage(const char* program) {
    std::cout << "Usage: " << program << " [OPTIONS]\n"
              << "Options:\n"
              << "  --server         Run in server mode (keep running)\n"
              << "  --port <port>    HTTP server port (default: 9100)\n"
              << "  --disabled       Start with metrics collection disabled\n"
              << "  --help           Show this help message\n";
}

int main(int argc, char* argv[]) {
    std::cout << "=== TENT Metrics HTTP Server Example ===" << std::endl;

#if TENT_METRICS_ENABLED
    std::cout << "Compile-time metrics: ENABLED" << std::endl;
#else
    std::cout << "Compile-time metrics: DISABLED (zero overhead)" << std::endl;
#endif

    // Parse command line arguments
    bool server_mode = false;
    bool start_disabled = false;
    uint16_t port = 9100;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--server") == 0) {
            server_mode = true;
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = static_cast<uint16_t>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--disabled") == 0) {
            start_disabled = true;
        } else if (std::strcmp(argv[i], "--help") == 0) {
            printUsage(argv[0]);
            return 0;
        }
    }

    // 1. Load configuration (use defaults or from config file)
    auto config = MetricsConfigLoader::loadWithDefaults();
    config.enabled = !start_disabled;  // Can be disabled via --disabled flag
    config.http_port = port;
    config.http_host = "0.0.0.0";
    config.report_interval_seconds =
        0;  // Disable periodic logging for this example

    std::cout << "\nConfiguration:" << std::endl;
    std::cout << "  - HTTP Server: " << config.http_host << ":"
              << config.http_port << std::endl;
    std::cout << "  - Prometheus: "
              << (config.enable_prometheus ? "enabled" : "disabled")
              << std::endl;
    std::cout << "  - Runtime metrics: "
              << (config.enabled ? "enabled" : "disabled") << std::endl;

    // 2. Initialize metrics system (this starts the HTTP server)
    auto status = TentMetrics::instance().initialize(config);
    if (!status.ok()) {
        std::cerr << "Failed to initialize metrics: " << status.ToString()
                  << std::endl;
        return 1;
    }

    std::cout << "\nHTTP Server started. Available endpoints:" << std::endl;
    std::cout << "  - http://localhost:" << config.http_port
              << "/metrics        (Prometheus format)" << std::endl;
    std::cout << "  - http://localhost:" << config.http_port
              << "/metrics/summary (Human readable)" << std::endl;
    std::cout << "  - http://localhost:" << config.http_port
              << "/metrics/json    (JSON format)" << std::endl;
    std::cout << "  - http://localhost:" << config.http_port
              << "/health          (Health check)" << std::endl;

    // 3. Simulate transfer operations with manual latency recording
    std::cout << "\n--- Manual Latency Recording ---" << std::endl;
    std::cout
        << "Simulating transfer operations with explicit latency values..."
        << std::endl;

    for (int i = 0; i < 10; ++i) {
        // Simulate successful reads with manual latency
        size_t read_bytes = 1024 * 1024 * (i + 1);   // 1-10 MB
        double read_latency = 0.001 + (i * 0.0005);  // 1-5ms
        TENT_RECORD_READ_COMPLETED(read_bytes, read_latency);

        // Simulate successful writes with manual latency
        size_t write_bytes = 512 * 1024 * (i + 1);    // 512KB - 5MB
        double write_latency = 0.002 + (i * 0.0003);  // 2-4.7ms
        TENT_RECORD_WRITE_COMPLETED(write_bytes, write_latency);

        // Simulate some failures
        if (i % 3 == 0) {
            TENT_RECORD_READ_FAILED(1024);
        }
        if (i % 4 == 0) {
            TENT_RECORD_WRITE_FAILED(512);
        }

        std::cout << "  Iteration " << (i + 1) << ": Read "
                  << read_bytes / 1024 / 1024
                  << " MB (latency: " << read_latency * 1000 << "ms)"
                  << ", Write " << write_bytes / 1024
                  << " KB (latency: " << write_latency * 1000 << "ms)"
                  << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // 4. Demonstrate ScopedLatencyRecorder for automatic latency measurement
    std::cout << "\n--- Automatic Latency Recording (ScopedLatencyRecorder) ---"
              << std::endl;
    std::cout << "Using RAII-style automatic latency measurement..."
              << std::endl;

    // Example 1: Using TENT_SCOPED_READ_LATENCY macro
    {
        TENT_SCOPED_READ_LATENCY(2 * 1024 * 1024);  // 2 MB read
        // Simulate work (the latency is automatically measured)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        std::cout << "  Scoped read: 2 MB with ~5ms simulated work"
                  << std::endl;
    }  // Latency automatically recorded when scope ends

    // Example 2: Using TENT_SCOPED_WRITE_LATENCY macro
    {
        TENT_SCOPED_WRITE_LATENCY(1 * 1024 * 1024);  // 1 MB write
        // Simulate work
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
        std::cout << "  Scoped write: 1 MB with ~3ms simulated work"
                  << std::endl;
    }  // Latency automatically recorded when scope ends

    // Example 3: Using ScopedLatencyRecorder directly with failure handling
    {
        ScopedLatencyRecorder recorder(
            ScopedLatencyRecorder::OperationType::Read, 512 * 1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        // Simulate a failure condition
        bool operation_failed = true;  // Simulated failure
        if (operation_failed) {
            recorder.markFailed();
            std::cout << "  Scoped read with failure: 512 KB marked as failed"
                      << std::endl;
        }
    }

    // Example 4: Multiple scoped operations in a loop
    std::cout << "\n  Running 5 scoped read operations..." << std::endl;
    for (int i = 0; i < 5; ++i) {
        TENT_SCOPED_READ_LATENCY(256 * 1024 * (i + 1));  // 256KB - 1.25MB
        std::this_thread::sleep_for(std::chrono::milliseconds(1 + i));
        std::cout << "    Scoped read " << (i + 1) << ": " << 256 * (i + 1)
                  << " KB with ~" << (1 + i) << "ms work" << std::endl;
    }

    // 5. Demonstrate runtime enable/disable switch (only if not started with
    // --disabled)
    std::cout << "\n--- Runtime Enable/Disable Switch ---" << std::endl;
    std::cout << "Current state: "
              << (TentMetrics::isEnabled() ? "enabled" : "disabled")
              << std::endl;

    if (!start_disabled) {
        // Disable metrics at runtime
        std::cout << "Disabling metrics collection..." << std::endl;
        TentMetrics::setEnabled(false);

        // These calls will return immediately with minimal overhead
        for (int i = 0; i < 1000; ++i) {
            TENT_RECORD_READ_COMPLETED(
                1024, 0.001);  // These are no-ops when disabled
        }
        std::cout << "  1000 record calls completed (no-op, metrics disabled)"
                  << std::endl;

        // Re-enable metrics
        std::cout << "Re-enabling metrics collection..." << std::endl;
        TentMetrics::setEnabled(true);

        // Now these will be recorded
        TENT_RECORD_READ_COMPLETED(1024 * 1024, 0.005);
        std::cout << "  Recorded 1 MB read after re-enabling" << std::endl;
    } else {
        std::cout << "  Skipping enable/disable demo (started with --disabled)"
                  << std::endl;
    }

    // 6. Display summary
    std::cout << "\n=== Metrics Summary ===" << std::endl;
    std::cout << TentMetrics::instance().getSummaryString() << std::endl;

    // 7. Display Prometheus format (includes latency histogram buckets)
    std::cout << "\n=== Prometheus Metrics (includes latency histograms) ==="
              << std::endl;
    std::string prometheus_metrics =
        TentMetrics::instance().getPrometheusMetrics();
    if (!prometheus_metrics.empty()) {
        std::cout << prometheus_metrics << std::endl;
    }

    // 8. Keep server running for testing (or exit immediately)
    if (server_mode) {
        std::cout << "\nServer mode: Press Ctrl+C to exit..." << std::endl;
        std::cout << "You can now query metrics via:" << std::endl;
        std::cout << "  curl http://localhost:" << config.http_port
                  << "/metrics          # Prometheus format" << std::endl;
        std::cout << "  curl http://localhost:" << config.http_port
                  << "/metrics/summary  # Human readable summary" << std::endl;
        std::cout << "  curl http://localhost:" << config.http_port
                  << "/metrics/json     # JSON format" << std::endl;
        std::cout << "  curl http://localhost:" << config.http_port
                  << "/health           # Health check" << std::endl;

        // Keep running until interrupted
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            // Simulate ongoing traffic
            TENT_RECORD_READ_COMPLETED(1024 * 1024, 0.001);
            TENT_RECORD_WRITE_COMPLETED(512 * 1024, 0.002);
        }
    }

    // 9. Cleanup
    TentMetrics::instance().shutdown();

    std::cout << "\nExample completed successfully!" << std::endl;
    return 0;
}
