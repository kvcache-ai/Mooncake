// mooncake_conductor entry point.
//
// Logging levels map to glog as
//   DEBUG -> INFO + VLOG(1) enabled
//   INFO  -> INFO,  WARN -> WARNING,  ERROR -> ERROR
// via FLAGS_minloglevel / FLAGS_v.

#include <glog/logging.h>

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <mutex>

#include "conductor/common/utils.h"
#include "conductor/kvevent/config.h"
#include "conductor/kvevent/event_manager.h"

namespace {

std::atomic<int> g_signal{0};
std::mutex g_shutdown_mu;
std::condition_variable g_shutdown_cv;

void HandleSignal(int sig) {
    g_signal.store(sig);
    g_shutdown_cv.notify_one();
}

}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging(argc > 0 ? argv[0] : "mooncake_conductor");
    FLAGS_logtostderr = true;

    // TODO: support conductor metrics
    const auto log_level = conductor::common::ParseLogLevel();
    switch (log_level) {
        case conductor::common::LogLevel::kDebug:
            FLAGS_minloglevel = google::GLOG_INFO;
            FLAGS_v = 1;
            break;
        case conductor::common::LogLevel::kInfo:
            FLAGS_minloglevel = google::GLOG_INFO;
            break;
        case conductor::common::LogLevel::kWarn:
            FLAGS_minloglevel = google::GLOG_WARNING;
            break;
        case conductor::common::LogLevel::kError:
            FLAGS_minloglevel = google::GLOG_ERROR;
            break;
    }

    LOG(INFO) << "Starting Conductor KV Event Manager (C++)..."
              << " logLevel=" << static_cast<int>(log_level);

    // httpServerPort defaults to 13333, overwritten by the
    // config file whenever it parses.
    int http_server_port = 13333;
    auto services = conductor::kvevent::ParseConfig(&http_server_port);

    conductor::kvevent::EventManager manager(std::move(services),
                                             http_server_port);

    if (!manager.StartHTTPServer()) {
        LOG(ERROR) << "Failed to start HTTP server";
    }

    manager.Start();

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    LOG(INFO) << "Manager is running. Press Ctrl+C to stop.";
    {
        std::unique_lock<std::mutex> lk(g_shutdown_mu);
        g_shutdown_cv.wait(lk, [] { return g_signal.load() != 0; });
    }

    LOG(INFO) << "Shutting down...";
    manager.Stop();
    return 0;
}
