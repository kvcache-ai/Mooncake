// Example CLI driver for the HKVM DiffServer.
//
// Subscribes to two Mooncake masters' KV-event streams, bootstraps each from
// /get_all_keys, and periodically prints the symmetric difference plus stats.
//
//   ./mooncake_hkvm_diff_example \
//     --master1_zmq=tcp://10.0.0.1:5557 --master1_host=10.0.0.1 --master1_port=9003 \
//     --master2_zmq=tcp://10.0.0.2:5557 --master2_host=10.0.0.2 --master2_port=9003 \
//     --interval=5

#include "hkvm/diff_server.h"

#include <csignal>
#include <cstdlib>
#include <chrono>
#include <atomic>
#include <iostream>
#include <string>
#include <thread>

namespace {
std::atomic<bool> g_stop{false};

void OnSignal(int) { g_stop.store(true); }

std::string GetArg(int argc, char** argv, const std::string& key,
                   const std::string& def) {
  const std::string prefix = "--" + key + "=";
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind(prefix, 0) == 0) return a.substr(prefix.size());
  }
  return def;
}

int GetIntArg(int argc, char** argv, const std::string& key, int def) {
  return std::atoi(GetArg(argc, argv, key, std::to_string(def)).c_str());
}

void PrintKeyList(const char* label, const std::vector<std::string>& keys,
                  int max_print) {
  std::cout << label << ": " << keys.size() << "\n";
  int shown = 0;
  for (const auto& k : keys) {
    if (shown >= max_print) break;
    std::cout << "  + " << k << "\n";
    ++shown;
  }
  if (static_cast<int>(keys.size()) > max_print) {
    std::cout << "  ... (" << (keys.size() - max_print) << " more)\n";
  }
}
}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  using mooncake::hkvm::DiffMasterConfig;
  using mooncake::hkvm::DiffServerConfig;
  using mooncake::hkvm::DiffServer;

  DiffMasterConfig m1;
  m1.id = "master1";
  m1.endpoint = GetArg(argc, argv, "master1_zmq", "tcp://127.0.0.1:5557");
  m1.http_host = GetArg(argc, argv, "master1_host", "127.0.0.1");
  m1.metrics_port = GetIntArg(argc, argv, "master1_port", 9003);

  DiffMasterConfig m2;
  m2.id = "master2";
  m2.endpoint = GetArg(argc, argv, "master2_zmq", "tcp://127.0.0.1:5558");
  m2.http_host = GetArg(argc, argv, "master2_host", "127.0.0.1");
  m2.metrics_port = GetIntArg(argc, argv, "master2_port", 9004);

  DiffServerConfig cfg;
  cfg.masters.push_back(m1);
  cfg.masters.push_back(m2);
  cfg.snapshot_settle_ms = GetIntArg(argc, argv, "settle_ms", 300);
  const int interval = GetIntArg(argc, argv, "interval", 5);
  const int max_keys = GetIntArg(argc, argv, "max_keys", 20);

  std::cerr << "[diff_server_example] master1 zmq=" << m1.endpoint << " http="
            << m1.http_host << ":" << m1.metrics_port << "\n";
  std::cerr << "[diff_server_example] master2 zmq=" << m2.endpoint << " http="
            << m2.http_host << ":" << m2.metrics_port << "\n";

  DiffServer server(std::move(cfg));
  if (!server.Start()) {
    std::cerr << "[diff_server_example] Start failed\n";
    return 1;
  }

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    const auto diff = server.GetDiff();
    std::cout << "=== diff ===\n";
    std::cout << "master1 keys: " << diff.master1_key_count
              << ", master2 keys: " << diff.master2_key_count << "\n";
    PrintKeyList("only_in_master1", diff.only_in_master1, max_keys);
    PrintKeyList("only_in_master2", diff.only_in_master2, max_keys);

    const auto stats = server.GetStats();
    for (size_t i = 0; i < stats.size(); ++i) {
      std::cout << "master" << (i + 1)
                << " stats: recv=" << stats[i].events_received
                << " stored=" << stats[i].stored_events
                << " removed=" << stats[i].removed_events
                << " malformed=" << stats[i].malformed_events
                << " seq_gaps=" << stats[i].seq_gaps
                << " snapshot=" << stats[i].snapshot_key_count
                << " bootstrapped=" << stats[i].bootstrapped << "\n";
    }
    std::cout.flush();
  }

  server.Stop();
  return 0;
}
