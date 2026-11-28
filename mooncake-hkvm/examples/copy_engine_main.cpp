// Example CLI driver running the HKVM DiffServer + CopyEngine together.
//
// It bootstraps a DiffServer against two masters, then repeatedly runs a
// CopyEngine pass to reconcile their KV stores toward identity.
//
//   ./mooncake_hkvm_copy_example \
//     --master1_zmq=tcp://10.0.0.1:5557 --master1_rpc=10.0.0.1:50051 \
//     --master1_host=10.0.0.1 --master1_port=9003 \
//     --master2_zmq=tcp://10.0.0.2:5557 --master2_rpc=10.0.0.2:50051 \
//     --master2_host=10.0.0.2 --master2_port=9003 \
//     --metadata=etcd://10.0.0.1:2379 --local_name=copy-engine-1 \
//     --local_host=10.0.0.1 --interval=10

#include "hkvm/copy_engine.h"
#include "hkvm/diff_server.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
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
}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  using mooncake::hkvm::CopyEngine;
  using mooncake::hkvm::CopyEngineConfig;
  using mooncake::hkvm::CopyMasterConfig;
  using mooncake::hkvm::DiffServer;
  using mooncake::hkvm::DiffServerConfig;
  using mooncake::hkvm::DiffMasterConfig;

  // --- DiffServer config (event stream + snapshot) ---
  DiffMasterConfig dm1, dm2;
  dm1.id = "master1";
  dm1.endpoint = GetArg(argc, argv, "master1_zmq", "tcp://127.0.0.1:5557");
  dm1.http_host = GetArg(argc, argv, "master1_host", "127.0.0.1");
  dm1.metrics_port = GetIntArg(argc, argv, "master1_port", 9003);
  dm2.id = "master2";
  dm2.endpoint = GetArg(argc, argv, "master2_zmq", "tcp://127.0.0.1:5558");
  dm2.http_host = GetArg(argc, argv, "master2_host", "127.0.0.1");
  dm2.metrics_port = GetIntArg(argc, argv, "master2_port", 9004);

  DiffServerConfig dcfg;
  dcfg.masters = {dm1, dm2};
  dcfg.snapshot_settle_ms = GetIntArg(argc, argv, "settle_ms", 300);

  DiffServer diff_server(std::move(dcfg));
  if (!diff_server.Start()) {
    std::cerr << "[copy_example] DiffServer start failed\n";
    return 1;
  }

  // --- CopyEngine config (RPC + /query_key + TransferEngine) ---
  CopyMasterConfig cm1, cm2;
  cm1.id = "master1";
  cm1.master_addr = GetArg(argc, argv, "master1_rpc", "127.0.0.1:50051");
  cm1.http_host = dm1.http_host;
  cm1.metrics_port = dm1.metrics_port;
  cm2.id = "master2";
  cm2.master_addr = GetArg(argc, argv, "master2_rpc", "127.0.0.1:50052");
  cm2.http_host = dm2.http_host;
  cm2.metrics_port = dm2.metrics_port;

  CopyEngineConfig ccfg;
  ccfg.masters = {cm1, cm2};
  ccfg.metadata_conn_string = GetArg(argc, argv, "metadata", "");
  ccfg.local_server_name = GetArg(argc, argv, "local_name", "copy-engine");
  ccfg.local_hostname = GetArg(argc, argv, "local_host", "127.0.0.1");
  ccfg.transfer_rpc_port = GetIntArg(argc, argv, "transfer_port", 12345);
  ccfg.transports = {GetArg(argc, argv, "transport", "tcp")};
  const int interval = GetIntArg(argc, argv, "interval", 10);

  CopyEngine copy_engine(std::move(ccfg), diff_server);
  if (!copy_engine.Start()) {
    std::cerr << "[copy_example] CopyEngine start failed\n";
    diff_server.Stop();
    return 1;
  }

  std::cerr << "[copy_example] running; interval=" << interval << "s\n";
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval));
    if (g_stop.load()) break;

    const auto before = diff_server.GetDiff();
    const auto stats = copy_engine.RunOnce();
    const auto after = diff_server.GetDiff();

    std::cout << "=== reconcile pass ===\n";
    std::cout << "copied m1->m2: " << stats.copied_m1_to_m2
              << ", m2->m1: " << stats.copied_m2_to_m1
              << ", failed: " << stats.failed << "\n";
    std::cout << "diff before: only_m1=" << before.only_in_master1.size()
              << " only_m2=" << before.only_in_master2.size()
              << " | after: only_m1=" << after.only_in_master1.size()
              << " only_m2=" << after.only_in_master2.size() << "\n";
    std::cout.flush();
  }

  copy_engine.Stop();
  diff_server.Stop();
  return 0;
}
