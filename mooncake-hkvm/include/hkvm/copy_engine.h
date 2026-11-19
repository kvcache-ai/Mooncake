#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "hkvm/diff_server.h"

namespace mooncake {
namespace hkvm {

// Description of one master as seen by the CopyEngine: it needs the RPC
// address (to PutStart/PutEnd on the destination) and the HTTP metrics
// endpoint (to query /query_key on the source for transfer descriptors).
struct CopyMasterConfig {
  std::string id;
  std::string master_addr;  // master RPC address IP:Port (MasterClient::Connect)
  std::string http_host;    // metrics/admin HTTP host (for /query_key)
  int metrics_port{0};      // metrics_port (default 9003 on the master)
};

struct CopyEngineConfig {
  std::vector<CopyMasterConfig> masters;  // exactly 2 entries
  // TransferEngine bootstrap. The engine must share the same metadata server
  // as the masters so it can resolve their segments.
  std::string metadata_conn_string;  // e.g. "etcd://host:2379"
  std::string local_server_name;     // unique name for this engine instance
  std::string local_hostname;        // advertised hostname/IP for TE init
  uint64_t transfer_rpc_port{12345};
  // Transports to install on the engine (must cover the protocols advertised
  // by the masters' segments, e.g. {"tcp"} or {"rdma","tcp"}).
  std::vector<std::string> transports{"tcp"};
  int transfer_timeout_ms{30000};
  int http_timeout_ms{5000};
  // Replication factor used for PutStart on the destination master.
  size_t replica_num{1};
};

// Reconciles two masters' KV stores toward identity by consuming the
// DiffServer's two diff lists.
//
// For each key only in master1 it queries master1's /query_key for the source
// replica descriptor, relays the bytes through a local registered buffer
// (READ remote->local, then WRITE local->remote into a PutStart-allocated
// destination buffer), and commits with PutEnd. Keys only in master2 are
// copied the other way. After a pass it calls DiffServer::Rebootstrap() to
// clear the diff lists from ground truth.
class CopyEngine {
 public:
  CopyEngine(CopyEngineConfig config, DiffServer& diff_server);
  ~CopyEngine();

  CopyEngine(const CopyEngine&) = delete;
  CopyEngine& operator=(const CopyEngine&) = delete;

  // Initializes the TransferEngine, installs transports, and connects a
  // MasterClient to each master. Returns false on failure.
  bool Start();
  void Stop();
  bool running() const { return running_; }

  struct RunStats {
    size_t copied_m1_to_m2{0};
    size_t copied_m2_to_m1{0};
    size_t failed{0};
  };
  // One reconciliation pass: copy each diff key toward identity, then
  // Rebootstrap the DiffServer to clear its diff lists.
  RunStats RunOnce();

  // Copy a single key from masters[src] to masters[dst]. Public so callers can
  // drive individual copies. Returns true on successful PutEnd.
  bool CopyKey(const std::string& key, size_t src, size_t dst);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  CopyEngineConfig config_;
  DiffServer& diff_server_;
  bool running_{false};
};

}  // namespace hkvm
}  // namespace mooncake
