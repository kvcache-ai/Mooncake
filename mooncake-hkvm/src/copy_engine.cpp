#include "hkvm/copy_engine.h"

#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"            // ReplicaType
#include "error.h"                // ERR_INVALID_ARGUMENT
#include "master_client.h"        // MasterClient
#include "replica.h"              // ReplicateConfig, Replica::Descriptor
#include "rpc_types.h"            // ObjectMeta
#include "transfer_engine.h"      // TransferEngine
#include "transport/transport.h"  // TransferRequest, TransferStatus
#include "types.h"                // generate_uuid, UUID

namespace mooncake {
namespace hkvm {

namespace {

constexpr const char* kLogTag = "[copy_engine]";

// ---------------------------------------------------------------------------
// Minimal HTTP/1.0 GET (raw sockets; same approach as diff_server.cpp).
// ---------------------------------------------------------------------------

bool WriteAll(int fd, const char* data, size_t len) {
  size_t sent = 0;
  while (sent < len) {
    ssize_t n = write(fd, data + sent, len - sent);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return false;
    sent += static_cast<size_t>(n);
  }
  return true;
}

bool HttpGetText(const std::string& host, int port, const std::string& path,
                 int timeout_ms, std::string& body, std::string& err) {
  struct addrinfo hints{};
  struct addrinfo* res = nullptr;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  const std::string port_str = std::to_string(port);
  if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0 || !res) {
    err = "getaddrinfo failed for " + host;
    return false;
  }
  int fd = -1;
  for (struct addrinfo* p = res; p; p = p->ai_next) {
    fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0) continue;
    if (connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) {
    err = "connect failed to " + host + ":" + port_str;
    return false;
  }
  timeval tv;
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  std::string request = "GET " + path + " HTTP/1.0\r\nHost: " + host +
                        "\r\nConnection: close\r\n\r\n";
  if (!WriteAll(fd, request.data(), request.size())) {
    err = "write failed";
    close(fd);
    return false;
  }
  std::string raw;
  char buf[8192];
  ssize_t n;
  while ((n = read(fd, buf, sizeof(buf))) > 0) {
    raw.append(buf, static_cast<size_t>(n));
  }
  close(fd);
  if (n < 0) {
    err = "read failed/timeout";
    return false;
  }
  size_t sep = raw.find("\r\n\r\n");
  if (sep == std::string::npos) {
    err = "malformed HTTP response";
    return false;
  }
  size_t sp = raw.find(' ');
  if (sp == std::string::npos || raw.compare(0, 5, "HTTP/") != 0) {
    err = "malformed HTTP status line";
    return false;
  }
  int status = std::atoi(raw.c_str() + sp + 1);
  if (status != 200) {
    err = "HTTP status " + std::to_string(status);
    return false;
  }
  body = raw.substr(sep + 4);
  return true;
}

// ---------------------------------------------------------------------------
// /query_key JSON parsing.
//   {"success":true,"data":[{"size_":N,"buffer_address_":A,
//                             "protocol_":"...","transport_endpoint_":"..."}, ...]}
// A minimal targeted extractor (the schema is fixed by HandleQueryKey).
// ---------------------------------------------------------------------------

struct ReplicaInfo {
  uint64_t size{0};
  uint64_t buffer_address{0};
  std::string protocol;
  std::string transport_endpoint;
};

bool ExtractString(const std::string& obj, const std::string& key,
                   std::string& out) {
  const std::string needle = "\"" + key + "\":\"";
  size_t k = obj.find(needle);
  if (k == std::string::npos) return false;
  size_t start = k + needle.size();
  size_t end = start;
  while (end < obj.size() && obj[end] != '"') {
    if (obj[end] == '\\' && end + 1 < obj.size())
      end += 2;
    else
      ++end;
  }
  if (end >= obj.size()) return false;
  out = obj.substr(start, end - start);
  return true;
}

bool ExtractUint(const std::string& obj, const std::string& key,
                 uint64_t& out) {
  const std::string needle = "\"" + key + "\":";
  size_t k = obj.find(needle);
  if (k == std::string::npos) return false;
  size_t start = k + needle.size();
  while (start < obj.size() && (obj[start] == ' ' || obj[start] == '\t'))
    ++start;
  if (start >= obj.size() || !std::isdigit(static_cast<unsigned char>(obj[start])))
    return false;
  char* endp = nullptr;
  out = std::strtoull(obj.c_str() + start, &endp, 10);
  return true;
}

bool ParseQueryKey(const std::string& body, std::vector<ReplicaInfo>& out) {
  out.clear();
  size_t data = body.find("\"data\":");
  if (data == std::string::npos) return false;
  size_t arr = body.find('[', data);
  if (arr == std::string::npos) return false;
  size_t p = arr + 1;
  while (p < body.size()) {
    size_t ob = body.find('{', p);
    if (ob == std::string::npos) break;
    size_t oe = body.find('}', ob);
    if (oe == std::string::npos) break;
    std::string obj = body.substr(ob, oe - ob + 1);
    ReplicaInfo r;
    ExtractUint(obj, "size_", r.size);
    ExtractUint(obj, "buffer_address_", r.buffer_address);
    ExtractString(obj, "protocol_", r.protocol);
    ExtractString(obj, "transport_endpoint_", r.transport_endpoint);
    if (r.size > 0 && !r.transport_endpoint.empty()) out.push_back(std::move(r));
    p = oe + 1;
    size_t next = body.find_first_of("]", p);
    if (next == std::string::npos) break;
  }
  return !out.empty();
}

// ---------------------------------------------------------------------------
// Synchronous single-request transfer (READ or WRITE) via the TransferEngine.
// `local_buf` must already be registered with the engine. `remote_offset` is
// the remote segment's buffer_address_ (used as target_offset, per the store's
// own transfer-task construction).
// ---------------------------------------------------------------------------

bool TransferOnce(TransferEngine& engine, TransferRequest::OpCode op,
                  void* local_buf, const std::string& endpoint,
                  uint64_t remote_offset, size_t length, int timeout_ms) {
  SegmentHandle seg = engine.openSegment(endpoint);
  if (seg == static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT)) return false;

  BatchID batch = engine.allocateBatchID(1);
  if (batch == INVALID_BATCH_ID) return false;

  TransferRequest req;
  req.opcode = op;
  req.source = local_buf;
  req.target_id = seg;
  req.target_offset = remote_offset;
  req.length = length;

  Status s = engine.submitTransfer(batch, {req});
  if (!s.ok()) {
    engine.freeBatchID(batch);
    return false;
  }

  bool ok = false;
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    TransferStatus st;
    if (engine.getTransferStatus(batch, 0, st).ok()) {
      if (st.s == TransferStatusEnum::COMPLETED) {
        ok = true;
        break;
      }
      if (st.s == TransferStatusEnum::FAILED ||
          st.s == TransferStatusEnum::TIMEOUT ||
          st.s == TransferStatusEnum::INVALID ||
          st.s == TransferStatusEnum::CANCELED) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  engine.freeBatchID(batch);
  return ok;
}

}  // namespace

// ---------------------------------------------------------------------------
// CopyEngine::Impl
// ---------------------------------------------------------------------------

struct CopyEngine::Impl {
  UUID client_id{generate_uuid()};
  std::unique_ptr<TransferEngine> engine;
  std::unique_ptr<MasterClient> clients[2];
};

CopyEngine::CopyEngine(CopyEngineConfig config, DiffServer& diff_server)
    : config_(std::move(config)), diff_server_(diff_server),
      impl_(std::make_unique<Impl>()) {}

CopyEngine::~CopyEngine() { Stop(); }

bool CopyEngine::Start() {
  if (running_) return true;
  if (config_.masters.size() < 2) {
    std::cerr << kLogTag << " requires >= 2 masters, got "
              << config_.masters.size() << "\n";
    return false;
  }
  if (config_.metadata_conn_string.empty()) {
    std::cerr << kLogTag << " metadata_conn_string is empty\n";
    return false;
  }

  impl_->engine = std::make_unique<TransferEngine>();
  if (impl_->engine->init(config_.metadata_conn_string,
                          config_.local_server_name, config_.local_hostname,
                          config_.transfer_rpc_port) != 0) {
    std::cerr << kLogTag << " TransferEngine::init failed\n";
    return false;
  }
  for (const auto& proto : config_.transports) {
    // tcp/nvlink take nullptr args; rdma would need device args. Only the
    // nullptr-using transports are wired here.
    if (!impl_->engine->installTransport(proto, nullptr)) {
      std::cerr << kLogTag << " installTransport(" << proto << ") failed\n";
    }
  }

  for (size_t i = 0; i < 2; ++i) {
    impl_->clients[i] =
        std::make_unique<MasterClient>(impl_->client_id, nullptr, "default");
    auto ec = impl_->clients[i]->Connect(config_.masters[i].master_addr);
    if (ec != ErrorCode::OK) {
      std::cerr << kLogTag << " connect to master" << i << " ("
                << config_.masters[i].master_addr << ") failed\n";
      return false;
    }
  }

  running_ = true;
  return true;
}

void CopyEngine::Stop() {
  if (!running_) return;
  running_ = false;
  if (impl_) {
    impl_->clients[0].reset();
    impl_->clients[1].reset();
    if (impl_->engine) {
      impl_->engine->freeEngine();
      impl_->engine.reset();
    }
  }
}

bool CopyEngine::CopyKey(const std::string& key, size_t src, size_t dst) {
  if (!running_ || !impl_ || !impl_->engine) return false;

  // 1. Source transfer descriptor from the source master's /query_key.
  std::string body, err;
  const auto& src_cfg = config_.masters[src];
  if (!HttpGetText(src_cfg.http_host, src_cfg.metrics_port,
                   "/query_key?key=" + key, config_.http_timeout_ms, body,
                   err)) {
    std::cerr << kLogTag << " /query_key failed for '" << key << "' on master"
              << src << ": " << err << "\n";
    return false;
  }
  std::vector<ReplicaInfo> src_replicas;
  if (!ParseQueryKey(body, src_replicas)) {
    std::cerr << kLogTag << " parse /query_key failed for '" << key << "'\n";
    return false;
  }
  const ReplicaInfo src_info = src_replicas.front();
  const size_t size = static_cast<size_t>(src_info.size);

  // 2. Allocate + register a local relay buffer.
  void* buf = nullptr;
  if (::posix_memalign(&buf, 4096, size) != 0 || !buf) {
    std::cerr << kLogTag << " alloc " << size << " bytes failed\n";
    return false;
  }
  if (impl_->engine->registerLocalMemory(buf, size) != 0) {
    std::cerr << kLogTag << " registerLocalMemory failed\n";
    std::free(buf);
    return false;
  }

  bool ok = false;
  // 3. READ: source replica (remote) -> local relay buffer.
  if (TransferOnce(*impl_->engine, TransferRequest::READ, buf,
                   src_info.transport_endpoint, src_info.buffer_address, size,
                   config_.transfer_timeout_ms)) {
    // 4. PutStart on the destination to allocate a destination buffer.
    ReplicateConfig cfg;
    cfg.replica_num = config_.replica_num;
    auto ps = impl_->clients[dst]->PutStart(key, std::vector<size_t>{size}, cfg);
    if (ps.has_value() && !ps->empty()) {
      bool wrote = false;
      for (const auto& rd : ps.value()) {
        if (!rd.is_memory_replica()) continue;
        const auto& bd = rd.get_memory_descriptor().buffer_descriptor;
        if (TransferOnce(*impl_->engine, TransferRequest::WRITE, buf,
                         bd.transport_endpoint_, bd.buffer_address_, size,
                         config_.transfer_timeout_ms)) {
          wrote = true;
          break;
        }
      }
      if (wrote) {
        // 5. Commit.
        auto pe = impl_->clients[dst]->PutEnd(ObjectMeta{key, std::nullopt},
                                              ReplicaType::MEMORY);
        ok = pe.has_value();
      } else {
        impl_->clients[dst]->PutRevoke(key);
      }
    }
  }

  impl_->engine->unregisterLocalMemory(buf);
  std::free(buf);
  if (!ok) {
    std::cerr << kLogTag << " copy '" << key << "' master" << src << "->master"
              << dst << " failed\n";
  }
  return ok;
}

CopyEngine::RunStats CopyEngine::RunOnce() {
  RunStats stats;
  if (!running_) return stats;

  const auto diff = diff_server_.GetDiff();
  for (const auto& key : diff.only_in_master1) {
    if (CopyKey(key, 0, 1))
      ++stats.copied_m1_to_m2;
    else
      ++stats.failed;
  }
  for (const auto& key : diff.only_in_master2) {
    if (CopyKey(key, 1, 0))
      ++stats.copied_m2_to_m1;
    else
      ++stats.failed;
  }

  // Clear the diff server's diff lists: recompute from ground truth so the
  // diff reflects the copies just performed (successful copies drop out;
  // any keys that failed to copy remain flagged on the next pass).
  diff_server_.Rebootstrap();
  return stats;
}

}  // namespace hkvm
}  // namespace mooncake
