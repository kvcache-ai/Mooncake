#include "hkvm/diff_server.h"

#include <msgpack.hpp>
#include <zmq.h>

#include <cerrno>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <iostream>
#include <string>
#include <vector>

namespace mooncake {
namespace hkvm {

namespace {

constexpr const char* kLogTag = "[diff_server]";

// ---------------------------------------------------------------------------
// msgpack field helpers
// ---------------------------------------------------------------------------

// Extracts a string field from a msgpack map object. Returns false if the key
// is absent or the value is not a string.
bool GetStrField(const msgpack::object_map& map, const std::string& key,
                 std::string& out) {
  for (uint32_t i = 0; i < map.size; ++i) {
    const auto& k = map.ptr[i].key;
    if (k.type != msgpack::type::STR) continue;
    if (k.via.str.size == key.size() &&
        std::memcmp(k.via.str.ptr, key.data(), key.size()) == 0) {
      const auto& v = map.ptr[i].val;
      if (v.type != msgpack::type::STR) return false;
      out.assign(v.via.str.ptr, v.via.str.size);
      return true;
    }
  }
  return false;
}

// Decodes the KV object key from an event map. Prefers the "object_key" field
// (present when the publisher is configured with emit_object_key=true, the
// master default); falls back to the first entry of "seq_hashes" stringified.
bool DecodeObjectKey(const msgpack::object_map& map, std::string& key) {
  std::string object_key;
  if (GetStrField(map, "object_key", object_key) && !object_key.empty()) {
    key = std::move(object_key);
    return true;
  }
  for (uint32_t i = 0; i < map.size; ++i) {
    const auto& k = map.ptr[i].key;
    if (k.type != msgpack::type::STR || k.via.str.size != 10 ||
        std::memcmp(k.via.str.ptr, "seq_hashes", 10) != 0)
      continue;
    const auto& v = map.ptr[i].val;
    if (v.type != msgpack::type::ARRAY || v.via.array.size == 0) return false;
    const auto& elem = v.via.array.ptr[0];
    if (elem.type == msgpack::type::POSITIVE_INTEGER ||
        elem.type == msgpack::type::NEGATIVE_INTEGER) {
      key = std::to_string(elem.via.u64);
      return true;
    }
    return false;
  }
  return false;
}

// ---------------------------------------------------------------------------
// ZMQ multipart receive
// ---------------------------------------------------------------------------

bool RecvMultipart(void* socket, std::vector<std::vector<uint8_t>>& frames) {
  frames.clear();
  while (true) {
    zmq_msg_t msg;
    if (zmq_msg_init(&msg) != 0) return false;
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (rc < 0) {
      zmq_msg_close(&msg);
      return false;
    }
    const uint8_t* data = static_cast<const uint8_t*>(zmq_msg_data(&msg));
    frames.emplace_back(data, data + zmq_msg_size(&msg));
    int more = zmq_msg_more(&msg);
    zmq_msg_close(&msg);
    if (!more) break;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Minimal HTTP/1.0 GET client (raw sockets, no external HTTP dependency).
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

// Issues "GET <path> HTTP/1.0" to host:port and returns the response body on
// HTTP 200. Uses Connection: close and reads until EOF, so no chunked/content
// -length parsing is needed. `timeout_ms` bounds recv latency.
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

  std::string request = "GET " + path +
                        " HTTP/1.0\r\nHost: " + host +
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
    err = "malformed HTTP response (no header/body separator)";
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

// Splits a newline-separated key body into individual keys (empty lines
// ignored). The master's /get_all_keys emits one key per line.
std::vector<std::string> SplitLines(const std::string& body) {
  std::vector<std::string> keys;
  size_t start = 0;
  while (start <= body.size()) {
    size_t nl = body.find('\n', start);
    if (nl == std::string::npos) {
      std::string line = body.substr(start);
      if (!line.empty()) keys.push_back(std::move(line));
      break;
    }
    std::string line = body.substr(start, nl - start);
    if (!line.empty()) keys.push_back(std::move(line));
    start = nl + 1;
  }
  return keys;
}

}  // namespace

// ---------------------------------------------------------------------------
// DiffServer
// ---------------------------------------------------------------------------

DiffServer::DiffServer(DiffServerConfig config)
    : config_(std::move(config)) {
  for (auto& m : config_.masters) {
    auto state = std::make_unique<MasterState>();
    state->config = m;
    masters_.push_back(std::move(state));
  }
}

DiffServer::~DiffServer() { Stop(); }

bool DiffServer::Start() {
  if (running_.load()) return true;
  if (masters_.size() < 2) {
    std::cerr << kLogTag << " requires >= 2 masters, got " << masters_.size()
              << "\n";
    return false;
  }

  zmq_context_ = zmq_ctx_new();
  if (!zmq_context_) {
    std::cerr << kLogTag << " zmq_ctx_new failed: " << zmq_strerror(zmq_errno())
              << "\n";
    return false;
  }

  for (size_t i = 0; i < masters_.size(); ++i) {
    auto& s = masters_[i];
    s->socket = zmq_socket(zmq_context_, ZMQ_SUB);
    if (!s->socket) {
      std::cerr << kLogTag << " zmq_socket failed for " << s->config.id << ": "
                << zmq_strerror(zmq_errno()) << "\n";
      CloseSockets();
      return false;
    }
    int hwm = config_.recv_hwm;
    zmq_setsockopt(s->socket, ZMQ_RCVHWM, &hwm, sizeof(hwm));
    int linger_ms = 0;
    zmq_setsockopt(s->socket, ZMQ_LINGER, &linger_ms, sizeof(linger_ms));
    if (zmq_setsockopt(s->socket, ZMQ_SUBSCRIBE, "", 0) != 0) {
      std::cerr << kLogTag << " ZMQ_SUBSCRIBE failed for " << s->config.id
                << ": " << zmq_strerror(zmq_errno()) << "\n";
      CloseSockets();
      return false;
    }
    if (zmq_connect(s->socket, s->config.endpoint.c_str()) != 0) {
      std::cerr << kLogTag << " zmq_connect failed for " << s->config.endpoint
                << ": " << zmq_strerror(zmq_errno()) << "\n";
      CloseSockets();
      return false;
    }
  }

  running_.store(true);

  // Start receivers in buffering mode so no event between SUB connect and the
  // snapshot is lost.
  for (size_t i = 0; i < masters_.size(); ++i) {
    masters_[i]->stop_flag.store(false);
    masters_[i]->thread = std::thread(&DiffServer::ReceiverLoop, this, i);
  }

  // Let SUB connections establish before snapshotting so the buffered replay
  // covers the snapshot boundary (see class doc).
  if (config_.snapshot_settle_ms > 0) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(config_.snapshot_settle_ms));
  }

  for (size_t i = 0; i < masters_.size(); ++i) {
    BootstrapSnapshot(i);
  }
  return true;
}

void DiffServer::Stop() {
  if (!running_.load()) return;
  running_.store(false);
  for (auto& s : masters_) s->stop_flag.store(true);
  for (auto& s : masters_) {
    if (s->thread.joinable()) s->thread.join();
  }
  CloseSockets();
}

void DiffServer::CloseSockets() {
  for (auto& s : masters_) {
    if (s->socket) {
      zmq_close(s->socket);
      s->socket = nullptr;
    }
  }
  if (zmq_context_) {
    zmq_ctx_destroy(zmq_context_);
    zmq_context_ = nullptr;
  }
}

void DiffServer::Rebootstrap() {
  if (!running_.load()) return;
  // Flip receivers back into buffering mode (under each mutex so the receiver
  // observes the change consistently). Events arriving during the snapshot are
  // captured in `pending` and replayed after seeding — same correctness
  // argument as the initial bootstrap (see class doc).
  for (auto& s : masters_) {
    std::lock_guard<std::mutex> lock(s->mutex);
    s->buffering = true;
  }
  if (config_.snapshot_settle_ms > 0) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(config_.snapshot_settle_ms));
  }
  for (size_t i = 0; i < masters_.size(); ++i) {
    BootstrapSnapshot(i);
  }
}

void DiffServer::ReceiverLoop(size_t index) {
  MasterState& s = *masters_[index];
  while (!s.stop_flag.load()) {
    zmq_pollitem_t item;
    item.socket = s.socket;
    item.events = ZMQ_POLLIN;
    item.fd = 0;
    item.revents = 0;
    int rc = zmq_poll(&item, 1, config_.poll_timeout_ms);
    if (rc < 0) {
      if (zmq_errno() == ETERM) break;  // context torn down during shutdown
      continue;
    }
    if (rc == 0) continue;  // timeout, re-check stop_flag

    std::vector<std::vector<uint8_t>> frames;
    if (!RecvMultipart(s.socket, frames)) continue;

    // Expected frames: [topic (empty)] [seq, 8 bytes BE] [msgpack payload].
    if (frames.size() < 3) {
      s.malformed_events.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    if (frames[1].size() == sizeof(uint64_t)) {
      uint64_t seq;
      std::memcpy(&seq, frames[1].data(), sizeof(seq));
      seq = be64toh(seq);
      bool had = s.has_last_seq.load(std::memory_order_relaxed);
      uint64_t last = s.last_seq.load(std::memory_order_relaxed);
      if (had && seq > last + 1) {
        s.seq_gaps.fetch_add(seq - last - 1, std::memory_order_relaxed);
      }
      s.last_seq.store(seq, std::memory_order_relaxed);
      s.has_last_seq.store(true, std::memory_order_relaxed);
    }

    ApplyEvent(s, frames[2].data(), frames[2].size());
  }
}

bool DiffServer::ApplyEvent(MasterState& s, const void* payload, size_t size) {
  try {
    msgpack::object_handle oh =
        msgpack::unpack(static_cast<const char*>(payload), size);
    const msgpack::object& root = oh.get();
    if (root.type != msgpack::type::ARRAY || root.via.array.size < 2) {
      s.malformed_events.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    const msgpack::object& events_obj = root.via.array.ptr[1];
    if (events_obj.type != msgpack::type::ARRAY) {
      s.malformed_events.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    for (uint32_t i = 0; i < events_obj.via.array.size; ++i) {
      const msgpack::object& ev = events_obj.via.array.ptr[i];
      if (ev.type != msgpack::type::MAP) {
        s.malformed_events.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      const auto& map = ev.via.map;

      std::string event_type;
      if (!GetStrField(map, "event_type", event_type)) {
        s.malformed_events.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      bool stored;
      if (event_type == "stored") {
        stored = true;
      } else if (event_type == "removed") {
        stored = false;
      } else {
        s.malformed_events.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      if (!s.config.backend_id_filter.empty()) {
        std::string backend_id;
        if (!GetStrField(map, "backend_id", backend_id) ||
            backend_id != s.config.backend_id_filter) {
          continue;  // belongs to a different backend on the same socket
        }
      }

      std::string object_key;
      if (!DecodeObjectKey(map, object_key)) {
        s.malformed_events.fetch_add(1, std::memory_order_relaxed);
        continue;
      }

      s.events_received.fetch_add(1, std::memory_order_relaxed);
      if (stored) {
        s.stored_events.fetch_add(1, std::memory_order_relaxed);
      } else {
        s.removed_events.fetch_add(1, std::memory_order_relaxed);
      }
      RecordEvent(s, stored, object_key);
    }
    return true;
  } catch (const std::exception&) {
    s.malformed_events.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
}

void DiffServer::RecordEvent(MasterState& s, bool stored,
                             const std::string& key) {
  std::lock_guard<std::mutex> lock(s.mutex);
  if (s.buffering) {
    s.pending.push_back(MasterState::PendingEvent{stored, key});
  } else {
    if (stored) {
      s.live_keys.insert(key);
    } else {
      s.live_keys.erase(key);
    }
  }
}

void DiffServer::BootstrapSnapshot(size_t index) {
  MasterState& s = *masters_[index];
  std::vector<std::string> keys;

  if (s.config.http_host.empty() || s.config.metrics_port <= 0) {
    std::cerr << kLogTag << " " << s.config.id
              << ": no snapshot configured, running event-only\n";
    DrainAndGoLive(s, std::move(keys));
    return;
  }

  std::string body, err;
  if (!HttpGetText(s.config.http_host, s.config.metrics_port, "/get_all_keys",
                   config_.snapshot_timeout_ms, body, err)) {
    std::cerr << kLogTag << " " << s.config.id
              << ": snapshot fetch failed (" << err << "), event-only fallback\n";
    DrainAndGoLive(s, std::move(keys));
    return;
  }
  keys = SplitLines(body);
  std::cerr << kLogTag << " " << s.config.id << ": snapshot seeded "
            << keys.size() << " keys\n";
  DrainAndGoLive(s, std::move(keys));
}

void DiffServer::DrainAndGoLive(MasterState& s,
                                std::vector<std::string> snapshot_keys) {
  std::lock_guard<std::mutex> lock(s.mutex);
  // Seed from the snapshot (master state as-of request time T_q).
  s.live_keys.clear();
  for (auto& k : snapshot_keys) s.live_keys.insert(std::move(k));
  s.snapshot_key_count.store(s.live_keys.size(),
                             std::memory_order_relaxed);
  // Replay all events buffered since SUB connect (T_c < T_q), in arrival
  // order. This reconciles the snapshot with concurrent mutations.
  for (const auto& e : s.pending) {
    if (e.stored) {
      s.live_keys.insert(e.key);
    } else {
      s.live_keys.erase(e.key);
    }
  }
  s.pending.clear();
  s.buffering = false;
  s.bootstrapped.store(true, std::memory_order_relaxed);
}

DiffServer::DiffResult DiffServer::GetDiff() const {
  DiffResult result;
  if (masters_.size() < 2) return result;

  // Lock in fixed index order (0 then 1) to avoid deadlock with concurrent
  // GetDiff() calls. Receiver threads only ever lock a single master.
  std::lock_guard<std::mutex> l0(masters_[0]->mutex);
  std::lock_guard<std::mutex> l1(masters_[1]->mutex);

  const auto& a = masters_[0]->live_keys;
  const auto& b = masters_[1]->live_keys;
  result.master1_key_count = a.size();
  result.master2_key_count = b.size();
  for (const auto& k : a) {
    if (b.find(k) == b.end()) result.only_in_master1.push_back(k);
  }
  for (const auto& k : b) {
    if (a.find(k) == a.end()) result.only_in_master2.push_back(k);
  }
  std::sort(result.only_in_master1.begin(), result.only_in_master1.end());
  std::sort(result.only_in_master2.begin(), result.only_in_master2.end());
  return result;
}

std::vector<DiffServer::MasterStats> DiffServer::GetStats() const {
  std::vector<MasterStats> out;
  out.reserve(masters_.size());
  for (const auto& s : masters_) {
    MasterStats st;
    st.events_received = s->events_received.load(std::memory_order_relaxed);
    st.stored_events = s->stored_events.load(std::memory_order_relaxed);
    st.removed_events = s->removed_events.load(std::memory_order_relaxed);
    st.malformed_events = s->malformed_events.load(std::memory_order_relaxed);
    st.seq_gaps = s->seq_gaps.load(std::memory_order_relaxed);
    st.last_seq = s->last_seq.load(std::memory_order_relaxed);
    st.has_last_seq = s->has_last_seq.load(std::memory_order_relaxed);
    st.snapshot_key_count =
        s->snapshot_key_count.load(std::memory_order_relaxed);
    st.bootstrapped = s->bootstrapped.load(std::memory_order_relaxed);
    out.push_back(st);
  }
  return out;
}

}  // namespace hkvm
}  // namespace mooncake
