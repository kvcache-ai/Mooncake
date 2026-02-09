// ============================================================================
// KV Auto Converter - C2C cross-model projection
// ============================================================================
// Based on thu-nics/C2C: MLP projector + gated residual
// Formula: output = gate * sigmoid(scalar) * projector(source_kv)
// Optimized with OpenBLAS + AVX2 SIMD
// ============================================================================
#include "kv_auto_converter.h"

#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <json/reader.h>
#include <json/value.h>

#ifdef USE_CURL
#include <curl/curl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <sstream>
#include <iomanip>
#endif

#ifdef USE_OPENBLAS
#include <cblas.h>
#endif

// SIMD support detection
#if defined(__AVX2__)
#include <immintrin.h>
#define USE_AVX2 1
#endif

namespace mooncake {

// ============================================================================
// Matrix transpose: [rows, cols] -> [cols, rows]
// Done once at load time so runtime sgemv uses CblasNoTrans (row-contiguous)
// ============================================================================
static void transpose_matrix(std::vector<float>& mat, int rows, int cols) {
    std::vector<float> tmp(mat.size());
    for (int r = 0; r < rows; ++r) {
        for (int c = 0; c < cols; ++c) {
            tmp[c * rows + r] = mat[r * cols + c];
        }
    }
    mat = std::move(tmp);
}

KVAutoConverter& KVAutoConverter::instance() {
    static KVAutoConverter inst;
    return inst;
}

// ============================================================================
// Cache directory + URL hashing utilities
// ============================================================================
#ifdef USE_CURL
std::string KVAutoConverter::get_cache_dir() {
    std::string home;
    const char* h = std::getenv("HOME");
    if (h)
        home = h;
    else
        home = "/tmp";
    std::string dir = home + "/.cache/mooncake/c2c";
    // mkdir -p equivalent (EEXIST is expected and harmless)
    mkdir((home + "/.cache").c_str(), 0755);
    mkdir((home + "/.cache/mooncake").c_str(), 0755);
    if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        LOG(WARNING) << "[C2C] Failed to create cache dir: " << dir << " ("
                     << strerror(errno) << ")";
    }
    return dir;
}

// Simple hash: djb2 -> hex prefix (no OpenSSL dependency)
std::string KVAutoConverter::url_to_cache_path(const std::string& url) {
    uint64_t hash = 5381;
    for (char c : url) hash = ((hash << 5) + hash) + static_cast<uint8_t>(c);
    // Extract filename from URL
    std::string fname = "projector.bin";
    size_t slash = url.rfind('/');
    if (slash != std::string::npos && slash + 1 < url.size()) {
        fname = url.substr(slash + 1);
        // Strip query params
        size_t q = fname.find('?');
        if (q != std::string::npos) fname = fname.substr(0, q);
    }
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(16) << hash;
    return get_cache_dir() + "/" + oss.str() + "_" + fname;
}

// libcurl write-to-string callback
static size_t curl_string_cb(void* ptr, size_t size, size_t nmemb,
                             void* userdata) {
    auto* s = static_cast<std::string*>(userdata);
    s->append(static_cast<const char*>(ptr), size * nmemb);
    return size * nmemb;
}

// ============================================================================
// Multi-threaded parallel download with progress bar
// ============================================================================
// Strategy: HEAD to get Content-Length + Accept-Ranges, then split into N
// chunks downloaded in parallel threads. Fallback to single-thread if server
// doesn't support Range requests.
// ============================================================================

// Shared progress state across all download threads
struct MultiDownloadProgress {
    std::atomic<int64_t> downloaded{0};
    int64_t total{0};
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point last_update;
    int64_t last_downloaded{0};  // bytes at last speed sample
    double current_speed{0.0};   // smoothed instantaneous speed (MB/s)
    std::mutex mtx;
};

static void print_progress_bar(MultiDownloadProgress& prog,
                               bool final = false) {
    auto now = std::chrono::steady_clock::now();
    int64_t dl = prog.downloaded.load(std::memory_order_relaxed);
    int64_t total = prog.total;

    // Instantaneous speed: bytes since last update / time since last update
    // Exponential moving average (alpha=0.3) for smooth display
    double dt = std::chrono::duration<double>(now - prog.last_update).count();
    if (dt > 0.1) {
        double instant = (dl - prog.last_downloaded) / 1048576.0 / dt;
        prog.current_speed = prog.current_speed * 0.7 + instant * 0.3;
        prog.last_downloaded = dl;
        prog.last_update = now;
    }

    if (total > 0) {
        // Known size: percentage bar
        int pct = static_cast<int>(dl * 100 / total);
        int filled = pct / 2;
        fprintf(stderr, "\r[C2C] [");
        for (int i = 0; i < 50; ++i) fputc(i < filled ? '#' : '.', stderr);
        fprintf(stderr, "] %3d%% %lld/%lld MB  %.1f MB/s", pct,
                (long long)(dl >> 20), (long long)(total >> 20),
                prog.current_speed);
    } else {
        // Unknown size: spinner with downloaded bytes
        static const char spin[] = "|/-\\";
        static int idx = 0;
        fprintf(stderr, "\r[C2C] %c  %lld MB  %.1f MB/s", spin[(idx++) & 3],
                (long long)(dl >> 20), prog.current_speed);
    }

    if (final) fputc('\n', stderr);
    fflush(stderr);
}

// Per-chunk write callback: write to file at offset, update global progress
struct ChunkWriter {
    int fd;
    int64_t offset;
    MultiDownloadProgress* prog;
};

static size_t chunk_write_cb(void* ptr, size_t size, size_t nmemb,
                             void* userdata) {
    auto* cw = static_cast<ChunkWriter*>(userdata);
    size_t bytes = size * nmemb;
    ssize_t written = pwrite(cw->fd, ptr, bytes, cw->offset);
    if (written <= 0) return 0;
    cw->offset += written;
    cw->prog->downloaded.fetch_add(written, std::memory_order_relaxed);
    return static_cast<size_t>(written);
}

// Single-thread fallback write callback
struct SingleWriter {
    std::ofstream* out;
    MultiDownloadProgress* prog;
};

static size_t single_write_cb(void* ptr, size_t size, size_t nmemb,
                              void* userdata) {
    auto* sw = static_cast<SingleWriter*>(userdata);
    size_t bytes = size * nmemb;
    sw->out->write(static_cast<const char*>(ptr), bytes);
    if (!sw->out->good()) return 0;
    sw->prog->downloaded.fetch_add(bytes, std::memory_order_relaxed);
    return bytes;
}

// Progress callback: just renders the bar, actual byte counting is in write cb
static int multi_progress_cb(void* clientp, curl_off_t /*dltotal*/,
                             curl_off_t /*dlnow*/, curl_off_t /*ultotal*/,
                             curl_off_t /*ulnow*/) {
    auto* prog = static_cast<MultiDownloadProgress*>(clientp);
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(prog->mtx);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now - prog->last_update)
                  .count();
    if (ms >= 500) {
        print_progress_bar(*prog);
        prog->last_update = now;
    }
    return 0;
}

// Parse Content-Range header to extract total file size
// Format: "Content-Range: bytes 0-0/123456789"
static size_t range_header_cb(char* buf, size_t sz, size_t n, void* ud) {
    size_t len = sz * n;
    auto* total = static_cast<int64_t*>(ud);
    if (len > 22 && strncasecmp(buf, "Content-Range:", 14) == 0) {
        const char* slash = static_cast<const char*>(memchr(buf, '/', len));
        if (slash) *total = strtoll(slash + 1, nullptr, 10);
    }
    return len;
}

// Probe file size via Range request (more reliable than HEAD)
// GET with Range: bytes=0-0 → 206 + Content-Range: bytes 0-0/TOTAL
static bool probe_file_size(const std::string& url, int64_t& file_size) {
    CURL* curl = curl_easy_init();
    if (!curl) return false;

    std::string dummy;
    file_size = -1;

    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "mooncake-c2c/1.0");
    curl_easy_setopt(curl, CURLOPT_RANGE, "0-0");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_string_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &dummy);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, range_header_cb);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &file_size);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 15L);

    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);

    // 206 = server supports Range, file_size parsed from Content-Range
    if (res == CURLE_OK && http_code == 206 && file_size > 0) {
        LOG(INFO) << "[C2C] Probed file size: " << (file_size >> 20) << " MB";
        return true;
    }
    return false;
}
#else
std::string KVAutoConverter::get_cache_dir() { return "/tmp"; }
std::string KVAutoConverter::url_to_cache_path(const std::string&) {
    return "";
}
#endif

// ============================================================================
// Download file from URL (multi-threaded parallel, skip if cached)
// ============================================================================
static constexpr int kDownloadThreads = 4;
static constexpr int64_t kMinChunkSize = 4 * 1024 * 1024;  // 4MB min per chunk

bool KVAutoConverter::download_file(const std::string& url,
                                    const std::string& local_path) {
#ifdef USE_CURL
    // Cache hit
    struct stat st;
    if (stat(local_path.c_str(), &st) == 0 && st.st_size > 0) {
        LOG(INFO) << "[C2C] Cache hit: " << local_path;
        return true;
    }

    LOG(INFO) << "[C2C] Downloading: " << url;
    std::string tmp_path = local_path + ".tmp";

    // Probe: Range GET to get file size and confirm range support
    int64_t file_size = 0;
    bool can_parallel = probe_file_size(url, file_size);

    int num_threads = 1;
    if (can_parallel && file_size >= kMinChunkSize * 2) {
        num_threads = kDownloadThreads;
    }

    MultiDownloadProgress prog;
    prog.total = file_size > 0 ? file_size : 0;
    prog.start = std::chrono::steady_clock::now();
    prog.last_update = prog.start;

    if (num_threads <= 1) {
        // ================================================================
        // Single-thread fallback
        // ================================================================
        std::ofstream out(tmp_path, std::ios::binary);
        if (!out.is_open()) {
            LOG(ERROR) << "[C2C] Cannot create temp file: " << tmp_path;
            return false;
        }

        CURL* curl = curl_easy_init();
        if (!curl) return false;

        SingleWriter sw{&out, &prog};
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_USERAGENT, "mooncake-c2c/1.0");
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, single_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &sw);
        curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT,
                         1024L);  // abort if < 1KB/s
        curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 30L);  // for 30 seconds
        curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
        curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, multi_progress_cb);
        curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &prog);
        curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        out.close();
        print_progress_bar(prog, true);

        if (res != CURLE_OK) {
            LOG(ERROR) << "[C2C] Download failed: " << url << " ("
                       << curl_easy_strerror(res) << ")";
            std::remove(tmp_path.c_str());
            return false;
        }
    } else {
        // ================================================================
        // Multi-threaded range download
        // ================================================================
        LOG(INFO) << "[C2C] Parallel download: " << num_threads << " threads, "
                  << (file_size >> 20) << " MB";

        // Pre-allocate file
        int fd = open(tmp_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0) {
            LOG(ERROR) << "[C2C] Cannot create temp file: " << tmp_path;
            return false;
        }
        if (ftruncate(fd, file_size) != 0) {
            LOG(ERROR) << "[C2C] ftruncate failed: " << tmp_path;
            close(fd);
            return false;
        }

        int64_t chunk_size = file_size / num_threads;
        std::atomic<bool> any_failed{false};
        std::vector<std::thread> threads;
        std::vector<ChunkWriter> writers(num_threads);

        for (int i = 0; i < num_threads; ++i) {
            int64_t start = i * chunk_size;
            int64_t end =
                (i == num_threads - 1) ? file_size - 1 : start + chunk_size - 1;
            writers[i] = {fd, start, &prog};

            threads.emplace_back([&, i, start, end]() {
                static constexpr int kMaxRetries = 3;
                int64_t cur_offset = start;

                for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
                    if (attempt > 0) {
                        LOG(WARNING) << "[C2C] Chunk " << i << " retry "
                                     << attempt << "/" << kMaxRetries
                                     << " from offset " << cur_offset;
                    }

                    CURL* curl = curl_easy_init();
                    if (!curl) {
                        any_failed = true;
                        return;
                    }

                    // Resume from where we left off
                    char range_buf[64];
                    snprintf(range_buf, sizeof(range_buf), "%lld-%lld",
                             (long long)cur_offset, (long long)end);
                    writers[i].offset = cur_offset;

                    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
                    curl_easy_setopt(curl, CURLOPT_USERAGENT,
                                     "mooncake-c2c/1.0");
                    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION,
                                     CURL_HTTP_VERSION_1_1);
                    curl_easy_setopt(curl, CURLOPT_RANGE, range_buf);
                    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION,
                                     chunk_write_cb);
                    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &writers[i]);
                    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
                    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
                    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 30L);
                    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
                    curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION,
                                     multi_progress_cb);
                    curl_easy_setopt(curl, CURLOPT_XFERINFODATA, &prog);
                    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);

                    CURLcode res = curl_easy_perform(curl);
                    cur_offset = writers[i].offset;  // update resume point
                    curl_easy_cleanup(curl);

                    if (res == CURLE_OK) return;  // success

                    LOG(WARNING) << "[C2C] Chunk " << i
                                 << " error: " << curl_easy_strerror(res);
                }

                LOG(ERROR) << "[C2C] Chunk " << i << " failed after "
                           << kMaxRetries << " retries";
                any_failed = true;
            });
        }

        for (auto& t : threads) t.join();
        close(fd);

        // Final progress bar flush
        print_progress_bar(prog, true);

        if (any_failed) {
            LOG(ERROR) << "[C2C] Parallel download failed: " << url;
            std::remove(tmp_path.c_str());
            return false;
        }
    }

    // Atomic rename
    if (std::rename(tmp_path.c_str(), local_path.c_str()) != 0) {
        LOG(ERROR) << "[C2C] Rename failed: " << tmp_path << " -> "
                   << local_path;
        std::remove(tmp_path.c_str());
        return false;
    }

    LOG(INFO) << "[C2C] Downloaded: " << local_path;
    return true;
#else
    LOG(ERROR) << "[C2C] URL download requires libcurl (USE_CURL not enabled)";
    return false;
#endif
}

// ============================================================================
// Fetch model params from HuggingFace config.json
// ============================================================================
bool KVAutoConverter::fetch_hf_model_info(const std::string& hf_model,
                                          KVModelInfo& info) {
#ifdef USE_CURL
    std::string url =
        "https://huggingface.co/" + hf_model + "/resolve/main/config.json";
    LOG(INFO) << "[C2C] Fetching HF config: " << url;

    CURL* curl = curl_easy_init();
    if (!curl) return false;

    std::string body;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "mooncake-c2c/1.0");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_string_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        LOG(ERROR) << "[C2C] HF config fetch failed: " << hf_model << " ("
                   << curl_easy_strerror(res) << ")";
        return false;
    }

    // Parse JSON
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value root;
    std::string errs;
    if (!reader->parse(body.data(), body.data() + body.size(), &root, &errs)) {
        LOG(ERROR) << "[C2C] HF config parse error: " << errs;
        return false;
    }

    // Extract model parameters
    if (root.isMember("num_hidden_layers"))
        info.num_layers = root["num_hidden_layers"].asInt();
    if (root.isMember("num_key_value_heads"))
        info.num_kv_heads = root["num_key_value_heads"].asInt();

    // head_dim: explicit field or fallback hidden_size / num_attention_heads
    if (root.isMember("head_dim")) {
        info.head_dim = root["head_dim"].asInt();
    } else if (root.isMember("hidden_size") &&
               root.isMember("num_attention_heads")) {
        info.head_dim =
            root["hidden_size"].asInt() / root["num_attention_heads"].asInt();
    }

    LOG(INFO) << "[C2C] HF config: " << hf_model
              << " (layers=" << info.num_layers
              << ", kv_heads=" << info.num_kv_heads
              << ", head_dim=" << info.head_dim << ")";
    return true;
#else
    LOG(ERROR)
        << "[C2C] HF config fetch requires libcurl (USE_CURL not enabled)";
    return false;
#endif
}

void KVAutoConverter::init(int workers) {
    if (running_.exchange(true)) return;
    last_log_time_ = std::chrono::steady_clock::now();
    for (int i = 0; i < workers; ++i) {
        workers_.emplace_back(&KVAutoConverter::worker, this);
    }
    LOG(INFO) << "[C2C] Auto converter started, workers=" << workers;
}

void KVAutoConverter::shutdown() {
    if (!running_.exchange(false)) return;
    cv_.notify_all();
    for (auto& w : workers_)
        if (w.joinable()) w.join();
    workers_.clear();
    put_fn_ = nullptr;  // prevent dangling callback after caller destruction
    batch_put_fn_ = nullptr;
}

// ============================================================================
// Load C2C projector weight file (multi-layer format)
// ============================================================================
// File format:
//   [num_layers:i32][src_dim:i32][tgt_dim:i32][hidden_dim:i32]
//   For each layer:
//     [key_gate_logit:f32][value_gate_logit:f32]
//     [key_in_weight: src*hidden][key_in_bias: hidden]
//     [key_mlp1_weight: hidden*hidden][key_mlp1_bias: hidden]
//     [key_proj_weight: hidden*tgt][key_proj_bias: tgt]
//     [value_in_weight...][value_in_bias...]
//     [value_mlp1_weight...][value_mlp1_bias...]
//     [value_proj_weight...][value_proj_bias...]
// ============================================================================
bool KVAutoConverter::load_projector_file(const std::string& path,
                                          KVConversionRule& rule) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) {
        LOG(ERROR) << "[C2C] Failed to open projector file: " << path;
        return false;
    }

    // Read header
    f.read(reinterpret_cast<char*>(&rule.num_layers), sizeof(int32_t));
    f.read(reinterpret_cast<char*>(&rule.src_dim), sizeof(int32_t));
    f.read(reinterpret_cast<char*>(&rule.tgt_dim), sizeof(int32_t));
    f.read(reinterpret_cast<char*>(&rule.hidden_dim), sizeof(int32_t));

    // Validate header to prevent OOM from corrupted files
    if (rule.num_layers <= 0 || rule.num_layers > 1024 || rule.src_dim <= 0 ||
        rule.src_dim > 65536 || rule.tgt_dim <= 0 || rule.tgt_dim > 65536 ||
        rule.hidden_dim <= 0 || rule.hidden_dim > 65536) {
        LOG(ERROR) << "[C2C] Invalid projector header: layers="
                   << rule.num_layers << " src=" << rule.src_dim
                   << " tgt=" << rule.tgt_dim << " hidden=" << rule.hidden_dim;
        return false;
    }

    size_t in_sz = static_cast<size_t>(rule.src_dim) * rule.hidden_dim;
    size_t mlp1_sz = static_cast<size_t>(rule.hidden_dim) * rule.hidden_dim;
    size_t proj_sz = static_cast<size_t>(rule.hidden_dim) * rule.tgt_dim;

    rule.layers.resize(rule.num_layers);

    for (int32_t L = 0; L < rule.num_layers; ++L) {
        auto& lw = rule.layers[L];

        // Gate scalars
        f.read(reinterpret_cast<char*>(&lw.key_gate_logit), sizeof(float));
        f.read(reinterpret_cast<char*>(&lw.value_gate_logit), sizeof(float));

        // Key weights
        lw.key_in_weight.resize(in_sz);
        lw.key_in_bias.resize(rule.hidden_dim);
        lw.key_mlp1_weight.resize(mlp1_sz);
        lw.key_mlp1_bias.resize(rule.hidden_dim);
        lw.key_proj_weight.resize(proj_sz);
        lw.key_proj_bias.resize(rule.tgt_dim);

        f.read(reinterpret_cast<char*>(lw.key_in_weight.data()),
               in_sz * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.key_in_bias.data()),
               rule.hidden_dim * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.key_mlp1_weight.data()),
               mlp1_sz * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.key_mlp1_bias.data()),
               rule.hidden_dim * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.key_proj_weight.data()),
               proj_sz * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.key_proj_bias.data()),
               rule.tgt_dim * sizeof(float));

        // Value weights
        lw.value_in_weight.resize(in_sz);
        lw.value_in_bias.resize(rule.hidden_dim);
        lw.value_mlp1_weight.resize(mlp1_sz);
        lw.value_mlp1_bias.resize(rule.hidden_dim);
        lw.value_proj_weight.resize(proj_sz);
        lw.value_proj_bias.resize(rule.tgt_dim);

        f.read(reinterpret_cast<char*>(lw.value_in_weight.data()),
               in_sz * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.value_in_bias.data()),
               rule.hidden_dim * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.value_mlp1_weight.data()),
               mlp1_sz * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.value_mlp1_bias.data()),
               rule.hidden_dim * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.value_proj_weight.data()),
               proj_sz * sizeof(float));
        f.read(reinterpret_cast<char*>(lw.value_proj_bias.data()),
               rule.tgt_dim * sizeof(float));
    }

    if (!f) {
        LOG(ERROR) << "[C2C] Failed to read projector file: " << path;
        return false;
    }

    // =========================================================================
    // Pre-transpose all weight matrices: [K, N] -> [N, K]
    // Runtime sgemv switches from CblasTrans to CblasNoTrans (row-contiguous)
    // One-time cost for cache-friendly access on every inference
    // =========================================================================
    for (int32_t L = 0; L < rule.num_layers; ++L) {
        auto& lw = rule.layers[L];
        // Precompute gate sigmoid: eliminate runtime exp
        lw.key_gate = 1.0f / (1.0f + std::exp(-lw.key_gate_logit));
        lw.value_gate = 1.0f / (1.0f + std::exp(-lw.value_gate_logit));
        // in_weight: [src_dim, hidden_dim] -> [hidden_dim, src_dim]
        transpose_matrix(lw.key_in_weight, rule.src_dim, rule.hidden_dim);
        transpose_matrix(lw.value_in_weight, rule.src_dim, rule.hidden_dim);
        // mlp1_weight: [hidden_dim, hidden_dim] -> [hidden_dim, hidden_dim]
        transpose_matrix(lw.key_mlp1_weight, rule.hidden_dim, rule.hidden_dim);
        transpose_matrix(lw.value_mlp1_weight, rule.hidden_dim,
                         rule.hidden_dim);
        // proj_weight: [hidden_dim, tgt_dim] -> [tgt_dim, hidden_dim]
        transpose_matrix(lw.key_proj_weight, rule.hidden_dim, rule.tgt_dim);
        transpose_matrix(lw.value_proj_weight, rule.hidden_dim, rule.tgt_dim);
    }

    LOG(INFO) << "[C2C] Loaded projector: " << path
              << " (layers=" << rule.num_layers << ", src_dim=" << rule.src_dim
              << ", tgt_dim=" << rule.tgt_dim << ", hidden=" << rule.hidden_dim
              << ", weights pre-transposed)";
    return true;
}

// ============================================================================
// Parse buffer_size from C2C config JSON
// ============================================================================
size_t KVAutoConverter::parse_buffer_size(const std::string& config_json) {
    if (config_json.empty()) return kC2cDefaultBufferSize;

    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value root;
    std::string errs;

    if (!reader->parse(config_json.data(),
                       config_json.data() + config_json.size(), &root, &errs))
        return kC2cDefaultBufferSize;

    if (root.isMember("buffer_size") && root["buffer_size"].isUInt64())
        return static_cast<size_t>(root["buffer_size"].asUInt64());

    return kC2cDefaultBufferSize;
}

bool KVAutoConverter::load_config(const std::string& config_json) {
    if (config_json.empty()) return false;

    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    Json::Value root;
    std::string errs;

    if (!reader->parse(config_json.data(),
                       config_json.data() + config_json.size(), &root, &errs)) {
        LOG(ERROR) << "[C2C] Config parse error: " << errs;
        return false;
    }

    // Load model info
    if (root.isMember("models") && root["models"].isArray()) {
        for (const auto& m : root["models"]) {
            KVModelInfo info;
            info.model_id = m["model_id"].asString();
            info.hf_model = m.get("hf_model", "").asString();
            info.num_layers = m.get("num_layers", 0).asInt();
            info.num_kv_heads = m.get("num_kv_heads", 0).asInt();
            info.head_dim = m.get("head_dim", 0).asInt();

            // Auto-fetch from HuggingFace if hf_model specified and fields
            // missing
            if (!info.hf_model.empty() &&
                (info.num_layers == 0 || info.num_kv_heads == 0 ||
                 info.head_dim == 0)) {
                KVModelInfo hf_info = info;
                if (fetch_hf_model_info(info.hf_model, hf_info)) {
                    // Fill only missing fields (manual values take priority)
                    if (info.num_layers == 0)
                        info.num_layers = hf_info.num_layers;
                    if (info.num_kv_heads == 0)
                        info.num_kv_heads = hf_info.num_kv_heads;
                    if (info.head_dim == 0) info.head_dim = hf_info.head_dim;
                } else {
                    LOG(WARNING)
                        << "[C2C] HF fetch failed for: " << info.hf_model
                        << ", using manual config";
                }
            }

            add_model(info);
        }
    }

    // Load conversion rules
    if (root.isMember("rules") && root["rules"].isArray()) {
        for (const auto& r : root["rules"]) {
            KVConversionRule rule;
            rule.source_model = r["source_model"].asString();
            rule.target_model = r["target_model"].asString();
            rule.source_model_name = r.get("source_model_name", "").asString();
            rule.target_model_name = r.get("target_model_name", "").asString();
            rule.projector_url = r.get("projector_url", "").asString();
            rule.projector_file = r.get("projector_file", "").asString();

            // Resolve projector: URL takes priority over local file
            std::string projector_path;
            if (!rule.projector_url.empty()) {
                projector_path = url_to_cache_path(rule.projector_url);
                if (!download_file(rule.projector_url, projector_path)) {
                    LOG(ERROR)
                        << "[C2C] Skip rule (download failed): "
                        << rule.source_model << " -> " << rule.target_model;
                    continue;
                }
            } else if (!rule.projector_file.empty()) {
                projector_path = rule.projector_file;
            }

            if (!projector_path.empty()) {
                if (!load_projector_file(projector_path, rule)) {
                    LOG(ERROR) << "[C2C] Skip rule: " << rule.source_model
                               << " -> " << rule.target_model;
                    continue;
                }
            }

            if (!rule.is_valid()) {
                LOG(ERROR) << "[C2C] Rule missing projector weights: "
                           << rule.source_model << " -> " << rule.target_model;
                continue;
            }

            add_rule(rule);
        }
    }

    LOG(INFO) << "[C2C] Config loaded, " << rules_.size() << " rules";
    return !rules_.empty();
}

void KVAutoConverter::add_model(const KVModelInfo& info) {
    models_[info.model_id] = info;
    LOG(INFO) << "[C2C] Model: " << info.model_id
              << " (layers=" << info.num_layers
              << ", kv_heads=" << info.num_kv_heads
              << ", head_dim=" << info.head_dim << ")";
}

void KVAutoConverter::add_rule(const KVConversionRule& rule) {
    rules_[rule.source_model] = rule;
    LOG(INFO) << "[C2C] Rule: " << rule.source_model << " -> "
              << rule.target_model;
}

// Match key by source_model_name substring, return rule directly
const KVConversionRule* KVAutoConverter::match_rule(const std::string& key) {
    for (const auto& [id, rule] : rules_) {
        if (!rule.source_model_name.empty() &&
            key.find(rule.source_model_name) != std::string::npos) {
            return &rule;
        }
    }
    return nullptr;
}

void KVAutoConverter::on_put(const std::string& key, const void* data,
                             size_t size) {
    if (!running_) return;

    // Skip already-converted keys
    if (key.find("::") != std::string::npos) return;

    // Match by source_model_name substring
    const KVConversionRule* rule = match_rule(key);
    if (!rule) return;

    auto src_it = models_.find(rule->source_model);
    auto tgt_it = models_.find(rule->target_model);
    if (src_it == models_.end() || tgt_it == models_.end()) return;

    LOG(INFO) << "[C2C] on_put: matched " << rule->source_model
              << " for key: " << key;

    Task task;
    task.src_key = key;
    // Generate target key: replace model name
    task.tgt_key = key;
    size_t pos = task.tgt_key.find(rule->source_model_name);
    if (pos != std::string::npos) {
        task.tgt_key.replace(pos, rule->source_model_name.length(),
                             rule->target_model_name);
    }
    task.data.resize(size);
    std::memcpy(task.data.data(), data, size);
    task.src_info = src_it->second;
    task.tgt_info = tgt_it->second;
    task.rule = rule;

    {
        std::lock_guard<std::mutex> lk(mtx_);
        queue_.push(std::vector<Task>{std::move(task)});
    }
    cv_.notify_one();
}

// ============================================================================
// Batch entry: batch_put_from naturally provides a batch, use it directly
// ============================================================================
// Group by (rule, is_key), merge same group into large matrix for sgemm
// Different groups enqueued as independent batches for parallel workers
// ============================================================================
void KVAutoConverter::on_batch_put(const std::vector<std::string>& keys,
                                   const std::vector<void*>& buffers,
                                   const std::vector<size_t>& sizes) {
    if (!running_ || keys.empty()) return;

    // Group by (rule pointer, is_key)
    struct GroupKey {
        const KVConversionRule* rule;
        bool is_key;
        bool operator==(const GroupKey& o) const {
            return rule == o.rule && is_key == o.is_key;
        }
    };
    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const {
            return std::hash<const void*>()(k.rule) ^
                   (std::hash<bool>()(k.is_key) << 1);
        }
    };
    std::unordered_map<GroupKey, std::vector<Task>, GroupKeyHash> groups;

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        if (key.find("::") != std::string::npos) continue;

        const KVConversionRule* rule = match_rule(key);
        if (!rule) continue;

        auto src_it = models_.find(rule->source_model);
        auto tgt_it = models_.find(rule->target_model);
        if (src_it == models_.end() || tgt_it == models_.end()) continue;

        // Determine key/value type
        size_t upos = key.rfind('_');
        if (upos == std::string::npos || upos + 1 >= key.size()) continue;
        bool is_key = (key[upos + 1] == 'k');

        Task task;
        task.src_key = key;
        task.tgt_key = key;
        size_t pos = task.tgt_key.find(rule->source_model_name);
        if (pos != std::string::npos) {
            task.tgt_key.replace(pos, rule->source_model_name.length(),
                                 rule->target_model_name);
        }
        task.data.resize(sizes[i]);
        std::memcpy(task.data.data(), buffers[i], sizes[i]);
        task.src_info = src_it->second;
        task.tgt_info = tgt_it->second;
        task.rule = rule;

        groups[{rule, is_key}].push_back(std::move(task));
    }

    // Enqueue each group as a batch
    if (!groups.empty()) {
        std::lock_guard<std::mutex> lk(mtx_);
        for (auto& [gk, batch] : groups) {
            queue_.push(std::move(batch));
        }
    }
    cv_.notify_all();
}

void KVAutoConverter::worker() {
    while (running_) {
        std::vector<Task> batch;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [this] { return !queue_.empty() || !running_; });
            if (!running_ && queue_.empty()) break;
            if (queue_.empty()) continue;
            batch = std::move(queue_.front());
            queue_.pop();
        }
        if (batch.size() == 1) {
            convert(batch[0]);
        } else {
            convert_batch(batch);
        }

        // Periodic progress log (every 5s) or drain summary
        std::lock_guard<std::mutex> log_lk(log_mtx_);
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                           now - last_log_time_)
                           .count();
        size_t pending;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            pending = queue_.size();
        }
        if (pending == 0 || elapsed >= 5) {
            auto keys = completed_keys_.exchange(0, std::memory_order_relaxed);
            auto pages =
                completed_pages_.exchange(0, std::memory_order_relaxed);
            auto ms = total_ms_.exchange(0, std::memory_order_relaxed);
            if (keys > 0) {
                if (pending == 0) {
                    LOG(INFO) << "[C2C] Done: " << keys << " keys, " << pages
                              << " pages, " << ms << "ms";
                } else {
                    LOG(INFO)
                        << "[C2C] Progress: " << keys << " keys, " << pages
                        << " pages, " << ms << "ms, " << pending << " pending";
                }
            }
            last_log_time_ = now;
        }
    }
}

// ============================================================================
// Fast tanh approximation (Pade, error < 0.001)
// ============================================================================
static inline float fast_tanh(float x) {
    if (x < -3.0f) return -1.0f;
    if (x > 3.0f) return 1.0f;
    float x2 = x * x;
    return x * (27.0f + x2) / (27.0f + 9.0f * x2);
}

// ============================================================================
// AVX2 vectorized post-processing: bias+GELU / bias+scale
// Process 8 floats at once, 8x throughput
// ============================================================================
#ifdef USE_AVX2
// AVX2 fast_tanh: clamp(-3,3) + Pade approximation
static inline __m256 fast_tanh_avx2(__m256 x) {
    __m256 neg3 = _mm256_set1_ps(-3.0f);
    __m256 pos3 = _mm256_set1_ps(3.0f);
    x = _mm256_max_ps(x, neg3);
    x = _mm256_min_ps(x, pos3);
    __m256 x2 = _mm256_mul_ps(x, x);
    __m256 c27 = _mm256_set1_ps(27.0f);
    __m256 c9 = _mm256_set1_ps(9.0f);
    // x * (27 + x2) / (27 + 9 * x2)
    __m256 num = _mm256_mul_ps(x, _mm256_add_ps(c27, x2));
    __m256 den = _mm256_add_ps(c27, _mm256_mul_ps(c9, x2));
    return _mm256_div_ps(num, den);
}

// bias + GELU: v = C[i]+bias[i], out =
// 0.5*v*(1+tanh(sqrt(2/pi)*(v+0.044715*v^3)))
static void fused_bias_gelu_avx2(float* C, const float* bias, int total) {
    __m256 sqrt2pi = _mm256_set1_ps(0.7978845608f);
    __m256 coeff = _mm256_set1_ps(0.044715f);
    __m256 half = _mm256_set1_ps(0.5f);
    __m256 one = _mm256_set1_ps(1.0f);
    int i = 0;
    for (; i + 8 <= total; i += 8) {
        __m256 c = _mm256_loadu_ps(C + i);
        __m256 b = _mm256_loadu_ps(bias + i);
        __m256 v = _mm256_add_ps(c, b);
        __m256 v2 = _mm256_mul_ps(v, v);
        __m256 v3 = _mm256_mul_ps(v2, v);
        // inner = sqrt(2/pi) * (v + 0.044715 * v^3)
        __m256 inner =
            _mm256_mul_ps(sqrt2pi, _mm256_add_ps(v, _mm256_mul_ps(coeff, v3)));
        // 0.5 * v * (1 + fast_tanh(inner))
        __m256 t = fast_tanh_avx2(inner);
        __m256 result =
            _mm256_mul_ps(half, _mm256_mul_ps(v, _mm256_add_ps(one, t)));
        _mm256_storeu_ps(C + i, result);
    }
    // Scalar tail
    constexpr float sqrt_2_over_pi = 0.7978845608f;
    constexpr float coeff_s = 0.044715f;
    for (; i < total; ++i) {
        float v = C[i] + bias[i];
        float v3 = v * v * v;
        float inn = sqrt_2_over_pi * (v + coeff_s * v3);
        C[i] = 0.5f * v * (1.0f + fast_tanh(inn));
    }
}

// bias + scale: out = scale * (C[i] + bias[i])
static void fused_bias_scale_avx2(float* C, const float* bias, int total,
                                  float scale) {
    __m256 vscale = _mm256_set1_ps(scale);
    int i = 0;
    for (; i + 8 <= total; i += 8) {
        __m256 c = _mm256_loadu_ps(C + i);
        __m256 b = _mm256_loadu_ps(bias + i);
        __m256 result = _mm256_mul_ps(vscale, _mm256_add_ps(c, b));
        _mm256_storeu_ps(C + i, result);
    }
    for (; i < total; ++i) {
        C[i] = scale * (C[i] + bias[i]);
    }
}
#endif

// ============================================================================
// Matmul + bias + GELU (OpenBLAS + AVX2 optimized)
// ============================================================================
// C = A @ B^T + bias, then GELU(C)
// A: [M, K], B_T: [N, K] (pre-transposed), C: [M, N], bias: [N]
// ============================================================================
static void matmul_bias_gelu(const float* A, const float* B_T,
                             const float* bias, float* C, int M, int K, int N) {
#ifdef USE_OPENBLAS
    if (M == 1) {
        cblas_sgemv(CblasRowMajor, CblasNoTrans, N, K, 1.0f, B_T, K, A, 1, 0.0f,
                    C, 1);
    } else {
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasTrans, M, N, K, 1.0f, A,
                    K, B_T, K, 0.0f, C, N);
    }
#ifdef USE_AVX2
    // Bias repeats per row, AVX2 version processes row by row
    for (int m = 0; m < M; ++m) {
        fused_bias_gelu_avx2(C + m * N, bias, N);
    }
#else
    constexpr float sqrt_2_over_pi = 0.7978845608f;
    constexpr float coeff = 0.044715f;
    for (int m = 0; m < M; ++m) {
        float* row = C + m * N;
        for (int n = 0; n < N; ++n) {
            float v = row[n] + bias[n];
            float v3 = v * v * v;
            float inner = sqrt_2_over_pi * (v + coeff * v3);
            row[n] = 0.5f * v * (1.0f + fast_tanh(inner));
        }
    }
#endif
#else
    for (int m = 0; m < M; ++m) {
        for (int n = 0; n < N; ++n) {
            float sum = bias[n];
            const float* b_row = B_T + n * K;
            for (int k = 0; k < K; ++k) {
                sum += A[m * K + k] * b_row[k];
            }
            constexpr float sqrt_2_over_pi = 0.7978845608f;
            constexpr float coeff = 0.044715f;
            float v3 = sum * sum * sum;
            float inner = sqrt_2_over_pi * (sum + coeff * v3);
            C[m * N + n] = 0.5f * sum * (1.0f + fast_tanh(inner));
        }
    }
#endif
}

// Matmul + bias + scale (output layer, weights pre-transposed)
static void matmul_bias_scale(const float* A, const float* B_T,
                              const float* bias, float* C, int M, int K, int N,
                              float scale) {
#ifdef USE_OPENBLAS
    if (M == 1) {
        cblas_sgemv(CblasRowMajor, CblasNoTrans, N, K, 1.0f, B_T, K, A, 1, 0.0f,
                    C, 1);
    } else {
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasTrans, M, N, K, 1.0f, A,
                    K, B_T, K, 0.0f, C, N);
    }
#ifdef USE_AVX2
    for (int m = 0; m < M; ++m) {
        fused_bias_scale_avx2(C + m * N, bias, N, scale);
    }
#else
    for (int m = 0; m < M; ++m) {
        float* row = C + m * N;
        for (int n = 0; n < N; ++n) {
            row[n] = scale * (row[n] + bias[n]);
        }
    }
#endif
#else
    for (int m = 0; m < M; ++m) {
        for (int n = 0; n < N; ++n) {
            float sum = bias[n];
            const float* b_row = B_T + n * K;
            for (int k = 0; k < K; ++k) {
                sum += A[m * K + k] * b_row[k];
            }
            C[m * N + n] = scale * sum;
        }
    }
#endif
}

// ============================================================================
// C2C projection (batch tokens) - OpenBLAS optimized
// ============================================================================
// Architecture: src -> input_proj -> mlp1 -> proj_out
// Formula: output = gate * sigmoid(scalar) * projected
// Optimization: external buffers to avoid repeated allocation
// ============================================================================
void KVAutoConverter::c2c_project_layer(
    const float* src, float* out, const KVConversionRule::LayerWeights& lw,
    bool is_key, int num_tokens, int src_dim, int tgt_dim, int hidden_dim,
    float* h1, float* h2) {
    const auto& in_w = is_key ? lw.key_in_weight : lw.value_in_weight;
    const auto& in_b = is_key ? lw.key_in_bias : lw.value_in_bias;
    const auto& mlp1_w = is_key ? lw.key_mlp1_weight : lw.value_mlp1_weight;
    const auto& mlp1_b = is_key ? lw.key_mlp1_bias : lw.value_mlp1_bias;
    const auto& proj_w = is_key ? lw.key_proj_weight : lw.value_proj_weight;
    const auto& proj_b = is_key ? lw.key_proj_bias : lw.value_proj_bias;
    float gate = is_key ? lw.key_gate : lw.value_gate;  // Precomputed sigmoid

    // Layer 1: H1 = GELU(src @ in_w + in_b)
    matmul_bias_gelu(src, in_w.data(), in_b.data(), h1, num_tokens, src_dim,
                     hidden_dim);

    // Layer 2: H2 = GELU(H1 @ mlp1_w + mlp1_b)
    matmul_bias_gelu(h1, mlp1_w.data(), mlp1_b.data(), h2, num_tokens,
                     hidden_dim, hidden_dim);

    // Output: out = gate * (H2 @ proj_w + proj_b)
    matmul_bias_scale(h2, proj_w.data(), proj_b.data(), out, num_tokens,
                      hidden_dim, tgt_dim, gate);
}

// ============================================================================
// SIMD optimized bf16 <-> float batch conversion
// ============================================================================
#ifdef USE_AVX2
static void bf16_to_float_avx2(const uint16_t* src, float* dst, int n) {
    int i = 0;
    // Process 8 elements at once
    for (; i + 8 <= n; i += 8) {
        // Load 8 bf16 values
        __m128i bf16_vals =
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i));
        // Zero-extend to 32-bit and shift left 16
        __m256i lo = _mm256_cvtepu16_epi32(bf16_vals);
        __m256i shifted = _mm256_slli_epi32(lo, 16);
        // Cast to float
        __m256 floats = _mm256_castsi256_ps(shifted);
        _mm256_storeu_ps(dst + i, floats);
    }
    // Handle remaining elements
    for (; i < n; ++i) {
        uint32_t bits = static_cast<uint32_t>(src[i]) << 16;
        std::memcpy(&dst[i], &bits, sizeof(float));
    }
}

static void float_to_bf16_avx2(const float* src, uint16_t* dst, int n) {
    int i = 0;
    // Process 8 elements at once
    for (; i + 8 <= n; i += 8) {
        __m256 floats = _mm256_loadu_ps(src + i);
        __m256i ints = _mm256_castps_si256(floats);
        // Right shift 16 bits
        __m256i shifted = _mm256_srli_epi32(ints, 16);
        // Pack to 16-bit
        __m128i lo = _mm256_castsi256_si128(shifted);
        __m128i hi = _mm256_extracti128_si256(shifted, 1);
        __m128i packed = _mm_packus_epi32(lo, hi);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + i), packed);
    }
    // Handle remaining elements
    for (; i < n; ++i) {
        uint32_t bits;
        std::memcpy(&bits, &src[i], sizeof(float));
        dst[i] = static_cast<uint16_t>(bits >> 16);
    }
}
#endif

static inline float bf16_to_float(uint16_t bf16) {
    uint32_t bits = static_cast<uint32_t>(bf16) << 16;
    float result;
    std::memcpy(&result, &bits, sizeof(float));
    return result;
}

static inline uint16_t float_to_bf16(float f) {
    uint32_t bits;
    std::memcpy(&bits, &f, sizeof(float));
    return static_cast<uint16_t>(bits >> 16);
}

// Batch conversion (auto-select optimal implementation)
static void bf16_to_float_batch(const uint16_t* src, float* dst, int n) {
#ifdef USE_AVX2
    bf16_to_float_avx2(src, dst, n);
#else
    for (int i = 0; i < n; ++i) {
        dst[i] = bf16_to_float(src[i]);
    }
#endif
}

static void float_to_bf16_batch(const float* src, uint16_t* dst, int n) {
#ifdef USE_AVX2
    float_to_bf16_avx2(src, dst, n);
#else
    for (int i = 0; i < n; ++i) {
        dst[i] = float_to_bf16(src[i]);
    }
#endif
}

// ============================================================================
// Parse SGLang key format: model_hash_tprank_type
// e.g. Qwen/Qwen3-4B_<hash>_0_k -> tp_rank=0, is_key=true
// Note: _0 is TP rank, not layer index! Data contains all layers
// ============================================================================
static bool parse_sglang_key(const std::string& key, bool& is_key) {
    // Find trailing _k or _v
    size_t pos = key.rfind('_');
    if (pos == std::string::npos || pos == 0) return false;

    char type = key[pos + 1];
    if (type != 'k' && type != 'v') return false;
    is_key = (type == 'k');
    return true;
}

// ============================================================================
// Conversion core (SGLang packed all-layers format) - optimized
// ============================================================================
// SGLang key format: model_hash_tprank_type (e.g. Qwen/Qwen3-4B_xxx_0_k)
// Data layout: [num_pages, num_layers, num_heads, head_dim] (bf16)
// Each key contains all layers of K or V data
// Optimization: batch all pages per layer, reduce function call overhead
// ============================================================================
void KVAutoConverter::convert(Task& t) {
    if (!t.rule || !t.rule->is_valid()) {
        LOG(ERROR) << "[C2C] Invalid rule for: " << t.src_key;
        return;
    }

    // Parse key to get type (K or V)
    bool is_key;
    if (!parse_sglang_key(t.src_key, is_key)) {
        LOG(ERROR) << "[C2C] Failed to parse key: " << t.src_key;
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    int32_t src_dim = t.rule->src_dim;  // num_heads * head_dim
    int32_t tgt_dim = t.rule->tgt_dim;
    int32_t hidden_dim = t.rule->hidden_dim;
    int32_t num_proj_layers =
        t.rule->num_layers;  // Projector layers (target model layers)
    int32_t num_src_layers = t.src_info.num_layers;  // Source model layers

    // SGLang data layout: [num_pages, num_layers, num_heads, head_dim]
    // Each page contains all layers
    size_t elem_sz = 2;                     // bf16
    size_t layer_size = src_dim * elem_sz;  // Single layer, single token size
    size_t page_size = num_src_layers * layer_size;  // Single page size

    if (page_size == 0) {
        LOG(ERROR) << "[C2C] Zero page_size for: " << t.src_key
                   << " (src_dim=" << src_dim
                   << ", num_src_layers=" << num_src_layers << ")";
        return;
    }

    if (t.data.size() % page_size != 0) {
        LOG(ERROR) << "[C2C] Invalid data size for: " << t.src_key
                   << " (size=" << t.data.size() << ", page_size=" << page_size
                   << ", num_src_layers=" << num_src_layers
                   << ", src_dim=" << src_dim << ")";
        return;
    }

    int32_t num_pages = static_cast<int32_t>(t.data.size() / page_size);
    if (num_pages <= 0) {
        LOG(ERROR) << "[C2C] No pages in data for: " << t.src_key;
        return;
    }

    // C2C uses last_aligned strategy: last N layers of source map to target's N
    // layers
    int32_t layer_offset = num_src_layers - num_proj_layers;
    if (layer_offset < 0) layer_offset = 0;

    // Output layout: [num_pages, num_proj_layers, tgt_dim]
    size_t tgt_layer_size = tgt_dim * elem_sz;
    size_t tgt_page_size = num_proj_layers * tgt_layer_size;
    size_t tgt_total_size = num_pages * tgt_page_size;
    std::vector<uint8_t> tgt_data(tgt_total_size);

    const uint8_t* src_base = t.data.data();
    uint8_t* tgt_base = tgt_data.data();

    // thread_local buffers: reused per worker thread, avoid repeated
    // malloc/free
    thread_local std::vector<float> src_batch, tgt_batch, h1, h2;
    size_t src_need = num_pages * src_dim;
    size_t tgt_need = num_pages * tgt_dim;
    size_t hid_need = num_pages * hidden_dim;
    if (src_batch.size() < src_need) src_batch.resize(src_need);
    if (tgt_batch.size() < tgt_need) tgt_batch.resize(tgt_need);
    if (h1.size() < hid_need) h1.resize(hid_need);
    if (h2.size() < hid_need) h2.resize(hid_need);

    for (int32_t proj_layer = 0; proj_layer < num_proj_layers; ++proj_layer) {
        int32_t src_layer = layer_offset + proj_layer;

        // Batch bf16 -> float (SIMD optimized)
        for (int32_t p = 0; p < num_pages; ++p) {
            const uint8_t* src_ptr =
                src_base + p * page_size + src_layer * layer_size;
            auto* src_bf16 = reinterpret_cast<const uint16_t*>(src_ptr);
            float* dst = src_batch.data() + p * src_dim;
            bf16_to_float_batch(src_bf16, dst, src_dim);
        }

        // C2C projection (OpenBLAS optimized, reuse buffers)
        c2c_project_layer(src_batch.data(), tgt_batch.data(),
                          t.rule->layers[proj_layer], is_key, num_pages,
                          src_dim, tgt_dim, hidden_dim, h1.data(), h2.data());

        // Batch float -> bf16 (SIMD optimized)
        for (int32_t p = 0; p < num_pages; ++p) {
            uint8_t* tgt_ptr =
                tgt_base + p * tgt_page_size + proj_layer * tgt_layer_size;
            auto* tgt_bf16 = reinterpret_cast<uint16_t*>(tgt_ptr);
            const float* src = tgt_batch.data() + p * tgt_dim;
            float_to_bf16_batch(src, tgt_bf16, tgt_dim);
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    if (put_fn_) {
        int ret = put_fn_(t.tgt_key, tgt_data.data(), tgt_data.size());
        if (ret != 0) {
            LOG(ERROR) << "[C2C] put failed: " << t.tgt_key << " ret=" << ret;
        }
        completed_keys_.fetch_add(1, std::memory_order_relaxed);
        completed_pages_.fetch_add(num_pages, std::memory_order_relaxed);
        total_ms_.fetch_add(ms, std::memory_order_relaxed);
    }
}

// ============================================================================
// Batch conversion - merge same-rule same-type tasks into large matrix
// ============================================================================
// batch_put_from naturally provides a batch, grouped by (rule, is_key)
// sgemm with M=total_pages is far more efficient than per-task sgemv
// ============================================================================
void KVAutoConverter::convert_batch(std::vector<Task>& tasks) {
    if (tasks.empty()) return;
    if (tasks.size() == 1) {
        convert(tasks[0]);
        return;
    }

    auto t0 = std::chrono::high_resolution_clock::now();

    const KVConversionRule* rule = tasks[0].rule;
    bool is_key;
    if (!parse_sglang_key(tasks[0].src_key, is_key)) {
        for (auto& t : tasks) convert(t);
        return;
    }

    int32_t src_dim = rule->src_dim;
    int32_t tgt_dim = rule->tgt_dim;
    int32_t hidden_dim = rule->hidden_dim;
    int32_t num_proj_layers = rule->num_layers;
    int32_t num_src_layers = tasks[0].src_info.num_layers;

    size_t elem_sz = 2;
    size_t layer_size = src_dim * elem_sz;
    size_t page_size = num_src_layers * layer_size;

    if (page_size == 0) {
        LOG(ERROR) << "[C2C] Zero page_size in batch convert";
        return;
    }

    // Count total pages
    int32_t total_pages = 0;
    std::vector<int32_t> page_offsets(tasks.size() + 1);
    std::vector<int32_t> task_pages(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        page_offsets[i] = total_pages;
        task_pages[i] = static_cast<int32_t>(tasks[i].data.size() / page_size);
        total_pages += task_pages[i];
    }
    page_offsets[tasks.size()] = total_pages;

    int32_t layer_offset = num_src_layers - num_proj_layers;
    if (layer_offset < 0) layer_offset = 0;

    size_t tgt_layer_size = tgt_dim * elem_sz;
    size_t tgt_page_size = num_proj_layers * tgt_layer_size;
    std::vector<std::vector<uint8_t>> tgt_data(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        tgt_data[i].resize(task_pages[i] * tgt_page_size);
    }

    thread_local std::vector<float> src_batch, tgt_batch, h1, h2;
    size_t src_need = total_pages * src_dim;
    size_t tgt_need = total_pages * tgt_dim;
    size_t hid_need = total_pages * hidden_dim;
    if (src_batch.size() < src_need) src_batch.resize(src_need);
    if (tgt_batch.size() < tgt_need) tgt_batch.resize(tgt_need);
    if (h1.size() < hid_need) h1.resize(hid_need);
    if (h2.size() < hid_need) h2.resize(hid_need);

    for (int32_t proj_layer = 0; proj_layer < num_proj_layers; ++proj_layer) {
        int32_t src_layer = layer_offset + proj_layer;

        // Merge all tasks' same-layer data into contiguous memory
        for (size_t ti = 0; ti < tasks.size(); ++ti) {
            const uint8_t* base = tasks[ti].data.data();
            int32_t np = task_pages[ti];
            int32_t off = page_offsets[ti];
            for (int32_t p = 0; p < np; ++p) {
                auto* bf16 = reinterpret_cast<const uint16_t*>(
                    base + p * page_size + src_layer * layer_size);
                bf16_to_float_batch(
                    bf16, src_batch.data() + (off + p) * src_dim, src_dim);
            }
        }

        // Single sgemm: M = total_pages
        c2c_project_layer(src_batch.data(), tgt_batch.data(),
                          rule->layers[proj_layer], is_key, total_pages,
                          src_dim, tgt_dim, hidden_dim, h1.data(), h2.data());

        // Split results back to individual tasks
        for (size_t ti = 0; ti < tasks.size(); ++ti) {
            uint8_t* base = tgt_data[ti].data();
            int32_t np = task_pages[ti];
            int32_t off = page_offsets[ti];
            for (int32_t p = 0; p < np; ++p) {
                auto* bf16 = reinterpret_cast<uint16_t*>(
                    base + p * tgt_page_size + proj_layer * tgt_layer_size);
                float_to_bf16_batch(tgt_batch.data() + (off + p) * tgt_dim,
                                    bf16, tgt_dim);
            }
        }
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    // Batch put: one RPC round-trip for all keys (vs 2*N for loop put)
    if (batch_put_fn_) {
        std::vector<std::string> keys(tasks.size());
        std::vector<void*> bufs(tasks.size());
        std::vector<size_t> szs(tasks.size());
        for (size_t i = 0; i < tasks.size(); ++i) {
            keys[i] = tasks[i].tgt_key;
            bufs[i] = tgt_data[i].data();
            szs[i] = tgt_data[i].size();
        }
        int ret = batch_put_fn_(keys, bufs, szs);
        if (ret != 0) {
            LOG(ERROR) << "[C2C] batch put failed, ret=" << ret;
        }
    } else if (put_fn_) {
        for (size_t i = 0; i < tasks.size(); ++i) {
            put_fn_(tasks[i].tgt_key, tgt_data[i].data(), tgt_data[i].size());
        }
    }
    completed_keys_.fetch_add(tasks.size(), std::memory_order_relaxed);
    completed_pages_.fetch_add(total_pages, std::memory_order_relaxed);
    total_ms_.fetch_add(ms, std::memory_order_relaxed);
}

}  // namespace mooncake
