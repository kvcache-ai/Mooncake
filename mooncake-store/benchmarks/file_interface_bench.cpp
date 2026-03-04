#include <iostream>
#include <fstream>
#include <random>
#include <chrono>
#include <iomanip>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <cerrno>
#include <cmath>
#include <limits>
#include <fcntl.h>
#include <unistd.h>
#include <glog/logging.h>

#include "file_interface.h"

using namespace mooncake;

// Aligned memory allocation for O_DIRECT
void* aligned_alloc_buffer(size_t size, size_t alignment = 4096) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return nullptr;
    }
    return ptr;
}

void aligned_free_buffer(void* ptr) { free(ptr); }

struct BenchmarkConfig {
    std::string file_path = "/tmp/file_bench_test.dat";
    size_t data_size = 1024 * 1024 * 100;  // 100 MB
    bool use_uring = false;
    unsigned uring_queue_depth = 32;
    bool verify_data = true;
    int iterations = 5;  // Number of iterations for each test
    bool use_direct_io =
        false;  // Use O_DIRECT with raw syscalls (bypasses StorageFile)
    bool use_uring_direct_io =
        false;  // Use O_DIRECT with UringFile interface (with copy)
    bool use_uring_direct_io_zero_copy =
        false;  // Use O_DIRECT with zero-copy aligned interface
    bool use_registered_buffers =
        false;  // Register buffers with io_uring for optimal performance
    size_t alignment = 4096;  // Alignment for O_DIRECT (4KB)
    size_t chunk_size =
        1024 * 1024;  // Chunk size for I/O operations (1MB default)
};

struct BenchmarkStats {
    double min = std::numeric_limits<double>::max();
    double max = 0;
    double sum = 0;
    double sum_sq = 0;
    int count = 0;

    void Add(double value) {
        min = std::min(min, value);
        max = std::max(max, value);
        sum += value;
        sum_sq += value * value;
        count++;
    }

    double Mean() const { return count > 0 ? sum / count : 0; }

    double StdDev() const {
        if (count < 2) return 0;
        double mean = Mean();
        return std::sqrt((sum_sq / count) - (mean * mean));
    }

    void Print(const std::string& label) const {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << label << ": " << Mean() << " MB/s (min=" << min
                  << ", max=" << max << ", stddev=" << StdDev() << ")"
                  << std::endl;
    }
};

class FileInterfaceBenchmark {
   public:
    explicit FileInterfaceBenchmark(const BenchmarkConfig& config)
        : config_(config) {}

    void Run() {
        std::cout << "=== File Interface Benchmark ===" << std::endl;
        std::cout << "File path: " << config_.file_path << std::endl;
        std::cout << "Data size: " << FormatSize(config_.data_size)
                  << std::endl;
        std::cout << "Use io_uring: " << (config_.use_uring ? "Yes" : "No")
                  << std::endl;
        if (config_.use_uring) {
            std::cout << "io_uring queue depth: " << config_.uring_queue_depth
                      << std::endl;
            std::cout << "io_uring O_DIRECT: "
                      << (config_.use_uring_direct_io ? "Yes (with copy)"
                                                      : "No")
                      << std::endl;
            std::cout << "io_uring O_DIRECT zero-copy: "
                      << (config_.use_uring_direct_io_zero_copy ? "Yes" : "No")
                      << std::endl;
            std::cout << "io_uring registered buffers: "
                      << (config_.use_registered_buffers ? "Yes" : "No")
                      << std::endl;
        }
        std::cout << "Use O_DIRECT (raw syscalls): "
                  << (config_.use_direct_io ? "Yes" : "No") << std::endl;
        if (config_.use_direct_io) {
            std::cout << "Alignment: " << config_.alignment << " bytes"
                      << std::endl;
        }
        std::cout << "Chunk size: " << FormatSize(config_.chunk_size)
                  << std::endl;
        std::cout << "Iterations: " << config_.iterations << std::endl;
        std::cout << std::endl;

        // Ensure data size is aligned for O_DIRECT
        size_t actual_size = config_.data_size;
        if (config_.use_direct_io || config_.use_uring_direct_io_zero_copy) {
            actual_size = ((config_.data_size + config_.alignment - 1) /
                           config_.alignment) *
                          config_.alignment;
            if (actual_size != config_.data_size) {
                std::cout << "Adjusted data size to " << FormatSize(actual_size)
                          << " for O_DIRECT alignment" << std::endl;
            }
        }

        // Generate test data
        std::cout << "Generating test data..." << std::endl;
        void* write_data_ptr = nullptr;
        std::vector<char> write_data_vec;

        if (config_.use_direct_io || config_.use_uring_direct_io_zero_copy) {
            // Allocate aligned buffer for O_DIRECT (raw syscall or zero-copy
            // mode)
            write_data_ptr =
                aligned_alloc_buffer(actual_size, config_.alignment);
            if (!write_data_ptr) {
                std::cerr << "Failed to allocate aligned buffer!" << std::endl;
                return;
            }
            GenerateRandomDataAligned(write_data_ptr, actual_size);
        } else {
            // UringFile or PosixFile: use std::vector (UringFile will handle
            // alignment internally)
            write_data_vec = GenerateRandomData(actual_size);
            write_data_ptr = write_data_vec.data();
        }
        std::cout << "Test data generated." << std::endl << std::endl;

        BenchmarkStats write_stats, read_stats;

        for (int iter = 0; iter < config_.iterations; ++iter) {
            std::cout << "--- Iteration " << (iter + 1) << "/"
                      << config_.iterations << " ---" << std::endl;

            if (config_.use_uring_direct_io_zero_copy) {
                // Zero-copy aligned I/O mode
                auto write_result_aligned =
                    BenchmarkAlignedIO(write_data_ptr, actual_size, true);
                if (!write_result_aligned) {
                    std::cerr << "Zero-copy aligned write failed!" << std::endl;
                    aligned_free_buffer(write_data_ptr);
                    return;
                }
                write_stats.Add(write_result_aligned->first);

                // Sync and drop cache before read
                SyncAndDropCache();

                auto read_result_aligned =
                    BenchmarkAlignedIO(nullptr, actual_size, false);
                if (!read_result_aligned) {
                    std::cerr << "Zero-copy aligned read failed!" << std::endl;
                    aligned_free_buffer(write_data_ptr);
                    return;
                }
                read_stats.Add(read_result_aligned->first);

                // Verify data consistency (only first iteration)
                if (config_.verify_data && iter == 0) {
                    bool verified =
                        VerifyData(write_data_ptr, read_result_aligned->second,
                                   actual_size);
                    if (!verified) {
                        std::cerr << "Data verification FAILED!" << std::endl;
                        aligned_free_buffer(read_result_aligned->second);
                        aligned_free_buffer(write_data_ptr);
                        return;
                    }
                    std::cout << "Data verification PASSED" << std::endl;
                }

                // Free read buffer
                aligned_free_buffer(read_result_aligned->second);

            } else if (config_.use_direct_io) {
                // Use direct I/O syscalls
                auto write_result_direct =
                    BenchmarkDirectIO(write_data_ptr, actual_size, true);
                if (!write_result_direct) {
                    std::cerr << "Direct I/O write benchmark failed!"
                              << std::endl;
                    aligned_free_buffer(write_data_ptr);
                    return;
                }
                write_stats.Add(write_result_direct->first);

                // Sync and drop cache before read
                SyncAndDropCache();

                auto read_result_direct =
                    BenchmarkDirectIO(nullptr, actual_size, false);
                if (!read_result_direct) {
                    std::cerr << "Direct I/O read benchmark failed!"
                              << std::endl;
                    aligned_free_buffer(write_data_ptr);
                    return;
                }
                read_stats.Add(read_result_direct->first);

                // Verify data consistency (only first iteration)
                if (config_.verify_data && iter == 0) {
                    bool verified =
                        VerifyData(write_data_ptr, read_result_direct->second,
                                   actual_size);
                    if (!verified) {
                        std::cerr << "Data verification FAILED!" << std::endl;
                        aligned_free_buffer(read_result_direct->second);
                        aligned_free_buffer(write_data_ptr);
                        return;
                    }
                    std::cout << "Data verification PASSED" << std::endl;
                }

                // Free read buffer
                aligned_free_buffer(read_result_direct->second);

            } else {
                // Use StorageFile interface
                auto write_result = BenchmarkWrite(write_data_ptr, actual_size);
                if (!write_result) {
                    std::cerr << "Write benchmark failed!" << std::endl;
                    return;
                }
                write_stats.Add(*write_result);

                auto read_result = BenchmarkRead(actual_size);
                if (!read_result) {
                    std::cerr << "Read benchmark failed!" << std::endl;
                    return;
                }
                read_stats.Add(read_result->first);

                // Verify data consistency (only first iteration)
                if (config_.verify_data && iter == 0) {
                    bool verified = VerifyData(
                        write_data_ptr, read_result->second, actual_size);
                    if (!verified) {
                        std::cerr << "Data verification FAILED!" << std::endl;
                        aligned_free_buffer(read_result->second);
                        return;
                    }
                    std::cout << "Data verification PASSED" << std::endl;
                }

                // Free read buffer (always allocated in BenchmarkRead now)
                aligned_free_buffer(read_result->second);
            }

            // Cleanup for next iteration
            unlink(config_.file_path.c_str());
            std::cout << std::endl;
        }

        // Cleanup aligned buffer
        if (config_.use_direct_io || config_.use_uring_direct_io_zero_copy) {
            aligned_free_buffer(write_data_ptr);
        }

        // Print summary
        std::cout << "=== Summary ===" << std::endl;
        write_stats.Print("Write bandwidth");
        read_stats.Print("Read bandwidth");
    }

   private:
    std::vector<char> GenerateRandomData(size_t size) {
        std::vector<char> data(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<unsigned char> dist(0, 255);

        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<char>(dist(gen));
        }
        return data;
    }

    void GenerateRandomDataAligned(void* ptr, size_t size) {
        char* data = static_cast<char*>(ptr);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<unsigned char> dist(0, 255);

        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<char>(dist(gen));
        }
    }

    std::unique_ptr<StorageFile> CreateFile(int fd) {
#ifdef USE_URING
        if (config_.use_uring) {
            return std::make_unique<UringFile>(config_.file_path, fd,
                                               config_.uring_queue_depth,
                                               config_.use_uring_direct_io);
        }
#endif
        return std::make_unique<PosixFile>(config_.file_path, fd);
    }

    bool SyncAndDropCache() {
#ifdef __linux__
        // Flush dirty pages
        ::sync();

        if (::geteuid() != 0) {
            std::cerr << "[CACHE] Not root (euid=" << ::geteuid()
                      << "). Cannot write /proc/sys/vm/drop_caches.\n"
                      << "        Run: sudo -E ./file_interface_bench ...\n";
            return false;
        }

        std::ofstream drop("/proc/sys/vm/drop_caches");
        if (!drop.is_open()) {
            std::cerr << "[CACHE] Failed to open /proc/sys/vm/drop_caches: "
                      << std::strerror(errno) << "\n";
            return false;
        }

        drop << "3" << std::flush;
        if (drop.fail()) {
            std::cerr
                << "[CACHE] Failed to write to /proc/sys/vm/drop_caches\n";
            return false;
        }
        drop.close();

        std::cout << "Synced and dropped page cache" << std::endl;
        return true;
#else
        std::cerr << "[CACHE] drop_caches not supported on this platform\n";
        return false;
#endif
    }

    std::optional<double> BenchmarkWrite(void* data, size_t size) {
        std::cout << "=== Write Benchmark ===" << std::endl;

        // Open file for writing
        int flags = O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC;
        if (config_.use_uring_direct_io) {
            flags |= O_DIRECT;
            std::cout << "Using UringFile with O_DIRECT" << std::endl;
        }
        int fd = open(config_.file_path.c_str(), flags, 0644);
        if (fd < 0) {
            std::cerr << "Failed to open file for writing: "
                      << config_.file_path << " - " << std::strerror(errno)
                      << std::endl;
            return std::nullopt;
        }

        auto file = CreateFile(fd);
        if (!file) {
            std::cerr << "Failed to create file object" << std::endl;
            close(fd);
            return std::nullopt;
        }

        // Perform write operation
        auto start = std::chrono::high_resolution_clock::now();

        auto result = file->write(
            std::span<const char>(static_cast<const char*>(data), size), size);

        auto end = std::chrono::high_resolution_clock::now();

        if (!result) {
            std::cerr << "Write operation failed with error code: "
                      << static_cast<int>(result.error()) << std::endl;
            return std::nullopt;
        }

        size_t bytes_written = result.value();
        auto duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count();
        double duration_s = duration_us / 1000000.0;
        double bandwidth_mbps =
            (bytes_written / (1024.0 * 1024.0)) / duration_s;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Bytes written: " << FormatSize(bytes_written)
                  << std::endl;
        std::cout << "Time: " << duration_us << " us (" << duration_s << " s)"
                  << std::endl;
        std::cout << "Bandwidth: " << bandwidth_mbps << " MB/s" << std::endl;

        // Sync to disk and drop page cache
        SyncAndDropCache();

        return bandwidth_mbps;
    }

    // Zero-copy aligned I/O using UringFile's aligned interface
    std::optional<std::pair<double, void*>> BenchmarkAlignedIO(void* write_data,
                                                               size_t size,
                                                               bool is_write) {
        std::cout << "=== Zero-Copy Aligned I/O "
                  << (is_write ? "Write" : "Read")
                  << " Benchmark ===" << std::endl;

        // Open file with O_DIRECT
        int flags = is_write
                        ? (O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT | O_CLOEXEC)
                        : (O_RDONLY | O_DIRECT | O_CLOEXEC);
        int fd = open(config_.file_path.c_str(), flags, 0644);
        if (fd < 0) {
            std::cerr << "Failed to open file with O_DIRECT: "
                      << config_.file_path << " - " << std::strerror(errno)
                      << std::endl;
            return std::nullopt;
        }

        auto file = CreateFile(fd);
        if (!file) {
            std::cerr << "Failed to create file object" << std::endl;
            close(fd);
            return std::nullopt;
        }

        void* buffer = nullptr;
        if (is_write) {
            buffer = write_data;
        } else {
            buffer = aligned_alloc_buffer(size, config_.alignment);
            if (!buffer) {
                std::cerr << "Failed to allocate aligned buffer" << std::endl;
                return std::nullopt;
            }
        }

        // Register buffer if requested (NOT counted in I/O time)
        auto* uring_file = dynamic_cast<UringFile*>(file.get());
        if (config_.use_registered_buffers && uring_file) {
            auto reg_start = std::chrono::high_resolution_clock::now();
            bool registered = uring_file->register_buffer(buffer, size);
            auto reg_end = std::chrono::high_resolution_clock::now();
            auto reg_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              reg_end - reg_start)
                              .count();

            if (registered) {
                std::cout << "Buffer registration: " << reg_us
                          << " us (excluded from I/O time)" << std::endl;
            } else {
                std::cerr
                    << "Warning: Buffer registration failed, continuing without"
                    << std::endl;
            }
        }

        // Perform zero-copy I/O operation
        auto start = std::chrono::high_resolution_clock::now();

        tl::expected<size_t, ErrorCode> result;
        if (is_write) {
            auto* uring_file = dynamic_cast<UringFile*>(file.get());
            if (uring_file) {
                result = uring_file->write_aligned(buffer, size, 0);
            } else {
                std::cerr << "Not a UringFile instance!" << std::endl;
                if (!is_write) aligned_free_buffer(buffer);
                return std::nullopt;
            }
        } else {
            auto* uring_file = dynamic_cast<UringFile*>(file.get());
            if (uring_file) {
                result = uring_file->read_aligned(buffer, size, 0);
            } else {
                std::cerr << "Not a UringFile instance!" << std::endl;
                aligned_free_buffer(buffer);
                return std::nullopt;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();

        // Unregister buffer if it was registered (NOT counted in I/O time)
        if (config_.use_registered_buffers && uring_file &&
            uring_file->is_buffer_registered()) {
            auto unreg_start = std::chrono::high_resolution_clock::now();
            uring_file->unregister_buffer();
            auto unreg_end = std::chrono::high_resolution_clock::now();
            auto unreg_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    unreg_end - unreg_start)
                    .count();
            std::cout << "Buffer unregistration: " << unreg_us
                      << " us (excluded from I/O time)" << std::endl;
        }

        if (!result) {
            std::cerr << "Zero-copy I/O operation failed with error code: "
                      << static_cast<int>(result.error()) << std::endl;
            if (!is_write) aligned_free_buffer(buffer);
            return std::nullopt;
        }

        size_t total_bytes = result.value();
        auto duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count();
        double duration_s = duration_us / 1000000.0;
        double bandwidth_mbps = (total_bytes / (1024.0 * 1024.0)) / duration_s;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Bytes " << (is_write ? "written" : "read") << ": "
                  << FormatSize(total_bytes) << std::endl;
        std::cout << "Time: " << duration_us << " us (" << duration_s << " s)"
                  << std::endl;
        std::cout << "Bandwidth: " << bandwidth_mbps << " MB/s" << std::endl;
        std::cout << "Zero-copy: YES (no memory copy overhead)" << std::endl;

        return std::make_pair(bandwidth_mbps, buffer);
    }

    // Direct I/O benchmark using raw syscalls (bypasses StorageFile interface)
    std::optional<std::pair<double, void*>> BenchmarkDirectIO(void* write_data,
                                                              size_t size,
                                                              bool is_write) {
        std::cout << "=== Direct I/O " << (is_write ? "Write" : "Read")
                  << " Benchmark ===" << std::endl;

        // Open file with O_DIRECT
        int flags = is_write ? (O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT)
                             : (O_RDONLY | O_DIRECT);
        int fd = open(config_.file_path.c_str(), flags, 0644);
        if (fd < 0) {
            std::cerr << "Failed to open file with O_DIRECT: "
                      << config_.file_path << " - " << std::strerror(errno)
                      << std::endl;
            return std::nullopt;
        }

        void* buffer = nullptr;
        if (is_write) {
            buffer = write_data;
        } else {
            buffer = aligned_alloc_buffer(size, config_.alignment);
            if (!buffer) {
                std::cerr << "Failed to allocate aligned buffer" << std::endl;
                close(fd);
                return std::nullopt;
            }
        }

        // Perform I/O operation
        auto start = std::chrono::high_resolution_clock::now();

        size_t total_bytes = 0;
        size_t remaining = size;
        off_t offset = 0;

        while (remaining > 0) {
            size_t this_chunk = std::min(config_.chunk_size, remaining);
            ssize_t ret;

            if (is_write) {
                ret = pwrite(fd, static_cast<char*>(buffer) + offset,
                             this_chunk, offset);
            } else {
                ret = pread(fd, static_cast<char*>(buffer) + offset, this_chunk,
                            offset);
            }

            if (ret < 0) {
                std::cerr << "I/O error: " << std::strerror(errno) << std::endl;
                if (!is_write) aligned_free_buffer(buffer);
                close(fd);
                return std::nullopt;
            }

            if (ret == 0) break;  // EOF for read

            total_bytes += ret;
            offset += ret;
            remaining -= ret;
        }

        auto end = std::chrono::high_resolution_clock::now();

        close(fd);

        auto duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count();
        double duration_s = duration_us / 1000000.0;
        double bandwidth_mbps = (total_bytes / (1024.0 * 1024.0)) / duration_s;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Bytes " << (is_write ? "written" : "read") << ": "
                  << FormatSize(total_bytes) << std::endl;
        std::cout << "Time: " << duration_us << " us (" << duration_s << " s)"
                  << std::endl;
        std::cout << "Bandwidth: " << bandwidth_mbps << " MB/s" << std::endl;

        return std::make_pair(bandwidth_mbps, buffer);
    }

    std::optional<std::pair<double, void*>> BenchmarkRead(size_t size) {
        std::cout << "=== Read Benchmark ===" << std::endl;

        // Open file for reading
        int flags = O_RDONLY | O_CLOEXEC;
        if (config_.use_uring_direct_io) {
            flags |= O_DIRECT;
            std::cout << "Using UringFile with O_DIRECT (with copy)"
                      << std::endl;
        }
        int fd = open(config_.file_path.c_str(), flags);
        if (fd < 0) {
            std::cerr << "Failed to open file for reading: "
                      << config_.file_path << " - " << std::strerror(errno)
                      << std::endl;
            return std::nullopt;
        }

        auto file = CreateFile(fd);
        if (!file) {
            std::cerr << "Failed to create file object" << std::endl;
            close(fd);
            return std::nullopt;
        }

        // Allocate a buffer that persists after function return
        void* read_buffer = aligned_alloc_buffer(size, config_.alignment);
        if (!read_buffer) {
            std::cerr << "Failed to allocate read buffer" << std::endl;
            return std::nullopt;
        }

        // Perform read operation into a temporary string
        std::string read_data_str;

        auto start = std::chrono::high_resolution_clock::now();

        auto result = file->read(read_data_str, size);

        auto end = std::chrono::high_resolution_clock::now();

        if (!result) {
            std::cerr << "Read operation failed with error code: "
                      << static_cast<int>(result.error()) << std::endl;
            aligned_free_buffer(read_buffer);
            return std::nullopt;
        }

        size_t bytes_read = result.value();

        // Copy data to persistent buffer
        std::memcpy(read_buffer, read_data_str.data(), bytes_read);

        auto duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start)
                .count();
        double duration_s = duration_us / 1000000.0;
        double bandwidth_mbps = (bytes_read / (1024.0 * 1024.0)) / duration_s;

        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Bytes read: " << FormatSize(bytes_read) << std::endl;
        std::cout << "Time: " << duration_us << " us (" << duration_s << " s)"
                  << std::endl;
        std::cout << "Bandwidth: " << bandwidth_mbps << " MB/s" << std::endl;

        return std::make_pair(bandwidth_mbps, read_buffer);
    }

    bool VerifyData(void* expected, void* actual, size_t size) {
        bool match = std::memcmp(expected, actual, size) == 0;
        if (!match) {
            // Find first mismatch position
            const char* exp_data = static_cast<const char*>(expected);
            const char* act_data = static_cast<const char*>(actual);
            for (size_t i = 0; i < size; ++i) {
                if (exp_data[i] != act_data[i]) {
                    std::cerr << "First mismatch at offset " << i << ": "
                              << "expected 0x" << std::hex
                              << (int)(unsigned char)exp_data[i] << ", got 0x"
                              << (int)(unsigned char)act_data[i] << std::dec
                              << std::endl;
                    break;
                }
            }
        }

        return match;
    }

    static std::string FormatSize(size_t bytes) {
        const char* units[] = {"B", "KB", "MB", "GB"};
        int unit_index = 0;
        double size = static_cast<double>(bytes);

        while (size >= 1024.0 && unit_index < 3) {
            size /= 1024.0;
            unit_index++;
        }

        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << size << " "
            << units[unit_index];
        return oss.str();
    }

    BenchmarkConfig config_;
};

void PrintUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [options]" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --file <path>        Path to test file (default: "
                 "/tmp/file_bench_test.dat)"
              << std::endl;
    std::cout << "  --size <bytes>       Size of test data in bytes (default: "
                 "104857600 = 100MB)"
              << std::endl;
    std::cout
        << "  --size-mb <MB>       Size of test data in MB (overrides --size)"
        << std::endl;
    std::cout << "  --chunk-size <KB>    I/O chunk size in KB (default: 1024)"
              << std::endl;
#ifdef USE_URING
    std::cout << "  --use-uring              Use io_uring for I/O operations"
              << std::endl;
    std::cout << "  --queue-depth <n>        io_uring queue depth (default: 32)"
              << std::endl;
    std::cout << "  --uring-direct-io        Enable O_DIRECT in UringFile "
                 "(with memory copy)"
              << std::endl;
    std::cout << "  --uring-direct-io-zerocopy  Enable O_DIRECT with zero-copy "
                 "(requires aligned buffer)"
              << std::endl;
    std::cout << "  --use-registered-buffers Register buffers with io_uring "
                 "for optimal performance"
              << std::endl;
#endif
    std::cout << "  --direct-io              Use O_DIRECT with raw syscalls "
                 "(bypasses StorageFile)"
              << std::endl;
    std::cout << "  --iterations <n>     Number of iterations (default: 5)"
              << std::endl;
    std::cout << "  --no-verify          Skip data verification" << std::endl;
    std::cout << "  --help               Show this help message" << std::endl;
}

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);

    BenchmarkConfig config;

    // Parse command line arguments
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help") {
            PrintUsage(argv[0]);
            return 0;
        } else if (arg == "--file" && i + 1 < argc) {
            config.file_path = argv[++i];
        } else if (arg == "--size" && i + 1 < argc) {
            config.data_size = std::stoull(argv[++i]);
        } else if (arg == "--size-mb" && i + 1 < argc) {
            config.data_size = std::stoull(argv[++i]) * 1024 * 1024;
        } else if (arg == "--chunk-size" && i + 1 < argc) {
            config.chunk_size =
                std::stoull(argv[++i]) * 1024;  // Convert KB to bytes
        } else if (arg == "--use-uring") {
#ifdef USE_URING
            config.use_uring = true;
#else
            std::cerr << "Warning: io_uring support not compiled in"
                      << std::endl;
#endif
        } else if (arg == "--queue-depth" && i + 1 < argc) {
            config.uring_queue_depth = std::stoul(argv[++i]);
        } else if (arg == "--uring-direct-io") {
#ifdef USE_URING
            config.use_uring_direct_io = true;
            if (!config.use_uring) {
                std::cerr << "Note: --uring-direct-io requires --use-uring, "
                             "enabling io_uring"
                          << std::endl;
                config.use_uring = true;
            }
#else
            std::cerr << "Warning: io_uring support not compiled in"
                      << std::endl;
#endif
        } else if (arg == "--uring-direct-io-zerocopy") {
#ifdef USE_URING
            config.use_uring_direct_io_zero_copy = true;
            if (!config.use_uring) {
                std::cerr << "Note: --uring-direct-io-zerocopy requires "
                             "--use-uring, enabling io_uring"
                          << std::endl;
                config.use_uring = true;
            }
#else
            std::cerr << "Warning: io_uring support not compiled in"
                      << std::endl;
#endif
        } else if (arg == "--use-registered-buffers") {
#ifdef USE_URING
            config.use_registered_buffers = true;
            if (!config.use_uring) {
                std::cerr << "Note: --use-registered-buffers requires "
                             "--use-uring, enabling io_uring"
                          << std::endl;
                config.use_uring = true;
            }
            if (!config.use_uring_direct_io_zero_copy) {
                std::cerr << "Note: --use-registered-buffers works best with "
                             "--uring-direct-io-zerocopy"
                          << std::endl;
            }
#else
            std::cerr << "Warning: io_uring support not compiled in"
                      << std::endl;
#endif
        } else if (arg == "--direct-io") {
            config.use_direct_io = true;
        } else if (arg == "--iterations" && i + 1 < argc) {
            config.iterations = std::stoi(argv[++i]);
        } else if (arg == "--no-verify") {
            config.verify_data = false;
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            PrintUsage(argv[0]);
            return 1;
        }
    }

    FileInterfaceBenchmark benchmark(config);
    benchmark.Run();

    return 0;
}
