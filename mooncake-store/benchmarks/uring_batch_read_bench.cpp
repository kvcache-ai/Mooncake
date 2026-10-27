#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "file_interface.h"

#ifdef USE_URING
namespace {

constexpr size_t kAlignment = 4096;

struct FreeDeleter {
    void operator()(void* pointer) const { std::free(pointer); }
};

using AlignedBuffer = std::unique_ptr<void, FreeDeleter>;

AlignedBuffer AllocateAligned(size_t size) {
    void* pointer = nullptr;
    if (posix_memalign(&pointer, kAlignment, size) != 0) return nullptr;
    return AlignedBuffer(pointer);
}

bool PrepareFile(const std::string& path, size_t size) {
    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        perror("open");
        return false;
    }

    constexpr size_t kWriteSize = 4 * 1024 * 1024;
    auto buffer = AllocateAligned(kWriteSize);
    if (!buffer) {
        close(fd);
        return false;
    }
    std::memset(buffer.get(), 0x5a, kWriteSize);

    size_t written = 0;
    while (written < size) {
        size_t length = std::min(kWriteSize, size - written);
        ssize_t result = pwrite(fd, buffer.get(), length, written);
        if (result != static_cast<ssize_t>(length)) {
            perror("pwrite");
            close(fd);
            return false;
        }
        written += length;
    }

    bool ok = fdatasync(fd) == 0;
    if (!ok) perror("fdatasync");
    close(fd);
    return ok;
}

struct Result {
    double seconds;
    uint64_t operations;
    uint64_t bytes;
};

Result RunSequential(mooncake::UringFile& file,
                     std::vector<mooncake::UringFile::ReadDesc>& descs,
                     const std::vector<std::vector<off_t>>& offsets) {
    auto start = std::chrono::steady_clock::now();
    uint64_t operations = 0;
    uint64_t bytes = 0;
    for (const auto& iteration_offsets : offsets) {
        for (size_t i = 0; i < descs.size(); ++i) {
            auto result = file.read_aligned(descs[i].buf, descs[i].len,
                                            iteration_offsets[i]);
            if (!result || result.value() != descs[i].len) {
                std::cerr << "sequential read failed at request " << i << '\n';
                std::exit(2);
            }
            ++operations;
            bytes += result.value();
        }
    }
    auto end = std::chrono::steady_clock::now();
    return {std::chrono::duration<double>(end - start).count(), operations,
            bytes};
}

Result RunBatch(mooncake::UringFile& file,
                std::vector<mooncake::UringFile::ReadDesc>& descs,
                const std::vector<std::vector<off_t>>& offsets) {
    auto start = std::chrono::steady_clock::now();
    uint64_t operations = 0;
    uint64_t bytes = 0;
    for (const auto& iteration_offsets : offsets) {
        for (size_t i = 0; i < descs.size(); ++i) {
            descs[i].off = iteration_offsets[i];
        }
        auto result = file.batch_read(descs.data(), descs.size());
        if (!result) {
            std::cerr << "batch read failed\n";
            std::exit(3);
        }
        for (const auto& desc : descs) {
            if (!desc.completed || desc.error != mooncake::ErrorCode::OK ||
                desc.bytes_read != desc.len) {
                std::cerr << "batch read returned an invalid result\n";
                std::exit(4);
            }
            ++operations;
            bytes += desc.bytes_read;
        }
    }
    auto end = std::chrono::steady_clock::now();
    return {std::chrono::duration<double>(end - start).count(), operations,
            bytes};
}

void PrintResult(const std::string& name, const Result& result) {
    double iops = static_cast<double>(result.operations) / result.seconds;
    double mib_per_second =
        static_cast<double>(result.bytes) / (1024.0 * 1024.0) / result.seconds;
    double latency_us = result.seconds * 1e6 / result.operations;
    std::cout << std::fixed << std::setprecision(2) << name
              << ": seconds=" << result.seconds << " iops=" << iops
              << " MiB/s=" << mib_per_second
              << " average_us_per_request=" << latency_us << '\n';
}

size_t ParseSize(const char* value, size_t fallback) {
    if (value == nullptr) return fallback;
    char* end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    return end != value && *end == '\0' ? static_cast<size_t>(parsed)
                                        : fallback;
}

}  // namespace

int main(int argc, char** argv) {
    std::string path = argc > 1 ? argv[1] : "/tmp/uring_batch_read_bench.dat";
    size_t file_size_mib = ParseSize(argc > 2 ? argv[2] : nullptr, 1024);
    size_t request_count = ParseSize(argc > 3 ? argv[3] : nullptr, 32);
    size_t block_size = ParseSize(argc > 4 ? argv[4] : nullptr, 4096);
    size_t iterations = ParseSize(argc > 5 ? argv[5] : nullptr, 10000);
    size_t rounds = ParseSize(argc > 6 ? argv[6] : nullptr, 4);

    if (request_count == 0 || block_size == 0 || iterations == 0 ||
        block_size % kAlignment != 0) {
        std::cerr << "invalid benchmark parameters\n";
        return 1;
    }

    size_t file_size = file_size_mib * 1024 * 1024;
    file_size -= file_size % block_size;
    std::cout << "preparing " << file_size_mib << " MiB file at " << path
              << '\n';
    if (!PrepareFile(path, file_size)) return 1;

    int fd = open(path.c_str(), O_RDONLY | O_DIRECT | O_CLOEXEC);
    if (fd < 0) {
        perror("open O_DIRECT");
        return 1;
    }
    mooncake::UringFile file(path, fd, 32, true);

    auto buffer = AllocateAligned(request_count * block_size);
    if (!buffer) return 1;
    mooncake::UringFile::register_global_buffer(buffer.get(),
                                                request_count * block_size);

    std::vector<mooncake::UringFile::ReadDesc> descs;
    descs.reserve(request_count);
    for (size_t i = 0; i < request_count; ++i) {
        descs.push_back(mooncake::UringFile::ReadDesc{
            static_cast<char*>(buffer.get()) + i * block_size, block_size, 0});
    }

    std::mt19937_64 generator(20260817);
    std::uniform_int_distribution<uint64_t> distribution(
        0, file_size / block_size - 1);
    std::vector<std::vector<off_t>> offsets(iterations,
                                            std::vector<off_t>(request_count));
    for (auto& iteration_offsets : offsets) {
        for (auto& offset : iteration_offsets) {
            offset = static_cast<off_t>(distribution(generator) * block_size);
        }
    }

    RunSequential(file, descs, {offsets.front()});
    RunBatch(file, descs, {offsets.front()});

    double sequential_seconds = 0;
    double batch_seconds = 0;
    uint64_t operations = 0;
    uint64_t bytes = 0;
    for (size_t round = 0; round < rounds; ++round) {
        Result sequential;
        Result batch;
        if (round % 2 == 0) {
            sequential = RunSequential(file, descs, offsets);
            batch = RunBatch(file, descs, offsets);
        } else {
            batch = RunBatch(file, descs, offsets);
            sequential = RunSequential(file, descs, offsets);
        }
        PrintResult("sequential round " + std::to_string(round), sequential);
        PrintResult("batch round " + std::to_string(round), batch);
        sequential_seconds += sequential.seconds;
        batch_seconds += batch.seconds;
        operations += sequential.operations;
        bytes += sequential.bytes;
    }

    Result sequential_total{sequential_seconds, operations, bytes};
    Result batch_total{batch_seconds, operations, bytes};
    PrintResult("sequential total", sequential_total);
    PrintResult("batch total", batch_total);
    std::cout << "speedup=" << sequential_seconds / batch_seconds << "x\n";

    mooncake::UringFile::unregister_global_buffer();
    unlink(path.c_str());
    return 0;
}
#else
int main() {
    std::cerr << "io_uring support is not enabled\n";
    return 1;
}
#endif
