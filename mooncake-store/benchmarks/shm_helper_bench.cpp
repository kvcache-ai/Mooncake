#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

#include "shm_helper.h"

namespace {

size_t parse_size_mb(const char *value) {
    char *end = nullptr;
    const unsigned long mb = std::strtoul(value, &end, 10);
    if (end == value || *end != '\0' || mb == 0) {
        throw std::invalid_argument("size_mb must be a positive integer");
    }
    return static_cast<size_t>(mb) * 1024 * 1024;
}

}  // namespace

int main(int argc, char **argv) {
    const size_t size_bytes =
        argc > 1 ? parse_size_mb(argv[1]) : 1024UL * 1024 * 1024;
    const auto start = std::chrono::steady_clock::now();
    void *buffer = mooncake::ShmHelper::getInstance()->allocate(size_bytes);
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start)
            .count();

    std::cout << "size_bytes=" << size_bytes << " elapsed_ms=" << elapsed_ms
              << " threads="
              << (std::getenv("MC_STORE_SHM_POPULATE_THREADS")
                      ? std::getenv("MC_STORE_SHM_POPULATE_THREADS")
                      : "default")
              << std::endl;
    return mooncake::ShmHelper::getInstance()->free(buffer);
}
