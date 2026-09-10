#pragma once

#include <zstd.h>
#include <vector>

namespace mooncake {

// zstd compression - supports custom compression level
static inline std::vector<uint8_t> zstd_compress(const std::string &data,
                                                 int compression_level = 1) {
    size_t compress_bound = ZSTD_compressBound(data.size());
    std::vector<uint8_t> compressed_data(compress_bound);

    size_t compressed_size =
        ZSTD_compress(compressed_data.data(), compress_bound,
                      reinterpret_cast<const void *>(data.data()), data.size(),
                      compression_level);

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error(
            "ZSTD compression failed: " +
            std::string(ZSTD_getErrorName(compressed_size)));
    }

    compressed_data.resize(compressed_size);
    return compressed_data;
}

// zstd compression - binary data version
static inline std::vector<uint8_t> zstd_compress(
    const std::vector<uint8_t> &data, int compression_level = 1) {
    size_t compress_bound = ZSTD_compressBound(data.size());
    std::vector<uint8_t> compressed_data(compress_bound);

    size_t compressed_size =
        ZSTD_compress(compressed_data.data(), compress_bound, data.data(),
                      data.size(), compression_level);

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error(
            "ZSTD compression failed: " +
            std::string(ZSTD_getErrorName(compressed_size)));
    }

    compressed_data.resize(compressed_size);
    return compressed_data;
}

// zstd compression - binary data version
static inline std::vector<uint8_t> zstd_compress(const uint8_t *data,
                                                 size_t size,
                                                 int compression_level = 1) {
    size_t compress_bound = ZSTD_compressBound(size);
    std::vector<uint8_t> compressed_data;
    compressed_data.resize(compress_bound);

    size_t compressed_size = ZSTD_compress(
        compressed_data.data(), compress_bound, data, size, compression_level);

    if (ZSTD_isError(compressed_size)) {
        throw std::runtime_error(
            "ZSTD compression failed: " +
            std::string(ZSTD_getErrorName(compressed_size)));
    }

    compressed_data.resize(compressed_size);
    return compressed_data;
}

// zstd decompression - binary data version
static inline std::vector<uint8_t> zstd_decompress(
    const std::vector<uint8_t> &compressed_data, size_t original_size) {
    std::vector<uint8_t> decompressed_data(original_size);

    size_t decompressed_size =
        ZSTD_decompress(decompressed_data.data(), original_size,
                        compressed_data.data(), compressed_data.size());

    if (ZSTD_isError(decompressed_size)) {
        throw std::runtime_error(
            "ZSTD decompression failed: " +
            std::string(ZSTD_getErrorName(decompressed_size)));
    }

    decompressed_data.resize(decompressed_size);
    return decompressed_data;
}

// Decompression function with automatic size detection
static inline std::string zstd_decompress_to_string(
    const std::vector<uint8_t> &compressed_data) {
    // First get the decompressed size
    unsigned long long decompressed_size = ZSTD_getFrameContentSize(
        compressed_data.data(), compressed_data.size());

    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error(
            "ZSTD decompression failed: not a valid compressed frame");
    }

    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw std::runtime_error(
            "ZSTD decompression failed: original size unknown");
    }

    std::string decompressed_data(decompressed_size, 0);

    size_t result =
        ZSTD_decompress(decompressed_data.data(), decompressed_size,
                        compressed_data.data(), compressed_data.size());

    if (ZSTD_isError(result)) {
        throw std::runtime_error("ZSTD decompression failed: " +
                                 std::string(ZSTD_getErrorName(result)));
    }
    decompressed_data.resize(result);
    return decompressed_data;
}

// Decompression function with automatic size detection - binary version
static inline std::vector<uint8_t> zstd_decompress(
    const std::vector<uint8_t> &compressed_data) {
    // First get the decompressed size
    unsigned long long decompressed_size = ZSTD_getFrameContentSize(
        compressed_data.data(), compressed_data.size());

    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error(
            "ZSTD decompression failed: not a valid compressed frame");
    }

    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw std::runtime_error(
            "ZSTD decompression failed: original size unknown");
    }

    std::vector<uint8_t> decompressed_data(decompressed_size);

    size_t result =
        ZSTD_decompress(decompressed_data.data(), decompressed_size,
                        compressed_data.data(), compressed_data.size());

    if (ZSTD_isError(result)) {
        throw std::runtime_error("ZSTD decompression failed: " +
                                 std::string(ZSTD_getErrorName(result)));
    }
    decompressed_data.resize(result);
    return decompressed_data;
}

static inline std::vector<uint8_t> zstd_decompress(const uint8_t *data,
                                                   size_t size) {
    // First get the decompressed size
    unsigned long long decompressed_size = ZSTD_getFrameContentSize(data, size);

    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error(
            "ZSTD decompression failed: not a valid compressed frame");
    }

    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw std::runtime_error(
            "ZSTD decompression failed: original size unknown");
    }

    std::vector<uint8_t> decompressed_data;
    decompressed_data.resize(decompressed_size);

    size_t result = ZSTD_decompress(decompressed_data.data(), decompressed_size,
                                    data, size);

    if (ZSTD_isError(result)) {
        throw std::runtime_error("ZSTD decompression failed: " +
                                 std::string(ZSTD_getErrorName(result)));
    }
    decompressed_data.resize(result);
    return decompressed_data;
}

static inline std::vector<uint8_t> zstd_decompress(
    const uint8_t *data, size_t size, size_t max_decompressed_size) {
    if (data == nullptr || size == 0) {
        throw std::runtime_error("zstd_decompress: empty input data");
    }

    // Check decompressed size before allocating
    unsigned long long decompressed_size = ZSTD_getFrameContentSize(data, size);

    if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw std::runtime_error(
            "zstd_decompress: unknown decompressed size in frame");
    }

    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
        throw std::runtime_error("zstd_decompress: invalid zstd frame header");
    }

    if (decompressed_size > max_decompressed_size) {
        throw std::runtime_error("zstd_decompress: decompressed size " +
                                 std::to_string(decompressed_size) +
                                 " exceeds maximum allowed " +
                                 std::to_string(max_decompressed_size));
    }

    std::vector<uint8_t> decompressed(decompressed_size);

    size_t actual_size =
        ZSTD_decompress(decompressed.data(), decompressed.size(), data, size);

    if (ZSTD_isError(actual_size)) {
        throw std::runtime_error(std::string("zstd_decompress failed: ") +
                                 ZSTD_getErrorName(actual_size));
    }

    decompressed.resize(actual_size);
    return decompressed;
}

}  // namespace mooncake
