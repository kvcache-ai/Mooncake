#include "codec/kv_codec.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <iostream>

using namespace mooncake::codec;

namespace {
void Require(bool condition) {
    if (!condition) throw std::runtime_error("Codec buffer contract failed");
}

template <typename Exception, typename F>
void Reject(F&& operation) {
    try {
        operation();
    } catch (const Exception&) {
        return;
    }
    throw std::runtime_error("Expected codec rejection");
}

void TestBufferContract() {
    ScaledFp8Codec codec(2);
    TensorDesc desc{Dtype::Float16, {4}};
    const std::array<uint16_t, 4> values{0x3c00, 0xbc00, 0, 0x8000};
    // Deliberately unaligned native FP16 input and output with guard bytes.
    std::array<uint8_t, 10> source{}, destination{};
    std::memcpy(source.data() + 1, values.data(), sizeof(values));
    destination.fill(0xa5);
    auto input = std::span<const uint8_t>(source).subspan(1, 8);
    auto output = std::span<uint8_t>(destination).subspan(1, 8);
    const size_t size = codec.EncodedSize(desc);
    std::vector<uint8_t> record(size + 2, 0xa5);
    auto encoded = std::span<uint8_t>(record).subspan(1, size);
    Require(codec.Encode(desc, input, encoded) == size);
    Require(record.front() == 0xa5 && record.back() == 0xa5);
    codec.Decode(encoded, desc, output);
    Require(std::equal(input.begin(), input.end(), output.begin()));
    Require(destination.front() == 0xa5 && destination.back() == 0xa5);

    Reject<std::invalid_argument>(
        [&] { codec.Encode(desc, input, encoded.first(size - 1)); });
    Reject<std::invalid_argument>(
        [&] { codec.Encode(desc, encoded.first(8), encoded); });
    Reject<std::invalid_argument>(
        [&] { codec.Decode(encoded, desc, encoded.first(8)); });
    Reject<std::invalid_argument>(
        [&] { codec.Decode(encoded, desc, output.first(7)); });

    destination.fill(0xa5);
    const auto before = destination;
    // Every truncated length and every single flipped bit must be rejected
    // before touching the output. Run this under ASan/UBSan as well.
    for (size_t length = 0; length < size; ++length) {
        Reject<InvalidRecord>(
            [&] { codec.Decode(encoded.first(length), desc, output); });
        Require(destination == before);
    }
    for (size_t i = 0; i < size; ++i) {
        for (unsigned bit = 0; bit < 8; ++bit) {
            encoded[i] ^= uint8_t(1U << bit);
            Reject<InvalidRecord>([&] { codec.Decode(encoded, desc, output); });
            Require(destination == before);
            encoded[i] ^= uint8_t(1U << bit);
        }
    }
}
}  // namespace

int main() {
    try {
        TestBufferContract();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
    return 0;
}
