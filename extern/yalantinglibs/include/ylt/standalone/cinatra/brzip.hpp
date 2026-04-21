#pragma once
#include <brotli/decode.h>
#include <brotli/encode.h>

#include <array>
#include <sstream>
#include <string>
#include <string_view>

namespace cinatra::br_codec {

#define BROTLI_BUFFER_SIZE 1024

inline bool brotli_compress(std::string_view input, std::string &output) {
  auto instance = BrotliEncoderCreateInstance(nullptr, nullptr, nullptr);
  std::array<uint8_t, BROTLI_BUFFER_SIZE> buffer;
  std::stringstream result;

  size_t available_in = input.size(), available_out = buffer.size();
  const uint8_t *next_in = reinterpret_cast<const uint8_t *>(input.data());
  uint8_t *next_out = buffer.data();

  do {
    int ret = BrotliEncoderCompressStream(instance, BROTLI_OPERATION_FINISH,
                                          &available_in, &next_in,
                                          &available_out, &next_out, nullptr);
    if (!ret)
      return false;
    result.write(reinterpret_cast<const char *>(buffer.data()),
                 buffer.size() - available_out);
    available_out = buffer.size();
    next_out = buffer.data();
  } while (!(available_in == 0 && BrotliEncoderIsFinished(instance)));

  BrotliEncoderDestroyInstance(instance);
  output = result.str();
  return true;
}

inline bool brotli_decompress(std::string_view input,
                              std::string &decompressed) {
  if (input.size() == 0)
    return false;

  size_t available_in = input.size();
  auto next_in = (const uint8_t *)(input.data());
  decompressed = std::string(available_in * 3, 0);
  size_t available_out = decompressed.size();
  auto next_out = (uint8_t *)(decompressed.data());
  size_t total_out{0};
  bool done = false;
  auto s = BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
  while (!done) {
    auto result = BrotliDecoderDecompressStream(
        s, &available_in, &next_in, &available_out, &next_out, &total_out);
    if (result == BROTLI_DECODER_RESULT_SUCCESS) {
      decompressed.resize(total_out);
      BrotliDecoderDestroyInstance(s);
      return true;
    }
    else if (result == BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT) {
      if (total_out != decompressed.size()) {
        return false;
      }
      decompressed.resize(total_out * 2);
      next_out = (uint8_t *)(decompressed.data() + total_out);
      available_out = total_out;
    }
    else {
      decompressed.resize(0);
      BrotliDecoderDestroyInstance(s);
      return true;
    }
  }
  return true;
}
}  // namespace cinatra::br_codec

// namespace cinatra::br_codec