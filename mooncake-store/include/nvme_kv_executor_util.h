#pragma once

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "nvme_kv_executor.h"
#include "types.h"

namespace mooncake {

struct NvmeKvPackedKeyFields {
    uint32_t cdw2 = 0;
    uint32_t cdw3 = 0;
    uint32_t cdw14 = 0;
    uint32_t cdw15 = 0;
};

constexpr uint32_t kDefaultNvmeKvQueueDepth = 256;
constexpr uint32_t kDefaultNvmeKvRuntimeTransferLimit = 270336;
constexpr uint32_t kDefaultNvmeKvProtocolMaxValueSize = 512 * 1024;
constexpr uint32_t kDefaultNvmeKvTransferAlignmentBytes = 4096;
constexpr uint32_t kDefaultNvmeKvValueBlockUnitBytes = 512;
constexpr uint32_t kNvmeKvMaxKeySizeBytes = 16;
constexpr uint32_t kNvmeKvCommandTimeoutMs = 30000;
constexpr uint32_t kNvmeKvStoreIfNotExistsOption = 0x2;
constexpr uint8_t kNvmeKvCommandSetIdentifier = 0x01;
constexpr uint8_t kNvmeKvStoreOpcode = 0x01;
constexpr uint8_t kNvmeKvRetrieveOpcode = 0x02;
constexpr uint8_t kNvmeKvDeleteOpcode = 0x10;

struct NvmeKvFreeDeleter {
    void operator()(void *ptr) const { std::free(ptr); }
};

using NvmeKvAlignedBuffer = std::unique_ptr<char, NvmeKvFreeDeleter>;

uint32_t ParseNvmeKvU32EnvOr(const char *name, uint32_t fallback);
NvmeKvAlignedBuffer AllocateNvmeKvAlignedBuffer(size_t size);
uint32_t NvmeKvTransferAlignmentBytes();
uint32_t NvmeKvValueBlockUnitBytes();
uint32_t RoundUpToNvmeKvTransferBytes(uint32_t bytes);
uint32_t RoundDownToNvmeKvTransferBytes(uint32_t bytes);
uint32_t ComputeNvmeKvValueBlockCountMinusOne(uint32_t bytes);
uint32_t ResolveNvmeKvStoreSubmissionBytes(uint32_t logical_bytes);
uint32_t ResolveNvmeKvInitialRetrieveBytes(uint32_t size_hint,
                                           uint32_t effective_max_value_size);
uint32_t ResolveNvmeKvRetrievedValueSize(const char *buffer,
                                         uint32_t returned_size,
                                         uint32_t max_size, uint32_t size_hint);
bool ShouldRetryNvmeKvRetrieveWithMaxBuffer(ErrorCode error, uint32_t size_hint,
                                            uint32_t request_bytes,
                                            uint32_t effective_max_value_size);
NvmeKvCommandExecutor::Capabilities BuildNvmeKvCapabilities(
    uint32_t default_queue_depth, uint32_t queue_depth,
    uint32_t runtime_transfer_limit);
std::string NvmeKvPhysicalKeyToHex(
    const NvmeKvCommandExecutor::PhysicalKey &key);
NvmeKvPackedKeyFields PackNvmeKvPhysicalKey(
    const NvmeKvCommandExecutor::PhysicalKey &key);
ErrorCode MapNvmeKvStatus(uint32_t status, bool is_write);
ErrorCode MapNvmeKvTransportError(int err, bool is_write);
bool IsNvmeKvControlFlowError(ErrorCode error);

inline uint32_t BuildNvmeKvKeyLengthField(size_t key_length) {
    return static_cast<uint32_t>(key_length) & 0xFFu;
}

template <typename Command>
void EncodeNvmeKvKey(const NvmeKvCommandExecutor::PhysicalKey &key,
                     Command &cmd) {
    const auto fields = PackNvmeKvPhysicalKey(key);
    cmd.cdw2 = fields.cdw2;
    cmd.cdw3 = fields.cdw3;
    cmd.cdw14 = fields.cdw14;
    cmd.cdw15 = fields.cdw15;
}

template <typename Command>
void BuildNvmeKvStoreCommand(Command &cmd, uint32_t nsid,
                             const NvmeKvCommandExecutor::PhysicalKey &key,
                             const void *data, uint32_t transfer_bytes) {
    cmd = {};
    cmd.opcode = kNvmeKvStoreOpcode;
    cmd.nsid = nsid;
    cmd.addr = reinterpret_cast<uint64_t>(data);
    cmd.data_len = transfer_bytes;
    cmd.cdw10 = transfer_bytes;
    cmd.cdw11 = (kNvmeKvStoreIfNotExistsOption << 8) |
                BuildNvmeKvKeyLengthField(key.size());
    cmd.cdw12 = ComputeNvmeKvValueBlockCountMinusOne(transfer_bytes);
    cmd.timeout_ms = kNvmeKvCommandTimeoutMs;
    EncodeNvmeKvKey(key, cmd);
}

template <typename Command>
void BuildNvmeKvRetrieveCommand(Command &cmd, uint32_t nsid,
                                const NvmeKvCommandExecutor::PhysicalKey &key,
                                void *data, uint32_t transfer_bytes) {
    cmd = {};
    cmd.opcode = kNvmeKvRetrieveOpcode;
    cmd.nsid = nsid;
    cmd.addr = reinterpret_cast<uint64_t>(data);
    cmd.data_len = transfer_bytes;
    cmd.cdw10 = transfer_bytes;
    cmd.cdw11 = BuildNvmeKvKeyLengthField(key.size());
    cmd.cdw12 = ComputeNvmeKvValueBlockCountMinusOne(transfer_bytes);
    cmd.timeout_ms = kNvmeKvCommandTimeoutMs;
    EncodeNvmeKvKey(key, cmd);
}

template <typename Command>
void BuildNvmeKvDeleteCommand(Command &cmd, uint32_t nsid,
                              const NvmeKvCommandExecutor::PhysicalKey &key) {
    cmd = {};
    cmd.opcode = kNvmeKvDeleteOpcode;
    cmd.nsid = nsid;
    cmd.cdw11 = BuildNvmeKvKeyLengthField(key.size());
    cmd.timeout_ms = kNvmeKvCommandTimeoutMs;
    EncodeNvmeKvKey(key, cmd);
}

}  // namespace mooncake
