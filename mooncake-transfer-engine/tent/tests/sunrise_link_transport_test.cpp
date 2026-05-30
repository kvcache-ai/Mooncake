#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "gpu_vendor/sunrise.h"
#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"

namespace mooncake {
namespace tent {
namespace {

void CheckTangError(tangError_t result, const char* message) {
    ASSERT_EQ(result, tangSuccess)
        << message << " (error=" << result
        << ", detail=" << tangGetErrorString(result) << ")";
}

void WaitBatchDone(TransferEngine* engine, BatchID batch_id) {
    TransferStatus status;
    for (int i = 0; i < 5000; ++i) {
        auto s = engine->getTransferStatus(batch_id, status);
        ASSERT_TRUE(s.ok()) << "getTransferStatus failed: " << s.ToString();
        if (status.s == TransferStatusEnum::COMPLETED) return;
        ASSERT_NE(status.s, TransferStatusEnum::FAILED);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    FAIL() << "timeout waiting batch completion";
}

void RunWriteReadCase(int server_gpu, int client_gpu) {
    constexpr size_t kDataLength = 4 * 1024 * 1024;
    const std::string server_local_name =
        "sunrise_ut_server_" + std::to_string(getpid());

    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);
        auto server_conf = std::make_shared<Config>();
        server_conf->set("metadata_type", "p2p");
        server_conf->set("metadata_servers", "P2PHANDSHAKE");
        server_conf->set("local_segment_name", server_local_name);
        server_conf->set("transports/sunrise_link/enable", true);
        auto server_engine = std::make_unique<TransferEngine>(server_conf);
        if (!server_engine->available()) _exit(2);
        void* server_buf = nullptr;
        if (tangSetDevice(server_gpu) != tangSuccess) _exit(3);
        if (tangMalloc(&server_buf, kDataLength * 2) != tangSuccess) _exit(4);
        MemoryOptions server_opts;
        server_opts.location = "cuda:" + std::to_string(server_gpu);
        auto s = server_engine->registerLocalMemory(server_buf, kDataLength * 2,
                                                    server_opts);
        if (!s.ok()) _exit(5);
        const std::string seg = server_engine->getSegmentName();
        uint32_t len = static_cast<uint32_t>(seg.size());
        if (write(ready_pipe[1], &len, sizeof(len)) != sizeof(len)) _exit(6);
        if (write(ready_pipe[1], seg.data(), len) != static_cast<ssize_t>(len))
            _exit(7);
        char stop = 0;
        (void)read(stop_pipe[0], &stop, 1);
        (void)server_engine->unregisterLocalMemory(server_buf);
        tangSetDevice(server_gpu);
        tangFree(server_buf);
        _exit(0);
    }

    close(ready_pipe[1]);
    close(stop_pipe[0]);
    uint32_t seg_len = 0;
    ssize_t got = read(ready_pipe[0], &seg_len, sizeof(seg_len));
    if (got != static_cast<ssize_t>(sizeof(seg_len))) {
        int wstatus = 0;
        (void)waitpid(child, &wstatus, 0);
        if (got == 0 && WIFEXITED(wstatus)) {
            GTEST_SKIP() << "server init failed for case " << server_gpu << "->"
                         << client_gpu
                         << ", child exit=" << WEXITSTATUS(wstatus);
        }
        ASSERT_EQ(got, static_cast<ssize_t>(sizeof(seg_len)));
    }
    ASSERT_GT(seg_len, 0u);
    std::string server_segment(seg_len, '\0');
    ASSERT_EQ(read(ready_pipe[0], server_segment.data(), seg_len),
              static_cast<ssize_t>(seg_len));

    int gpu_count = 0;
    CheckTangError(tangGetDeviceCount(&gpu_count), "tangGetDeviceCount");
    if (gpu_count <= 0) {
        const char stop = 'Q';
        (void)write(stop_pipe[1], &stop, 1);
        int wstatus = 0;
        (void)waitpid(child, &wstatus, 0);
        GTEST_SKIP() << "no sunrise GPU detected";
    }
    if (server_gpu < 0 || client_gpu < 0 || server_gpu >= gpu_count ||
        client_gpu >= gpu_count) {
        const char stop = 'Q';
        (void)write(stop_pipe[1], &stop, 1);
        int wstatus = 0;
        (void)waitpid(child, &wstatus, 0);
        GTEST_SKIP() << "insufficient GPUs for case " << server_gpu << "->"
                     << client_gpu << ", detected: " << gpu_count;
    }

    auto client_conf = std::make_shared<Config>();
    client_conf->set("metadata_type", "p2p");
    client_conf->set("metadata_servers", "P2PHANDSHAKE");
    client_conf->set("local_segment_name", "sunrise_ut_client");
    client_conf->set("transports/sunrise_link/enable", true);
    auto client_engine = std::make_unique<TransferEngine>(client_conf);
    ASSERT_TRUE(client_engine->available());

    void* client_buf = nullptr;
    CheckTangError(tangSetDevice(client_gpu), "tangSetDevice(client)");
    CheckTangError(tangMalloc(&client_buf, kDataLength * 2),
                   "tangMalloc(client)");
    MemoryOptions client_opts;
    client_opts.location = "cuda:" + std::to_string(client_gpu);
    auto s = client_engine->registerLocalMemory(client_buf, kDataLength * 2,
                                                client_opts);
    ASSERT_TRUE(s.ok()) << s.ToString();

    SegmentID segment_id = 0;
    for (int i = 0; i < 100; ++i) {
        s = client_engine->openSegment(segment_id, server_segment);
        if (s.ok()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(s.ok()) << s.ToString();

    SegmentInfo segment_info;
    s = client_engine->getSegmentInfo(segment_id, segment_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(segment_info.buffers.empty());
    const uint64_t remote_base = segment_info.buffers[0].base;

    std::vector<char> host_data(kDataLength, 'S');
    CheckTangError(tangSetDevice(client_gpu), "tangSetDevice(client copy)");
    CheckTangError(tangMemcpy(client_buf, host_data.data(), kDataLength,
                              tangMemcpyHostToDevice),
                   "tangMemcpy H2D");

    BatchID batch = client_engine->allocateBatch(1);
    Request req;
    req.opcode = Request::WRITE;
    req.length = kDataLength;
    req.source = static_cast<uint8_t*>(client_buf);
    req.target_id = segment_id;
    req.target_offset = remote_base;
    s = client_engine->submitTransfer(batch, {req});
    ASSERT_TRUE(s.ok()) << s.ToString();
    WaitBatchDone(client_engine.get(), batch);
    s = client_engine->freeBatch(batch);
    ASSERT_TRUE(s.ok()) << s.ToString();

    batch = client_engine->allocateBatch(1);
    req.opcode = Request::READ;
    req.source = static_cast<uint8_t*>(client_buf) + kDataLength;
    s = client_engine->submitTransfer(batch, {req});
    ASSERT_TRUE(s.ok()) << s.ToString();
    WaitBatchDone(client_engine.get(), batch);
    s = client_engine->freeBatch(batch);
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::vector<char> host_check(kDataLength);
    CheckTangError(tangSetDevice(client_gpu), "tangSetDevice(client verify)");
    CheckTangError(tangMemcpy(host_check.data(),
                              static_cast<uint8_t*>(client_buf) + kDataLength,
                              kDataLength, tangMemcpyDeviceToHost),
                   "tangMemcpy D2H");
    for (size_t i = 0; i < kDataLength; ++i) ASSERT_EQ(host_check[i], 'S');

    s = client_engine->unregisterLocalMemory(client_buf);
    ASSERT_TRUE(s.ok()) << s.ToString();
    CheckTangError(tangSetDevice(client_gpu), "tangSetDevice(client free)");
    CheckTangError(tangFree(client_buf), "tangFree(client)");

    const char stop = 'Q';
    ASSERT_EQ(write(stop_pipe[1], &stop, 1), 1);
    int wstatus = 0;
    ASSERT_EQ(waitpid(child, &wstatus, 0), child);
    ASSERT_TRUE(WIFEXITED(wstatus));
    ASSERT_EQ(WEXITSTATUS(wstatus), 0);
}

TEST(SunriseLinkTransportTest, WriteAndRead_0_to_1) { RunWriteReadCase(0, 1); }

TEST(SunriseLinkTransportTest, WriteAndRead_1_to_0) { RunWriteReadCase(1, 0); }

TEST(SunriseLinkTransportTest, WriteAndRead_2_to_4) { RunWriteReadCase(2, 4); }

}  // namespace
}  // namespace tent
}  // namespace mooncake
