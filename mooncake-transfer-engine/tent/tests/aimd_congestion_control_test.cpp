// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/runtime/aimd_congestion_control.h"

#include <cstring>
#include <memory>

#include <gtest/gtest.h>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {
namespace {

// Helper to build a minimally-populated Request for AdmitContext.
Request makeTestRequest(size_t length, int priority = PRIO_HIGH) {
    Request req{};
    req.opcode = Request::WRITE;
    req.source = nullptr;
    req.target_id = 1;
    req.target_offset = 0;
    req.length = length;
    req.priority = priority;
    return req;
}

// Helper to build an AdmitContext from inflight bytes / device / request.
AdmitContext makeAdmitCtx(const Request& req, uint64_t device_inflight,
                          int device_id, int priority = PRIO_HIGH) {
    return AdmitContext{req, /*global_inflight_bytes=*/0, device_inflight,
                        device_id, priority};
}

// Helper to build a successful CompletionEvent for a device.
CompletionEvent makeCompletion(int device_id, uint64_t bytes,
                               TransferStatusEnum status =
                                   TransferStatusEnum::COMPLETED) {
    CompletionEvent ev{};
    ev.device_id = device_id;
    ev.bytes = bytes;
    ev.latency_us = 0.0;
    ev.status = status;
    ev.priority = PRIO_HIGH;
    return ev;
}

// Helper to build a CongestionSignal for a device with a given severity.
CongestionSignal makeSignal(int device_id, double severity,
                            CongestionSignal::Type type =
                                CongestionSignal::TIMEOUT_SPIKE) {
    CongestionSignal sig{};
    sig.type = type;
    sig.device_id = device_id;
    sig.timestamp_ns = 0;
    sig.severity = severity;
    return sig;
}

// ---------------------------------------------------------------------------
// 1. DefaultConfig
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, DefaultConfig) {
    AimdConfig cfg;
    AimdCongestionControlPlugin plugin(cfg);
    EXPECT_STREQ(plugin.getName(), "aimd");
    // After construction, every device should have the initial window.
    EXPECT_EQ(plugin.getWindowBytes(0), cfg.initial_window_bytes);
    EXPECT_EQ(plugin.getWindowBytes(AimdConfig::kMaxDevices - 1),
              cfg.initial_window_bytes);
}

// ---------------------------------------------------------------------------
// 2. AdmitUnderWindow
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, AdmitUnderWindow) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 1024;
    cfg.window_cap_bytes = 1024;
    cfg.window_floor_bytes = 64;
    AimdCongestionControlPlugin plugin(cfg);

    Request req = makeTestRequest(/*length=*/256);
    AdmitContext ctx = makeAdmitCtx(req, /*device_inflight=*/256,
                                    /*device_id=*/0);
    AdmitDecision decision = plugin.onAdmit(ctx);
    EXPECT_TRUE(decision.admit);
    EXPECT_EQ(decision.defer_us, 0.0f);
    EXPECT_EQ(decision.device_mask_override, 0u);
}

// ---------------------------------------------------------------------------
// 3. RejectOverWindow
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, RejectOverWindow) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 1024;
    cfg.window_cap_bytes = 1024;
    cfg.defer_us = 75.0f;
    AimdCongestionControlPlugin plugin(cfg);

    Request req = makeTestRequest(/*length=*/512);
    // inflight (800) + length (512) = 1312 > 1024 window.
    AdmitContext ctx = makeAdmitCtx(req, /*device_inflight=*/800,
                                    /*device_id=*/0);
    AdmitDecision decision = plugin.onAdmit(ctx);
    EXPECT_FALSE(decision.admit);
    EXPECT_FLOAT_EQ(decision.defer_us, 75.0f);
}

// ---------------------------------------------------------------------------
// 4. AdmitInvalidDevice
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, AdmitInvalidDevice) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 16;  // intentionally tiny
    cfg.window_cap_bytes = 16;
    AimdCongestionControlPlugin plugin(cfg);

    Request req = makeTestRequest(/*length=*/1ull << 30);  // way over window

    // Negative device id — must admit.
    AdmitDecision d1 =
        plugin.onAdmit(makeAdmitCtx(req, /*device_inflight=*/0,
                                    /*device_id=*/-1));
    EXPECT_TRUE(d1.admit);
    EXPECT_EQ(d1.defer_us, 0.0f);

    // device_id == kMaxDevices — out of range, must admit.
    AdmitDecision d2 =
        plugin.onAdmit(makeAdmitCtx(req, /*device_inflight=*/0,
                                    AimdConfig::kMaxDevices));
    EXPECT_TRUE(d2.admit);
    EXPECT_EQ(d2.defer_us, 0.0f);

    // device_id far beyond max — also admit.
    AdmitDecision d3 =
        plugin.onAdmit(makeAdmitCtx(req, /*device_inflight=*/0,
                                    AimdConfig::kMaxDevices + 100));
    EXPECT_TRUE(d3.admit);
}

// ---------------------------------------------------------------------------
// 5. AdditiveIncrease
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, AdditiveIncrease) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 1000;
    cfg.window_cap_bytes = 1'000'000;
    cfg.ai_increment_bytes = 100;
    AimdCongestionControlPlugin plugin(cfg);

    const int device_id = 3;
    EXPECT_EQ(plugin.getWindowBytes(device_id), 1000u);

    plugin.onCompletion(makeCompletion(device_id, /*bytes=*/4096));
    EXPECT_EQ(plugin.getWindowBytes(device_id), 1100u);

    plugin.onCompletion(makeCompletion(device_id, /*bytes=*/4096));
    plugin.onCompletion(makeCompletion(device_id, /*bytes=*/4096));
    EXPECT_EQ(plugin.getWindowBytes(device_id), 1300u);
}

// ---------------------------------------------------------------------------
// 6. WindowCap
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, WindowCap) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 100;
    cfg.window_cap_bytes = 500;
    cfg.ai_increment_bytes = 50;
    AimdCongestionControlPlugin plugin(cfg);

    const int device_id = 0;
    // 8 increments of 50 would reach 500 then exceed; should clamp at cap.
    for (int i = 0; i < 20; ++i) {
        plugin.onCompletion(makeCompletion(device_id, /*bytes=*/1024));
    }
    EXPECT_EQ(plugin.getWindowBytes(device_id), cfg.window_cap_bytes);

    // Further completions should not grow beyond cap.
    plugin.onCompletion(makeCompletion(device_id, /*bytes=*/1024));
    EXPECT_EQ(plugin.getWindowBytes(device_id), cfg.window_cap_bytes);
}

// ---------------------------------------------------------------------------
// 7. MultiplicativeDecrease
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, MultiplicativeDecrease) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 1000;
    cfg.window_cap_bytes = 1000;
    cfg.window_floor_bytes = 1;
    cfg.md_factor = 0.5;
    AimdCongestionControlPlugin plugin(cfg);

    const int device_id = 2;
    EXPECT_EQ(plugin.getWindowBytes(device_id), 1000u);

    // severity = 1.0, md_factor = 0.5 → factor = 0.5 → 1000 * 0.5 = 500.
    plugin.onCongestionSignal(makeSignal(device_id, /*severity=*/1.0));
    EXPECT_EQ(plugin.getWindowBytes(device_id), 500u);

    // severity = 0.5, md_factor = 0.5 → factor = 0.75 → 500 * 0.75 = 375.
    plugin.onCongestionSignal(makeSignal(device_id, /*severity=*/0.5));
    EXPECT_EQ(plugin.getWindowBytes(device_id), 375u);
}

// ---------------------------------------------------------------------------
// 8. WindowFloor
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, WindowFloor) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 1000;
    cfg.window_cap_bytes = 1000;
    cfg.window_floor_bytes = 200;
    cfg.md_factor = 1.0;
    AimdCongestionControlPlugin plugin(cfg);

    const int device_id = 5;
    // severity 1.0 with md_factor 1.0 → factor 0.0 → window collapses,
    // must clamp to floor.
    plugin.onCongestionSignal(makeSignal(device_id, /*severity=*/1.0));
    EXPECT_EQ(plugin.getWindowBytes(device_id), cfg.window_floor_bytes);

    // Subsequent severe signals must not drop below floor either.
    plugin.onCongestionSignal(makeSignal(device_id, /*severity=*/1.0));
    plugin.onCongestionSignal(makeSignal(device_id, /*severity=*/1.0));
    EXPECT_EQ(plugin.getWindowBytes(device_id), cfg.window_floor_bytes);
}

// ---------------------------------------------------------------------------
// 9. MultiDeviceIsolation
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, MultiDeviceIsolation) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 1000;
    cfg.window_cap_bytes = 4000;
    cfg.window_floor_bytes = 1;
    cfg.ai_increment_bytes = 100;
    cfg.md_factor = 0.5;
    AimdCongestionControlPlugin plugin(cfg);

    // Shrink device 0 with a severe congestion signal.
    plugin.onCongestionSignal(makeSignal(/*device_id=*/0, /*severity=*/1.0));
    EXPECT_EQ(plugin.getWindowBytes(0), 500u);

    // Device 1 must remain untouched.
    EXPECT_EQ(plugin.getWindowBytes(1), 1000u);

    // Grow device 1 with completions; device 0 must not change.
    plugin.onCompletion(makeCompletion(/*device_id=*/1, 4096));
    plugin.onCompletion(makeCompletion(/*device_id=*/1, 4096));
    EXPECT_EQ(plugin.getWindowBytes(1), 1200u);
    EXPECT_EQ(plugin.getWindowBytes(0), 500u);
}

// ---------------------------------------------------------------------------
// 10. ResetWindow
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, ResetWindow) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 2048;
    cfg.window_cap_bytes = 8192;
    cfg.window_floor_bytes = 1;
    cfg.md_factor = 0.5;
    AimdCongestionControlPlugin plugin(cfg);

    const int device_id = 7;
    // Perturb the window.
    plugin.onCongestionSignal(makeSignal(device_id, /*severity=*/1.0));
    EXPECT_NE(plugin.getWindowBytes(device_id), cfg.initial_window_bytes);

    plugin.resetWindow(device_id);
    EXPECT_EQ(plugin.getWindowBytes(device_id), cfg.initial_window_bytes);

    // Out-of-range resetWindow must be a safe no-op.
    plugin.resetWindow(-1);
    plugin.resetWindow(AimdConfig::kMaxDevices);
    EXPECT_EQ(plugin.getWindowBytes(device_id), cfg.initial_window_bytes);

    // resetAllWindows must restore every slot.
    plugin.onCongestionSignal(makeSignal(/*device_id=*/0, /*severity=*/1.0));
    plugin.onCongestionSignal(
        makeSignal(/*device_id=*/AimdConfig::kMaxDevices - 1,
                   /*severity=*/1.0));
    plugin.resetAllWindows();
    EXPECT_EQ(plugin.getWindowBytes(0), cfg.initial_window_bytes);
    EXPECT_EQ(plugin.getWindowBytes(AimdConfig::kMaxDevices - 1),
              cfg.initial_window_bytes);
}

// ---------------------------------------------------------------------------
// 11. FactoryFunction
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, FactoryFunction) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 4096;
    cfg.window_cap_bytes = 4096;
    auto plugin = createAimdCongestionControlPlugin(cfg);
    ASSERT_NE(plugin, nullptr);
    EXPECT_STREQ(plugin->getName(), "aimd");

    // The returned plugin should behave like the concrete type.
    Request req = makeTestRequest(/*length=*/100);
    AdmitDecision decision =
        plugin->onAdmit(makeAdmitCtx(req, /*device_inflight=*/0,
                                     /*device_id=*/0));
    EXPECT_TRUE(decision.admit);

    // Default-arg factory call must also succeed.
    auto default_plugin = createAimdCongestionControlPlugin();
    ASSERT_NE(default_plugin, nullptr);
    EXPECT_STREQ(default_plugin->getName(), "aimd");
}

// ---------------------------------------------------------------------------
// 12. CompletionIgnoresFailure
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, CompletionIgnoresFailure) {
    AimdConfig cfg;
    cfg.initial_window_bytes = 1000;
    cfg.window_cap_bytes = 10000;
    cfg.ai_increment_bytes = 100;
    AimdCongestionControlPlugin plugin(cfg);

    const int device_id = 1;
    const uint64_t baseline = plugin.getWindowBytes(device_id);

    for (auto status : {TransferStatusEnum::INITIAL,
                        TransferStatusEnum::PENDING,
                        TransferStatusEnum::INVALID,
                        TransferStatusEnum::CANCELED,
                        TransferStatusEnum::TIMEOUT,
                        TransferStatusEnum::FAILED}) {
        plugin.onCompletion(makeCompletion(device_id, /*bytes=*/4096, status));
    }
    EXPECT_EQ(plugin.getWindowBytes(device_id), baseline);

    // Out-of-range device on a successful completion must also be a no-op.
    plugin.onCompletion(makeCompletion(/*device_id=*/-1, /*bytes=*/4096));
    plugin.onCompletion(makeCompletion(/*device_id=*/AimdConfig::kMaxDevices,
                                       /*bytes=*/4096));
    EXPECT_EQ(plugin.getWindowBytes(device_id), baseline);

    // Sanity: a real success still grows the window.
    plugin.onCompletion(makeCompletion(device_id, /*bytes=*/4096));
    EXPECT_EQ(plugin.getWindowBytes(device_id), baseline + 100);
}

// ---------------------------------------------------------------------------
// 13. GetDeviceRateLimit
// ---------------------------------------------------------------------------
TEST(AimdCongestionControlTest, GetDeviceRateLimit) {
    AimdCongestionControlPlugin plugin{};
    EXPECT_EQ(plugin.getDeviceRateLimit(0), 0u);
    EXPECT_EQ(plugin.getDeviceRateLimit(7), 0u);
    EXPECT_EQ(plugin.getDeviceRateLimit(AimdConfig::kMaxDevices - 1), 0u);
    EXPECT_EQ(plugin.getDeviceRateLimit(-1), 0u);
    EXPECT_EQ(plugin.getDeviceRateLimit(AimdConfig::kMaxDevices + 1), 0u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
