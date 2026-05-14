// Copyright 2024 KVCache.AI
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

#ifndef URMA_ENDPOINT_H
#define URMA_ENDPOINT_H
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include "common.h"
#include "config.h"
#include "urma_api.h"
#include "transport/kunpeng_transport/ub_context.h"
#include "transport/kunpeng_transport/ub_endpoint.h"

namespace mooncake {
struct UrmaJFC {
    UrmaJFC() : native(nullptr), outstanding(0) {}

    urma_jfc_t* native;
    volatile int outstanding;
};

struct UrmaJFR {
    UrmaJFR() : native(nullptr), outstanding(0) {}

    urma_jfr_t* native;
    volatile int outstanding;
};

static urma_import_seg_flag_t import_flag = {
    .bs = {.cacheable = URMA_NON_CACHEABLE,
           .access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC,
           .mapping = URMA_SEG_NOMAP,
           .reserved = 0}};

// define the UrmaContext class
class UrmaContext : public UbContext {
    friend class UrmaEndpoint;

   public:
    UrmaContext(UbTransport& engine, std::string device_name,
                int max_endpoints);
    ~UrmaContext();
    int registerMemoryRegion(uint64_t va, size_t length) override;
    int unregisterMemoryRegion(uint64_t va) override;
    int doProcessContextEvents() override;
    void* retrieveRemoteSeg(const std::string& value) override;
    int poll(int num_entries, Transport::Slice** cr, int jfc_index) override;
    volatile int* outstandingCount(int jfc_index) override;
    int submitPostSend(
        const std::vector<Transport::Slice*>& slice_list) override;
    int buildLocalBufferDesc(uint64_t addr,
                             UbTransport::BufferDesc& buffer_desc) override;
    void* localSegWithIndex(unsigned value) override;
    int jfcCount() override;
    int getAsyncFd() override;
    std::string getEid() override;
    std::string toString() override;
    std::shared_ptr<UbEndPoint> makeEndpoint() override;
    std::string eid() const;
    std::string eid(urma_eid_t eid);
    bool transEidFromString(const std::string& eid_str, urma_eid_t& eid);
    urma_jfc_t* jfc();
    urma_jfr_t* jfr();
    urma_jfce_t* JFCE();
    static bool uninit();
    static bool init();

   private:
    int construct(GlobalConfig& config) override;
    int deconstruct() override;
    int openDevice(const std::string& device_name, uint8_t port,
                   int& eid_index) override;

    urma_target_seg_t* seg(uint64_t addr);

    std::vector<urma_seg_t*>& remote_seg_list() { return remote_seg_list_; }

    std::vector<urma_target_seg_t*>& imported_seg_list() {
        return imported_seg_list_;
    }

    std::vector<urma_target_seg_t*>& local_tseg_list() {
        return local_tseg_list_;
    }

    void updateUrmaGlobalConfig(urma_device_attr_t& device_attr) {
        auto& config = globalConfig();
        if (config.max_ep_per_ctx * config.num_jetty_per_ep >
            (size_t)device_attr.dev_cap.max_jetty) {
            config.max_ep_per_ctx =
                device_attr.dev_cap.max_jetty / config.num_jetty_per_ep;
        }
        if (config.num_jfc_per_ctx > (size_t)device_attr.dev_cap.max_jfc) {
            config.num_jfc_per_ctx = device_attr.dev_cap.max_jfc;
        }
    }

   private:
    std::vector<UrmaJFC> jfc_list_;
    urma_token_t urma_token = {.token = 0xACFE};
    urma_context_t* urma_context_ = nullptr;
    // ibv_pd *pd_ = nullptr;
    uint64_t max_seg_size{};
    urma_mtu active_mtu_;
    urma_eid_t eid_{};
    urma_device_attr_t dev_attr_{};

    int eid_index_ = -1;
    int active_speed_ = -1;

    RWSpinlock seg_region_lock_;
    std::vector<std::pair<urma_target_seg_t*, uint64_t>> seg_region_list_;
    std::vector<urma_target_seg_t*> local_tseg_list_;
    std::vector<urma_seg_t*> remote_seg_list_;
    std::vector<urma_target_seg_t*> imported_seg_list_;

    std::vector<UrmaJFR> jfr_list_;

    size_t num_JFCE_ = 0;
    urma_jfce_t** jfce_ = nullptr;

    std::vector<std::thread> background_thread_;
    std::atomic<bool> threads_running_;

    std::atomic<int> next_jfce_index_;
    std::atomic<int> next_jfce_vector_index_;
    std::atomic<int> next_jfc_list_index_;
    std::atomic<int> next_jfr_list_index_;
    std::vector<urma_jfc_t*> jfc_r_list_;

    urma_import_seg_flag_t import_flag_ = mooncake::import_flag;
    std::unordered_map<std::string, urma_target_seg_t*> import_tseg_map;
};

// define the UrmaEndpoint class
class UrmaEndpoint : public UbEndPoint {
   public:
    UrmaEndpoint(UrmaContext* context)
        : context_(context), jfc_outstanding_(nullptr) {}

    int construct(GlobalConfig& config) override;

    int deconstruct() override;

    void setPeerNicPath(const std::string& peer_nic_path) override;

    int setupConnectionsByActive() override;

    int setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                  HandShakeDesc& local_desc) override;

    bool hasOutstandingSlice() const override;

    int submitPostSend(
        std::vector<Transport::Slice*>& slice_list,
        std::vector<Transport::Slice*>& failed_slice_list) override;

    const std::string toString() const override;

   private:
    void disconnectUnlocked() override;

   private:
    std::vector<uint32_t> JettyNum() const;

    int doSetupConnection(const std::string& peer_eid,
                          std::vector<uint32_t> peer_jetty_num_list,
                          std::string* reply_msg = nullptr);

    int doSetupConnection(int qp_index, const std::string& peer_eid,
                          uint32_t peer_jetty_num,
                          std::string* reply_msg = nullptr);

   private:
    UrmaContext* context_;
    urma_token_t urma_token = {.token = 0xACFE};
    std::vector<urma_jetty_t*> jetty_list_;
    volatile int* wr_depth_list_;
    int max_wr_depth_;
    volatile int* jfc_outstanding_;
    std::unordered_map<urma_jetty_t*, urma_target_jetty_t*> imported_jetty_map_;
};
}  // namespace mooncake
#endif  // URMA_ENDPOINT_H
