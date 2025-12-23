// Copyright 2025 KVCache.AI
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

#ifndef TENT_IBV_LOADER_H
#define TENT_IBV_LOADER_H

#include <infiniband/verbs.h>

namespace mooncake {
namespace tent {
struct IbvSymbols {
    ibv_device** (*ibv_get_device_list)(int* num_devices);
    void (*ibv_free_device_list)(ibv_device** list);

    ibv_context* (*ibv_open_device)(ibv_device* device);
    int (*ibv_close_device)(ibv_context* context);
    int (*ibv_query_device)(struct ibv_context* context,
                            struct ibv_device_attr* device_attr);
    int (*ibv_query_gid)(struct ibv_context* context, uint8_t port_num,
                         int index, union ibv_gid* gid);
    int (*ibv_query_port_default)(ibv_context* context, uint8_t port_num,
                                  ibv_port_attr* port_attr);
    const char* (*ibv_get_device_name)(struct ibv_device* device);

    ibv_pd* (*ibv_alloc_pd)(ibv_context* context);
    int (*ibv_dealloc_pd)(ibv_pd* pd);

    ibv_comp_channel* (*ibv_create_comp_channel)(ibv_context* context);
    int (*ibv_destroy_comp_channel)(ibv_comp_channel* channel);

    ibv_cq* (*ibv_create_cq)(ibv_context* context, int cqe, void* cq_context,
                             ibv_comp_channel* channel, int comp_vector);
    int (*ibv_destroy_cq)(ibv_cq* cq);

    ibv_qp* (*ibv_create_qp)(ibv_pd* pd, ibv_qp_init_attr* qp_init_attr);
    int (*ibv_destroy_qp)(ibv_qp* qp);
    int (*ibv_modify_qp)(struct ibv_qp* qp, struct ibv_qp_attr* attr,
                         int attr_mask);

    int (*ibv_get_cq_event)(ibv_comp_channel* channel, ibv_cq** cq,
                            void** cq_context);
    void (*ibv_ack_cq_events)(ibv_cq* cq, unsigned int nevents);

    ibv_mr* (*ibv_reg_mr_default)(ibv_pd* pd, void* addr, size_t length,
                                  int access);
    ibv_mr* (*ibv_reg_mr_iova2)(struct ibv_pd* pd, void* addr, size_t length,
                                uint64_t iova, unsigned int access);
    int (*ibv_dereg_mr)(ibv_mr* mr);

    int (*ibv_fork_init)(void);
};

class IbvLoader {
   public:
    static IbvLoader& Instance();

    bool available() const { return available_; }

    const IbvSymbols& sym() const { return symbols_; }

   private:
    IbvLoader();
    ~IbvLoader();

    IbvLoader(const IbvLoader&) = delete;
    IbvLoader& operator=(const IbvLoader&) = delete;

   private:
    void* handle_ = nullptr;
    IbvSymbols symbols_{};
    bool available_ = false;
};

}  // namespace tent
}  // namespace mooncake
#endif