/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description:
 * Author: l30050806
 * Create: 2024-12-27
 */

#ifndef TRANSPORT_MEM_H
#define TRANSPORT_MEM_H

#include <hccl/hccl_types.h>
#include <atomic>
#include "dispatcher.h"
#include "notify_pool.h"
#include "hccl_socket.h"
#include "hccl_network_pub.h"
#include "hccl_common.h"

namespace hccl {

enum class RmaMemType : int {
    DEVICE = 0,  // device侧内存
    HOST = 1,    // host侧内存
    TYPE_NUM
};

constexpr size_t TRANSPORT_EMD_ESC_SIZE = 512U - (sizeof(u32) * 2);

class TransportMem {
   public:
    enum class TpType : int { IPC = 0, ROCE = 1, TYPE_NUM };

    struct AttrInfo {
        u32 localRankId;
        u32 remoteRankId;
        u32 sdid;      // 本端所属超节点
        u32 serverId;  // 本端所属server
    };

    struct RmaMemDesc {
        u32 localRankId;
        u32 remoteRankId;
        char memDesc[TRANSPORT_EMD_ESC_SIZE];
    };

    struct RmaMemDescs {
        RmaMemDesc *array;
        u32 arrayLength;
    };

    struct RmaOpMem {
        void *addr;
        u64 size;
    };

    struct RmaMem {
        RmaMemType type;  // segment的内存类型
        void *addr;       // segment的虚拟地址
        u64 size;         // segment的size
    };

    static std::shared_ptr<TransportMem> Create(
        TpType tpType, const std::unique_ptr<NotifyPool> &notifyPool,
        const HcclNetDevCtx &netDevCtx, const HcclDispatcher &dispatcher,
        AttrInfo &attrInfo);

    explicit TransportMem(const std::unique_ptr<NotifyPool> &notifyPool,
                          const HcclNetDevCtx &netDevCtx,
                          const HcclDispatcher &dispatcher, AttrInfo &attrInfo);
    virtual ~TransportMem();
    virtual HcclResult ExchangeMemDesc(const RmaMemDescs &localMemDescs,
                                       RmaMemDescs &remoteMemDescs,
                                       u32 &actualNumOfRemote) = 0;
    virtual HcclResult EnableMemAccess(const RmaMemDesc &remoteMemDesc,
                                       RmaMem &remoteMem) = 0;
    virtual HcclResult DisableMemAccess(const RmaMemDesc &remoteMemDesc) = 0;
    virtual HcclResult SetDataSocket(const std::shared_ptr<HcclSocket> &socket);

    virtual HcclResult SetSocket(const std::shared_ptr<HcclSocket> &socket) = 0;
    virtual HcclResult Connect(s32 timeoutSec) = 0;
    virtual HcclResult Write(const RmaOpMem &remoteMem,
                             const RmaOpMem &localMem,
                             const rtStream_t &stream) = 0;
    virtual HcclResult Read(const RmaOpMem &localMem, const RmaOpMem &remoteMem,
                            const rtStream_t &stream) = 0;
    virtual HcclResult AddOpFence(const rtStream_t &stream) = 0;

   protected:
    // 从 string 拷贝到 memDesc
    HcclResult RmaMemDescCopyFromStr(RmaMemDesc &rmaMemDesc,
                                     const std::string &memDescStr) const {
        if (memcpy_s(rmaMemDesc.memDesc, TRANSPORT_EMD_ESC_SIZE,
                     memDescStr.c_str(), memDescStr.size() + 1) != EOK) {
            return HCCL_E_INTERNAL;
        }
        return HCCL_SUCCESS;
    }

    // 从 memDesc 转换为 string
    std::string RmaMemDescCopyToStr(const RmaMemDesc &rmaMemDesc) const {
        return std::string(rmaMemDesc.memDesc, TRANSPORT_EMD_ESC_SIZE);
    }

    HcclResult DoExchangeMemDesc(const RmaMemDescs &localMemDescs,
                                 RmaMemDescs &remoteMemDescs,
                                 u32 &actualNumOfRemote);
    HcclResult SendLocalMemDesc(const RmaMemDescs &localMemDescs);
    HcclResult ReceiveRemoteMemDesc(RmaMemDescs &remoteMemDescs,
                                    u32 &actualNumOfRemote);

    const std::unique_ptr<NotifyPool> &notifyPool_;
    HcclNetDevCtx netDevCtx_{nullptr};
    HcclDispatcher dispatcher_{nullptr};

    u32 localRankId_{0};
    u32 remoteRankId_{0};
    std::shared_ptr<HcclSocket> socket_{nullptr};

    std::shared_ptr<HcclSocket> dataSocket_{nullptr};
};
}  // namespace hccl
#endif