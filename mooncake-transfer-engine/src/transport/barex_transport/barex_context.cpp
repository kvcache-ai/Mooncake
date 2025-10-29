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

#include "transport/barex_transport/barex_context.h"

namespace mooncake {

using namespace accl::barex;

BarexContext::BarexContext(XContext* xcontext, bool use_cpu, int device_id) : xcontext_(xcontext), barex_use_cpu_(use_cpu), barex_local_device_(device_id) {}


BarexContext::~BarexContext() {
    if (xcontext_) {
        xcontext_->Shutdown();
        xcontext_->WaitStop();
        delete xcontext_;
    }
}

int BarexContext::addChannel(SegmentID sid, int device_id, XChannel* ch) {
    channel_cache_.put(sid, device_id, ch);
    return 0;
}

XChannel* BarexContext::getChannel(SegmentID sid, int device_id, int idx) {
    XChannel* channel = channel_cache_.find(sid, device_id, idx);
    return channel;
}

int BarexContext::checkStatus(SegmentID sid) {
    return channel_cache_.RemoveInvalidChannels(sid);
}

XContext* BarexContext::getCtx() {
    return xcontext_;
}

std::vector<XChannel*> BarexContext::getAllChannel() {
    return channel_cache_.copyAll();
}

int BarexContext::submitPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
    std::unordered_map<SegmentID, std::unordered_map<int, std::vector<accl::barex::rw_memp_t>>> sid_dev_data_map;
    std::unordered_map<SegmentID, std::unordered_map<int, std::vector<Transport::Slice *>>> sid_dev_slice_map;
    for (auto slice : slice_list) {
        accl::barex::rw_memp_t w_m;
        w_m.sg.addr = (uint64_t)slice->source_addr;
        w_m.sg.length = (uint32_t)slice->length;
        w_m.sg.lkey = slice->rdma.source_lkey;
        w_m.data.d_type = barex_use_cpu_ ? CPU : GPU;
        w_m.data.device_id = barex_local_device_;
        w_m.r_addr = slice->rdma.dest_addr;
        w_m.r_key = slice->rdma.dest_rkey;
        w_m.r_ttl_ms = UINT64_MAX;
        auto& dev_map = sid_dev_data_map[slice->target_id];
        int lkey_index = slice->rdma.lkey_index;
        dev_map[lkey_index].push_back(w_m);
        auto& slice_map = sid_dev_slice_map[slice->target_id];
        slice_map[lkey_index].push_back(slice);
    }
    for (auto& pair : sid_dev_data_map) {
        SegmentID sid = pair.first;
        auto& dev_map = pair.second;

        for (auto& dev_pair : dev_map) {
            int dev = dev_pair.first;
            std::vector<accl::barex::rw_memp_t>& data_vec = dev_pair.second;
            std::vector<Transport::Slice *>& slice_vec = sid_dev_slice_map[sid][dev];
            size_t data_size = data_vec.size();
            int qp_in_use = qp_num_per_ctx_;
            if (data_size < (size_t)qp_num_per_ctx_) {
                qp_in_use = data_size;
            }
            size_t begin_idx = 0;
            size_t end_idx = 0;
            size_t batch_size = data_size / qp_in_use;
            size_t reminder = data_size % qp_in_use;
            
            int retry_cnt = 5;
            for (int i=0; i < qp_in_use; i++) {
                XChannel* channel = nullptr;
                for (int j=0; j < retry_cnt; j++) {
                    channel = channel_cache_.find(sid, dev, i);
                    if (!channel) {
                        LOG(ERROR) << "Write fail, sid " << sid << ", dev " << dev << ", id " << i << " not found, retry " << j << "/" << retry_cnt;
                        break;
                    }
                    if (!channel->IsActive()) {
                        LOG(WARNING) << "Write fail, channel status error " << channel << " retry " << j << "/" << retry_cnt;
                        channel_cache_.erase(sid, dev, i);
                        continue;
                    }
                }
                if (!channel) {
                    LOG(ERROR) << "Write fail, no channel found";
                    return -1;
                }
                
                end_idx += batch_size;
                if (i == qp_in_use - 1) {
                    end_idx += reminder;
                }
                int peer_nic_id = channel->GetPeerNicId();
                auto data_chunk_read = std::make_shared<std::vector<accl::barex::rw_memp_t>>();
                auto data_chunk_write = std::make_shared<std::vector<accl::barex::rw_memp_t>>();
                auto slice_chunk_read = std::make_shared<std::vector<Transport::Slice *>>();
                auto slice_chunk_write = std::make_shared<std::vector<Transport::Slice *>>();
                for (size_t idx = begin_idx; idx < end_idx; idx++) {
                    data_vec[idx].r_key = slice_vec[idx]->dest_rkeys[peer_nic_id];
                    if (slice_vec[idx]->opcode == Transport::TransferRequest::READ) {
                        data_chunk_read->emplace_back(data_vec[idx]);
                        slice_chunk_read->emplace_back(slice_vec[idx]); 
                    } else {
                        data_chunk_write->emplace_back(data_vec[idx]);
                        slice_chunk_write->emplace_back(slice_vec[idx]); 
                    }
                }

                if (!data_chunk_write->empty()) {
                    BarexResult r = channel->WriteBatch(
                        data_chunk_write,
                        [slice_chunk_write](accl::barex::Status s) {
                            if(!s.IsOk()) {
                                LOG(ERROR) << "WriteBatch fail, " << s.ErrMsg().c_str();
                                for (auto slice : *slice_chunk_write) {
                                    slice->markFailed();
                                }
                            } else {
                                for (auto slice : *slice_chunk_write) {
                                    slice->markSuccess();
                                }
                            }
                        }, true);
                    if (r != accl::barex::BAREX_SUCCESS) {
                        LOG(ERROR) << "WriteBatch fail, ret " << r;
                        return -2;
                    }
                }
                if (!data_chunk_read->empty()) {
                    BarexResult r = channel->ReadBatch(
                        data_chunk_read,
                        [slice_chunk_read](accl::barex::Status s) {
                            if(!s.IsOk()) {
                                LOG(ERROR) << "ReadBatch fail, " << s.ErrMsg().c_str();
                                for (auto slice : *slice_chunk_read) {
                                    slice->markFailed();
                                }
                            } else {
                                for (auto slice : *slice_chunk_read) {
                                    slice->markSuccess();
                                }
                            }
                        }, true);
                    if (r != accl::barex::BAREX_SUCCESS) {
                        LOG(ERROR) << "ReadBatch fail, ret " << r;
                        return -2;
                    }
                }
                begin_idx += batch_size;
            }
        }
    }
    return 0;
}

}  // namespace mooncake