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

#include "segment.h"

namespace mooncake {
static inline const std::string toString(SegmentType type) {
    if (type == MemoryKind)
        return "memory";
    else if (type == CufileKind)
        return "cufile";
    else
        return "unknown";
}

static inline SegmentType parseSegmentType(const std::string &type) {
    if (type == "memory")
        return MemoryKind;
    else if (type == "cufile")
        return CufileKind;
    else
        return UnknownKind;
}

Json::Value SegmentDescUtils::encode(const SegmentDesc &attr) {
    try {
        Json::Value segmentJSON;
        segmentJSON["name"] = attr.name;
        segmentJSON["type"] = toString(attr.type);
        if (attr.type == MemoryKind) {
            segmentJSON["topology"] = attr.memory.topology.toJson();
            Json::Value buffersJSON(Json::arrayValue);
            for (const auto &buffer : attr.memory.buffers) {
                Json::Value bufferJSON;
                bufferJSON["location"] = buffer.location;
                bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
                bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
                Json::Value rkeyJSON(Json::arrayValue);
                if (!buffer.rkey.empty()) {
                    for (auto &entry : buffer.rkey) rkeyJSON.append(entry);
                    bufferJSON["rkey"] = rkeyJSON;
                }
                if (!buffer.shm_path.empty())
                    bufferJSON["shm_path"] = buffer.shm_path;
                buffersJSON.append(bufferJSON);
            }
            if (!buffersJSON.empty()) segmentJSON["buffers"] = buffersJSON;

            Json::Value rdmaJSON(Json::arrayValue);
            for (const auto &entry : attr.memory.rdma) {
                Json::Value entryJSON;
                entryJSON["name"] = entry.name;
                entryJSON["lid"] = entry.lid;
                entryJSON["gid"] = entry.gid;
                rdmaJSON.append(entryJSON);
            }
            if (!rdmaJSON.empty()) segmentJSON["rdma"] = rdmaJSON;

            if (!attr.memory.tcp.ip_or_hostname.empty()) {
                Json::Value tcpJSON;
                tcpJSON["ip_or_hostname"] = attr.memory.tcp.ip_or_hostname;
                tcpJSON["port"] = static_cast<Json::UInt>(attr.memory.tcp.port);
                segmentJSON["tcp"] = tcpJSON;
            }
        }
        return segmentJSON;
    } catch (...) {
        Json::Value segmentJSON;
        return segmentJSON;
    }
}

int SegmentDescUtils::decode(Json::Value segmentJSON, SegmentDesc &attr) {
    try {
        attr.name = segmentJSON["name"].asString();
        attr.type = parseSegmentType(segmentJSON["type"].asString());
        if (attr.type == MemoryKind) {
            int ret = attr.memory.topology.parse(
                segmentJSON["topology"].toStyledString());
            if (ret) return ret;
            if (segmentJSON.isMember("buffers")) {
                for (const auto &bufferJSON : segmentJSON["buffers"]) {
                    BufferAttr buffer;
                    buffer.location = bufferJSON["location"].asString();
                    buffer.addr = bufferJSON["addr"].asUInt64();
                    buffer.length = bufferJSON["length"].asUInt64();
                    if (bufferJSON.isMember("shm_path"))
                        buffer.shm_path = bufferJSON["shm_path"].asString();
                    if (bufferJSON.isMember("rkey"))
                        for (const auto &rkeyJSON : bufferJSON["rkey"])
                            buffer.rkey.push_back(rkeyJSON.asUInt());
                    if (buffer.location.empty() || !buffer.addr ||
                        !buffer.length || buffer.rkey.empty()) {
                        return ERR_MALFORMED_JSON;
                    }
                    attr.memory.buffers.push_back(buffer);
                }
            }
            if (segmentJSON.isMember("rdma")) {
                for (const auto &rdmaJSON : segmentJSON["rdma"]) {
                    RdmaAttr rdma;
                    rdma.name = rdmaJSON["name"].asString();
                    rdma.lid = rdmaJSON["lid"].asUInt();
                    rdma.gid = rdmaJSON["gid"].asString();
                    if (rdma.name.empty() || rdma.gid.empty()) {
                        return ERR_MALFORMED_JSON;
                    }
                    attr.memory.rdma.push_back(rdma);
                }
            }
            if (segmentJSON.isMember("tcp")) {
                auto &tcpJSON = segmentJSON["tcp"];
                attr.memory.tcp.ip_or_hostname =
                    tcpJSON["ip_or_hostname"].asString();
                attr.memory.tcp.port = uint16_t(tcpJSON["port"].asUInt());
            }
        } else if (attr.type == CufileKind) {
            if (segmentJSON.isMember("cufile")) {
                auto &cufileJSON = segmentJSON["cufile"];
                attr.cufile.relative_path =
                    cufileJSON["relative_path"].asString();
                attr.cufile.length = cufileJSON["length"].asUInt64();
            }
        } else {
            return ERR_MALFORMED_JSON;
        }
        return 0;
    } catch (...) {
        return ERR_MALFORMED_JSON;
    }
}

Status SegmentDesc::addBuffer(const BufferAttr &attr) {
    if (type != MemoryKind) return Status::InvalidArgument("unsupported type");
    memory.buffers.push_back(attr);
    return Status::OK();
}

Status SegmentDesc::removeBuffer(void *addr) {
    if (type != MemoryKind) return Status::InvalidArgument("unsupported type");
    for (auto iter = memory.buffers.begin(); iter != memory.buffers.end(); ++iter) {
        if (iter->addr == (uint64_t)addr) {
            memory.buffers.erase(iter);
            return Status::OK();
        }
    }
    return Status::AddressNotRegistered("address not registered");
}
}  // namespace mooncake