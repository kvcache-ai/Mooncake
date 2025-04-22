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

#ifndef METADATA_PLUGIN_ETCD_H
#define METADATA_PLUGIN_ETCD_H

#include <glog/logging.h>
#include <libetcd_wrapper.h>

#include "metadata/plugin.h"

namespace mooncake {
struct EtcdMetadataPlugin : public MetadataPlugin {
    EtcdMetadataPlugin(const std::string &metadata_uri)
        : metadata_uri_(metadata_uri) {
        auto ret = NewEtcdClient((char *)metadata_uri_.c_str(), &err_msg_);
        if (ret) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to connect "
                       << metadata_uri_ << ": " << err_msg_;
            // free the memory for storing error message
            free(err_msg_);
            err_msg_ = nullptr;
        }
    }

    virtual ~EtcdMetadataPlugin() { EtcdCloseWrapper(); }

    virtual Status get(const std::string &key, std::string &value) {
        char *raw_value = nullptr;
        char *err_msg_;
        auto ret = EtcdGetWrapper((char *)key.c_str(), &raw_value, &err_msg_);
        if (ret) {
            std::string err_msg =
                "etcd: get \'" + key + "\' failed: " + err_msg_;
            free(err_msg_);  // free the memory for storing error message
            err_msg_ = nullptr;
            return Status::Metadata(err_msg);
        }
        if (!raw_value) return Status::NoSuchKey();
        value = std::string(raw_value);
        free(raw_value);  // free the memory allocated by EtcdGetWrapper
        return Status::OK();
    }

    virtual Status set(const std::string &key, const std::string &value) {
        char *err_msg_;
        auto ret = EtcdPutWrapper((char *)key.c_str(), (char *)value.c_str(),
                                  &err_msg_);
        if (ret) {
            std::string err_msg =
                "etcd: set \'" + key + "\' failed: " + err_msg_;
            free(err_msg_);  // free the memory for storing error message
            return Status::Metadata(err_msg);
        }
        return Status::OK();
    }

    virtual Status remove(const std::string &key) {
        char *err_msg_;
        auto ret = EtcdDeleteWrapper((char *)key.c_str(), &err_msg_);
        if (ret) {
            std::string err_msg =
                "etcd: remove \'" + key + "\' failed: " + err_msg_;
            free(err_msg_);  // free the memory for storing error message
            return Status::Metadata(err_msg);
        }
        return Status::OK();
    }

    const std::string metadata_uri_;
};
}  // namespace mooncake

#endif  // METADATA_PLUGIN_ETCD_H