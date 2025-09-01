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

#include <atomic>

#include "v1/metadata/plugin.h"

namespace mooncake {
namespace v1 {
class EtcdMetadataPlugin : public MetadataPlugin {
   public:
    EtcdMetadataPlugin() {}

    virtual ~EtcdMetadataPlugin() { disconnect(); }

    virtual Status connect(const std::string &endpoint) {
        char *err_str;
        if (connected_) {
            return Status::MetadataError(
                "Etcd connection already established" LOC_MARK);
        }
        auto ret = NewEtcdClient((char *)endpoint.c_str(), &err_str);
        if (ret) {
            std::string message =
                "Etcd cannot connect \'" + endpoint + "\': " + err_str;
            free(err_str);
            return Status::MetadataError(message + LOC_MARK);
        }
        connected_ = true;
        return Status::OK();
    }

    Status disconnect() {
        if (connected_) {
            EtcdCloseWrapper();
            connected_ = false;
        }
        return Status::OK();
    }

    virtual Status get(const std::string &key, std::string &value) {
        char *raw_value = nullptr;
        char *err_str;
        if (!connected_) {
            return Status::MetadataError(
                "Etcd connection not available" LOC_MARK);
        }
        auto ret = EtcdGetWrapper((char *)key.c_str(), &raw_value, &err_str);
        if (ret) {
            std::string message =
                "Etcd failed to get \'" + key + "\': " + err_str;
            free(err_str);  // free the memory for storing error message
            err_str = nullptr;
            return Status::MetadataError(message + LOC_MARK);
        }
        if (!raw_value) return Status::InvalidEntry(key);
        value = std::string(raw_value);
        free(raw_value);  // free the memory allocated by EtcdGetWrapper
        return Status::OK();
    }

    virtual Status set(const std::string &key, const std::string &value) {
        char *err_str;
        if (!connected_) {
            return Status::MetadataError(
                "Etcd connection not available" LOC_MARK);
        }
        auto ret = EtcdPutWrapper((char *)key.c_str(), (char *)value.c_str(),
                                  &err_str);
        if (ret) {
            std::string message =
                "Etcd failed to set \'" + key + "\': " + err_str;
            free(err_str);  // free the memory for storing error message
            return Status::MetadataError(message);
        }
        return Status::OK();
    }

    virtual Status remove(const std::string &key) {
        char *err_str;
        if (!connected_) {
            return Status::MetadataError(
                "Etcd connection not available" LOC_MARK);
        }
        auto ret = EtcdDeleteWrapper((char *)key.c_str(), &err_str);
        if (ret) {
            std::string message =
                "Etcd failed to delete \'" + key + "\': " + err_str;
            free(err_str);  // free the memory for storing error message
            return Status::MetadataError(message);
        }
        return Status::OK();
    }

   private:
    std::atomic<bool> connected_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // METADATA_PLUGIN_ETCD_H