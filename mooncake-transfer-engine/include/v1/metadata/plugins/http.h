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

#ifndef METADATA_PLUGIN_HTTP_H
#define METADATA_PLUGIN_HTTP_H

#include <curl/curl.h>
#include <glog/logging.h>

#include <atomic>

#include "v1/metadata/plugin.h"

namespace mooncake {
namespace v1 {
class HttpMetadataPlugin : public MetadataPlugin {
   public:
    HttpMetadataPlugin() {}

    virtual ~HttpMetadataPlugin() { disconnect(); }

    virtual Status connect(const std::string &endpoint) {
        if (connected_) {
            return Status::MetadataError(
                "HTTP connection already established" LOC_MARK);
        }
        curl_global_init(CURL_GLOBAL_ALL);
        client_ = curl_easy_init();
        if (!client_) {
            return Status::InternalError(
                "HTTP cannot allocate curl objects" LOC_MARK);
        }
        endpoint_ = endpoint;
        connected_ = true;
        return Status::OK();
    }

    Status disconnect() {
        if (connected_) {
            curl_easy_cleanup(client_);
            curl_global_cleanup();
            connected_ = false;
        }
        return Status::OK();
    }

    virtual Status get(const std::string &key, std::string &value) {
        if (!connected_) {
            return Status::MetadataError(
                "HTTP connection not available" LOC_MARK);
        }

        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);
        CURLcode res = curl_easy_perform(client_);
        if (res != CURLE_OK) {
            return Status::MetadataError(
                std::string("HTTP failed to post request: ") +
                curl_easy_strerror(res) + LOC_MARK);
        }

        // Get the HTTP response code
        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode == 404) {
            return Status::InvalidEntry(key);
        } else if (responseCode != 200) {
            std::string message =
                std::to_string(responseCode) + ": " + readBuffer;
            return Status::MetadataError(
                std::string("HTTP received unexpected response: ") + message +
                LOC_MARK);
        }
        value = std::string(readBuffer);
        return Status::OK();
    }

    virtual Status set(const std::string &key, const std::string &value) {
        if (!connected_) {
            return Status::MetadataError(
                "HTTP connection not available" LOC_MARK);
        }

        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(client_, CURLOPT_POSTFIELDS, value.c_str());
        curl_easy_setopt(client_, CURLOPT_POSTFIELDSIZE, value.size());
        curl_easy_setopt(client_, CURLOPT_CUSTOMREQUEST, "PUT");

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);

        // set content-type to application/json
        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(client_, CURLOPT_HTTPHEADER, headers);
        CURLcode res = curl_easy_perform(client_);
        curl_slist_free_all(headers);  // free headers
        if (res != CURLE_OK) {
            return Status::MetadataError(
                std::string("HTTP failed to post request: ") +
                curl_easy_strerror(res) + LOC_MARK);
        }

        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            std::string message =
                std::to_string(responseCode) + ": " + readBuffer;
            return Status::MetadataError(
                std::string("HTTP received unexpected response: ") + message +
                LOC_MARK);
        }

        return Status::OK();
    }

    virtual Status remove(const std::string &key) {
        if (!connected_) {
            return Status::MetadataError(
                "HTTP connection not available" LOC_MARK);
        }

        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(client_, CURLOPT_CUSTOMREQUEST, "DELETE");

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);
        CURLcode res = curl_easy_perform(client_);
        if (res != CURLE_OK) {
            return Status::MetadataError(
                std::string("HTTP failed to post request: ") +
                curl_easy_strerror(res) + LOC_MARK);
        }

        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            std::string message =
                std::to_string(responseCode) + ": " + readBuffer;
            return Status::MetadataError(
                std::string("HTTP received unexpected response: ") + message +
                LOC_MARK);
        }

        return Status::OK();
    }

   private:
    static size_t writeCallback(void *contents, size_t size, size_t nmemb,
                                std::string *userp) {
        userp->append(static_cast<char *>(contents), size * nmemb);
        return size * nmemb;
    }

    std::string encodeUrl(const std::string &key) {
        char *newkey = curl_easy_escape(client_, key.c_str(), key.size());
        std::string encodedKey(newkey);
        std::string url = endpoint_ + "?key=" + encodedKey;
        curl_free(newkey);
        return url;
    }

   private:
    std::atomic<bool> connected_;
    CURL *client_;
    std::string endpoint_;
};
}  // namespace v1
}  // namespace mooncake

#endif  // METADATA_PLUGIN_HTTP_H