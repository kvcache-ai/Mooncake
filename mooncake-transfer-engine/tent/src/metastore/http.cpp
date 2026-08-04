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

#include "tent/metastore/http.h"

#include <glog/logging.h>
#include <mutex>

namespace mooncake {
namespace tent {

static std::once_flag g_curl_global_init_flag;
static void ensureCurlGlobalInit() {
    std::call_once(g_curl_global_init_flag, []() {
        CURLcode rc = curl_global_init(CURL_GLOBAL_ALL);
        if (rc != CURLE_OK) {
            LOG(ERROR) << "curl_global_init failed: "
                       << curl_easy_strerror(rc);
        }
    });
}

HttpMetaStore::HttpMetaStore() {}

HttpMetaStore::~HttpMetaStore() { disconnect(); }

Status HttpMetaStore::connect(const std::string &endpoint) {
    if (connected_) {
        return Status::MetadataError(
            "HTTP connection already established" LOC_MARK);
    }
    ensureCurlGlobalInit();
    endpoint_ = endpoint;
    connected_ = true;
    return Status::OK();
}

Status HttpMetaStore::disconnect() {
    connected_ = false;
    return Status::OK();
}

namespace {
struct ScopedCurl {
    CURL *h{nullptr};
    ScopedCurl() : h(curl_easy_init()) {}
    ~ScopedCurl() { if (h) curl_easy_cleanup(h); }
    ScopedCurl(const ScopedCurl &) = delete;
    ScopedCurl &operator=(const ScopedCurl &) = delete;
    operator CURL *() const { return h; }
    explicit operator bool() const { return h != nullptr; }
};
}  // namespace

Status HttpMetaStore::get(const std::string &key, std::string &value) {
    if (!connected_) {
        return Status::MetadataError("HTTP connection not available" LOC_MARK);
    }

    ScopedCurl client;
    if (!client) {
        return Status::InternalError(
            "HTTP cannot allocate curl handle" LOC_MARK);
    }
    curl_easy_setopt(client.h, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

    std::string url = encodeUrl(client.h, key);
    curl_easy_setopt(client.h, CURLOPT_URL, url.c_str());
    curl_easy_setopt(client.h, CURLOPT_WRITEFUNCTION, writeCallback);

    // get response body
    std::string readBuffer;
    curl_easy_setopt(client.h, CURLOPT_WRITEDATA, &readBuffer);
    CURLcode res = curl_easy_perform(client.h);
    if (res != CURLE_OK) {
        return Status::MetadataError(
            std::string("HTTP failed to post request: ") +
            curl_easy_strerror(res) + LOC_MARK);
    }

    // Get the HTTP response code
    long responseCode;
    curl_easy_getinfo(client.h, CURLINFO_RESPONSE_CODE, &responseCode);
    if (responseCode == 404) {
        return Status::InvalidEntry(key);
    } else if (responseCode != 200) {
        std::string message = std::to_string(responseCode) + ": " + readBuffer;
        return Status::MetadataError(
            std::string("HTTP received unexpected response: ") + message +
            LOC_MARK);
    }
    value = std::move(readBuffer);
    return Status::OK();
}

Status HttpMetaStore::set(const std::string &key, const std::string &value) {
    if (!connected_) {
        return Status::MetadataError("HTTP connection not available" LOC_MARK);
    }

    ScopedCurl client;
    if (!client) {
        return Status::InternalError(
            "HTTP cannot allocate curl handle" LOC_MARK);
    }
    curl_easy_setopt(client.h, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

    std::string url = encodeUrl(client.h, key);
    curl_easy_setopt(client.h, CURLOPT_URL, url.c_str());
    curl_easy_setopt(client.h, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(client.h, CURLOPT_POSTFIELDS, value.c_str());
    curl_easy_setopt(client.h, CURLOPT_POSTFIELDSIZE, value.size());
    curl_easy_setopt(client.h, CURLOPT_CUSTOMREQUEST, "PUT");

    // get response body
    std::string readBuffer;
    curl_easy_setopt(client.h, CURLOPT_WRITEDATA, &readBuffer);

    // set content-type to application/json
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    curl_easy_setopt(client.h, CURLOPT_HTTPHEADER, headers);
    CURLcode res = curl_easy_perform(client.h);
    curl_slist_free_all(headers);  // free headers
    if (res != CURLE_OK) {
        return Status::MetadataError(
            std::string("HTTP failed to post request: ") +
            curl_easy_strerror(res) + LOC_MARK);
    }

    long responseCode;
    curl_easy_getinfo(client.h, CURLINFO_RESPONSE_CODE, &responseCode);
    if (responseCode != 200) {
        std::string message = std::to_string(responseCode) + ": " + readBuffer;
        return Status::MetadataError(
            std::string("HTTP received unexpected response: ") + message +
            LOC_MARK);
    }

    return Status::OK();
}

Status HttpMetaStore::remove(const std::string &key) {
    if (!connected_) {
        return Status::MetadataError("HTTP connection not available" LOC_MARK);
    }

    ScopedCurl client;
    if (!client) {
        return Status::InternalError(
            "HTTP cannot allocate curl handle" LOC_MARK);
    }
    curl_easy_setopt(client.h, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

    std::string url = encodeUrl(client.h, key);
    curl_easy_setopt(client.h, CURLOPT_URL, url.c_str());
    curl_easy_setopt(client.h, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(client.h, CURLOPT_CUSTOMREQUEST, "DELETE");

    // get response body
    std::string readBuffer;
    curl_easy_setopt(client.h, CURLOPT_WRITEDATA, &readBuffer);
    CURLcode res = curl_easy_perform(client.h);
    if (res != CURLE_OK) {
        return Status::MetadataError(
            std::string("HTTP failed to post request: ") +
            curl_easy_strerror(res) + LOC_MARK);
    }

    long responseCode;
    curl_easy_getinfo(client.h, CURLINFO_RESPONSE_CODE, &responseCode);
    if (responseCode != 200) {
        std::string message = std::to_string(responseCode) + ": " + readBuffer;
        return Status::MetadataError(
            std::string("HTTP received unexpected response: ") + message +
            LOC_MARK);
    }

    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
