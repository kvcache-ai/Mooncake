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

#ifndef TENT_HTTP_H
#define TENT_HTTP_H

#include "tent/runtime/metastore.h"

#include <atomic>
#include <curl/curl.h>

namespace mooncake {
namespace tent {
class HttpMetaStore : public MetaStore {
   public:
    HttpMetaStore();

    virtual ~HttpMetaStore();

    virtual Status connect(const std::string &endpoint);

    Status disconnect();

    virtual Status get(const std::string &key, std::string &value);

    virtual Status set(const std::string &key, const std::string &value);

    virtual Status remove(const std::string &key);

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
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_HTTP_H