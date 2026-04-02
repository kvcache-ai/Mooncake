#include "connectors/huggingface_connector.h"

#include <cstdlib>
#include <glog/logging.h>
#include <stdexcept>

namespace mooncake {

static size_t WriteCallback(void* contents, size_t size, size_t nmemb,
                            void* userp) {
    size_t total_size = size * nmemb;
    auto* buffer = static_cast<std::vector<uint8_t>*>(userp);
    buffer->insert(buffer->end(), static_cast<uint8_t*>(contents),
                   static_cast<uint8_t*>(contents) + total_size);
    return total_size;
}

HuggingFaceConnector::HuggingFaceConnector() {
    const char* endpoint = std::getenv("MOONCAKE_HF_ENDPOINT");
    const char* token = std::getenv("MOONCAKE_HF_TOKEN");

    endpoint_ = endpoint ? endpoint : "https://huggingface.co";
    token_ = token ? token : "";

    curl_global_init(CURL_GLOBAL_ALL);
    curl_ = curl_easy_init();
    if (!curl_) {
        throw std::runtime_error("Failed to initialize libcurl");
    }
    LOG(INFO) << "HuggingFaceConnector initialized: " << GetConnectionInfo();
}

HuggingFaceConnector::~HuggingFaceConnector() {
    if (curl_) {
        curl_easy_cleanup(curl_);
    }
}

std::pair<std::string, std::string> HuggingFaceConnector::ParseKey(
    const std::string& key) {
    size_t pos = key.find('/');
    if (pos == std::string::npos) {
        return {key, ""};
    }
    return {key.substr(0, pos), key.substr(pos + 1)};
}

tl::expected<void, std::string> HuggingFaceConnector::HttpGet(
    const std::string& url, std::vector<uint8_t>& buffer) {
    buffer.clear();
    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &buffer);
    curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);

    struct curl_slist* headers = nullptr;
    if (!token_.empty()) {
        std::string auth = "Authorization: Bearer " + token_;
        headers = curl_slist_append(headers, auth.c_str());
        curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
    }

    CURLcode res = curl_easy_perform(curl_);
    if (headers) {
        curl_slist_free_all(headers);
    }

    if (res != CURLE_OK) {
        return tl::make_unexpected(std::string(curl_easy_strerror(res)));
    }
    return {};
}

tl::expected<void, std::string> HuggingFaceConnector::ListObjects(
    const std::string& prefix, std::vector<ExternalObject>& objects) {
    return tl::make_unexpected(
        "ListObjects not implemented for HuggingFace connector");
}

tl::expected<void, std::string> HuggingFaceConnector::DownloadObject(
    const std::string& key, std::vector<uint8_t>& buffer) {
    auto [repo_id, path] = ParseKey(key);
    if (path.empty()) {
        return tl::make_unexpected("Invalid key format: " + key);
    }

    std::string url = endpoint_ + "/" + repo_id + "/resolve/main/" + path;
    return HttpGet(url, buffer);
}

std::string HuggingFaceConnector::GetConnectionInfo() const {
    return "HuggingFaceConnector: endpoint=" + endpoint_ +
           ", token=" + (token_.empty() ? "not set" : "set");
}

}  // namespace mooncake
