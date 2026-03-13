#include "utils/s3_helper.h"

#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/Delete.h>
#include <optional>
#include <fstream>
#include <iostream>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <glog/logging.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <chrono>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include "utils/type_util.h"
#include "fmt/format.h"

namespace mooncake {

namespace {

constexpr int64_t kDefaultS3ConnectTimeoutMs = 10000;
constexpr int64_t kDefaultS3RequestTimeoutMs = 30000;

struct S3Env {
    std::string region;

    std::string endpoint;

    std::string bucket;

    std::string access_key;

    std::string secret_key;

    bool use_virtual_addressing = true;

    int64_t connect_timeout_ms = kDefaultS3ConnectTimeoutMs;

    int64_t request_timeout_ms = kDefaultS3RequestTimeoutMs;
};

S3Env s3_env;

void AssignStringFromEnv(const char *env_name, std::string &target) {
    const char *env_value = std::getenv(env_name);
    if (env_value && *env_value) {
        target = env_value;
    } else {
        target.clear();
    }
}

void AssignBoolFromEnv(const char *env_name, bool &target) {
    const char *env_value = std::getenv(env_name);
    if (env_value && *env_value) {
        bool parsed = true;
        if (TypeUtil::ParseBool(env_value, parsed)) {
            target = parsed;
            return;
        }
        LOG(WARNING) << "Invalid " << env_name << " value: " << env_value;
    }
}

void AssignTimeoutFromEnv(const char *env_name, int64_t default_value,
                          int64_t &target) {
    const char *env_value = std::getenv(env_name);
    if (env_value && *env_value) {
        int64_t parsed;
        if (TypeUtil::ParseInt64(env_value, parsed)) {
            target = parsed;
            return;
        }
        LOG(WARNING) << "Invalid " << env_name << " value: " << env_value;
    }
    target = default_value;
}

}  // namespace

bool S3Helper::aws_initialized = false;
Aws::SDKOptions S3Helper::options_;

void S3Helper::InitAPI() {
    options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Error;
    Aws::InitAPI(options_);
    aws_initialized = true;

    // Read environment variables once during initialization (fallback as needed
    // if not set)
    AssignStringFromEnv("MOONCAKE_AWS_REGION", s3_env.region);
    AssignStringFromEnv("MOONCAKE_AWS_S3_ENDPOINT", s3_env.endpoint);
    AssignStringFromEnv("MOONCAKE_AWS_BUCKET_NAME", s3_env.bucket);
    AssignStringFromEnv("MOONCAKE_AWS_ACCESS_KEY_ID", s3_env.access_key);
    AssignStringFromEnv("MOONCAKE_AWS_SECRET_ACCESS_KEY", s3_env.secret_key);

    AssignBoolFromEnv("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING",
                      s3_env.use_virtual_addressing);

    AssignTimeoutFromEnv("MOONCAKE_AWS_CONNECT_TIMEOUT_MS",
                         kDefaultS3ConnectTimeoutMs, s3_env.connect_timeout_ms);
    AssignTimeoutFromEnv("MOONCAKE_AWS_REQUEST_TIMEOUT_MS",
                         kDefaultS3RequestTimeoutMs, s3_env.request_timeout_ms);
}

void S3Helper::ShutdownAPI() {
    if (aws_initialized) {
        Aws::ShutdownAPI(options_);
        aws_initialized = false;
    }
}

S3Helper::S3Helper(const std::string &endpoint, const std::string &bucket,
                   const std::string &region) {
    Aws::Client::ClientConfiguration config(true);

    config.connectTimeoutMs = s3_env.connect_timeout_ms;
    config.requestTimeoutMs = s3_env.request_timeout_ms;
    config.scheme = Aws::Http::Scheme::HTTPS;

    if (!region.empty()) {
        config.region = region;
    } else {
        config.region = s3_env.region;
    }

    if (!endpoint.empty()) {
        config.endpointOverride = endpoint;
    } else {
        config.endpointOverride = s3_env.endpoint;
    }

    bucket_ = s3_env.bucket;
    if (!bucket.empty()) {
        bucket_ = bucket;
    }

    Aws::Auth::AWSCredentials credentials(s3_env.access_key, s3_env.secret_key);

    // Concatenate log information into member variable connection_info_
    connection_info_ = fmt::format(
        "S3 client config: "
        "region={} "
        "endpoint={} "
        "bucket={} "
        "connectTimeoutMs={} "
        "requestTimeoutMs={} "
        "scheme={} "
        "credentials={}/{} "
        "useVirtualAddressing={}",
        config.region.empty() ? "unset" : config.region,
        config.endpointOverride.empty() ? "unset" : config.endpointOverride,
        bucket_.empty() ? "unset" : bucket_, config.connectTimeoutMs,
        config.requestTimeoutMs,
        config.scheme == Aws::Http::Scheme::HTTPS ? "HTTPS" : "HTTP",
        !s3_env.access_key.empty() ? "set" : "unset",
        !s3_env.secret_key.empty() ? "set" : "unset",
        s3_env.use_virtual_addressing ? "true" : "false");

    s3_client_ = Aws::S3::S3Client(
        credentials, config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        s3_env.use_virtual_addressing);
}

S3Helper::~S3Helper() = default;

tl::expected<void, std::string> S3Helper::UploadBuffer(
    const std::string &key, const std::vector<uint8_t> &buffer) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_.c_str());
    request.SetKey(key.c_str());

    // Ensure buffer is not empty
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }

    // Set ContentLength correctly (using long long type)
    request.SetContentLength(static_cast<long long>(buffer.size()));
    const auto buffer_size = static_cast<std::streamsize>(buffer.size());

    // Create and write to stream
    auto stream = Aws::MakeShared<Aws::StringStream>("UploadBuffer");
    stream->write(reinterpret_cast<const char *>(buffer.data()), buffer_size);
    request.SetBody(stream);

    auto outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Upload failed: {}", outcome.GetError().GetMessage()));
    }
    return {};
}

tl::expected<void, std::string> S3Helper::UploadString(
    const std::string &key, const std::string &data) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_.c_str());
    request.SetKey(key.c_str());
    request.SetContentLength(static_cast<long long>(data.size()));

    auto stream = Aws::MakeShared<Aws::StringStream>("UploadString");
    stream->write(data.data(), data.size());
    request.SetBody(stream);

    auto outcome = s3_client_.PutObject(request);
    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Upload failed: {}", outcome.GetError().GetMessage()));
    }
    return {};
}

tl::expected<void, std::string> S3Helper::DownloadBuffer(
    const std::string &key, std::vector<uint8_t> &buffer) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_.c_str());
    request.SetKey(key.c_str());

    auto outcome = s3_client_.GetObject(request);
    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(fmt::format(
            "Download failed: {}", outcome.GetError().GetMessage()));
    }

    auto &result_stream = outcome.GetResult().GetBody();

    // Get stream size (Content-Length) to allocate memory at once
    std::streamsize size = outcome.GetResult().GetContentLength();
    if (size < 0) {
        return tl::make_unexpected(
            fmt::format("Invalid content length received: {}", size));
    }

    buffer.resize(static_cast<size_t>(size));
    result_stream.read(reinterpret_cast<char *>(buffer.data()), size);

    if (result_stream.gcount() != size) {
        return tl::make_unexpected(fmt::format(
            "Failed to read expected number of bytes. Expected: {}, Actual: {}",
            size, result_stream.gcount()));
    }
    return {};
}

tl::expected<void, std::string> S3Helper::DownloadBufferMultipart(
    const std::string &key, std::vector<uint8_t> &buffer) {
    // First get file size
    Aws::S3::Model::HeadObjectRequest head_request;
    head_request.SetBucket(bucket_.c_str());
    head_request.SetKey(key.c_str());

    auto head_outcome = s3_client_.HeadObject(head_request);
    if (!head_outcome.IsSuccess()) {
        return tl::make_unexpected(fmt::format(
            "HeadObject failed: {}", head_outcome.GetError().GetMessage()));
    }

    const int64_t total_size = head_outcome.GetResult().GetContentLength();
    if (total_size < 0) {
        return tl::make_unexpected(
            fmt::format("Invalid content length: {}", total_size));
    }

    const size_t file_size = static_cast<size_t>(total_size);
    // Large file threshold: 100MB, use multipart download for files larger than
    // this to avoid saturating the network
    const size_t multipart_threshold = 100 * 1024 * 1024;  // 100MB

    // Small files downloaded directly
    if (file_size < multipart_threshold) {
        return DownloadBuffer(key, buffer);
    }

    // Large files use multipart download with concurrency control to avoid
    // saturating the network
    const size_t part_size = 100 * 1024 * 1024;  // Part size: 100MB
    const size_t part_count = (file_size + part_size - 1) / part_size;

    // Pre-allocate buffer space, write directly to final position to avoid
    // extra copying
    buffer.resize(file_size);

    // Use simpler data structure, only mark completion status
    struct PartInfo {
        std::atomic<bool> completed{false};
    };

    std::vector<PartInfo> parts_info(part_count);

    // Concurrency control: limit concurrent downloads to 2 to avoid saturating
    // the network For GB-sized files, 2 concurrent downloads can fully utilize
    // bandwidth without overloading network resources
    const size_t max_concurrent = std::min(static_cast<size_t>(2), part_count);
    std::atomic<size_t> next_part_idx{0};
    std::atomic<bool> has_error{false};
    std::string error_message;
    std::mutex error_mutex;

    // Worker thread function
    auto worker = [&]() {
        while (!has_error) {
            // Get next part index to process (0-based)
            size_t part_idx = next_part_idx.fetch_add(1);
            if (part_idx >= part_count) {
                break;
            }

            size_t part_num = part_idx + 1;  // S3 part number is 1-based
            try {
                // Calculate part offset and size
                const size_t offset = part_idx * part_size;
                const size_t remaining = file_size - offset;
                const size_t current_part_size = std::min(part_size, remaining);

                // Write directly to final buffer position, avoid intermediate
                // copying
                uint8_t *part_buffer = buffer.data() + offset;

                // Add retry logic
                const int max_retries = 2;
                bool download_success = false;

                for (int retry = 0; retry < max_retries && !has_error;
                     ++retry) {
                    // Create Range request
                    Aws::S3::Model::GetObjectRequest part_request;
                    part_request.SetBucket(bucket_.c_str());
                    part_request.SetKey(key.c_str());
                    // Set Range header: bytes=start-end (end is inclusive)
                    std::string range_header = fmt::format(
                        "bytes={}-{}", offset, offset + current_part_size - 1);
                    part_request.SetRange(range_header.c_str());

                    // Execute part download
                    auto part_outcome = s3_client_.GetObject(part_request);
                    if (!part_outcome.IsSuccess()) {
                        if (retry == max_retries - 1) {
                            std::lock_guard<std::mutex> lock(error_mutex);
                            has_error = true;
                            error_message = fmt::format(
                                "Part {} download failed after {} retries: {}",
                                part_num, max_retries,
                                part_outcome.GetError().GetMessage());
                        } else {
                            std::this_thread::sleep_for(std::chrono::seconds(
                                1 << retry));  // Exponential backoff
                        }
                        continue;
                    }

                    // Read directly to final buffer position
                    auto &result_stream = part_outcome.GetResult().GetBody();
                    result_stream.read(reinterpret_cast<char *>(part_buffer),
                                       current_part_size);

                    if (!result_stream && !result_stream.eof()) {
                        // Stream error (not normal EOF)
                        if (retry == max_retries - 1) {
                            std::lock_guard<std::mutex> lock(error_mutex);
                            has_error = true;
                            error_message =
                                fmt::format("Part {} stream error", part_num);
                        } else {
                            std::this_thread::sleep_for(
                                std::chrono::seconds(1 << retry));
                        }
                        continue;
                    }

                    const std::streamsize bytes_read = result_stream.gcount();
                    if (bytes_read !=
                        static_cast<std::streamsize>(current_part_size)) {
                        if (retry == max_retries - 1) {
                            std::lock_guard<std::mutex> lock(error_mutex);
                            has_error = true;
                            error_message = fmt::format(
                                "Part {} read incomplete: expected {}, got {}",
                                part_num, current_part_size, bytes_read);
                        } else {
                            std::this_thread::sleep_for(std::chrono::seconds(
                                1 << retry));  // Exponential backoff
                        }
                        continue;
                    }

                    // Download successful
                    parts_info[part_idx].completed.store(true);
                    download_success = true;
                    break;
                }

                if (!download_success) {
                    break;
                }

            } catch (const std::exception &e) {
                std::lock_guard<std::mutex> lock(error_mutex);
                has_error = true;
                error_message =
                    fmt::format("Part {} exception: {}", part_num, e.what());
                break;
            } catch (...) {
                std::lock_guard<std::mutex> lock(error_mutex);
                has_error = true;
                error_message = fmt::format("Part {} unknown error", part_num);
                break;
            }
        }
    };

    // Create and start worker threads
    std::vector<std::thread> threads;
    threads.reserve(max_concurrent);

    for (size_t i = 0; i < max_concurrent; ++i) {
        threads.emplace_back(worker);
    }

    // Wait for all worker threads to complete
    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // Check if any error occurred
    if (has_error) {
        buffer.clear();
        return tl::make_unexpected(error_message);
    }

    // Check if all parts are completed
    for (size_t i = 0; i < part_count; ++i) {
        if (!parts_info[i].completed.load()) {
            buffer.clear();
            return tl::make_unexpected(
                fmt::format("Part {} was not completed successfully", i + 1));
        }
    }

    // All data has been written directly to buffer, no additional merge step
    // needed
    return {};
}

tl::expected<void, std::string> S3Helper::UploadBufferMultipart(
    const std::string &key, const std::vector<uint8_t> &buffer) {
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }

    const size_t total_size = buffer.size();
    const size_t multipart_threshold = 100 * 1024 * 1024;

    if (total_size < multipart_threshold) {
        return UploadBuffer(key, buffer);
    }

    const size_t part_size = 100 * 1024 * 1024;
    const size_t part_count = (total_size + part_size - 1) / part_size;

    // 1. Initialize multipart upload
    Aws::S3::Model::CreateMultipartUploadRequest create_request;
    create_request.SetBucket(bucket_.c_str());
    create_request.SetKey(key.c_str());

    auto create_outcome = s3_client_.CreateMultipartUpload(create_request);
    if (!create_outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Init multipart failed: {}",
                        create_outcome.GetError().GetMessage()));
    }

    std::string upload_id = create_outcome.GetResult().GetUploadId();

    // 2. Use safer data structures
    struct PartInfo {
        std::atomic<bool> completed{false};
        std::string e_tag;
        int part_number{0};
    };

    std::vector<PartInfo> parts_info(part_count);
    std::vector<std::optional<Aws::S3::Model::CompletedPart>> completed_parts(
        part_count);

    // 3. Concurrency control
    const size_t max_concurrent = std::min(static_cast<size_t>(2), part_count);
    std::atomic<size_t> next_part_idx{0};
    std::atomic<bool> has_error{false};
    std::string error_message;
    std::mutex error_mutex;
    std::mutex parts_mutex;  // Protects parts_info writes

    // 4. Worker thread function
    auto worker = [&]() {
        while (!has_error) {
            size_t part_idx = next_part_idx.fetch_add(1);
            if (part_idx >= part_count) {
                break;
            }

            size_t part_num = part_idx + 1;
            try {
                const size_t offset = part_idx * part_size;
                const size_t remaining = total_size - offset;
                const size_t current_part_size = std::min(part_size, remaining);

                // Use original buffer pointer directly, avoid copying
                const uint8_t *part_data = buffer.data() + offset;

                // Add retry logic
                const int max_retries = 2;
                bool upload_success = false;

                for (int retry = 0; retry < max_retries && !has_error;
                     ++retry) {
                    Aws::S3::Model::UploadPartRequest part_request;
                    part_request.SetBucket(bucket_.c_str());
                    part_request.SetKey(key.c_str());
                    part_request.SetUploadId(upload_id);
                    part_request.SetPartNumber(static_cast<int>(part_num));
                    part_request.SetContentLength(
                        static_cast<long long>(current_part_size));

                    // Use temporary stream
                    auto stream =
                        Aws::MakeShared<Aws::StringStream>("UploadPart");
                    stream->write(reinterpret_cast<const char *>(part_data),
                                  current_part_size);
                    part_request.SetBody(stream);

                    auto part_outcome = s3_client_.UploadPart(part_request);

                    if (part_outcome.IsSuccess()) {
                        std::lock_guard<std::mutex> lock(parts_mutex);
                        parts_info[part_idx].completed = true;
                        parts_info[part_idx].e_tag =
                            part_outcome.GetResult().GetETag();
                        parts_info[part_idx].part_number = part_num;
                        upload_success = true;
                        break;
                    }

                    if (retry == max_retries - 1) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        has_error = true;
                        error_message = fmt::format(
                            "Part {} upload failed after {} retries: {}",
                            part_num, max_retries,
                            part_outcome.GetError().GetMessage());
                    } else {
                        std::this_thread::sleep_for(std::chrono::seconds(
                            1 << retry));  // Exponential backoff
                    }
                }

                if (!upload_success) {
                    break;
                }

            } catch (const std::exception &e) {
                std::lock_guard<std::mutex> lock(error_mutex);
                has_error = true;
                error_message =
                    fmt::format("Part {} exception: {}", part_num, e.what());
                break;
            } catch (...) {
                std::lock_guard<std::mutex> lock(error_mutex);
                has_error = true;
                error_message = fmt::format("Part {} unknown error", part_num);
                break;
            }
        }
    };

    // 5. Start threads
    std::vector<std::thread> threads;
    threads.reserve(max_concurrent);

    for (size_t i = 0; i < max_concurrent; ++i) {
        threads.emplace_back(worker);
    }

    // 6. Wait for completion
    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // 7. Error handling
    auto cleanup = [&]() {
        Aws::S3::Model::AbortMultipartUploadRequest abort_request;
        abort_request.SetBucket(bucket_.c_str());
        abort_request.SetKey(key.c_str());
        abort_request.SetUploadId(upload_id);
        s3_client_.AbortMultipartUpload(abort_request);
    };

    if (has_error) {
        cleanup();
        return tl::make_unexpected(error_message);
    }

    // 8. Verify all parts completed
    for (size_t i = 0; i < part_count; ++i) {
        if (!parts_info[i].completed.load()) {
            cleanup();
            return tl::make_unexpected(
                fmt::format("Part {} was not completed successfully", i + 1));
        }
    }

    // 9. Complete upload
    Aws::S3::Model::CompleteMultipartUploadRequest complete_request;
    complete_request.SetBucket(bucket_.c_str());
    complete_request.SetKey(key.c_str());
    complete_request.SetUploadId(upload_id);

    Aws::S3::Model::CompletedMultipartUpload completed_upload;

    for (size_t i = 0; i < part_count; ++i) {
        Aws::S3::Model::CompletedPart part;
        part.SetPartNumber(parts_info[i].part_number);
        part.SetETag(parts_info[i].e_tag);
        completed_upload.AddParts(std::move(part));
    }

    complete_request.SetMultipartUpload(completed_upload);

    auto complete_outcome =
        s3_client_.CompleteMultipartUpload(complete_request);
    if (!complete_outcome.IsSuccess()) {
        cleanup();
        return tl::make_unexpected(
            fmt::format("Complete multipart failed: {}",
                        complete_outcome.GetError().GetMessage()));
    }

    return {};
}

// Implement DownloadString method
tl::expected<void, std::string> S3Helper::DownloadString(const std::string &key,
                                                         std::string &data) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(bucket_);
    request.WithKey(key);

    auto outcome = s3_client_.GetObject(request);

    if (outcome.IsSuccess()) {
        auto &body = outcome.GetResult().GetBody();
        std::stringstream buffer;
        buffer << body.rdbuf();
        data = buffer.str();
        return {};
    } else {
        return tl::make_unexpected(fmt::format(
            "GetObject failed: {}", outcome.GetError().GetMessage()));
    }
}

tl::expected<void, std::string> S3Helper::DeleteObject(const std::string &key) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.SetBucket(bucket_.c_str());
    request.SetKey(key.c_str());

    auto outcome = s3_client_.DeleteObject(request);
    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Delete failed: {}", outcome.GetError().GetMessage()));
    }
    return {};
}

tl::expected<void, std::string> S3Helper::DeleteObjects(
    const std::vector<std::string> &keys) {
    // If no objects to delete, return success directly
    if (keys.empty()) {
        return {};
    }

    // Delete objects in batches (S3 can delete up to 1000 objects per request)
    const size_t batch_size = 1000;
    for (size_t i = 0; i < keys.size(); i += batch_size) {
        Aws::S3::Model::DeleteObjectsRequest request;
        request.WithBucket(bucket_);

        Aws::S3::Model::Delete delete_config;
        std::vector<Aws::S3::Model::ObjectIdentifier> objects_to_delete;

        // Build the list of objects to delete for this batch
        size_t end = std::min(i + batch_size, keys.size());
        for (size_t j = i; j < end; ++j) {
            Aws::S3::Model::ObjectIdentifier obj_id;
            obj_id.SetKey(keys[j]);
            objects_to_delete.push_back(obj_id);
        }

        delete_config.SetObjects(objects_to_delete);
        request.SetDelete(delete_config);

        // Execute delete operation
        auto outcome = s3_client_.DeleteObjects(request);
        if (!outcome.IsSuccess()) {
            return tl::make_unexpected(fmt::format(
                "DeleteObjects failed: {}", outcome.GetError().GetMessage()));
        }

        // Check if any objects failed to delete
        const auto &result = outcome.GetResult();
        if (!result.GetErrors().empty()) {
            std::string error_message;
            for (const auto &error : result.GetErrors()) {
                error_message.append(
                    fmt::format("key:{}, code:{}, message:{}", error.GetKey(),
                                error.GetCode(), error.GetMessage()));
            }
            return tl::make_unexpected(error_message);
        }
    }

    return {};
}

tl::expected<void, std::string> S3Helper::UploadFile(
    const Aws::String &file_path, const Aws::String &key) {
    // Determine S3 object name
    Aws::String s3_object_name =
        key.empty() ? file_path.substr(file_path.find_last_of("/\\") + 1) : key;

    // Use AWS SDK's automatic file handling
    auto input_data = Aws::MakeShared<Aws::FStream>(
        "PutObjectInputStream", file_path.c_str(),
        std::ios_base::in | std::ios_base::binary);

    if (!input_data->is_open()) {
        return tl::make_unexpected(
            fmt::format("Failed to open file: {}", file_path));
    }

    // Build upload request
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_);
    request.SetKey(s3_object_name);
    request.SetBody(input_data);

    // Execute upload
    auto outcome = s3_client_.PutObject(request);

    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Upload error: {}", outcome.GetError().GetMessage()));
    }

    return {};
}

tl::expected<void, std::string> S3Helper::DownloadFile(
    const Aws::String &file_path, const Aws::String &key) {
    // Build download request
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_);
    request.SetKey(key);

    // Execute download
    auto outcome = s3_client_.GetObject(request);

    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Download error: {}", outcome.GetError().GetMessage()));
    }

    // Get result stream
    auto &result_stream = outcome.GetResult().GetBody();

    // Open local file for writing
    Aws::OFStream file_stream(file_path.c_str(),
                              std::ios_base::out | std::ios_base::binary);
    if (!file_stream.is_open()) {
        return tl::make_unexpected(fmt::format(
            "Failed to open local file for writing: {}", file_path));
    }

    // Write S3 object content to local file
    file_stream << result_stream.rdbuf();

    if (file_stream.fail()) {
        return tl::make_unexpected(
            fmt::format("Failed to write data to local file: {}", file_path));
    }

    return {};
}

// List objects with specified prefix
tl::expected<void, std::string> S3Helper::ListObjectsWithPrefix(
    const std::string &prefix, std::vector<std::string> &object_keys) {
    object_keys.clear();

    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucket_);
    request.WithPrefix(prefix);

    // Set maximum return count, pagination may be needed if there are many
    // objects
    request.WithMaxKeys(1000);

    bool done = false;
    while (!done) {
        auto outcome = s3_client_.ListObjects(request);
        if (!outcome.IsSuccess()) {
            return tl::make_unexpected(fmt::format(
                "ListObjects error: {}", outcome.GetError().GetMessage()));
        }

        const auto &result = outcome.GetResult();

        // Add all object keys to result list
        for (const auto &object : result.GetContents()) {
            object_keys.push_back(object.GetKey());
        }

        // Check if there are more objects to fetch
        if (result.GetIsTruncated()) {
            // Set marker to get next page
            request.WithMarker(result.GetNextMarker());
        } else {
            done = true;
        }
    }

    return {};
}

// Delete all objects with specified prefix
tl::expected<void, std::string> S3Helper::DeleteObjectsWithPrefix(
    const std::string &prefix) {
    // First list all objects matching the prefix
    std::vector<std::string> object_keys;
    if (!ListObjectsWithPrefix(prefix, object_keys)) {
        return tl::make_unexpected(
            fmt::format("Failed to list objects with prefix: {}", prefix));
    }

    // If no objects to delete, return success directly
    if (object_keys.empty()) {
        return {};
    }

    // Use existing DeleteObjects method to delete all objects
    return DeleteObjects(object_keys);
}

}  // namespace mooncake