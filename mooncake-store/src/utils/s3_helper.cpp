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

void AssignTimeoutFromEnv(const char *env_name, int64_t default_value, int64_t &target) {
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

    // 在初始化时一次性读取环境变量（若未设置则按需回退）
    AssignStringFromEnv("AWS_REGION", s3_env.region);
    AssignStringFromEnv("AWS_S3_ENDPOINT", s3_env.endpoint);
    AssignStringFromEnv("AWS_BUCKET_NAME", s3_env.bucket);
    AssignStringFromEnv("AWS_ACCESS_KEY_ID", s3_env.access_key);
    AssignStringFromEnv("AWS_SECRET_ACCESS_KEY", s3_env.secret_key);

    AssignBoolFromEnv("AWS_USE_VIRTUAL_ADDRESSING",
                      s3_env.use_virtual_addressing);

    AssignTimeoutFromEnv("AWS_CONNECT_TIMEOUT_MS", kDefaultS3ConnectTimeoutMs,
                         s3_env.connect_timeout_ms);
    AssignTimeoutFromEnv("AWS_REQUEST_TIMEOUT_MS", kDefaultS3RequestTimeoutMs,
                         s3_env.request_timeout_ms);
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
    config.scheme = Aws::Http::Scheme::HTTP;

    if (!endpoint.empty()) {
        config.region = endpoint;
    } else {
        config.region = s3_env.region;
    }

    if (!endpoint.empty()) {
        config.endpointOverride = endpoint;
    } else {
        config.region = s3_env.endpoint;
    }

    bucket_ = s3_env.bucket;
    if (!bucket.empty()) {
        bucket_ = bucket;
    }

    Aws::Auth::AWSCredentials credentials(s3_env.access_key, s3_env.secret_key);

    // 将日志信息拼接到成员变量connection_info_中
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
        bucket_.empty() ? "unset" : bucket_, config.connectTimeoutMs, config.requestTimeoutMs,
        config.scheme == Aws::Http::Scheme::HTTP ? "HTTP" : "HTTPS",
        !s3_env.access_key.empty() ? "set" : "unset",
        !s3_env.secret_key.empty() ? "set" : "unset",
        s3_env.use_virtual_addressing ? "true" : "false");

    s3_client_ = Aws::S3::S3Client(credentials, config,
                                   Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                                   s3_env.use_virtual_addressing);
}

S3Helper::~S3Helper() = default;

tl::expected<void, std::string> S3Helper::UploadBuffer(const std::string &key,
                                                       const std::vector<uint8_t> &buffer) {
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_.c_str());
    request.SetKey(key.c_str());

    // 确保 buffer 不为空
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }

    // 正确设置 ContentLength（使用 long long 类型）
    request.SetContentLength(static_cast<long long>(buffer.size()));
    const auto buffer_size = static_cast<std::streamsize>(buffer.size());

    // 创建并写入流
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

tl::expected<void, std::string> S3Helper::UploadString(const std::string &key,
                                                       const std::string &data) {
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

tl::expected<void, std::string> S3Helper::DownloadBuffer(const std::string &key,
                                                         std::vector<uint8_t> &buffer) {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_.c_str());
    request.SetKey(key.c_str());

    auto outcome = s3_client_.GetObject(request);
    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Download failed: {}", outcome.GetError().GetMessage()));
    }

    auto &result_stream = outcome.GetResult().GetBody();

    // 获取流大小（Content-Length）以一次性分配内存
    std::streamsize size = outcome.GetResult().GetContentLength();
    if (size < 0) {
        return tl::make_unexpected(fmt::format("Invalid content length received: {}", size));
    }

    buffer.resize(static_cast<size_t>(size));
    result_stream.read(reinterpret_cast<char *>(buffer.data()), size);

    if (result_stream.gcount() != size) {
        return tl::make_unexpected(
            fmt::format("Failed to read expected number of bytes. Expected: {}, Actual: {}", size,
                        result_stream.gcount()));
    }
    return {};
}

tl::expected<void, std::string> S3Helper::DownloadBufferMultipart(const std::string &key,
                                                                  std::vector<uint8_t> &buffer) {
    // 首先获取文件大小
    Aws::S3::Model::HeadObjectRequest head_request;
    head_request.SetBucket(bucket_.c_str());
    head_request.SetKey(key.c_str());

    auto head_outcome = s3_client_.HeadObject(head_request);
    if (!head_outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("HeadObject failed: {}", head_outcome.GetError().GetMessage()));
    }

    const int64_t total_size = head_outcome.GetResult().GetContentLength();
    if (total_size < 0) {
        return tl::make_unexpected(fmt::format("Invalid content length: {}", total_size));
    }

    const size_t file_size = static_cast<size_t>(total_size);
    // 大文件阈值：100MB，超过此大小使用分片下载以避免网卡被打满
    const size_t multipart_threshold = 100 * 1024 * 1024;  // 100MB

    // 小文件直接下载
    if (file_size < multipart_threshold) {
        return DownloadBuffer(key, buffer);
    }

    // 大文件使用分片下载，并控制并发以避免网卡被打满
    const size_t part_size = 100 * 1024 * 1024;  // 分片大小：100MB
    const size_t part_count = (file_size + part_size - 1) / part_size;

    // 预分配 buffer 空间，直接写入最终位置，避免额外拷贝
    buffer.resize(file_size);

    // 使用更简单的数据结构，只标记完成状态
    struct PartInfo {
        std::atomic<bool> completed{false};
    };

    std::vector<PartInfo> parts_info(part_count);

    // 并发控制：限制并发数为2，避免网卡被打满
    // 对于GB级文件，2个并发已经能充分利用带宽，同时不会过度占用网络资源
    const size_t max_concurrent = std::min(static_cast<size_t>(2), part_count);
    std::atomic<size_t> next_part_idx{0};
    std::atomic<bool> has_error{false};
    std::string error_message;
    std::mutex error_mutex;

    // 工作线程函数
    auto worker = [&]() {
        while (!has_error) {
            // 获取下一个要处理的分片索引（0-based）
            size_t part_idx = next_part_idx.fetch_add(1);
            if (part_idx >= part_count) {
                break;
            }

            size_t part_num = part_idx + 1;  // S3 part number 是 1-based
            try {
                // 计算分片偏移和大小
                const size_t offset = part_idx * part_size;
                const size_t remaining = file_size - offset;
                const size_t current_part_size = std::min(part_size, remaining);

                // 直接写入到最终 buffer 的对应位置，避免中间拷贝
                uint8_t* part_buffer = buffer.data() + offset;

                // 添加重试逻辑
                const int max_retries = 2;
                bool download_success = false;

                for (int retry = 0; retry < max_retries && !has_error; ++retry) {
                    // 创建 Range 请求
                    Aws::S3::Model::GetObjectRequest part_request;
                    part_request.SetBucket(bucket_.c_str());
                    part_request.SetKey(key.c_str());
                    // 设置 Range 头：bytes=start-end (end 是包含的)
                    std::string range_header = fmt::format("bytes={}-{}", offset, offset + current_part_size - 1);
                    part_request.SetRange(range_header.c_str());

                    // 执行分片下载
                    auto part_outcome = s3_client_.GetObject(part_request);
                    if (!part_outcome.IsSuccess()) {
                        if (retry == max_retries - 1) {
                            std::lock_guard<std::mutex> lock(error_mutex);
                            has_error = true;
                            error_message = fmt::format("Part {} download failed after {} retries: {}",
                                                        part_num, max_retries,
                                                        part_outcome.GetError().GetMessage());
                        } else {
                            std::this_thread::sleep_for(std::chrono::seconds(1 << retry));  // 指数退避
                        }
                        continue;
                    }

                    // 直接读取到最终 buffer 位置
                    auto &result_stream = part_outcome.GetResult().GetBody();
                    result_stream.read(reinterpret_cast<char *>(part_buffer), current_part_size);

                    if (!result_stream && !result_stream.eof()) {
                        // 流出错（不是正常的EOF）
                        if (retry == max_retries - 1) {
                            std::lock_guard<std::mutex> lock(error_mutex);
                            has_error = true;
                            error_message = fmt::format("Part {} stream error", part_num);
                        } else {
                            std::this_thread::sleep_for(std::chrono::seconds(1 << retry));
                        }
                        continue;
                    }

                    const std::streamsize bytes_read = result_stream.gcount();
                    if (bytes_read != static_cast<std::streamsize>(current_part_size)) {
                        if (retry == max_retries - 1) {
                            std::lock_guard<std::mutex> lock(error_mutex);
                            has_error = true;
                            error_message = fmt::format("Part {} read incomplete: expected {}, got {}",
                                                        part_num, current_part_size, bytes_read);
                        } else {
                            std::this_thread::sleep_for(std::chrono::seconds(1 << retry));  // 指数退避
                        }
                        continue;
                    }

                    // 下载成功
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
                error_message = fmt::format("Part {} exception: {}", part_num, e.what());
                break;
            } catch (...) {
                std::lock_guard<std::mutex> lock(error_mutex);
                has_error = true;
                error_message = fmt::format("Part {} unknown error", part_num);
                break;
            }
        }
    };

    // 创建并启动工作线程
    std::vector<std::thread> threads;
    threads.reserve(max_concurrent);

    for (size_t i = 0; i < max_concurrent; ++i) {
        threads.emplace_back(worker);
    }

    // 等待所有工作线程完成
    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // 检查是否有错误发生
    if (has_error) {
        buffer.clear();
        return tl::make_unexpected(error_message);
    }

    // 检查所有分片是否都已完成
    for (size_t i = 0; i < part_count; ++i) {
        if (!parts_info[i].completed.load()) {
            buffer.clear();
            return tl::make_unexpected(
                fmt::format("Part {} was not completed successfully", i + 1));
        }
    }

    // 所有数据已经直接写入 buffer，无需额外合并步骤
    return {};
}

tl::expected<void, std::string> S3Helper::UploadBufferMultipart(const std::string &key,
                                                                const std::vector<uint8_t> &buffer) {
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

    // 1. 初始化分片上传
    Aws::S3::Model::CreateMultipartUploadRequest create_request;
    create_request.SetBucket(bucket_.c_str());
    create_request.SetKey(key.c_str());

    auto create_outcome = s3_client_.CreateMultipartUpload(create_request);
    if (!create_outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Init multipart failed: {}", create_outcome.GetError().GetMessage()));
    }

    std::string upload_id = create_outcome.GetResult().GetUploadId();

    // 2. 使用更安全的数据结构
    struct PartInfo {
        std::atomic<bool> completed{false};
        std::string e_tag;
        int part_number{0};
    };

    std::vector<PartInfo> parts_info(part_count);
    std::vector<std::optional<Aws::S3::Model::CompletedPart>> completed_parts(part_count);

    // 3. 并发控制
    const size_t max_concurrent = std::min(static_cast<size_t>(2), part_count);
    std::atomic<size_t> next_part_idx{0};
    std::atomic<bool> has_error{false};
    std::string error_message;
    std::mutex error_mutex;
    std::mutex parts_mutex;  // 保护parts_info的写入

    // 4. 工作线程函数
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

                // 直接使用原始buffer指针，避免拷贝
                const uint8_t* part_data = buffer.data() + offset;

                // 添加重试逻辑
                const int max_retries = 2;
                bool upload_success = false;

                for (int retry = 0; retry < max_retries && !has_error; ++retry) {
                    Aws::S3::Model::UploadPartRequest part_request;
                    part_request.SetBucket(bucket_.c_str());
                    part_request.SetKey(key.c_str());
                    part_request.SetUploadId(upload_id);
                    part_request.SetPartNumber(static_cast<int>(part_num));
                    part_request.SetContentLength(static_cast<long long>(current_part_size));

                    // 使用临时stream
                    auto stream = Aws::MakeShared<Aws::StringStream>("UploadPart");
                    stream->write(reinterpret_cast<const char *>(part_data), current_part_size);
                    part_request.SetBody(stream);


                    auto part_outcome = s3_client_.UploadPart(part_request);

                    if (part_outcome.IsSuccess()) {
                        std::lock_guard<std::mutex> lock(parts_mutex);
                        parts_info[part_idx].completed = true;
                        parts_info[part_idx].e_tag = part_outcome.GetResult().GetETag();
                        parts_info[part_idx].part_number = part_num;
                        upload_success = true;
                        break;
                    }

                    if (retry == max_retries - 1) {
                        std::lock_guard<std::mutex> lock(error_mutex);
                        has_error = true;
                        error_message = fmt::format("Part {} upload failed after {} retries: {}",
                                                    part_num, max_retries,
                                                    part_outcome.GetError().GetMessage());
                    } else {
                        std::this_thread::sleep_for(std::chrono::seconds(1 << retry)); // 指数退避
                    }
                }

                if (!upload_success) {
                    break;
                }

            } catch (const std::exception &e) {
                std::lock_guard<std::mutex> lock(error_mutex);
                has_error = true;
                error_message = fmt::format("Part {} exception: {}", part_num, e.what());
                break;
            } catch (...) {
                std::lock_guard<std::mutex> lock(error_mutex);
                has_error = true;
                error_message = fmt::format("Part {} unknown error", part_num);
                break;
            }
        }
    };

    // 5. 启动线程
    std::vector<std::thread> threads;
    threads.reserve(max_concurrent);

    for (size_t i = 0; i < max_concurrent; ++i) {
        threads.emplace_back(worker);
    }

    // 6. 等待完成
    for (auto &thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // 7. 错误处理
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

    // 8. 验证所有分片完成
    for (size_t i = 0; i < part_count; ++i) {
        if (!parts_info[i].completed.load()) {
            cleanup();
            return tl::make_unexpected(
                fmt::format("Part {} was not completed successfully", i + 1));
        }
    }

    // 9. 完成上传
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

    auto complete_outcome = s3_client_.CompleteMultipartUpload(complete_request);
    if (!complete_outcome.IsSuccess()) {
        cleanup();
        return tl::make_unexpected(
            fmt::format("Complete multipart failed: {}",
                        complete_outcome.GetError().GetMessage()));
    }

    return {};
}

// 实现DownloadString方法
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
        return tl::make_unexpected(
            fmt::format("GetObject failed: {}", outcome.GetError().GetMessage()));
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

tl::expected<void, std::string> S3Helper::DeleteObjects(const std::vector<std::string> &keys) {
    // 如果没有对象需要删除，直接返回成功
    if (keys.empty()) {
        return {};
    }

    // 分批删除对象（S3每次最多可以删除1000个对象）
    const size_t batch_size = 1000;
    for (size_t i = 0; i < keys.size(); i += batch_size) {
        Aws::S3::Model::DeleteObjectsRequest request;
        request.WithBucket(bucket_);

        Aws::S3::Model::Delete delete_config;
        std::vector<Aws::S3::Model::ObjectIdentifier> objects_to_delete;

        // 构建这一批要删除的对象列表
        size_t end = std::min(i + batch_size, keys.size());
        for (size_t j = i; j < end; ++j) {
            Aws::S3::Model::ObjectIdentifier obj_id;
            obj_id.SetKey(keys[j]);
            objects_to_delete.push_back(obj_id);
        }

        delete_config.SetObjects(objects_to_delete);
        request.SetDelete(delete_config);

        // 执行删除操作
        auto outcome = s3_client_.DeleteObjects(request);
        if (!outcome.IsSuccess()) {
            return tl::make_unexpected(
                fmt::format("DeleteObjects failed: {}", outcome.GetError().GetMessage()));
        }

        // 检查是否有删除失败的对象
        const auto &result = outcome.GetResult();
        if (!result.GetErrors().empty()) {
            std::string error_message;
            for (const auto &error : result.GetErrors()) {
                error_message.append(fmt::format("key:{}, code:{}, message:{}", error.GetKey(),
                                                 error.GetCode(), error.GetMessage()));
            }
            return tl::make_unexpected(error_message);
        }
    }

    return {};
}

tl::expected<void, std::string> S3Helper::UploadFile(const Aws::String &file_path,
                                                     const Aws::String &key) {
    // 确定S3对象名称
    Aws::String s3_object_name =
        key.empty() ? file_path.substr(file_path.find_last_of("/\\") + 1) : key;

    // 使用AWS SDK的自动文件处理
    auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", file_path.c_str(),
                                                    std::ios_base::in | std::ios_base::binary);

    if (!input_data->is_open()) {
        return tl::make_unexpected(fmt::format("Failed to open file: {}", file_path));
    }

    // 构建上传请求
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_);
    request.SetKey(s3_object_name);
    request.SetBody(input_data);

    // 执行上传
    auto outcome = s3_client_.PutObject(request);

    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Upload error: {}", outcome.GetError().GetMessage()));
    }

    return {};
}

tl::expected<void, std::string> S3Helper::DownloadFile(const Aws::String &file_path,
                                                       const Aws::String &key) {
    // 构建下载请求
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_);
    request.SetKey(key);

    // 执行下载
    auto outcome = s3_client_.GetObject(request);

    if (!outcome.IsSuccess()) {
        return tl::make_unexpected(
            fmt::format("Download error: {}", outcome.GetError().GetMessage()));
    }

    // 获取结果流
    auto &result_stream = outcome.GetResult().GetBody();

    // 打开本地文件用于写入
    Aws::OFStream file_stream(file_path.c_str(), std::ios_base::out | std::ios_base::binary);
    if (!file_stream.is_open()) {
        return tl::make_unexpected(
            fmt::format("Failed to open local file for writing: {}", file_path));
    }

    // 将S3对象内容写入本地文件
    file_stream << result_stream.rdbuf();

    if (file_stream.fail()) {
        return tl::make_unexpected(
            fmt::format("Failed to write data to local file: {}", file_path));
    }

    return {};
}

// 列出指定前缀的对象
tl::expected<void, std::string> S3Helper::ListObjectsWithPrefix(
    const std::string &prefix, std::vector<std::string> &object_keys) {
    object_keys.clear();

    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucket_);
    request.WithPrefix(prefix);

    // 设置最大返回数量，如果对象很多，可能需要分页处理
    request.WithMaxKeys(1000);

    bool done = false;
    while (!done) {
        auto outcome = s3_client_.ListObjects(request);
        if (!outcome.IsSuccess()) {
            return tl::make_unexpected(
                fmt::format("ListObjects error: {}", outcome.GetError().GetMessage()));
        }

        const auto &result = outcome.GetResult();

        // 添加所有对象键到结果列表
        for (const auto &object : result.GetContents()) {
            object_keys.push_back(object.GetKey());
        }

        // 检查是否还有更多对象需要获取
        if (result.GetIsTruncated()) {
            // 设置marker以获取下一页
            request.WithMarker(result.GetNextMarker());
        } else {
            done = true;
        }
    }

    return {};
}

// 删除指定前缀的所有对象
tl::expected<void, std::string> S3Helper::DeleteObjectsWithPrefix(const std::string &prefix) {
    // 首先列出所有匹配前缀的对象
    std::vector<std::string> object_keys;
    if (!ListObjectsWithPrefix(prefix, object_keys)) {
        return tl::make_unexpected(fmt::format("Failed to list objects with prefix: {}", prefix));
    }

    // 如果没有对象需要删除，直接返回成功
    if (object_keys.empty()) {
        return {};
    }

    // 使用现有的DeleteObjects方法删除所有对象
    return DeleteObjects(object_keys);
}

}  // namespace mooncake