
// #include "storage_backend.h"
#include "file_storage_backend.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <string>
#include <vector>


namespace mooncake {
  
ErrorCode FileStorageBackend::StoreObject(const ObjectKey& key,
                                     const std::vector<Slice>& slices) {
    // if(!valid_) {
    //     LOG(INFO) << "FileStorageBackend is not valid. Cannot store object.";
    //     return ErrorCode::FILE_SYSTEM_UNINITIALIZED;
    // }

    std::string path = ResolvePath(key);

    if(std::filesystem::exists(path) == true) {
        // LOG(INFO) << "File does not exist: " << path;
        // LOG(INFO) << "File already exists: " << key;
        return ErrorCode::FILE_ALREADY_EXISTS;
    }

    FILE* file = fopen(path.c_str(), "wb");
    size_t slices_total_size = 0;
    std::vector<iovec> iovs;

    if (!file) {
        LOG(INFO) << "Failed to open file for writing: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }    
    
    // // 使用 unique_ptr 确保文件在异常情况下会被关闭
    // std::unique_ptr<FILE, decltype(&fclose)> fileGuard(file, &fclose);

    LocalFile local_file(path,file,ErrorCode::OK);

    for (const auto& slice : slices) {
        iovec io{ slice.ptr, slice.size };
        iovs.push_back(io);
        slices_total_size += slice.size;
    }

    ssize_t ret = local_file.pwritev(iovs.data(), static_cast<int>(iovs.size()), 0);
    if (ret < 0) {
        LOG(INFO) << "pwritev failed for: " << path;
        //TODO : add file delete logic here

        return ErrorCode::FILE_WRITE_FAIL;
    }
    if (ret != static_cast<ssize_t>(slices_total_size)) {
        LOG(INFO) << "Write size mismatch for: " << path
                   << ", expected: " << slices_total_size
                   << ", got: " << ret;

        //TODO : add file delete logic here

        return ErrorCode::FILE_WRITE_FAIL;
    }
    // Successfully wrote the object, now we can close the file
    // Note: fclose is not necessary here as LocalFile destructor will handle it

//   // 检查写入结果
//     if (ret < 0 || ret != static_cast<ssize_t>(slices_total_size)) {
//         LOG(ERROR) << "Write failed for: " << path 
//                    << ", expected: " << slices_total_size
//                    << ", got: " << ret;
        
//         // 显式关闭文件以确保可以删除
//         fileGuard.reset();
        
//         // 删除部分写入的文件
//         if (std::filesystem::exists(path)) {
//             std::error_code ec;
//             if (!std::filesystem::remove(path, ec)) {
//                 LOG(ERROR) << "Failed to remove partially written file: " << path
//                            << ", error: " << ec.message();
//             }
//         }
        
//         return  ErrorCode::FILE_WRITE_FAIL;
//         //TODO : local_file.errorcode_;
//     }

//     // 成功写入后释放文件所有权，让 LocalFile 析构函数处理关闭
//     fileGuard.release();


    return ErrorCode::OK;
}

ErrorCode FileStorageBackend::LoadObject(const ObjectKey& key,
                                    std::vector<Slice>& slices) {
    // if(!valid_) {
    //     LOG(INFO) << "FileStorageBackend is not valid. Cannot store object.";
    //     return ErrorCode::FILE_SYSTEM_UNINITIALIZED;
    // }

    std::string path = ResolvePath(key);

    FILE* file = fopen(path.c_str(), "rb");
    size_t slices_total_size=0;
    std::vector<iovec> iovs;

    if (!file) {
        LOG(INFO) << "Failed to open file for reading: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }
    LocalFile local_file(path,file,ErrorCode::OK);

    for (const auto& slice : slices) {
        iovec io{ slice.ptr, slice.size };
        iovs.push_back(io);
        slices_total_size += slice.size;
    }

    ssize_t ret = local_file.preadv(iovs.data(), static_cast<int>(iovs.size()), 0);
    if (ret < 0) {
        LOG(INFO) << "preadv failed for: " << path;

        return ErrorCode::FILE_READ_FAIL;
    }
    if (ret != static_cast<ssize_t>(slices_total_size)) {
        LOG(INFO) << "Read size mismatch for: " << path
                   << ", expected: " << slices_total_size
                   << ", got: " << ret;

        return ErrorCode::FILE_READ_FAIL;
    }
    // Successfully read the object, now we can close the file
    // Note: fclose is not necessary here as LocalFile destructor will handle it

    return ErrorCode::OK;
}

ErrorCode FileStorageBackend::LoadObject(const ObjectKey& key,
                                    std::string& str) {
    // if(!valid_) {
    //     LOG(INFO) << "FileStorageBackend is not valid. Cannot store object.";
    //     return ErrorCode::FILE_SYSTEM_UNINITIALIZED;
    // }

    std::string path = ResolvePath(key);
    FILE* file = fopen(path.c_str(), "rb");
    size_t file_total_size=0;

    if (!file) {
        // LOG(INFO) << "Failed to open file for reading: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }

    fseek(file, 0, SEEK_END);
    file_total_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    LocalFile local_file(path,file,ErrorCode::OK);


    ssize_t ret = local_file.read(str, file_total_size);
    if (ret < 0) {
        LOG(INFO) << "preadv failed for: " << path;

        return ErrorCode::FILE_READ_FAIL;
    }
    if (ret != static_cast<ssize_t>(file_total_size)) {
        LOG(INFO) << "Read size mismatch for: " << path
                   << ", expected: " << file_total_size
                   << ", got: " << ret;

        return ErrorCode::FILE_READ_FAIL;
    }
    // Successfully read the object, now we can close the file
    // Note: fclose is not necessary here as LocalFile destructor will handle it

    return ErrorCode::OK;
}

ErrorCode FileStorageBackend::StoreObject(const ObjectKey& key,
                                    std::string& str) {
    // if(!valid_) {
    //     LOG(INFO) << "FileStorageBackend is not valid. Cannot store object.";
    //     return ErrorCode::FILE_SYSTEM_UNINITIALIZED;
    // }

    std::string path = ResolvePath(key);

    if(std::filesystem::exists(path) == true) {
        // LOG(INFO) << "File does not exist: " << path;
        // LOG(INFO) << "File already exists: " << key;
        return ErrorCode::FILE_ALREADY_EXISTS;
    }
    
    FILE* file = fopen(path.c_str(), "wb");
    size_t file_total_size=str.size();

    if (!file) {
        LOG(INFO) << "Failed to open file for reading: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }

    LocalFile local_file(path,file,ErrorCode::OK);


    ssize_t ret = local_file.write(str, file_total_size);
    if (ret < 0) {
        LOG(INFO) << "pwritev failed for: " << path;

        return ErrorCode::FILE_WRITE_FAIL;
    }
    if (ret != static_cast<ssize_t>(file_total_size)) {
        LOG(INFO) << "Write size mismatch for: " << path
                   << ", expected: " << file_total_size
                   << ", got: " << ret;

        return ErrorCode::FILE_WRITE_FAIL;
    }
    // Successfully read the object, now we can close the file
    // Note: fclose is not necessary here as LocalFile destructor will handle it

    return ErrorCode::OK;
}



std::string FileStorageBackend::SanitizeKey(const ObjectKey& key) const {
    // 定义所有需要替换的非法字符集合
    constexpr std::string_view kInvalidChars = "/\\:*?\"<>|";
    std::string sanitized_key;
    sanitized_key.reserve(key.size());
    
    for (char c : key) {
        // 检查字符是否在非法字符集合中
        sanitized_key.push_back(
            kInvalidChars.find(c) != std::string_view::npos ? '_' : c
        );
    }
    return sanitized_key;
}

std::string FileStorageBackend::ResolvePath(const ObjectKey& key) const {
    // 计算Key的哈希值（示例用std::hash，实际可用更均匀的哈希如XXH3）
    size_t hash = std::hash<std::string>{}(key);
    
    // 取哈希的低8位，生成两级子目录（如 "a1/b2"）
    char dir1 = static_cast<char>('a' + (hash & 0x0F));       // 低4位 -> 16个目录
    char dir2 = static_cast<char>('a' + ((hash >> 4) & 0x0F)); // 次低4位 -> 16个子目录
    
    // 使用std::filesystem安全拼接路径
    namespace fs = std::filesystem;
    fs::path dir_path = fs::path(root_dir_) / std::string(1, dir1) / std::string(1, dir2);

    // 检查或创建目录
    std::error_code ec;
    if (!fs::exists(dir_path)) {
        if (!fs::create_directories(dir_path, ec) && ec) {
            LOG(INFO) << "Failed to create directory: " << dir_path << ", error: " << ec.message();
            return ""; // 返回空字符串表示失败
        }
    }

    fs::path full_path = dir_path / SanitizeKey(key);
    
    return full_path.lexically_normal().string();
}

}  // namespace mooncake