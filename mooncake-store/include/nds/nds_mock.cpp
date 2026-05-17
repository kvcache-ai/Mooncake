#include "nds_interface.h"
#include <cstring>
#include <algorithm>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <mutex>

namespace NDS {

namespace fs = std::filesystem;

// Global variables
std::mutex g_mutex;
bool g_initialized = false;

// Helper method to get filename from blockId
std::string getBlockFilename(uint64_t blockId) {
    // Create kv_data directory if it doesn't exist
    fs::path kv_dir = fs::current_path() / "kv_data";
    if (!fs::exists(kv_dir)) {
        fs::create_directories(kv_dir);
        std::cout << "DEBUG: Created kv_data directory: " << kv_dir.string() << std::endl;
    }
    
    std::string filename = std::to_string(blockId);
    fs::path full_path = kv_dir / filename;
    std::cout << "DEBUG: Block ID " << blockId << " maps to file: " << full_path.string() << std::endl;
    return full_path.string();
}

int32_t init(void* addr, uint64_t len) {
    std::lock_guard<std::mutex> lock(g_mutex);
    g_initialized = true;
    std::cout << "DEBUG: NDS initialized" << std::endl;
    return 0;  // Success
}

int32_t isExists(std::vector<uint64_t> blockIds) {
    std::lock_guard<std::mutex> lock(g_mutex);
    
    if (!g_initialized) {
        std::cout << "DEBUG: NDS not initialized" << std::endl;
        return -1;  // Not initialized
    }
    
    int32_t count = 0;
    for (uint64_t blockId : blockIds) {
        std::string filename = getBlockFilename(blockId);
        if (fs::exists(filename)) {
            std::cout << "DEBUG: Block ID " << blockId << " exists" << std::endl;
            ++count;
        } else {
            std::cout << "DEBUG: Block ID " << blockId << " does not exist" << std::endl;
        }
    }
    std::cout << "DEBUG: Found " << count << " existing blocks" << std::endl;
    return count;
}

int32_t get(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    std::lock_guard<std::mutex> lock(g_mutex);
    
    if (!g_initialized) {
        std::cout << "DEBUG: NDS not initialized" << std::endl;
        return -1;  // Not initialized
    }
    
    std::string filename = getBlockFilename(blockId);
    fs::path full_path = fs::absolute(filename);
    std::cout << "DEBUG: Getting data from file: " << full_path.string() << std::endl;
    std::cout << "DEBUG: Offset: " << offset << ", Length: " << len << std::endl;
    
    if (!fs::exists(filename)) {
        std::cout << "DEBUG: File does not exist" << std::endl;
        return -2;  // Block not found
    }
    
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        std::cout << "DEBUG: Failed to open file for reading" << std::endl;
        return -4;  // File open failed
    }
    
    // Get file size
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    std::cout << "DEBUG: File size: " << file_size << " bytes" << std::endl;
    file.seekg(0, std::ios::beg);
    
    // Check bounds
    if (offset + len > file_size) {
        std::cout << "DEBUG: Out of bounds - offset + len = " << (offset + len) << " > file size = " << file_size << std::endl;
        file.close();
        return -3;  // Out of bounds
    }
    
    // Seek to offset
    file.seekg(offset, std::ios::beg);
    if (!file.good()) {
        std::cout << "DEBUG: Seek failed" << std::endl;
        file.close();
        return -5;  // Seek failed
    }
    
    // Read data
    file.read(reinterpret_cast<char*>(blockAddr), len);
    if (!file.good()) {
        std::cout << "DEBUG: Read failed" << std::endl;
        file.close();
        return -6;  // Read failed
    }
    
    file.close();
    std::cout << "DEBUG: Successfully read data from file" << std::endl;
    return 0;  // Success
}

int32_t put(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    std::lock_guard<std::mutex> lock(g_mutex);
    
    if (!g_initialized) {
        std::cout << "DEBUG: NDS not initialized" << std::endl;
        return -1;  // Not initialized
    }
    
    std::string filename = getBlockFilename(blockId);
    fs::path full_path = fs::absolute(filename);
    std::cout << "DEBUG: Putting data to file: " << full_path.string() << std::endl;
    std::cout << "DEBUG: Offset: " << offset << ", Length: " << len << std::endl;
    
    // If file exists, read existing data
    std::vector<uint8_t> existing_data;
    size_t existing_size = 0;
    
    if (fs::exists(filename)) {
        std::cout << "DEBUG: File exists, reading existing data" << std::endl;
        std::ifstream file(filename, std::ios::binary);
        if (file.is_open()) {
            file.seekg(0, std::ios::end);
            existing_size = file.tellg();
            std::cout << "DEBUG: Existing file size: " << existing_size << " bytes" << std::endl;
            file.seekg(0, std::ios::beg);
            existing_data.resize(existing_size);
            file.read(reinterpret_cast<char*>(existing_data.data()), existing_size);
            file.close();
        }
    } else {
        std::cout << "DEBUG: File does not exist, creating new file" << std::endl;
    }
    
    // Calculate required size
    size_t required_size = std::max(existing_size, offset + len);
    std::cout << "DEBUG: Required size: " << required_size << " bytes" << std::endl;
    
    // Create new data buffer
    std::vector<uint8_t> new_data(required_size, 0);
    
    // Copy existing data
    if (existing_size > 0) {
        std::memcpy(new_data.data(), existing_data.data(), existing_size);
        std::cout << "DEBUG: Copied existing data" << std::endl;
    }
    
    // Copy new data
    std::memcpy(new_data.data() + offset, blockAddr, len);
    std::cout << "DEBUG: Copied new data" << std::endl;
    
    // Write to file
    std::ofstream file(filename, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
        std::cout << "DEBUG: Failed to open file for writing" << std::endl;
        return -4;  // File open failed
    }
    
    file.write(reinterpret_cast<char*>(new_data.data()), new_data.size());
    if (!file.good()) {
        std::cout << "DEBUG: Failed to write to file" << std::endl;
        file.close();
        return -7;  // Write failed
    }
    
    file.close();
    std::cout << "DEBUG: Successfully wrote data to file" << std::endl;
    return 0;  // Success
}

// C interface functions for linking
extern "C" {

int32_t NDS_init(void* addr, uint64_t len) {
    return NDS::init(addr, len);
}

int32_t NDS_isExists(uint64_t* blockIds, int32_t count) {
    std::vector<uint64_t> ids(blockIds, blockIds + count);
    return NDS::isExists(ids);
}

int32_t NDS_get(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    return NDS::get(blockId, blockAddr, offset, len);
}

int32_t NDS_put(uint64_t blockId, uint8_t* blockAddr, size_t offset, size_t len) {
    return NDS::put(blockId, blockAddr, offset, len);
}

} // extern "C"

} // namespace NDS
