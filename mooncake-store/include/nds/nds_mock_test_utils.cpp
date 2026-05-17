#include "nds_interface.h"
#include <cstring>
#include <algorithm>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <mutex>

namespace NDS {

namespace fs = std::filesystem;

// Global variables from nds_mock.cpp
extern std::mutex g_mutex;
extern bool g_initialized;

// Helper method from nds_mock.cpp
extern std::string getBlockFilename(uint64_t blockId);

// Helper methods for testing
void clear() {
    std::lock_guard<std::mutex> lock(g_mutex);
    
    std::cout << "DEBUG: Clearing all NDS files" << std::endl;
    
    // Check if kv_data directory exists
    fs::path kv_dir = fs::current_path() / "kv_data";
    if (fs::exists(kv_dir)) {
        // Iterate through all files in kv_data directory
        for (const auto& entry : fs::directory_iterator(kv_dir)) {
            if (entry.is_regular_file()) {
                // Check if filename is a valid uint64_t
                try {
                    uint64_t blockId = std::stoull(entry.path().filename().string());
                    // If it's a valid number, delete it
                    std::cout << "DEBUG: Deleting file: " << entry.path().string() << std::endl;
                    fs::remove(entry.path());
                } catch (...) {
                    // Not a number, skip
                }
            }
        }
    } else {
        std::cout << "DEBUG: kv_data directory does not exist" << std::endl;
    }
    
    std::cout << "DEBUG: Clear completed" << std::endl;
}

size_t size() {
    std::lock_guard<std::mutex> lock(g_mutex);
    
    size_t count = 0;
    // Check if kv_data directory exists
    fs::path kv_dir = fs::current_path() / "kv_data";
    if (fs::exists(kv_dir)) {
        // Iterate through all files in kv_data directory
        for (const auto& entry : fs::directory_iterator(kv_dir)) {
            if (entry.is_regular_file()) {
                // Check if filename is a valid uint64_t
                try {
                    std::stoull(entry.path().filename().string());
                    count++;
                } catch (...) {
                    // Not a number, skip
                }
            }
        }
    }
    std::cout << "DEBUG: Current NDS file count: " << count << std::endl;
    return count;
}

bool hasBlock(uint64_t blockId) {
    std::lock_guard<std::mutex> lock(g_mutex);
    std::string filename = getBlockFilename(blockId);
    bool exists = fs::exists(filename);
    std::cout << "DEBUG: Block ID " << blockId << " exists: " << (exists ? "YES" : "NO") << std::endl;
    return exists;
}

} // namespace NDS
