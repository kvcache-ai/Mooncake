#include <iostream>
#include <cstring>
#include <cassert>
#include "nds_mock_test_utils.h"

using namespace NDS;

void testInit() {
    std::cout << "Testing NDS::init..." << std::endl;
    
    int32_t result = NDS::init(nullptr, 0);
    assert(result == 0);
    std::cout << "  ✓ Init successful" << std::endl;
}

void testPutAndGet() {
    std::cout << "Testing NDS::put and NDS::get..." << std::endl;
    
    // Initialize
    NDS::init(nullptr, 0);
    NDS::clear();
    
    // Test data
    uint64_t blockId = 12345;
    const char* testData = "Hello, NDS Mock!";
    size_t dataLen = strlen(testData);
    
    // Put data
    int32_t putResult = NDS::put(blockId, 
        reinterpret_cast<uint8_t*>(const_cast<char*>(testData)), 0, dataLen);
    assert(putResult == 0);
    std::cout << "  ✓ Put successful" << std::endl;
    
    // Verify block exists
    assert(NDS::hasBlock(blockId));
    std::cout << "  ✓ Block exists check passed" << std::endl;
    
    // Get data
    char readBuffer[256] = {0};
    int32_t getResult = NDS::get(blockId, 
        reinterpret_cast<uint8_t*>(readBuffer), 0, dataLen);
    assert(getResult == 0);
    assert(strcmp(readBuffer, testData) == 0);
    std::cout << "  ✓ Get successful, data matches" << std::endl;
}

void testIsExists() {
    std::cout << "Testing NDS::isExists..." << std::endl;
    
    // Initialize and clear
    NDS::init(nullptr, 0);
    NDS::clear();
    
    // Create some test data
    std::vector<uint64_t> blockIds = {1001, 1002, 1003, 1004, 1005};
    
    // Add only some blocks (1001, 1003, 1005)
    for (size_t i = 0; i < blockIds.size(); i += 2) {
        uint8_t dummyData = static_cast<uint8_t>(blockIds[i] & 0xFF);
        NDS::put(blockIds[i], &dummyData, 0, 1);
    }
    
    // Check existence
    int32_t existCount = NDS::isExists(blockIds);
    assert(existCount == 3);  // Should find 1001, 1003, 1005
    std::cout << "  ✓ isExists returned correct count: " << existCount << std::endl;
    
    // Test empty list
    std::vector<uint64_t> emptyList;
    int32_t emptyCount = NDS::isExists(emptyList);
    assert(emptyCount == 0);
    std::cout << "  ✓ Empty list returns 0" << std::endl;
}

void testErrorCases() {
    std::cout << "Testing error cases..." << std::endl;
    
    // Initialize
    NDS::init(nullptr, 0);
    NDS::clear();
    
    uint64_t nonExistentBlock = 99999;
    char buffer[256] = {0};
    
    // Try to get non-existent block
    int32_t result = NDS::get(nonExistentBlock, 
        reinterpret_cast<uint8_t*>(buffer), 0, 100);
    assert(result == -2);  // Block not found
    std::cout << "  ✓ Get non-existent block returns error" << std::endl;
    
    // Test out of bounds read
    const char* testData = "Hello";
    uint64_t blockId = 123;
    NDS::put(blockId, 
        reinterpret_cast<uint8_t*>(const_cast<char*>(testData)), 0, 5);
    
    result = NDS::get(blockId, 
        reinterpret_cast<uint8_t*>(buffer), 0, 100);  // Read more than available
    assert(result == -3);  // Out of bounds
    std::cout << "  ✓ Out of bounds read returns error" << std::endl;
}

void testClear() {
    std::cout << "Testing clear functionality..." << std::endl;
    
    NDS::init(nullptr, 0);
    NDS::clear();
    
    // Add some data
    uint8_t data = 42;
    NDS::put(100, &data, 0, 1);
    NDS::put(200, &data, 0, 1);
    
    assert(NDS::size() == 2);
    std::cout << "  ✓ Size is 2 after adding data" << std::endl;
    
    // Clear
    NDS::clear();
    
    assert(NDS::size() == 0);
    assert(!NDS::hasBlock(100));
    std::cout << "  ✓ All data cleared successfully" << std::endl;
}

int main() {
    std::cout << "========================================" << std::endl;
    std::cout << "  NDS Mock Library Test Suite" << std::endl;
    std::cout << "========================================" << std::endl << std::endl;
    
    try {
        testInit();
        std::cout << std::endl;
        
        testPutAndGet();
        std::cout << std::endl;
        
        testIsExists();
        std::cout << std::endl;
        
        testErrorCases();
        std::cout << std::endl;
        
        testClear();
        std::cout << std::endl;
        
        std::cout << "========================================" << std::endl;
        std::cout << "  All tests passed!" << std::endl;
        std::cout << "========================================" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
