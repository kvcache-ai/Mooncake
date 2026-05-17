#include <iostream>
#include <string>
#include "types.h"

int main() {
    // Test conversion from ObjectKey to uint64_t
    mooncake::ObjectKey key1 = "test_key_123";
    uint64_t value1 = mooncake::objectKeyToUint64(key1);
    
    std::cout << "ObjectKey: " << key1 << " -> uint64_t: " << value1 << std::endl;
    
    // Test conversion from uint64_t to ObjectKey
    uint64_t value2 = 0x123456789ABCDEF0;
    mooncake::ObjectKey key2 = mooncake::uint64ToObjectKey(value2);
    
    std::cout << "uint64_t: 0x" << std::hex << value2 << " -> ObjectKey: " << key2 << std::endl;
    
    // Test round-trip conversion (note: this may not be the same due to hash collision possibility)
    mooncake::ObjectKey roundTripKey = mooncake::uint64ToObjectKey(value1);
    uint64_t roundTripValue = mooncake::objectKeyToUint64(key2);
    
    std::cout << "Round-trip: " << key1 << " -> " << value1 << " -> " << roundTripKey << std::endl;
    std::cout << "Round-trip: 0x" << std::hex << value2 << " -> " << key2 << " -> 0x" << roundTripValue << std::endl;
    
    return 0;
}