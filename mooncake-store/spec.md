# GDS KV Integration Specification

## 1. Overview

This specification outlines the integration of GDS KV storage into the Mooncake project. The goal is to replace the current file-based storage mechanism with GDS KV storage for object persistence.

## 2. GDS KV Interface

```cpp
namespace GDS {
    int32_t init(void* addr, uint64_t len);
    int32_t isExists(std::vector<uint64_t> blockIds); // Returns the count of existing blockIds in the input vector
    int32_t get(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t len);
    int32_t put(uint64_t blockId, uint8_t *blockAddr, size_t offset, size_t len);
}
```

## 3. Integration Points

### 3.1 ObjectKey to uint64_t Conversion

- **Function**: `objectKeyToUint64(const ObjectKey& key)`
- **Location**: `include/types.h`
- **Description**: Converts ObjectKey (std::string) to uint64_t using std::hash function
- **Return**: uint64_t hash value of the key

### 3.2 Put Operation

**Current Flow**:
```
Client::Put → TransferWrite → TransferData → TransferSubmitter::submit → StorageBackend::StoreObject
```

**New Flow**:
```
Client::Put → objectKeyToUint64 → GDS::put
```

**Changes to be made**:
1. Modify `Client::Put` in `src/client.cpp` to use GDS::put instead of file storage
2. Convert ObjectKey to uint64_t using `objectKeyToUint64`
3. Handle GDS::put return codes appropriately

### 3.3 Read Operation

**Current Flow**:
```
Client::Get → Query → Client::Get (overloaded) → TransferRead → TransferData → TransferSubmitter::submit → submitFileReadOperation → FilereadWorkerPool → StorageBackend::LoadObject
```

**New Flow**:
```
Client::Get → Query → Client::Get (overloaded) → TransferRead → objectKeyToUint64 → GDS::get
```

**Changes to be made**:
1. Modify `Client::TransferRead` in `src/client.cpp` to use GDS::get instead of file reading
2. Convert ObjectKey to uint64_t using `objectKeyToUint64`
3. Handle GDS::get return codes appropriately

### 3.4 Batch Operations

- Modify `Client::BatchPut` to use GDS::put for each key-value pair
- Modify `Client::BatchGet` to use GDS::get for each key-value pair
- Convert ObjectKey to uint64_t using `objectKeyToUint64` for each key

## 4. Build System Changes

### 4.1 CMake Configuration

- Add `gds_interface.h` to include directories
- Link with `libgdsclient.so` shared library
- Update CMakeLists.txt files accordingly

### 4.2 File Location

- Place `gds_interface.h` in an appropriate include directory (e.g., `include/gds/`)
- Ensure `libgdsclient.so` is accessible to the linker

## 5. Error Handling

- Map GDS error codes to Mooncake ErrorCode enum
- Handle cases where blockId does not exist
- Handle cases where GDS operations fail

## 6. Testing

### 6.1 Test Scenarios

**Normal Case**:
- Write data using Client::Put
- Read data using Client::Get
- Verify data integrity

**Exception Case**:
- Attempt to read non-existent blockId
- Verify appropriate error is returned

### 6.2 Test Files

- Create test cases to verify GDS integration
- Update existing tests to work with GDS storage

## 7. Dependencies

- GDS KV library (libgdsclient.so)
- GDS interface header (gds_interface.h)

## 8. Performance Considerations

- Ensure efficient conversion between ObjectKey and uint64_t
- Handle GDS operations efficiently
- Consider batch operations for improved performance
