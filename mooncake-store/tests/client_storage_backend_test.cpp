// Regression tests for Client::PrepareStorageBackend (issue #3134): an
// invalid storage configuration must surface as an error instead of
// dereferencing a null backend or leaving a half-initialized one behind.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>

#include "client_service.h"

namespace mooncake {
namespace {

class TestableClient : public Client {
   public:
    TestableClient()
        : Client(/*local_hostname=*/"localhost:9003",
                 /*metadata_connstring=*/"",
                 /*protocol=*/"tcp",
                 /*labels=*/{}) {}

    using Client::PrepareStorageBackend;
};

TEST(ClientPrepareStorageBackendTest, InvalidRootDirReturnsErrorWithoutCrash) {
    TestableClient client;
    // Before the fix this dereferenced a null StorageBackend and crashed.
    ErrorCode err = client.PrepareStorageBackend(
        "/nonexistent_mooncake_store_test_path/12345", "fsdir", true, 0);
    EXPECT_NE(err, ErrorCode::OK);
}

TEST(ClientPrepareStorageBackendTest, EmptyFsdirReturnsErrorWithoutCrash) {
    TestableClient client;
    ErrorCode err = client.PrepareStorageBackend(
        std::filesystem::current_path().string(), "", true, 0);
    EXPECT_NE(err, ErrorCode::OK);
}

TEST(ClientPrepareStorageBackendTest, ValidRootDirSucceeds) {
    std::string root = std::filesystem::current_path().string() +
                       "/data/client_prepare_storage_backend_test";
    std::filesystem::create_directories(root);

    TestableClient client;
    EXPECT_EQ(client.PrepareStorageBackend(root, "fsdir", true, 0),
              ErrorCode::OK);

    std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace mooncake
