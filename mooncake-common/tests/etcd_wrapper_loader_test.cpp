#include "etcd_wrapper_loader.h"

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

namespace mooncake::etcd_wrapper {
namespace {

TEST(EtcdWrapperLoaderTest, DoesNotLoadUntilInvoked) {
    EXPECT_FALSE(IsEtcdWrapperLoaded());
    EXPECT_EQ(dlopen("libetcd_wrapper.so", RTLD_LAZY | RTLD_NOLOAD), nullptr);

    // Closing before any etcd call must be a no-op and must not dlopen().
    EtcdCloseWrapper();
    EXPECT_FALSE(IsEtcdWrapperLoaded());
    EXPECT_EQ(dlopen("libetcd_wrapper.so", RTLD_LAZY | RTLD_NOLOAD), nullptr);
}

TEST(EtcdWrapperLoaderTest, MissingLibraryReturnsErrorWithoutCrash) {
    ASSERT_EQ(setenv("MOONCAKE_ETCD_WRAPPER_PATH",
                     "/nonexistent/path/libetcd_wrapper.so", 1),
              0);

    char* err_msg = nullptr;
    std::string endpoint = "127.0.0.1:2379";
    const GoInt ret = NewEtcdClient(endpoint.data(), &err_msg);
    EXPECT_EQ(ret, -1);
    ASSERT_NE(err_msg, nullptr);
    EXPECT_NE(std::string(err_msg).find("failed to load"), std::string::npos);
    free(err_msg);
    EXPECT_FALSE(IsEtcdWrapperLoaded());

    unsetenv("MOONCAKE_ETCD_WRAPPER_PATH");
}

TEST(EtcdWrapperLoaderTest, LoadsAndInvokesSymbolsOnDemand) {
    ASSERT_EQ(setenv("MOONCAKE_ETCD_WRAPPER_PATH",
                     MOONCAKE_MOCK_ETCD_WRAPPER_PATH, 1),
              0);

    char* err_msg = nullptr;
    std::string endpoint = "127.0.0.1:2379";
    ASSERT_EQ(NewEtcdClient(endpoint.data(), &err_msg), 0);
    EXPECT_EQ(err_msg, nullptr);
    EXPECT_TRUE(IsEtcdWrapperLoaded());

    std::string key = "k";
    char* val = nullptr;
    ASSERT_EQ(EtcdGetWrapper(key.data(), &val, &err_msg), 0);
    ASSERT_NE(val, nullptr);
    EXPECT_STREQ(val, "mock-value");
    free(val);

    EXPECT_EQ(EtcdStoreWaitKeepAliveReadyWrapper(42, 250, &err_msg), 0);
    EtcdCloseWrapper();

    void* handle =
        dlopen(MOONCAKE_MOCK_ETCD_WRAPPER_PATH, RTLD_LAZY | RTLD_NOLOAD);
    ASSERT_NE(handle, nullptr);
    auto* close_calls =
        reinterpret_cast<int (*)()>(dlsym(handle, "MockEtcdCloseCalls"));
    ASSERT_NE(close_calls, nullptr);
    EXPECT_EQ(close_calls(), 1);

    unsetenv("MOONCAKE_ETCD_WRAPPER_PATH");
}

}  // namespace
}  // namespace mooncake::etcd_wrapper
