#include "storage/distributed/kvcs/kvcs_efc_topology.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <optional>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

namespace mooncake {
namespace {

class ScopedEnv {
   public:
    explicit ScopedEnv(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) original_ = value;
        unsetenv(name);
    }
    ~ScopedEnv() {
        if (original_) setenv(name_.c_str(), original_->c_str(), 1);
        else unsetenv(name_.c_str());
    }
    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

TEST(KvcsEfcTopologyTest, MissingEnvironmentUsesBuiltInKvCacheStoreTarget) {
    ScopedEnv backend("KVCS_BACKEND");
    ScopedEnv extra_backends("KVCS_EXTRA_BACKENDS");
    ScopedEnv mountpoints("KVCS_MOUNTPOINTS_JSON");

    auto routes = LoadKvcsEfcTopology({});
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 1);
    EXPECT_EQ((*routes)[0].id, "kvcachestore-default");
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kKvCacheStore);
    EXPECT_EQ((*routes)[0].mountpoint_index, 1);
}

TEST(KvcsEfcTopologyTest, SocketPathIsNotParsedAsTopologyYaml) {
    ScopedEnv backend("KVCS_BACKEND");
    ScopedEnv extra_backends("KVCS_EXTRA_BACKENDS");
    ScopedEnv mountpoints("KVCS_MOUNTPOINTS_JSON");

    constexpr char kSocketPath[] = "/tmp/mooncake-kvcs-topology-test.sock";
    ::unlink(kSocketPath);
    const int socket_fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    ASSERT_GE(socket_fd, 0);
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    std::strncpy(address.sun_path, kSocketPath,
                 sizeof(address.sun_path) - 1);
    ASSERT_EQ(::bind(socket_fd, reinterpret_cast<sockaddr*>(&address),
                     sizeof(address)),
              0);

    auto routes = LoadKvcsEfcTopology(kSocketPath);
    ::close(socket_fd);
    ::unlink(kSocketPath);
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 1);
    EXPECT_EQ((*routes)[0].mountpoint_index, 1);
}

TEST(KvcsEfcTopologyTest, DirectoryTopologyPathIsRejected) {
    ScopedEnv backend("KVCS_BACKEND");
    ScopedEnv extra_backends("KVCS_EXTRA_BACKENDS");
    ScopedEnv mountpoints("KVCS_MOUNTPOINTS_JSON");

    constexpr char kDirectoryPath[] = "/tmp/mooncake-kvcs-topology-dir";
    ::rmdir(kDirectoryPath);
    ASSERT_EQ(::mkdir(kDirectoryPath, 0700), 0);
    auto routes = LoadKvcsEfcTopology(kDirectoryPath);
    ::rmdir(kDirectoryPath);
    ASSERT_FALSE(routes);
    EXPECT_EQ(routes.error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsEfcTopologyTest, MissingTopologyFileIsRejected) {
    ScopedEnv backend("KVCS_BACKEND");
    ScopedEnv extra_backends("KVCS_EXTRA_BACKENDS");
    ScopedEnv mountpoints("KVCS_MOUNTPOINTS_JSON");

    constexpr char kMissingPath[] =
        "/tmp/mooncake-kvcs-topology-test-missing.yaml";
    ::unlink(kMissingPath);
    auto routes = LoadKvcsEfcTopology(kMissingPath);
    ASSERT_FALSE(routes);
    EXPECT_EQ(routes.error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsEfcTopologyTest, ExplicitLocalBackendOverridesG35Default) {
    ScopedEnv backend("KVCS_BACKEND");
    ScopedEnv extra_backends("KVCS_EXTRA_BACKENDS");
    ScopedEnv mountpoints("KVCS_MOUNTPOINTS_JSON");
    backend.Set("disk");

    auto routes = LoadKvcsEfcTopology({});
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 1);
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kLocal);
    EXPECT_EQ((*routes)[0].mountpoint_index, 0);
}

TEST(KvcsEfcTopologyTest, ExplicitMountpointsStillOverrideDefaults) {
    ScopedEnv backend("KVCS_BACKEND");
    ScopedEnv extra_backends("KVCS_EXTRA_BACKENDS");
    ScopedEnv mountpoints("KVCS_MOUNTPOINTS_JSON");
    backend.Set("disk");
    extra_backends.Set("kvcachestore");
    mountpoints.Set("{\"mountPoints\":[{\"mountPointID\":\"remote-a\",\"subpath\":\"/sub\"}]}");

    auto routes = LoadKvcsEfcTopology({});
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 2);
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kLocal);
    EXPECT_EQ((*routes)[0].mountpoint_index, 0);
    EXPECT_EQ((*routes)[1].id, "remote-a/sub");
    EXPECT_EQ((*routes)[1].kind, KvcsEfcRouteKind::kKvCacheStore);
    EXPECT_EQ((*routes)[1].mountpoint_index, 1);
}

TEST(KvcsEfcTopologyTest, LocalBackendIsOneEfcOwnedRoute) {
    auto routes = ResolveKvcsEfcTopology({.backend = "disk"});
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 1);
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kLocal);
    EXPECT_EQ((*routes)[0].mountpoint_index, 0);
}

TEST(KvcsEfcTopologyTest, MixedPoolExposesLocalAndRemoteRoutes) {
    KvcsEfcDeployment deployment{
        .backend = "disk",
        .extra_backends = {"kvcachestore"},
        .mountpoints = {{.id = "remote-a", .index = 1}},
    };
    auto routes = ResolveKvcsEfcTopology(deployment);
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 2);
    EXPECT_EQ((*routes)[0].id, "local");
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kLocal);
    EXPECT_EQ((*routes)[0].mountpoint_index, 0);
    EXPECT_EQ((*routes)[1].id, "remote-a");
    EXPECT_EQ((*routes)[1].kind, KvcsEfcRouteKind::kKvCacheStore);
    EXPECT_EQ((*routes)[1].mountpoint_index, 1);
}

TEST(KvcsEfcTopologyTest, DirectKvCacheStoreExposesEveryMountpoint) {
    KvcsEfcDeployment deployment{
        .backend = "kvcachestore",
        .mountpoints = {{.id = "remote-a", .index = 1, .is_default = true},
                        {.id = "remote-b", .index = 2}},
        .require_single_default = true,
    };
    auto routes = ResolveKvcsEfcTopology(deployment);
    ASSERT_TRUE(routes);
    ASSERT_EQ(routes->size(), 2);
    EXPECT_EQ((*routes)[0].kind, KvcsEfcRouteKind::kKvCacheStore);
    EXPECT_EQ((*routes)[0].mountpoint_index, 1);
    EXPECT_EQ((*routes)[1].mountpoint_index, 2);
}

TEST(KvcsEfcTopologyTest, DisklessWithoutKvCacheStoreIsRejected) {
    auto routes = ResolveKvcsEfcTopology({.backend = "diskless"});
    ASSERT_FALSE(routes);
    EXPECT_EQ(routes.error(), ErrorCode::INVALID_PARAMS);
}

TEST(KvcsEfcTopologyTest, DirectMountpointsMustBeUnique) {
    KvcsEfcDeployment deployment{
        .backend = "kvcachestore",
        .mountpoints = {{.id = "remote-a", .index = 1, .is_default = true},
                        {.id = "remote-a", .index = 2}},
        .require_single_default = true,
    };
    EXPECT_FALSE(ResolveKvcsEfcTopology(deployment));
}

}  // namespace
}  // namespace mooncake
