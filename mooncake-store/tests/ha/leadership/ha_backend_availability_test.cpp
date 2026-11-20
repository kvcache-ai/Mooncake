#include "client_master_address.h"

#include <gtest/gtest.h>

#include <utility>

namespace mooncake {

namespace ha {
namespace {

TEST(HABackendAvailabilityTest, UnknownBackendIsInvalid) {
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              ValidateHABackendAvailability(HABackendType::UNKNOWN));
}

TEST(HABackendAvailabilityTest, EtcdAvailabilityMatchesBuildFlag) {
#ifdef STORE_USE_ETCD
    EXPECT_EQ(ErrorCode::OK,
              ValidateHABackendAvailability(HABackendType::ETCD));
#else
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHABackendAvailability(HABackendType::ETCD));
#endif
}

TEST(HABackendAvailabilityTest, RedisAvailabilityMatchesBuildFlag) {
#ifdef STORE_USE_REDIS
    EXPECT_EQ(ErrorCode::OK,
              ValidateHABackendAvailability(HABackendType::REDIS));
#else
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHABackendAvailability(HABackendType::REDIS));
#endif
}

TEST(HABackendAvailabilityTest, K8sAvailabilityMatchesBuildFlag) {
#ifdef STORE_USE_K8S_LEASE
    EXPECT_EQ(ErrorCode::OK, ValidateHABackendAvailability(HABackendType::K8S));
#else
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              ValidateHABackendAvailability(HABackendType::K8S));
#endif
}

TEST(HABackendAvailabilityTest, ClientSpecParsingMatchesK8sBuildFlag) {
    auto spec = ParseHABackendSpec("k8s://default/master");
#ifdef STORE_USE_K8S_LEASE
    ASSERT_TRUE(spec.has_value());
    ASSERT_TRUE(spec->has_value());
    EXPECT_EQ(HABackendType::K8S, (*spec)->type);
    EXPECT_EQ("default/master", (*spec)->connstring);
#else
    ASSERT_FALSE(spec.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, spec.error());
#endif
}

TEST(HABackendAvailabilityTest, RedisDiscoveryPreservesConnectionSemantics) {
    const std::pair<const char*, const char*> cases[] = {
        {"redis://127.0.0.1:6379", "127.0.0.1:6379"},
        {"redis+cluster://127.0.0.1:6379,127.0.0.2:6379",
         "redis+cluster://127.0.0.1:6379,127.0.0.2:6379"},
    };
    for (const auto& [entry, expected_connstring] : cases) {
        SCOPED_TRACE(entry);
        auto spec = ParseHABackendSpec(entry);
#ifdef STORE_USE_REDIS
        ASSERT_TRUE(spec.has_value());
        ASSERT_TRUE(spec->has_value());
        EXPECT_EQ(HABackendType::REDIS, (*spec)->type);
        EXPECT_EQ(expected_connstring, (*spec)->connstring);
        EXPECT_TRUE((*spec)->redis_read_only);
#else
        ASSERT_FALSE(spec.has_value());
        EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, spec.error());
#endif
    }
}

TEST(HABackendAvailabilityTest, ExplicitEtcdSchemeNormalizesEndpoints) {
#ifdef STORE_USE_ETCD
    const auto expect_etcd = [](const std::string& entry,
                                const std::string& connstring) {
        auto spec = ParseHABackendSpec(entry);
        ASSERT_TRUE(spec.has_value()) << entry;
        ASSERT_TRUE(spec->has_value()) << entry;
        EXPECT_EQ(HABackendType::ETCD, (*spec)->type);
        EXPECT_EQ(connstring, (*spec)->connstring);
    };

    expect_etcd("etcd://127.0.0.1:2379", "127.0.0.1:2379");
    expect_etcd("etcd://https://127.0.0.1:2379", "https://127.0.0.1:2379");
    expect_etcd("etcd://http://127.0.0.1:2379/", "http://127.0.0.1:2379/");
    expect_etcd("etcd://https://127.0.0.1:2379/", "https://127.0.0.1:2379/");

#else
    auto spec = ParseHABackendSpec("etcd://127.0.0.1:2379");
    ASSERT_FALSE(spec.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, spec.error());
#endif
}

TEST(HABackendAvailabilityTest, BareHostPortRemainsDirectMasterAddress) {
    for (const auto& entry : {"127.0.0.1:50051", "127.0.0.1:2379"}) {
        auto spec = ParseHABackendSpec(entry);
        ASSERT_TRUE(spec.has_value()) << entry;
        EXPECT_FALSE(spec->has_value()) << entry;
    }
}

TEST(HABackendAvailabilityTest, EtcdUnixEndpointsPreserveConnectionSemantics) {
    for (const auto& connstring :
         {"unix:///run/etcd.sock", "unixs:///run/etcd.sock", "unix://etcd.sock",
          "unixs://etcd.sock", "unix://run/etcd.sock", "unixs://run/etcd.sock",
          "unix:etcd.sock", "unixs:etcd.sock", "unix:/run/etcd.sock",
          "unixs:/run/etcd.sock"}) {
        SCOPED_TRACE(connstring);
        auto spec = ParseHABackendSpec(std::string("etcd://") + connstring);
#ifdef STORE_USE_ETCD
        ASSERT_TRUE(spec.has_value());
        ASSERT_TRUE(spec->has_value());
        EXPECT_EQ(HABackendType::ETCD, (*spec)->type);
        EXPECT_EQ(connstring, (*spec)->connstring);
#else
        ASSERT_FALSE(spec.has_value());
        EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, spec.error());
#endif
    }
}

TEST(HABackendAvailabilityTest, EtcdUnixAndTcpEndpointListsAreAccepted) {
    const std::pair<const char*, const char*> cases[] = {
        {"etcd://unix:///run/etcd.sock;http://127.0.0.1:2379/",
         "unix:///run/etcd.sock;http://127.0.0.1:2379/"},
        {"etcd://https://127.0.0.1:2379/,unixs:///run/etcd.sock",
         "https://127.0.0.1:2379/,unixs:///run/etcd.sock"},
        {"etcd:// unix://etcd.sock ; 127.0.0.1:2379 ",
         " unix://etcd.sock ; 127.0.0.1:2379"},
    };
    for (const auto& [entry, expected_connstring] : cases) {
        SCOPED_TRACE(entry);
        auto spec = ParseHABackendSpec(entry);
#ifdef STORE_USE_ETCD
        ASSERT_TRUE(spec.has_value());
        ASSERT_TRUE(spec->has_value());
        EXPECT_EQ(HABackendType::ETCD, (*spec)->type);
        EXPECT_EQ(expected_connstring, (*spec)->connstring);
#else
        ASSERT_FALSE(spec.has_value());
        EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE, spec.error());
#endif
    }
}

TEST(HABackendAvailabilityTest, MalformedEtcdUnixEndpointIsRejected) {
    for (const auto& entry :
         {"etcd://unix:", "etcd://unixs:", "etcd://unix://", "etcd://unixs://",
          "etcd://unix:///", "etcd://unixs:///", "etcd://unix:///run/",
          "etcd://unixs:///run/", "etcd://unix:///run/etcd.sock;",
          "etcd://unix+http:///run/etcd.sock"}) {
        SCOPED_TRACE(entry);
        auto spec = ParseHABackendSpec(entry);
        ASSERT_FALSE(spec.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, spec.error());
    }
}

TEST(HABackendAvailabilityTest, EtcdRequiresExplicitScheme) {
    for (const auto& entry :
         {"127.0.0.1:2379;127.0.0.2:2379", "127.0.0.1:2379,127.0.0.2:2379",
          "https://127.0.0.1:2379", "unix:///run/etcd.sock",
          "unixs:///run/etcd.sock"}) {
        auto spec = ParseHABackendSpec(entry);
        ASSERT_FALSE(spec.has_value()) << entry;
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, spec.error()) << entry;
    }
}

TEST(HABackendAvailabilityTest, MalformedBareEndpointListIsRejected) {
    auto spec = ParseHABackendSpec("127.0.0.1:2379;;127.0.0.2:2379");
    ASSERT_FALSE(spec.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, spec.error());
}

TEST(HABackendAvailabilityTest, MalformedEtcdEndpointIsRejected) {
    for (const auto& entry :
         {"etcd://", "etcd://127.0.0.1:70000", "etcd://127.0.0.1:2379;;"}) {
        auto spec = ParseHABackendSpec(entry);
        ASSERT_FALSE(spec.has_value()) << entry;
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, spec.error()) << entry;
    }
}

}  // namespace
}  // namespace ha
}  // namespace mooncake
