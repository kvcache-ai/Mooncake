#include <gtest/gtest.h>

#include "kv_event/key_util.h"
#include "kv_event/kv_event_publisher.h"

namespace mooncake {
namespace {

TEST(KvEventKeyUtilTest, ParseSeqHashFromObjectKey) {
    EXPECT_EQ(ParseSeqHashFromObjectKey("12345"), 12345u);
    EXPECT_EQ(ParseSeqHashFromObjectKey("0x2a"), 42u);
    EXPECT_EQ(ParseSeqHashFromObjectKey("0XFF"), 255u);
    EXPECT_FALSE(ParseSeqHashFromObjectKey("").has_value());
    EXPECT_FALSE(ParseSeqHashFromObjectKey("not-a-hash").has_value());
    EXPECT_FALSE(ParseSeqHashFromObjectKey("123abc").has_value());
    EXPECT_EQ(KvEventPublisher::ParseSeqHashFromObjectKey("99"), 99u);
}

TEST(KvEventPublisherTest, DisabledPublisherIsNoop) {
    KvEventConfig config;
    config.enabled = false;
    KvEventPublisher publisher(config);
    EXPECT_FALSE(publisher.enabled());
    publisher.PublishStored("42", "cpu");
    publisher.PublishRemoved("42", "cpu");
    const auto stats = publisher.GetStats();
    EXPECT_EQ(stats.published_events, 0u);
    EXPECT_EQ(stats.dropped_events, 0u);
}

}  // namespace
}  // namespace mooncake
