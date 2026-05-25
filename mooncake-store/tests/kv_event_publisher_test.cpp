#include <gtest/gtest.h>

#include "kv_event/kv_event_publisher.h"

namespace mooncake {
namespace {

TEST(KvEventPublisherTest, ParseSeqHashFromObjectKey) {
    EXPECT_EQ(KvEventPublisher::ParseSeqHashFromObjectKey("12345"), 12345u);
    EXPECT_EQ(KvEventPublisher::ParseSeqHashFromObjectKey("0x2a"), 42u);
    EXPECT_EQ(KvEventPublisher::ParseSeqHashFromObjectKey("0XFF"), 255u);
    EXPECT_FALSE(KvEventPublisher::ParseSeqHashFromObjectKey("").has_value());
    EXPECT_FALSE(
        KvEventPublisher::ParseSeqHashFromObjectKey("not-a-hash").has_value());
    EXPECT_FALSE(
        KvEventPublisher::ParseSeqHashFromObjectKey("123abc").has_value());
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
