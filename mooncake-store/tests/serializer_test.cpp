#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>

#include "serializer.h"
#include "serialize/serializer.h"
#include "segment.h"

namespace mooncake::test {

// Example class implementing serialization following the usage documentation
class ExampleClass {
   public:
    ExampleClass() : id_(0), value_(0.0), name_("") {}

    ExampleClass(int id, double value, const std::string& name)
        : id_(id), value_(value), name_(name) {}

    // Serialization method (works with both counter and writer)
    template <typename T>
    void serialize_to(T& serializer) const {
        serializer.write(&id_, sizeof(id_));
        serializer.write(&value_, sizeof(value_));

        // Serialize string length first, then string data
        size_t name_length = name_.length();
        serializer.write(&name_length, sizeof(name_length));
        if (!name_.empty()) {
            serializer.write(name_.data(), name_.length());
        }
    }

    // Deserialization method
    template <typename T>
    static std::shared_ptr<ExampleClass> deserialize_from(T& serializer) {
        try {
            auto obj = std::make_shared<ExampleClass>();

            // Deserialize basic members
            serializer.read(&obj->id_, sizeof(obj->id_));
            serializer.read(&obj->value_, sizeof(obj->value_));

            // Deserialize string
            size_t name_length;
            serializer.read(&name_length, sizeof(name_length));
            if (name_length > 0) {
                obj->name_.resize(name_length);
                serializer.read(&obj->name_[0], name_length);
            }

            return obj;
        } catch (const std::exception& e) {
            return nullptr;
        }
    }

    // Getters for testing
    int getId() const { return id_; }
    double getValue() const { return value_; }
    const std::string& getName() const { return name_; }

    // Equality operator for testing
    bool operator==(const ExampleClass& other) const {
        return id_ == other.id_ && value_ == other.value_ &&
               name_ == other.name_;
    }

   protected:
    int id_;
    double value_;
    std::string name_;
};

class ExampleClassWithException {
   public:
    ExampleClassWithException() : value_(0) {}
    ExampleClassWithException(int value) : value_(value) {}

    template <typename T>
    void serialize_to(T& serializer) const {
        serializer.write(&value_, sizeof(value_));
    }

    template <typename T>
    static std::shared_ptr<ExampleClassWithException> deserialize_from(
        T& serializer) {
        throw std::runtime_error("throw_exception");
    }

   private:
    int value_;
};

class SerializerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("SerializerTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(SerializerTest, ExampleClassSerialization) {
    // Create an example object
    ExampleClass original(42, 3.14159, "Test Object");

    // Test serialization
    std::vector<SerializedByte> buffer;
    ASSERT_EQ(serialize_to(original, buffer), ErrorCode::OK);
    ASSERT_FALSE(buffer.empty());

    // Test deserialization
    auto restored = deserialize_from<ExampleClass>(buffer);
    ASSERT_NE(restored, nullptr);

    // Verify the deserialized object matches the original
    EXPECT_EQ(restored->getId(), original.getId());
    EXPECT_DOUBLE_EQ(restored->getValue(), original.getValue());
    EXPECT_EQ(restored->getName(), original.getName());
    EXPECT_TRUE(*restored == original);
}

TEST_F(SerializerTest, ExampleClassSerializationWithSharedPtr) {
    // Test with shared_ptr
    auto original =
        std::make_shared<ExampleClass>(777, 2.718, "Shared Pointer Test");

    std::vector<SerializedByte> buffer;
    ASSERT_EQ(serialize_to(original, buffer), ErrorCode::OK);

    auto restored = deserialize_from<ExampleClass>(buffer);
    ASSERT_NE(restored, nullptr);
    EXPECT_TRUE(*restored == *original);
}

TEST_F(SerializerTest, ExampleClassSerializationNullPointer) {
    // Test with null shared_ptr
    std::shared_ptr<ExampleClass> null_ptr = nullptr;

    std::vector<SerializedByte> buffer;
    ASSERT_EQ(serialize_to(null_ptr, buffer), ErrorCode::INVALID_PARAMS);
}

TEST_F(SerializerTest, ExampleClassDeserializationCorruptedBuffer) {
    // Create a valid object and serialize it
    ExampleClass original(1, 1.0, "Test");
    std::vector<SerializedByte> buffer;
    ASSERT_EQ(serialize_to(original, buffer), ErrorCode::OK);

    // Corrupt the buffer by removing the last byte
    buffer.pop_back();

    // Try to deserialize corrupted buffer
    auto restored = deserialize_from<ExampleClass>(buffer);
    EXPECT_EQ(restored, nullptr);
}

TEST_F(SerializerTest, ExampleClassDeserializationWithException) {
    // Create a valid object and serialize it
    ExampleClassWithException original(1);
    std::vector<SerializedByte> buffer;
    ASSERT_EQ(serialize_to(original, buffer), ErrorCode::OK);

    // Try to deserialize the buffer, the deserialization method will throw an
    // exception.
    auto restored = deserialize_from<ExampleClassWithException>(buffer);
    EXPECT_EQ(restored, nullptr);
}

TEST_F(SerializerTest, MountedSegmentSerializationPreservesHostId) {
    MountedSegment original;
    original.segment.id = generate_uuid();
    original.segment.name = "segment_host1";
    original.segment.base = 0x300000000;
    original.segment.size = 1024 * 1024;
    original.segment.te_endpoint = "segment_host1";
    original.segment.host_id = "host1";
    original.status = SegmentStatus::OK;

    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    ASSERT_TRUE(
        Serializer<MountedSegment>::serialize(original, packer).has_value());

    auto object_handle = msgpack::unpack(buffer.data(), buffer.size());
    auto restored =
        Serializer<MountedSegment>::deserialize(object_handle.get());
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->segment.id, original.segment.id);
    EXPECT_EQ(restored->segment.name, original.segment.name);
    EXPECT_EQ(restored->segment.host_id, original.segment.host_id);
    EXPECT_EQ(restored->status, original.status);
}

TEST_F(SerializerTest, MountedSegmentDeserializesLegacyFormatWithoutHostId) {
    const UUID segment_id = generate_uuid();

    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    packer.pack_array(8);
    packer.pack(UuidToString(segment_id));
    packer.pack(std::string("legacy_segment"));
    packer.pack(static_cast<uint64_t>(0x300000000));
    packer.pack(static_cast<uint64_t>(1024 * 1024));
    packer.pack(std::string("legacy_segment"));
    packer.pack(static_cast<int16_t>(SegmentStatus::OK));
    packer.pack(false);
    packer.pack_nil();

    auto object_handle = msgpack::unpack(buffer.data(), buffer.size());
    auto restored =
        Serializer<MountedSegment>::deserialize(object_handle.get());
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->segment.id, segment_id);
    EXPECT_EQ(restored->segment.name, "legacy_segment");
    EXPECT_TRUE(restored->segment.host_id.empty());
    EXPECT_EQ(restored->status, SegmentStatus::OK);
}

TEST_F(SerializerTest, RestoredReplicaTracksRestoredSegmentLifetime) {
    SegmentManager source_manager(BufferAllocatorType::OFFSET);
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "snapshot_segment";
    segment.base = 0x300000000;
    segment.size = 16 * 1024 * 1024;
    segment.te_endpoint = segment.name;
    const UUID client_id = generate_uuid();

    {
        auto segment_access = source_manager.getSegmentAccess();
        ASSERT_EQ(ErrorCode::OK,
                  segment_access.MountSegment(segment, client_id));
    }

    std::vector<Replica> replicas;
    {
        auto allocator_access = source_manager.getAllocatorAccess();
        RandomAllocationStrategy strategy;
        auto result = strategy.Allocate(allocator_access.getAllocatorManager(),
                                        1024, 1, {segment.name}, {});
        ASSERT_TRUE(result.has_value());
        replicas = std::move(*result);
    }
    ASSERT_EQ(1u, replicas.size());
    replicas[0].mark_complete();

    msgpack::sbuffer replica_buffer;
    MsgpackPacker replica_packer(&replica_buffer);
    SegmentView source_view(&source_manager);
    ASSERT_TRUE(
        Serializer<Replica>::serialize(replicas[0], source_view, replica_packer)
            .has_value());

    SegmentSerializer source_serializer(&source_manager);
    auto segment_snapshot = source_serializer.Serialize();
    ASSERT_TRUE(segment_snapshot.has_value());

    SegmentManager restored_manager(BufferAllocatorType::OFFSET);
    SegmentSerializer restored_serializer(&restored_manager);
    ASSERT_TRUE(restored_serializer.Deserialize(*segment_snapshot).has_value());

    auto replica_object =
        msgpack::unpack(replica_buffer.data(), replica_buffer.size());
    SegmentView restored_view(&restored_manager);
    auto restored_replica =
        Serializer<Replica>::deserialize(replica_object.get(), restored_view);
    ASSERT_TRUE(restored_replica.has_value());
    ASSERT_TRUE((*restored_replica)->get_available_descriptor().has_value());

    // Keep the allocator alive so this assertion specifically verifies the
    // restored lifetime binding instead of weak_ptr expiration.
    MountedSegment restored_segment;
    ASSERT_EQ(ErrorCode::OK,
              restored_view.GetMountedSegment(segment.id, restored_segment));
    ASSERT_TRUE(restored_segment.buf_allocator);

    size_t metrics_dec_capacity = 0;
    {
        auto segment_access = restored_manager.getSegmentAccess();
        ASSERT_EQ(ErrorCode::OK, segment_access.PrepareUnmountSegment(
                                     segment.id, metrics_dec_capacity));
    }

    EXPECT_FALSE((*restored_replica)->get_available_descriptor().has_value());
}

TEST_F(SerializerTest, SameNameSegmentsRestoreIndependentRegistrations) {
    SegmentManager source_manager(BufferAllocatorType::OFFSET);
    Segment segment1;
    segment1.id = generate_uuid();
    segment1.name = "shared_snapshot_name";
    segment1.base = 0x300000000;
    segment1.size = 16 * 1024 * 1024;
    segment1.te_endpoint = segment1.name;

    Segment segment2 = segment1;
    segment2.id = generate_uuid();
    segment2.base = 0x400000000;
    const UUID client_id = generate_uuid();

    {
        auto segment_access = source_manager.getSegmentAccess();
        ASSERT_EQ(ErrorCode::OK,
                  segment_access.MountSegment(segment1, client_id));
        ASSERT_EQ(ErrorCode::OK,
                  segment_access.MountSegment(segment2, client_id));
    }

    SegmentView source_view(&source_manager);
    MountedSegment source_segment1;
    MountedSegment source_segment2;
    ASSERT_EQ(ErrorCode::OK,
              source_view.GetMountedSegment(segment1.id, source_segment1));
    ASSERT_EQ(ErrorCode::OK,
              source_view.GetMountedSegment(segment2.id, source_segment2));
    ASSERT_TRUE(source_segment1.allocator_registration);
    ASSERT_TRUE(source_segment2.allocator_registration);

    auto buffer1 = source_segment1.allocator_registration->allocate(1024);
    auto buffer2 = source_segment2.allocator_registration->allocate(1024);
    ASSERT_TRUE(buffer1);
    ASSERT_TRUE(buffer2);
    Replica replica1(std::move(buffer1), ReplicaStatus::COMPLETE);
    Replica replica2(std::move(buffer2), ReplicaStatus::COMPLETE);

    msgpack::sbuffer replica_buffer1;
    MsgpackPacker replica_packer1(&replica_buffer1);
    ASSERT_TRUE(
        Serializer<Replica>::serialize(replica1, source_view, replica_packer1)
            .has_value());
    msgpack::sbuffer replica_buffer2;
    MsgpackPacker replica_packer2(&replica_buffer2);
    ASSERT_TRUE(
        Serializer<Replica>::serialize(replica2, source_view, replica_packer2)
            .has_value());

    SegmentSerializer source_serializer(&source_manager);
    auto segment_snapshot = source_serializer.Serialize();
    ASSERT_TRUE(segment_snapshot.has_value());

    SegmentManager restored_manager(BufferAllocatorType::OFFSET);
    SegmentSerializer restored_serializer(&restored_manager);
    ASSERT_TRUE(restored_serializer.Deserialize(*segment_snapshot).has_value());

    SegmentView restored_view(&restored_manager);
    MountedSegment restored_segment1;
    MountedSegment restored_segment2;
    ASSERT_EQ(ErrorCode::OK,
              restored_view.GetMountedSegment(segment1.id, restored_segment1));
    ASSERT_EQ(ErrorCode::OK,
              restored_view.GetMountedSegment(segment2.id, restored_segment2));
    ASSERT_TRUE(restored_segment1.allocator_registration);
    ASSERT_TRUE(restored_segment2.allocator_registration);
    EXPECT_NE(restored_segment1.allocator_registration,
              restored_segment2.allocator_registration);

    {
        auto allocator_access = restored_manager.getAllocatorAccess();
        const auto* registrations =
            allocator_access.getAllocatorManager().getRegistrations(
                segment1.name);
        ASSERT_NE(nullptr, registrations);
        ASSERT_EQ(2u, registrations->size());
        EXPECT_NE(registrations->end(),
                  std::find(registrations->begin(), registrations->end(),
                            restored_segment1.allocator_registration));
        EXPECT_NE(registrations->end(),
                  std::find(registrations->begin(), registrations->end(),
                            restored_segment2.allocator_registration));
    }

    auto replica_object1 =
        msgpack::unpack(replica_buffer1.data(), replica_buffer1.size());
    auto restored_replica1 =
        Serializer<Replica>::deserialize(replica_object1.get(), restored_view);
    ASSERT_TRUE(restored_replica1.has_value());
    auto replica_object2 =
        msgpack::unpack(replica_buffer2.data(), replica_buffer2.size());
    auto restored_replica2 =
        Serializer<Replica>::deserialize(replica_object2.get(), restored_view);
    ASSERT_TRUE(restored_replica2.has_value());
    ASSERT_TRUE((*restored_replica1)->get_available_descriptor().has_value());
    ASSERT_TRUE((*restored_replica2)->get_available_descriptor().has_value());

    size_t metrics_dec_capacity = 0;
    {
        auto segment_access = restored_manager.getSegmentAccess();
        ASSERT_EQ(ErrorCode::OK, segment_access.PrepareUnmountSegment(
                                     segment1.id, metrics_dec_capacity));
    }

    EXPECT_FALSE((*restored_replica1)->get_available_descriptor().has_value());
    EXPECT_TRUE((*restored_replica2)->get_available_descriptor().has_value());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
