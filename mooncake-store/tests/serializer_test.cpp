#include <glog/logging.h>
#include <gtest/gtest.h>

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

TEST_F(SerializerTest, MountedSegmentPreservesHcaLocalityAndAllocator) {
    MountedSegment original;
    original.segment.id = generate_uuid();
    original.segment.name = "receiver";
    original.segment.base = 0x300000000;
    original.segment.size = 1024 * 1024;
    original.segment.te_endpoint = "receiver:1234";
    original.segment.host_id = "receiver";
    original.segment.locality = SegmentLocality{7, {"mlx5_0", "mlx5_2"}};
    original.status = SegmentStatus::OK;
    original.buf_allocator = std::make_shared<OffsetBufferAllocator>(
        original.segment.name, original.segment.base, original.segment.size,
        original.segment.te_endpoint);
    original.buf_allocator->SetLocality(original.segment.locality);
    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    ASSERT_TRUE(Serializer<MountedSegment>::serialize(original, packer));
    auto object = msgpack::unpack(buffer.data(), buffer.size());
    auto restored = Serializer<MountedSegment>::deserialize(object.get());
    ASSERT_TRUE(restored);
    ASSERT_TRUE(restored->segment.locality);
    EXPECT_EQ(*restored->segment.locality, *original.segment.locality);
    ASSERT_TRUE(restored->buf_allocator);
    EXPECT_EQ(restored->buf_allocator->GetLocality(),
              original.buf_allocator->GetLocality());
}

struct LegacyMemoryDescriptor {
    AllocatedBuffer::Descriptor buffer_descriptor;
};
YLT_REFL(LegacyMemoryDescriptor, buffer_descriptor);

struct LegacyReplicaDescriptor {
    ReplicaID id{0};
    std::variant<LegacyMemoryDescriptor, NoFDescriptor, DiskDescriptor,
                 LocalDiskDescriptor, DistributedFSDescriptor>
        descriptor_variant;
    ReplicaStatus status;
};
YLT_REFL(LegacyReplicaDescriptor, id, descriptor_variant, status);

TEST_F(SerializerTest, ReadLocalityIsCompatibleInsideReplicaVariant) {
    Replica::Descriptor replica;
    replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor memory;
    memory.buffer_descriptor = {1024, 0x300000000, "rdma", "receiver"};
    memory.locality = SegmentLocality{7, {"mlx5_0"}};
    replica.descriptor_variant = memory;
    auto old_reader = struct_pack::deserialize<LegacyReplicaDescriptor>(
        struct_pack::serialize(replica));
    ASSERT_TRUE(old_reader);
    EXPECT_EQ(std::get<LegacyMemoryDescriptor>(old_reader->descriptor_variant)
                  .buffer_descriptor.transport_endpoint_,
              "receiver");
    auto current = struct_pack::deserialize<Replica::Descriptor>(
        struct_pack::serialize(replica));
    ASSERT_TRUE(current);
    EXPECT_EQ(current->get_memory_descriptor().locality, memory.locality);
    auto new_reader = struct_pack::deserialize<Replica::Descriptor>(
        struct_pack::serialize(*old_reader));
    ASSERT_TRUE(new_reader);
    EXPECT_FALSE(new_reader->get_memory_descriptor().locality);
}

struct LegacySegment {
    UUID id{0, 0};
    std::string name;
    uintptr_t base{0};
    size_t size{0};
    std::string te_endpoint;
    std::string protocol;
    std::string host_id;
};
YLT_REFL(LegacySegment, id, name, base, size, te_endpoint, protocol, host_id);

TEST_F(SerializerTest, SegmentLocalityHasCompatibleWireFields) {
    LegacySegment old;
    old.id = generate_uuid();
    old.name = "receiver";
    old.base = 0x300000000;
    old.size = 1024 * 1024;
    old.te_endpoint = "receiver:1234";
    old.protocol = "rdma";
    old.host_id = "receiver";
    auto decoded =
        struct_pack::deserialize<Segment>(struct_pack::serialize(old));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->name, old.name);
    EXPECT_FALSE(decoded->locality);
    decoded->locality = SegmentLocality{7, {"mlx5_0"}};
    const auto encoded = struct_pack::serialize(*decoded);
    auto legacy = struct_pack::deserialize<LegacySegment>(encoded);
    ASSERT_TRUE(legacy);
    EXPECT_EQ(legacy->base, old.base);
    EXPECT_EQ(legacy->host_id, old.host_id);
    auto current = struct_pack::deserialize<Segment>(encoded);
    ASSERT_TRUE(current);
    ASSERT_TRUE(current->locality);
    EXPECT_EQ(*current->locality, *decoded->locality);
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

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
