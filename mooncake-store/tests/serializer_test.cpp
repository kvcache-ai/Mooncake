#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include <msgpack.hpp>

#include "allocator.h"
#include "segment.h"
#include "serializer.h"
#include "serialize/serializer.h"
#include "types.h"
#include "utils.h"

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

// ============================================================================
// Tests for the msgpack-based Serializer<MountedSegment> and
// Serializer<OffsetBufferAllocator> (issue #2636).
//
// These cover the round-trip of the protocol and replica_type fields that
// were previously dropped on the serialize side, and the legacy snapshot
// compatibility branches added alongside the new fields.
// ============================================================================

namespace {

// Build a MountedSegment with a real OffsetBufferAllocator and the given
// protocol string. Returned segment has SegmentStatus::OK and is suitable
// for a serialize -> deserialize round-trip.
MountedSegment MakeMountedSegment(const std::string& segment_name,
                                  const std::string& protocol,
                                  const std::string& host_id = "host1") {
    MountedSegment mounted;
    mounted.segment.id = {0x1234567890abcdefULL, 0xfedcba0987654321ULL};
    mounted.segment.name = segment_name;
    // Pick an arbitrary base/size; the allocator constructor only needs the
    // size to be a reasonable multiple of 4K to initialize the offset bins.
    mounted.segment.base = 0x100000000ULL;
    mounted.segment.size = 64ULL * 1024 * 1024;  // 64 MiB
    mounted.segment.te_endpoint = segment_name + ":12345";
    mounted.segment.protocol = protocol;
    mounted.segment.host_id = host_id;
    mounted.status = SegmentStatus::OK;
    mounted.buf_allocator = std::make_shared<OffsetBufferAllocator>(
        segment_name, mounted.segment.base, mounted.segment.size,
        mounted.segment.te_endpoint, ReplicaType::MEMORY);
    return mounted;
}

}  // namespace

// Issue #2636: MountedSegment.segment.protocol must round-trip through
// serialize -> deserialize. host_id was also added independently, so the
// current format is 10 elements: host_id then protocol.
TEST_F(SerializerTest, MountedSegmentProtocolRoundTrip) {
    MountedSegment original = MakeMountedSegment("seg_proto", "tcp");

    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);
    auto serialize_result =
        Serializer<MountedSegment>::serialize(original, packer);
    ASSERT_TRUE(serialize_result.has_value())
        << serialize_result.error().message;

    msgpack::object_handle handle = msgpack::unpack(sbuf.data(), sbuf.size());
    const auto& obj = handle.get();
    EXPECT_EQ(10u, obj.via.array.size)
        << "MountedSegment serialization must use the 10-element new format";
    ASSERT_EQ(msgpack::type::STR, obj.via.array.ptr[8].type);
    ASSERT_EQ(msgpack::type::STR, obj.via.array.ptr[9].type);
    EXPECT_EQ("host1", obj.via.array.ptr[8].as<std::string>());
    EXPECT_EQ("tcp", obj.via.array.ptr[9].as<std::string>());

    auto de_result = Serializer<MountedSegment>::deserialize(obj);
    ASSERT_TRUE(de_result.has_value()) << de_result.error().message;
    MountedSegment restored = std::move(de_result.value());

    EXPECT_EQ(original.segment.id, restored.segment.id);
    EXPECT_EQ(original.segment.name, restored.segment.name);
    EXPECT_EQ(original.segment.base, restored.segment.base);
    EXPECT_EQ(original.segment.size, restored.segment.size);
    EXPECT_EQ(original.segment.te_endpoint, restored.segment.te_endpoint);
    EXPECT_EQ(original.segment.protocol, restored.segment.protocol);
    EXPECT_EQ(original.segment.host_id, restored.segment.host_id);
    EXPECT_EQ("tcp", restored.segment.protocol);
    EXPECT_EQ("host1", restored.segment.host_id);
    EXPECT_EQ(SegmentStatus::OK, restored.status);
    ASSERT_NE(restored.buf_allocator, nullptr);
}

TEST_F(SerializerTest, MountedSegmentLegacyNineElementProtocolCompat) {
    UUID seg_uuid{0xaaaaaaaaaaaaaaaaULL, 0xbbbbbbbbbbbbbbbbULL};
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);
    packer.pack_array(9);
    packer.pack(UuidToString(seg_uuid));
    packer.pack(std::string("legacy_protocol_seg"));
    packer.pack(static_cast<uint64_t>(0x100000000ULL));
    packer.pack(static_cast<uint64_t>(64ULL * 1024 * 1024));
    packer.pack(std::string("legacy_protocol_seg:12345"));
    packer.pack(static_cast<int16_t>(static_cast<int>(SegmentStatus::OK)));
    packer.pack(false);
    packer.pack_nil();
    packer.pack(std::string("tcp"));

    msgpack::object_handle handle = msgpack::unpack(sbuf.data(), sbuf.size());
    auto de_result = Serializer<MountedSegment>::deserialize(handle.get());
    ASSERT_TRUE(de_result.has_value()) << de_result.error().message;
    EXPECT_EQ("tcp", de_result->segment.protocol);
    EXPECT_EQ("", de_result->segment.host_id);
}

TEST_F(SerializerTest, MountedSegmentLegacyNineElementHostIdCompat) {
    UUID seg_uuid{0xaaaaaaaaaaaaaaaaULL, 0xbbbbbbbbbbbbbbbbULL};
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);
    packer.pack_array(9);
    packer.pack(UuidToString(seg_uuid));
    packer.pack(std::string("legacy_host_seg"));
    packer.pack(static_cast<uint64_t>(0x100000000ULL));
    packer.pack(static_cast<uint64_t>(64ULL * 1024 * 1024));
    packer.pack(std::string("legacy_host_seg:12345"));
    packer.pack(static_cast<int16_t>(static_cast<int>(SegmentStatus::OK)));
    packer.pack(false);
    packer.pack_nil();
    packer.pack(std::string("host1"));

    msgpack::object_handle handle = msgpack::unpack(sbuf.data(), sbuf.size());
    auto de_result = Serializer<MountedSegment>::deserialize(handle.get());
    ASSERT_TRUE(de_result.has_value()) << de_result.error().message;
    EXPECT_EQ("", de_result->segment.protocol);
    EXPECT_EQ("host1", de_result->segment.host_id);
}

// Issue #2636: OffsetBufferAllocator::replica_type_ must round-trip through
// serialize -> deserialize. The fix appends `replica_type` as a new trailing
// field, so the new format is 7 elements long. A NoF_SSD allocator must
// come back as NoF_SSD, not the default MEMORY.
TEST_F(SerializerTest, OffsetBufferAllocatorReplicaTypeRoundTrip) {
    constexpr size_t kBase = 0x100000000ULL;
    constexpr size_t kSize = 64ULL * 1024 * 1024;
    OffsetBufferAllocator original("seg_nof", kBase, kSize, "seg_nof:12345",
                                   ReplicaType::NOF_SSD);

    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);
    auto serialize_result =
        Serializer<OffsetBufferAllocator>::serialize(original, packer);
    ASSERT_TRUE(serialize_result.has_value())
        << serialize_result.error().message;

    msgpack::object_handle handle = msgpack::unpack(sbuf.data(), sbuf.size());
    const auto& obj = handle.get();
    EXPECT_EQ(7u, obj.via.array.size)
        << "OffsetBufferAllocator serialization must use the 7-element "
           "new format";

    auto de_result = Serializer<OffsetBufferAllocator>::deserialize(obj);
    ASSERT_TRUE(de_result.has_value()) << de_result.error().message;
    auto restored = std::move(de_result.value());

    EXPECT_EQ(original.getSegmentName(), restored->getSegmentName());
    EXPECT_EQ(original.getTransportEndpoint(),
              restored->getTransportEndpoint());
    EXPECT_EQ(ReplicaType::NOF_SSD, restored->getReplicaType());
    // Make sure the allocator is still functional: an allocation should
    // succeed and not throw. ~AllocatedBuffer() (via unique_ptr's
    // destructor) handles the deallocate, so we do NOT call
    // restored->deallocate() manually - that would double-free and
    // double-decrement cur_size_ / metrics.
    auto buf = restored->allocate(4096);
    EXPECT_NE(buf, nullptr);
}

// Issue #2636: legacy MountedSegment snapshots (8 elements, no protocol)
// must remain deserializable so existing on-disk data is not lost on
// upgrade. Protocol defaults to empty string in the legacy path.
TEST_F(SerializerTest, MountedSegmentLegacyFormatCompat) {
    // Hand-build a legacy 8-element array with the same layout that the
    // pre-fix serializer would have produced: [segment_id, segment_name,
    // segment_base, segment_size, te_endpoint, status, has_buffer_allocator,
    // buffer_allocator_data].
    // has_buffer_allocator=false to keep the test independent of the
    // (more complex) OffsetBufferAllocator layout.
    UUID seg_uuid{0xaaaaaaaaaaaaaaaaULL, 0xbbbbbbbbbbbbbbbbULL};
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);
    packer.pack_array(8);
    packer.pack(UuidToString(seg_uuid));
    packer.pack(std::string("legacy_seg"));
    packer.pack(static_cast<uint64_t>(0x100000000ULL));
    packer.pack(static_cast<uint64_t>(64ULL * 1024 * 1024));
    packer.pack(std::string("legacy_seg:12345"));
    packer.pack(static_cast<int16_t>(static_cast<int>(SegmentStatus::OK)));
    packer.pack(false);  // has_buffer_allocator
    packer.pack_nil();   // buffer_allocator_data

    msgpack::object_handle handle = msgpack::unpack(sbuf.data(), sbuf.size());
    const auto& obj = handle.get();
    ASSERT_EQ(8u, obj.via.array.size);

    auto de_result = Serializer<MountedSegment>::deserialize(obj);
    ASSERT_TRUE(de_result.has_value())
        << "Legacy 8-element MountedSegment must still be deserializable: "
        << de_result.error().message;
    const auto& restored = de_result.value();

    EXPECT_EQ(seg_uuid, restored.segment.id);
    EXPECT_EQ("legacy_seg", restored.segment.name);
    EXPECT_EQ(0x100000000ULL, restored.segment.base);
    EXPECT_EQ(64ULL * 1024 * 1024, restored.segment.size);
    EXPECT_EQ("legacy_seg:12345", restored.segment.te_endpoint);
    EXPECT_EQ(SegmentStatus::OK, restored.status);
    // Legacy snapshots have no protocol field; it must default to empty
    // (matching the Segment default), NOT throw or corrupt other fields.
    EXPECT_EQ("", restored.segment.protocol);
    EXPECT_EQ("", restored.segment.host_id);
    // No buffer allocator was attached, so this must be a no-op.
    EXPECT_EQ(restored.buf_allocator, nullptr);
}

// Issue #2636: legacy OffsetBufferAllocator snapshots (6 elements, no
// replica_type) must remain deserializable. replica_type falls back to
// ReplicaType::MEMORY, which is the same (buggy) behavior the field had
// before the fix.
TEST_F(SerializerTest, OffsetBufferAllocatorLegacyFormatCompat) {
    // Build a real OffsetBufferAllocator to use as a source of valid
    // inner-state bytes for the first 6 elements. We discard the allocator
    // itself; we only need a 6-element array that the reader can decode.
    OffsetBufferAllocator source("legacy_nof_seg", 0x100000000ULL,
                                 64ULL * 1024 * 1024, "legacy_nof_seg:12345",
                                 ReplicaType::NOF_SSD);

    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);
    packer.pack_array(6);
    packer.pack(std::string("legacy_nof_seg"));
    packer.pack(static_cast<uint64_t>(0x100000000ULL));
    packer.pack(static_cast<uint64_t>(64ULL * 1024 * 1024));
    packer.pack(static_cast<uint64_t>(0ULL));  // cur_size
    packer.pack(std::string("legacy_nof_seg:12345"));
    // 6th element: serialize the offset_allocator using the same path
    // the production writer uses, so the inner layout matches exactly.
    auto serialize_oa =
        Serializer<offset_allocator::OffsetAllocator>::serialize(
            *source.getOffsetAllocator(), packer);
    ASSERT_TRUE(serialize_oa.has_value());

    msgpack::object_handle handle = msgpack::unpack(sbuf.data(), sbuf.size());
    const auto& obj = handle.get();
    ASSERT_EQ(6u, obj.via.array.size);

    auto de_result = Serializer<OffsetBufferAllocator>::deserialize(obj);
    ASSERT_TRUE(de_result.has_value())
        << "Legacy 6-element OffsetBufferAllocator must still be "
           "deserializable: "
        << de_result.error().message;
    auto restored = std::move(de_result.value());

    EXPECT_EQ("legacy_nof_seg", restored->getSegmentName());
    EXPECT_EQ(64ULL * 1024 * 1024, restored->capacity());
    EXPECT_EQ("legacy_nof_seg:12345", restored->getTransportEndpoint());
    // Legacy snapshots fall back to MEMORY. This is the same wrong default
    // as before the fix, and is intentional: a NoF_SSD segment restored
    // from a pre-fix snapshot continues to misreport as MEMORY. New
    // snapshots (size==7) carry the correct type.
    EXPECT_EQ(ReplicaType::MEMORY, restored->getReplicaType());
}

// OffsetBufferAllocator only has MEMORY and NOF_SSD accounting semantics.
// Other ReplicaType values may be valid for object replicas, but they must not
// be accepted as allocator types during snapshot restore.
TEST_F(SerializerTest, OffsetBufferAllocatorRejectsUnsupportedReplicaType) {
    OffsetBufferAllocator source(
        "seg_bad_replica_type", 0x100000000ULL, 64ULL * 1024 * 1024,
        "seg_bad_replica_type:12345", ReplicaType::MEMORY);

    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);
    packer.pack_array(7);
    packer.pack(std::string("seg_bad_replica_type"));
    packer.pack(static_cast<uint64_t>(0x100000000ULL));
    packer.pack(static_cast<uint64_t>(64ULL * 1024 * 1024));
    packer.pack(static_cast<uint64_t>(0ULL));
    packer.pack(std::string("seg_bad_replica_type:12345"));
    auto serialize_oa =
        Serializer<offset_allocator::OffsetAllocator>::serialize(
            *source.getOffsetAllocator(), packer);
    ASSERT_TRUE(serialize_oa.has_value());
    packer.pack(static_cast<int8_t>(ReplicaType::DISK));

    msgpack::object_handle handle = msgpack::unpack(sbuf.data(), sbuf.size());
    const auto& obj = handle.get();

    auto de_result = Serializer<OffsetBufferAllocator>::deserialize(obj);
    ASSERT_FALSE(de_result.has_value());
    EXPECT_NE(std::string::npos,
              de_result.error().message.find("invalid replica_type"));
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
