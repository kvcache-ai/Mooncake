#include "ha/snapshot/batch_oplog/batch_oplog_snapshot_gc.h"
#include <gtest/gtest.h>
#include <cstdint>
#include <map>
#include <string_view>
#include <vector>
#include "ha/kv/ha_kv_backend.h"
#include "ha/snapshot/snapshot_maintenance_lease.h"
#include "ha/snapshot/object/snapshot_object_store.h"
namespace mooncake::test {
class Backend : public HaKvBackend {
   public:
    std::map<std::string, std::string> v;
    ErrorCode Get(std::string_view k, std::string& x) override {
        auto i = v.find(std::string(k));
        if (i == v.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        x = i->second;
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view, std::string_view) override {
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view, std::string_view, size_t,
                    std::vector<KvPair>&) override {
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }
};
class Store : public SnapshotObjectStore {
   public:
    std::vector<std::string> deleted;
    tl::expected<void, std::string> UploadBuffer(
        const std::string&, const std::vector<uint8_t>&) override {
        return {};
    }
    tl::expected<void, std::string> DownloadBuffer(
        const std::string&, std::vector<uint8_t>&) override {
        return tl::make_unexpected("missing");
    }
    tl::expected<void, std::string> UploadString(const std::string&,
                                                 const std::string&) override {
        return {};
    }
    tl::expected<void, std::string> DownloadString(const std::string&,
                                                   std::string&) override {
        return tl::make_unexpected("missing");
    }
    tl::expected<void, std::string> DeleteObjectsWithPrefix(
        const std::string& p) override {
        deleted.push_back(p);
        return {};
    }
    tl::expected<void, std::string> ListObjectsWithPrefix(
        const std::string&, std::vector<std::string>&) override {
        return {};
    }
    std::string GetConnectionInfo() const override { return "test"; }
};
TEST(BatchOpLogSnapshotGcTest, PointerMismatchSkipsDeletion) {
    Backend b;
    Store s;
    auto l = SnapshotMaintenanceLease::MakeForTesting("c", "1");
    b.v[ha::BuildBatchOpLogSnapshotLatestKey("c")] = "changed";
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL,
              BatchOpLogSnapshotGc(b, s, "c", "r")
                  .Run(*l, "published", std::nullopt));
    EXPECT_TRUE(s.deleted.empty());
}
}  // namespace mooncake::test
