#include "gpu_staging.h"

#include <cuda_runtime.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <utility>
#include <vector>

#include "random.h"

namespace mooncake {
namespace {

using Bytes = std::vector<unsigned char>;
using Results = GpuStaging::Results;

bool HasDevice() {
    int devices = 0;
    return cudaGetDeviceCount(&devices) == cudaSuccess && devices > 0;
}

Bytes RandomBytes(size_t size, RandomEngine& engine) {
    Bytes bytes(size);
    for (auto& byte : bytes) byte = randomUniform<unsigned>(0, 255, engine);
    return bytes;
}

// Device memory the resolver reports as one registered buffer.
struct Arena {
    explicit Arena(size_t size) : bytes(size) {
        EXPECT_EQ(cudaSuccess, cudaMalloc(&base, size));
    }
    ~Arena() { cudaFree(base); }
    char* at(size_t offset) const { return static_cast<char*>(base) + offset; }
    void Upload(const Bytes& host) {
        ASSERT_EQ(cudaSuccess,
                  cudaMemcpy(base, host.data(), bytes, cudaMemcpyHostToDevice));
    }
    Bytes Download() const {
        Bytes host(bytes);
        EXPECT_EQ(cudaSuccess,
                  cudaMemcpy(host.data(), base, bytes, cudaMemcpyDeviceToHost));
        return host;
    }
    GpuStaging::ResolveRegion Resolver() const {
        return [this](void* ptr) -> std::optional<GpuStagingRegion> {
            const auto* p = static_cast<char*>(ptr);
            if (p < at(0) || p >= at(bytes)) return std::nullopt;
            return GpuStagingRegion{base, bytes};
        };
    }
    void* base = nullptr;
    size_t bytes;
};

// Every key's parts, in random order and with random gaps, in one arena.
struct Layout {
    std::vector<std::vector<size_t>> sizes;
    std::vector<std::vector<size_t>> offsets;
    size_t bytes = 0;

    Layout(std::vector<std::vector<size_t>> part_sizes, bool aligned,
           RandomEngine& engine)
        : sizes(std::move(part_sizes)), offsets(sizes.size()) {
        std::vector<std::pair<size_t, size_t>> order;
        for (size_t k = 0; k < sizes.size(); ++k) {
            offsets[k].resize(sizes[k].size());
            for (size_t p = 0; p < sizes[k].size(); ++p)
                order.emplace_back(k, p);
        }
        for (size_t i = order.size(); i > 1; --i)
            std::swap(order[i - 1], order[randomIndex(i, engine)]);
        for (const auto& [k, p] : order) {
            bytes += aligned ? 16 * randomUniform<size_t>(0, 4, engine)
                             : randomUniform<size_t>(0, 63, engine);
            offsets[k][p] = bytes;
            bytes += sizes[k][p];
        }
        bytes += 64;
    }

    std::vector<std::vector<Slice>> Slices(const Arena& arena) const {
        std::vector<std::vector<Slice>> parts(sizes.size());
        for (size_t k = 0; k < sizes.size(); ++k) {
            for (size_t p = 0; p < sizes[k].size(); ++p)
                parts[k].push_back({arena.at(offsets[k][p]), sizes[k][p]});
        }
        return parts;
    }

    std::vector<size_t> ObjectBytes() const {
        std::vector<size_t> totals;
        for (const auto& key : sizes) {
            size_t total = 0;
            for (size_t size : key) total += size;
            totals.push_back(total);
        }
        return totals;
    }

    // The arena after the per-part path reads `objects` into it.
    Bytes Read(const std::vector<Bytes>& objects, Bytes arena,
               const std::vector<size_t>& skip = {}) const {
        for (size_t k = 0; k < sizes.size(); ++k) {
            if (std::find(skip.begin(), skip.end(), k) != skip.end()) continue;
            size_t offset = 0;
            for (size_t p = 0; p < sizes[k].size(); ++p) {
                std::copy_n(objects[k].begin() + offset, sizes[k][p],
                            arena.begin() + offsets[k][p]);
                offset += sizes[k][p];
            }
        }
        return arena;
    }

    // The objects the per-part path writes from `arena`.
    std::vector<Bytes> Write(const Bytes& arena) const {
        std::vector<Bytes> objects(sizes.size());
        for (size_t k = 0; k < sizes.size(); ++k) {
            for (size_t p = 0; p < sizes[k].size(); ++p) {
                const auto begin = arena.begin() + offsets[k][p];
                objects[k].insert(objects[k].end(), begin, begin + sizes[k][p]);
            }
        }
        return objects;
    }
};

// Objects in host memory. Transfers record how many slices each key came
// with: one when staged, its own parts on the per-part path.
struct FakeStore {
    std::vector<Bytes> objects;
    std::vector<size_t> failing;
    std::vector<size_t> slices_seen;
    size_t calls = 0;

    explicit FakeStore(size_t keys) : objects(keys), slices_seen(keys, 0) {}

    bool Fails(size_t key) const {
        return std::find(failing.begin(), failing.end(), key) != failing.end();
    }

    GpuStaging::Transfer Transfer(bool read) {
        return [this, read](const std::vector<size_t>& indices,
                            const std::vector<std::vector<Slice>>& slices) {
            ++calls;
            Results results;
            for (size_t n = 0; n < indices.size(); ++n) {
                const size_t key = indices[n];
                slices_seen[key] = slices[n].size();
                if (Fails(key)) {
                    results.push_back(tl::unexpected(ErrorCode::TRANSFER_FAIL));
                    continue;
                }
                if (!read) objects[key].clear();
                size_t offset = 0;
                for (const auto& slice : slices[n]) {
                    if (read) {
                        EXPECT_EQ(
                            cudaSuccess,
                            cudaMemcpy(slice.ptr, objects[key].data() + offset,
                                       slice.size, cudaMemcpyHostToDevice));
                    } else {
                        objects[key].resize(offset + slice.size);
                        EXPECT_EQ(
                            cudaSuccess,
                            cudaMemcpy(objects[key].data() + offset, slice.ptr,
                                       slice.size, cudaMemcpyDeviceToHost));
                    }
                    offset += slice.size;
                }
                results.emplace_back();
            }
            return results;
        };
    }
};

// Random multi-part keys that fit in a 32 KiB half, then the fallbacks: a
// single-part key and a key larger than a half.
std::vector<std::vector<size_t>> PartSizes(bool aligned, RandomEngine& engine) {
    std::vector<std::vector<size_t>> sizes;
    for (int k = 0; k < 24; ++k) {
        std::vector<size_t> key(randomUniform<size_t>(2, 12, engine));
        for (auto& size : key) {
            size = aligned ? 16 * randomUniform<size_t>(1, 160, engine)
                           : randomUniform<size_t>(1, 2600, engine);
        }
        sizes.push_back(std::move(key));
    }
    sizes.push_back({4096});
    sizes.push_back({20 << 10, 20 << 10});
    return sizes;
}

constexpr size_t kSmallBuffer = 64 << 10;

TEST(GpuStagingTest, ReadMatchesThePerPartPathForRandomLayouts) {
    if (!HasDevice()) GTEST_SKIP() << "CUDA device unavailable";
    RandomEngine engine(20261111);
    for (bool aligned : {true, false}) {
        SCOPED_TRACE(aligned ? "aligned" : "unaligned");
        GpuStaging staging(nullptr, {.buffer_bytes = kSmallBuffer});
        const Layout layout(PartSizes(aligned, engine), aligned, engine);
        Arena arena(layout.bytes);
        const Bytes before(layout.bytes, 0xa5);
        arena.Upload(before);
        FakeStore store(layout.sizes.size());
        for (size_t k = 0; k < layout.sizes.size(); ++k)
            store.objects[k] = RandomBytes(layout.ObjectBytes()[k], engine);

        const auto results =
            staging.Read(layout.Slices(arena), layout.ObjectBytes(),
                         arena.Resolver(), store.Transfer(/*read=*/true));

        ASSERT_TRUE(results.has_value());
        ASSERT_EQ(results->size(), layout.sizes.size());
        for (const auto& result : *results) EXPECT_TRUE(result.has_value());
        EXPECT_EQ(arena.Download(), layout.Read(store.objects, before));
        for (size_t k = 0; k < 24; ++k) EXPECT_EQ(store.slices_seen[k], 1u);
        EXPECT_EQ(store.slices_seen[25], 2u);  // larger than a half
        EXPECT_GE(store.calls, 3u);  // the fallbacks, then several batches
    }
}

TEST(GpuStagingTest, WriteStoresTheObjectsOfThePerPartPath) {
    if (!HasDevice()) GTEST_SKIP() << "CUDA device unavailable";
    RandomEngine engine(20261112);
    for (bool aligned : {true, false}) {
        SCOPED_TRACE(aligned ? "aligned" : "unaligned");
        GpuStaging staging(nullptr, {.buffer_bytes = kSmallBuffer});
        const Layout layout(PartSizes(aligned, engine), aligned, engine);
        Arena arena(layout.bytes);
        const Bytes contents = RandomBytes(layout.bytes, engine);
        arena.Upload(contents);
        FakeStore store(layout.sizes.size());

        const auto results =
            staging.Write(layout.Slices(arena), arena.Resolver(),
                          store.Transfer(/*read=*/false));

        ASSERT_TRUE(results.has_value());
        for (const auto& result : *results) EXPECT_TRUE(result.has_value());
        EXPECT_EQ(store.objects, layout.Write(contents));
        for (size_t k = 0; k < 24; ++k) EXPECT_EQ(store.slices_seen[k], 1u);
        EXPECT_EQ(store.slices_seen[25], 2u);
        EXPECT_GE(store.calls, 3u);
    }
}

TEST(GpuStagingTest, FailedKeyKeepsItsResultAndPosition) {
    if (!HasDevice()) GTEST_SKIP() << "CUDA device unavailable";
    RandomEngine engine(20261113);
    GpuStaging staging(nullptr, {.buffer_bytes = kSmallBuffer});
    const Layout layout(PartSizes(true, engine), true, engine);
    Arena arena(layout.bytes);
    const Bytes before(layout.bytes, 0xa5);
    arena.Upload(before);
    FakeStore store(layout.sizes.size());
    for (size_t k = 0; k < layout.sizes.size(); ++k)
        store.objects[k] = RandomBytes(layout.ObjectBytes()[k], engine);
    store.failing = {3, 25};

    const auto results =
        staging.Read(layout.Slices(arena), layout.ObjectBytes(),
                     arena.Resolver(), store.Transfer(/*read=*/true));

    ASSERT_TRUE(results.has_value());
    for (size_t k = 0; k < results->size(); ++k) {
        if (store.Fails(k)) {
            ASSERT_FALSE((*results)[k].has_value());
            EXPECT_EQ((*results)[k].error(), ErrorCode::TRANSFER_FAIL);
        } else {
            EXPECT_TRUE((*results)[k].has_value()) << k;
        }
    }
    // A failed key's destinations are not touched.
    EXPECT_EQ(arena.Download(), layout.Read(store.objects, before, {3, 25}));
}

TEST(GpuStagingTest, TransferThatFailsLateQuarantinesItsHalf) {
    if (!HasDevice()) GTEST_SKIP() << "CUDA device unavailable";
    RandomEngine engine(20261114);
    // Every failure counts as late; each 20 KiB key fills a half.
    GpuStaging staging(nullptr, {.buffer_bytes = kSmallBuffer,
                                 .late_failure_ms = 0,
                                 .quarantine_ms = 600'000});
    const Layout layout(std::vector<std::vector<size_t>>(6, {10240, 10240}),
                        true, engine);
    Arena arena(layout.bytes);
    const Bytes before(layout.bytes, 0xa5);
    arena.Upload(before);
    FakeStore store(layout.sizes.size());
    for (size_t k = 0; k < layout.sizes.size(); ++k)
        store.objects[k] = RandomBytes(20480, engine);
    store.failing = {0, 1};

    const auto results =
        staging.Read(layout.Slices(arena), layout.ObjectBytes(),
                     arena.Resolver(), store.Transfer(/*read=*/true));

    ASSERT_TRUE(results.has_value());
    EXPECT_FALSE((*results)[0].has_value());
    EXPECT_FALSE((*results)[1].has_value());
    for (size_t k = 2; k < 6; ++k) EXPECT_TRUE((*results)[k].has_value());
    // Keys 0 and 1 each took a half; with both quarantined, the rest went
    // with their own parts.
    EXPECT_EQ(store.slices_seen, (std::vector<size_t>{1, 1, 2, 2, 2, 2}));
    EXPECT_EQ(arena.Download(), layout.Read(store.objects, before, {0, 1}));
}

TEST(GpuStagingTest, UnregisteredOrUnstageableKeysTakeThePerPartPath) {
    if (!HasDevice()) GTEST_SKIP() << "CUDA device unavailable";
    RandomEngine engine(20261115);
    GpuStaging staging(nullptr, {.buffer_bytes = kSmallBuffer});
    Arena arena(8192);
    Arena unregistered(1024);  // not known to the resolver
    const Bytes before(8192, 0xa5);
    arena.Upload(before);
    const std::vector<std::vector<Slice>> parts{
        {{arena.at(0), 1024}, {unregistered.at(0), 1024}},
        {{arena.at(2048), 1024}, {arena.at(4096), 1024}},
        {{arena.at(6144), 1024}, {arena.at(7168), 1024}},
    };
    FakeStore store(parts.size());
    for (auto& object : store.objects) object = RandomBytes(2048, engine);

    // Key 2's replica cannot be staged (object size 0).
    const auto results = staging.Read(parts, {2048, 2048, 0}, arena.Resolver(),
                                      store.Transfer(/*read=*/true));

    ASSERT_TRUE(results.has_value());
    for (const auto& result : *results) EXPECT_TRUE(result.has_value());
    EXPECT_EQ(store.slices_seen, (std::vector<size_t>{2, 1, 2}));
    Bytes expected = before;
    const auto place = [&](size_t key, size_t half, size_t offset) {
        std::copy_n(store.objects[key].begin() + half * 1024, 1024,
                    expected.begin() + offset);
    };
    place(0, 0, 0);
    place(1, 0, 2048);
    place(1, 1, 4096);
    place(2, 0, 6144);
    place(2, 1, 7168);
    EXPECT_EQ(arena.Download(), expected);
    const Bytes tail(store.objects[0].begin() + 1024, store.objects[0].end());
    EXPECT_EQ(unregistered.Download(), tail);
}

}  // namespace
}  // namespace mooncake
