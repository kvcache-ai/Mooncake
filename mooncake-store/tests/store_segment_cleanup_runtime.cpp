#include "store_segment_cleanup_fixture.h"

namespace mooncake::cleanup_test {
Scenario::Scenario() { active = this; }

Scenario::~Scenario() {
    fault = Fault::None;
    publication->fail_publication = false;
    if (real) real->tearDownAll();
    real.reset();
    client.reset();
    if (base) {
        EXPECT_TRUE(master.service()->UnmountSegment(id, owner));
        const int rc = RealUnregister(engine.get(), base, true);
        EXPECT_TRUE(rc == 0 || rc == ERR_ADDRESS_NOT_REGISTERED);
        EXPECT_EQ(engine->getMetadata()->updateLocalSegmentDesc(), 0);
        const bool safe =
            MasterRemoved() && !LocalContains() && !RemoteContains();
        EXPECT_TRUE(safe)
            << "Recovery cleanup remains unconfirmed; retain backing";
        if (released == 0 && safe) {
            if (mapped) {
                EXPECT_EQ(__real_munmap(base, size), 0);
            } else {
                RealFree("tcp", base, nullptr);
            }
        }
    }
    for (auto& pool : pools) {
        if (!pool.base || pool.released != 0) continue;
        const int rc = RealUnregister(engine.get(), pool.base, true);
        EXPECT_TRUE(rc == 0 || rc == ERR_ADDRESS_NOT_REGISTERED);
        EXPECT_EQ(engine->getMetadata()->updateLocalSegmentDesc(), 0);
        if (PoolSafe(pool)) __real_free(pool.base);
    }
    engine.reset();
    master.Stop();
    if (!path.empty()) unlink(path.c_str());
    active = nullptr;
}

void Scenario::Start(bool with_real, bool with_pools) {
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    int socket = -1;
    auto port = findAvailableTcpPort(socket);
    ASSERT_NE(port, 0);
    name = "127.0.0.1:" + std::to_string(port);
    close(socket);
    engine = std::make_shared<TransferEngine>(false);
    ASSERT_EQ(engine->init(uri, name, "127.0.0.1", 0), 0);
    ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
    if (with_real) {
        real = RealClient::create();
        ASSERT_EQ(real->setup_real(name, uri, with_pools ? size : 0,
                                   with_pools ? size : 0, "tcp", "",
                                   master.master_address(), engine),
                  0);
        ASSERT_NE(real->client_, nullptr);
        owner = real->client_->getClientId();
        retained_client = real->client_;
        if (with_pools) {
            ASSERT_EQ(real->segment_ptrs_.size(), 1u);
            auto segments = master.service()->GetSegmentsDetailForAdmin();
            ASSERT_TRUE(segments);
            ASSERT_EQ(segments->size(), 1u);
            pools[0].base = real->segment_ptrs_.front().get();
            pools[0].segment = segments->front().segment_id;
            ASSERT_EQ(segments->front().base_address,
                      reinterpret_cast<uintptr_t>(pools[0].base));
            pools[1].base = real->client_buffer_allocator_->getBase();
            ASSERT_NE(pools[0].base, pools[1].base);
            for (const auto& pool : pools) {
                EXPECT_TRUE(Contains(
                    engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID),
                    pool.base));
            }
        }
    } else {
        auto created = Client::Create(name, uri, "tcp", std::nullopt,
                                      master.master_address(), engine);
        ASSERT_TRUE(created.has_value());
        client = *created;
        owner = client->getClientId();
    }
}

void Scenario::Mount(bool file_mapping) {
    mapped = file_mapping;
    if (real) {
        std::vector<std::string> ids;
        if (mapped) {
            char temp[] = "/tmp/mooncake-cleanup-XXXXXX";
            int fd = mkstemp(temp);
            ASSERT_GE(fd, 0);
            path = temp;
            const int rc = ftruncate(fd, size);
            close(fd);
            ASSERT_EQ(rc, 0);
            ASSERT_EQ(real->mountSegment(path, 0, size, "tcp", "", ids), 0);
        } else {
            ASSERT_EQ(real->allocateAndMountSegment(size, "tcp", "", ids), 0);
        }
        ASSERT_EQ(ids.size(), 1u);
        ASSERT_TRUE(StringToUuid(ids.front(), id));
        auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        ASSERT_NE(desc, nullptr);
        std::vector<void*> dynamic_bases;
        for (const auto& buffer : desc->buffers) {
            auto address = reinterpret_cast<void*>(buffer.addr);
            if (address != pools[0].base && address != pools[1].base)
                dynamic_bases.push_back(address);
        }
        ASSERT_EQ(dynamic_bases.size(), 1u);
        base = dynamic_bases.front();
    } else {
        base = allocate_buffer_allocator_memory(size, "tcp");
        ASSERT_NE(base, nullptr);
        auto mounted = client->MountSegmentAndGetId(base, size, "tcp", "");
        ASSERT_TRUE(mounted.has_value());
        id = *mounted;
    }
    EXPECT_FALSE(MasterRemoved());
    EXPECT_TRUE(LocalContains());
    EXPECT_TRUE(RemoteContains());
}
}  // namespace mooncake::cleanup_test
