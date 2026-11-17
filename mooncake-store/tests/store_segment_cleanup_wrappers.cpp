#include "store_segment_cleanup_worker_fixture.h"

using namespace mooncake;
using namespace mooncake::cleanup_test;

extern "C" int WrapUnregister(TransferEngine*, void*,
                              bool) asm("__wrap_" CLEANUP_TE);
extern "C" int WrapUnregister(TransferEngine* engine, void* address,
                              bool publish) {
    if (active && active->fault == Fault::Transfer &&
        (address == active->base || address == active->pools[1].base)) {
        active->ObserveUnregisterFailure();
        return -1;
    }
    return RealUnregister(engine, address, publish);
}
extern "C" tl::expected<void, ErrorCode> WrapUnmount(
    MasterClient*, const UUID&) asm("__wrap_" CLEANUP_MASTER);
extern "C" tl::expected<void, ErrorCode> WrapUnmount(MasterClient* master,
                                                     const UUID& id) {
    if (active && id == active->id && active->fault == Fault::Master)
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    return RealUnmount(master, id);
}
extern "C" std::shared_ptr<MetadataStoragePlugin> WrapPlugin(
    const std::string&) asm("__wrap_" CLEANUP_PLUGIN);
extern "C" std::shared_ptr<MetadataStoragePlugin> WrapPlugin(
    const std::string& uri) {
    if (active && uri == active->uri) return active->publication;
    return RealPlugin(uri);
}
extern "C" void WrapFree(const std::string&, void*,
                         DmaBufferAllocator*) asm("__wrap_" CLEANUP_FREE);
extern "C" void WrapFree(const std::string& protocol, void* address,
                         DmaBufferAllocator* allocator) {
    if (active && address == active->base) {
        if (!active->ReleaseAuthorized()) return;
        if (active->released.fetch_add(1) != 0) {
            ADD_FAILURE() << "Backing allocation released more than once";
            return;
        }
    }
    RealFree(protocol, address, allocator);
}
extern "C" void __wrap_free(void* address) {
    if (active && !active->ReleasePool(address)) return;
    __real_free(address);
}
extern "C" int __wrap_munmap(void* address, size_t length) {
    if (active && address == active->base) {
        if (!active->ReleaseAuthorized())
            return 0;  // Guard unsafe baseline free.
        if (active->released.fetch_add(1) != 0) {
            ADD_FAILURE() << "File mapping released more than once";
            return 0;
        }
    }
    return __real_munmap(address, length);
}

#define CLEANUP_SUBMIT                                                \
    "_ZN8mooncake17TransferSubmitter6submitERKNS_"                    \
    "7Replica10DescriptorERSt6vector"                                 \
    "INS_5SliceESaIS6_EENS_9Transport15TransferRequest6OpCodeEPvmNS_" \
    "14TransferIntentE"
#define CLEANUP_JOIN "_ZNSt6thread4joinEv"

extern "C" std::optional<TransferFuture> RealSubmit(
    TransferSubmitter*, const Replica::Descriptor&, std::vector<Slice>&,
    TransferRequest::OpCode, void*, size_t,
    TransferIntent) asm("__real_" CLEANUP_SUBMIT);
extern "C" std::optional<TransferFuture> WrapSubmit(
    TransferSubmitter*, const Replica::Descriptor&, std::vector<Slice>&,
    TransferRequest::OpCode, void*, size_t,
    TransferIntent) asm("__wrap_" CLEANUP_SUBMIT);
extern "C" std::optional<TransferFuture> WrapSubmit(
    TransferSubmitter* submitter, const Replica::Descriptor& replica,
    std::vector<Slice>& slices, TransferRequest::OpCode operation, void* ptr,
    size_t size, TransferIntent intent) {
    auto gate = migration_gate.load();
    if (gate && intent == TransferIntent::kMigration && gate->Matches(slices)) {
        gate->Block(slices);
    }
    return RealSubmit(submitter, replica, slices, operation, ptr, size, intent);
}

extern "C" void RealJoin(std::thread*) asm("__real_" CLEANUP_JOIN);
extern "C" void WrapJoin(std::thread*) asm("__wrap_" CLEANUP_JOIN);
// Member-function pointers reserve the low address bit for virtual dispatch.
extern "C" __attribute__((aligned(2))) void WrapJoin(std::thread* worker) {
    auto gate = migration_gate.load();
    const bool closing =
        gate && gate->IsCloseThread() && gate->IsSourceWorker(*worker);
    if (closing) gate->BeginDrain();
    RealJoin(worker);
    if (closing) gate->EndDrain();
}
