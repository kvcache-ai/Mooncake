#include "buffer_pool.h"

#include <pybind11/gil.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "pyclient.h"
#include "real_client.h"
#include "types.h"

namespace py = pybind11;

namespace mooncake {
namespace {

constexpr char kPyClientCapsuleName[] = "mooncake.PyClient.shared_ptr";
constexpr char kPyClientCapsuleMethod[] = "_get_pyclient_capsule";

std::shared_ptr<PyClient> unwrap_pyclient_capsule(py::object capsule) {
    if (capsule.is_none()) {
        return nullptr;
    }
    if (!PyCapsule_CheckExact(capsule.ptr())) {
        throw std::runtime_error(
            "store wrapper returned a non-capsule PyClient handle");
    }
    py::capsule py_client_capsule(capsule);
    const char *capsule_name = py_client_capsule.name();
    if (capsule_name == nullptr ||
        std::strcmp(capsule_name, kPyClientCapsuleName) != 0) {
        throw std::runtime_error(
            "store wrapper returned an unexpected PyClient capsule type");
    }
    auto *ptr = static_cast<std::shared_ptr<PyClient> *>(
        py_client_capsule.get_pointer());
    if (ptr == nullptr) {
        throw std::runtime_error(
            "store wrapper returned an empty PyClient capsule");
    }
    return *ptr;
}

std::shared_ptr<PyClient> unwrap_store(py::object store_obj) {
    if (store_obj.is_none()) {
        return nullptr;
    }
    try {
        return store_obj.cast<std::shared_ptr<PyClient>>();
    } catch (const py::cast_error &) {
    }
    if (!py::hasattr(store_obj, kPyClientCapsuleMethod)) {
        throw std::runtime_error(
            "BufferPool store parameter must be a PyClient or store wrapper "
            "that "
            "implements _get_pyclient_capsule()");
    }
    return unwrap_pyclient_capsule(store_obj.attr(kPyClientCapsuleMethod)());
}

}  // namespace

class BufferPoolNative;
class BufferLeaseNative;

class BufferLeaseViewNative {
   public:
    BufferLeaseViewNative(std::shared_ptr<BufferLeaseNative> lease,
                          size_t size);
    ~BufferLeaseViewNative();

    BufferLeaseViewNative(const BufferLeaseViewNative &) = delete;
    BufferLeaseViewNative &operator=(const BufferLeaseViewNative &) = delete;

    py::buffer_info buffer_info();

   private:
    std::shared_ptr<BufferLeaseNative> lease_;
    size_t size_ = 0;
};

class BufferLeaseNative
    : public std::enable_shared_from_this<BufferLeaseNative> {
   public:
    BufferLeaseNative(std::shared_ptr<BufferPoolNative> pool,
                      std::shared_ptr<BufferHandle> handle,
                      size_t requested_size);
    ~BufferLeaseNative();

    BufferLeaseNative(const BufferLeaseNative &) = delete;
    BufferLeaseNative &operator=(const BufferLeaseNative &) = delete;

    uintptr_t ptr() const;
    size_t size() const { return requested_size_; }
    py::object buffer();
    py::buffer_info buffer_info(size_t size);
    void add_export();
    void release_export();
    BufferLeaseNative &enter() { return *this; }
    void exit(const py::object &, const py::object &, const py::object &) {
        release();
    }
    void release();

   private:
    void release_lease(bool check_exported_views);

    std::shared_ptr<BufferPoolNative> pool_;
    std::shared_ptr<BufferHandle> handle_;
    size_t requested_size_ = 0;
    bool closed_ = false;
    std::atomic<size_t> exports_{0};
};

class BufferPoolNative : public std::enable_shared_from_this<BufferPoolNative> {
   public:
    BufferPoolNative(const py::object &store, size_t max_bytes,
                     size_t min_size_class, py::object max_size_class,
                     size_t alignment, bool block_on_exhaustion,
                     py::object default_timeout, py::object max_regions);
    ~BufferPoolNative() {
        try {
            close();
        } catch (...) {
        }
    }

    std::shared_ptr<BufferLeaseNative> acquire(size_t size, py::object block,
                                               py::object timeout);
    std::shared_ptr<BufferLeaseNative> buffer(size_t size, py::object block,
                                              py::object timeout) {
        return acquire(size, block, timeout);
    }
    void prewarm(size_t size, size_t count);
    void close();

   private:
    struct Region {
        std::shared_ptr<BufferHandle> handle;
        size_t size = 0;
        bool overflow_registered = false;
    };

    friend class BufferLeaseNative;

    std::shared_ptr<BufferLeaseNative> make_lease_locked(Region region,
                                                         size_t requested_size);
    std::optional<Region> try_acquire_locked(std::unique_lock<std::mutex> &lock,
                                             size_t requested_size);
    std::shared_ptr<BufferHandle> allocate_overflow_unlocked(size_t size);
    void unregister_overflow_unlocked(const Region &region);
    void release_internal(std::shared_ptr<BufferHandle> &handle);
    void reserve_locked(size_t size_class);
    void unreserve_locked(size_t size_class);
    std::pair<size_t, bool> allocation_size(size_t size) const;
    bool has_capacity_for_locked(size_t size_class) const;
    void raise_if_open_locked() const;

    py::object store_obj_;
    std::shared_ptr<PyClient> py_client_;
    size_t local_buffer_capacity_ = 0;
    size_t max_bytes_ = 0;
    size_t min_size_class_ = 0;
    size_t max_size_class_ = 0;
    size_t alignment_ = 0;
    bool block_on_exhaustion_ = true;
    std::optional<double> default_timeout_;
    std::optional<size_t> max_regions_;

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::unordered_map<uintptr_t, Region> regions_;
    std::unordered_set<uintptr_t> in_use_;
    bool closed_ = false;
    bool closing_ = false;
    size_t total_bytes_ = 0;
    size_t reserved_bytes_ = 0;
    size_t reserved_regions_ = 0;
    size_t acquire_count_ = 0;
    size_t allocate_count_ = 0;
    size_t oversize_allocate_count_ = 0;
    size_t wait_count_ = 0;
    size_t release_count_ = 0;
};

BufferLeaseViewNative::BufferLeaseViewNative(
    std::shared_ptr<BufferLeaseNative> lease, size_t size)
    : lease_(std::move(lease)), size_(size) {
    lease_->add_export();
}

BufferLeaseViewNative::~BufferLeaseViewNative() { lease_->release_export(); }

py::buffer_info BufferLeaseViewNative::buffer_info() {
    return lease_->buffer_info(size_);
}

BufferLeaseNative::BufferLeaseNative(std::shared_ptr<BufferPoolNative> pool,
                                     std::shared_ptr<BufferHandle> handle,
                                     size_t requested_size)
    : pool_(std::move(pool)),
      handle_(std::move(handle)),
      requested_size_(requested_size) {}

BufferLeaseNative::~BufferLeaseNative() {
    try {
        release_lease(false);
    } catch (...) {
    }
}

uintptr_t BufferLeaseNative::ptr() const {
    if (!handle_) throw std::runtime_error("buffer lease is closed");
    return reinterpret_cast<uintptr_t>(handle_->ptr());
}

py::object BufferLeaseNative::buffer() {
    auto view = std::make_shared<BufferLeaseViewNative>(shared_from_this(),
                                                        requested_size_);
    return py::memoryview(py::cast(view));
}

py::buffer_info BufferLeaseNative::buffer_info(size_t size) {
    if (!handle_) throw std::runtime_error("buffer lease is closed");
    return py::buffer_info(handle_->ptr(), sizeof(uint8_t),
                           py::format_descriptor<uint8_t>::format(), 1, {size},
                           {sizeof(uint8_t)});
}

void BufferLeaseNative::add_export() {
    exports_.fetch_add(1, std::memory_order_relaxed);
}

void BufferLeaseNative::release_export() {
    exports_.fetch_sub(1, std::memory_order_relaxed);
}

void BufferLeaseNative::release() { release_lease(true); }

void BufferLeaseNative::release_lease(bool check_exported_views) {
    if (closed_) return;
    if (check_exported_views && exports_.load(std::memory_order_acquire) != 0) {
        throw std::runtime_error(
            "cannot release buffer while exported views exist");
    }
    auto pool = pool_;
    if (pool && handle_) {
        pool->release_internal(handle_);
    }
    pool_.reset();
    closed_ = true;
}

BufferPoolNative::BufferPoolNative(const py::object &store, size_t max_bytes,
                                   size_t min_size_class,
                                   py::object max_size_class, size_t alignment,
                                   bool block_on_exhaustion,
                                   py::object default_timeout,
                                   py::object max_regions)
    : store_obj_(store),
      py_client_(unwrap_store(store)),
      max_bytes_(max_bytes),
      min_size_class_(min_size_class),
      alignment_(alignment),
      block_on_exhaustion_(block_on_exhaustion) {
    if (store_obj_.is_none() || !py_client_) {
        throw std::runtime_error("MooncakeDistributedStore is not initialized");
    }
    if (min_size_class_ == 0 || alignment_ == 0) {
        throw std::runtime_error(
            "min_size_class and alignment must be positive");
    }
    if (alignment_ < sizeof(void *) || (alignment_ & (alignment_ - 1)) != 0) {
        throw std::runtime_error(
            "alignment must be a power of two and at least sizeof(void*)");
    }
    if (!py_client_->client_buffer_allocator_ ||
        py_client_->client_buffer_allocator_->size() == 0) {
        throw std::runtime_error(
            "BufferPool requires a store configured with a local buffer");
    }
    local_buffer_capacity_ = py_client_->client_buffer_allocator_->size();
    if (max_bytes == 0) {
        if (local_buffer_capacity_ >
            std::numeric_limits<size_t>::max() - local_buffer_capacity_) {
            max_bytes_ = local_buffer_capacity_;
        } else {
            max_bytes_ = local_buffer_capacity_ * 2;
        }
    } else {
        max_bytes_ = std::max(max_bytes, local_buffer_capacity_);
    }
    max_size_class_ =
        max_size_class.is_none() ? max_bytes_ : max_size_class.cast<size_t>();
    if (!default_timeout.is_none()) {
        default_timeout_ = default_timeout.cast<double>();
    }
    if (!max_regions.is_none()) {
        max_regions_ = max_regions.cast<size_t>();
    }
}

std::shared_ptr<BufferLeaseNative> BufferPoolNative::acquire(
    size_t size, py::object block, py::object timeout) {
    bool should_block =
        block.is_none() ? block_on_exhaustion_ : block.cast<bool>();
    std::optional<double> timeout_s = default_timeout_;
    if (!timeout.is_none()) timeout_s = timeout.cast<double>();
    if (timeout_s.has_value() && *timeout_s < 0) {
        throw std::runtime_error("timeout must be non-negative");
    }
    auto deadline = timeout_s.has_value()
                        ? std::chrono::steady_clock::now() +
                              std::chrono::duration<double>(*timeout_s)
                        : std::chrono::steady_clock::time_point::max();

    auto [size_class, _] = allocation_size(size);
    if (size_class > max_bytes_) {
        throw std::runtime_error("requested buffer size exceeds pool capacity");
    }
    py::gil_scoped_release release_gil;
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
        raise_if_open_locked();
        auto region = try_acquire_locked(lock, size);
        if (region.has_value())
            return make_lease_locked(std::move(*region), size);
        if (!should_block) throw std::runtime_error("buffer pool is exhausted");
        ++wait_count_;
        if (timeout_s.has_value()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error("timed out waiting for buffer");
            }
            condition_.wait_until(lock, deadline);
        } else {
            condition_.wait(lock);
        }
    }
}

void BufferPoolNative::prewarm(size_t size, size_t count) {
    (void)size;
    (void)count;
}

void BufferPoolNative::close() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) return;
    closing_ = true;
    lock.unlock();
    {
        py::gil_scoped_release release_gil;
        lock.lock();
        condition_.wait(lock, [&] { return reserved_regions_ == 0; });
        lock.unlock();
    }
    lock.lock();
    if (!in_use_.empty()) {
        closing_ = false;
        condition_.notify_all();
        throw std::runtime_error("cannot close buffer pool with active leases");
    }
    closed_ = true;
    closing_ = false;
    condition_.notify_all();
}

std::shared_ptr<BufferLeaseNative> BufferPoolNative::make_lease_locked(
    Region region, size_t requested_size) {
    auto lease = std::make_shared<BufferLeaseNative>(
        shared_from_this(), region.handle, requested_size);
    uintptr_t ptr = reinterpret_cast<uintptr_t>(region.handle->ptr());
    in_use_.insert(ptr);
    ++acquire_count_;
    return lease;
}

std::optional<BufferPoolNative::Region> BufferPoolNative::try_acquire_locked(
    std::unique_lock<std::mutex> &lock, size_t requested_size) {
    const size_t allocation_size = std::max<size_t>(requested_size, 1);
    if (!has_capacity_for_locked(allocation_size)) {
        return std::nullopt;
    }
    reserve_locked(allocation_size);
    lock.unlock();
    std::shared_ptr<BufferHandle> handle;
    bool overflow_registered = false;
    try {
        if (auto alloc_result =
                py_client_->client_buffer_allocator_->allocate(allocation_size);
            alloc_result.has_value()) {
            handle = std::make_shared<BufferHandle>(std::move(*alloc_result));
        } else {
            handle = allocate_overflow_unlocked(allocation_size);
            overflow_registered = handle != nullptr;
        }
    } catch (...) {
        lock.lock();
        unreserve_locked(allocation_size);
        throw;
    }
    lock.lock();
    unreserve_locked(allocation_size);
    if (!handle) {
        return std::nullopt;
    }
    if (closing_ || closed_) {
        Region region{handle, allocation_size, overflow_registered};
        lock.unlock();
        if (overflow_registered) {
            unregister_overflow_unlocked(region);
        }
        handle.reset();
        lock.lock();
        throw std::runtime_error("buffer pool is closing");
    }
    const uintptr_t ptr = reinterpret_cast<uintptr_t>(handle->ptr());
    Region region{handle, allocation_size, overflow_registered};
    regions_.emplace(ptr, region);
    total_bytes_ += allocation_size;
    ++allocate_count_;
    if (requested_size > max_size_class_) {
        ++oversize_allocate_count_;
    }
    return region;
}

std::shared_ptr<BufferHandle> BufferPoolNative::allocate_overflow_unlocked(
    size_t size) {
    void *ptr = nullptr;
    if (posix_memalign(&ptr, alignment_, size) != 0) {
        return nullptr;
    }
    auto data = std::shared_ptr<void>(ptr, std::free);
    int ret = -1;
    {
        py::gil_scoped_acquire acquire_gil;
        ret = py::cast<int>(store_obj_.attr("register_buffer")(
            reinterpret_cast<uintptr_t>(ptr), size));
    }
    if (ret != 0) {
        data.reset();
        throw std::runtime_error("overflow buffer registration failed");
    }
    return std::make_shared<BufferHandle>(
        ptr, size, [data = std::move(data)]() mutable { data.reset(); });
}

void BufferPoolNative::unregister_overflow_unlocked(const Region &region) {
    py::gil_scoped_acquire acquire_gil;
    int ret = py::cast<int>(store_obj_.attr("unregister_buffer")(
        reinterpret_cast<uintptr_t>(region.handle->ptr())));
    if (ret != 0) {
        throw std::runtime_error("overflow buffer unregistration failed");
    }
}

void BufferPoolNative::release_internal(std::shared_ptr<BufferHandle> &handle) {
    if (!handle) throw std::runtime_error("buffer lease is closed");
    uintptr_t ptr = reinterpret_cast<uintptr_t>(handle->ptr());
    Region region;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!in_use_.count(ptr)) {
            throw std::runtime_error("buffer lease is not active");
        }
        region = regions_.at(ptr);
        regions_.erase(ptr);
        if (total_bytes_ >= region.size) total_bytes_ -= region.size;
        in_use_.erase(ptr);
        ++release_count_;
    }
    if (region.overflow_registered) {
        unregister_overflow_unlocked(region);
    }
    region.handle.reset();
    handle.reset();
    condition_.notify_all();
}

void BufferPoolNative::reserve_locked(size_t size_class) {
    raise_if_open_locked();
    if (!has_capacity_for_locked(size_class)) {
        throw std::runtime_error("buffer pool capacity exceeded");
    }
    reserved_bytes_ += size_class;
    ++reserved_regions_;
}

void BufferPoolNative::unreserve_locked(size_t size_class) {
    reserved_bytes_ -= size_class;
    --reserved_regions_;
    condition_.notify_all();
}

std::pair<size_t, bool> BufferPoolNative::allocation_size(size_t size) const {
    size = std::max<size_t>(size, 1);
    return {size, size > max_size_class_};
}

bool BufferPoolNative::has_capacity_for_locked(size_t size_class) const {
    if (size_class > max_bytes_) {
        return false;
    }
    if (total_bytes_ > max_bytes_ - reserved_bytes_) {
        return false;
    }
    if (size_class > max_bytes_ - total_bytes_ - reserved_bytes_) {
        return false;
    }
    return !max_regions_.has_value() ||
           in_use_.size() + reserved_regions_ < *max_regions_;
}

void BufferPoolNative::raise_if_open_locked() const {
    if (closed_) {
        throw std::runtime_error("buffer pool is closed");
    }
    if (closing_) {
        throw std::runtime_error("buffer pool is closing");
    }
}

void bind_buffer_pool(py::module &m) {
    py::class_<BufferLeaseViewNative, std::shared_ptr<BufferLeaseViewNative>>(
        m, "BufferLeaseView", py::buffer_protocol())
        .def_property_readonly(
            "buffer",
            [](const py::object &self) { return py::memoryview(self); })
        .def_buffer(&BufferLeaseViewNative::buffer_info);

    py::class_<BufferLeaseNative, std::shared_ptr<BufferLeaseNative>>(
        m, "BufferLease")
        .def_property_readonly("ptr", &BufferLeaseNative::ptr)
        .def_property_readonly("size", &BufferLeaseNative::size)
        .def_property_readonly("buffer", &BufferLeaseNative::buffer)
        .def("release", &BufferLeaseNative::release)
        .def("__enter__", &BufferLeaseNative::enter,
             py::return_value_policy::reference_internal)
        .def("__exit__", &BufferLeaseNative::exit);

    py::class_<BufferPoolNative, std::shared_ptr<BufferPoolNative>>(
        m, "BufferPool")
        .def(py::init([](const py::object &store, size_t max_bytes,
                         size_t min_size_class, py::object max_size_class,
                         size_t alignment, bool block_on_exhaustion,
                         py::object default_timeout, py::object max_regions,
                         py::object prewarm_size, size_t prewarm_count) {
                 auto pool = std::make_shared<BufferPoolNative>(
                     store, max_bytes, min_size_class, max_size_class,
                     alignment, block_on_exhaustion, default_timeout,
                     max_regions);
                 if (!prewarm_size.is_none() && prewarm_count > 0) {
                     pool->prewarm(prewarm_size.cast<size_t>(), prewarm_count);
                 }
                 return pool;
             }),
             py::arg("store"), py::arg("max_bytes") = 0,
             py::arg("min_size_class") = 64 * 1024,
             py::arg("max_size_class") = py::none(),
             py::arg("alignment") = 8 * 1024 * 1024,
             py::arg("block_on_exhaustion") = true,
             py::arg("default_timeout") = py::none(),
             py::arg("max_regions") = py::none(),
             py::arg("prewarm_size") = py::none(), py::arg("prewarm_count") = 0)
        .def("acquire", &BufferPoolNative::acquire, py::arg("size"),
             py::arg("block") = py::none(), py::arg("timeout") = py::none())
        .def("buffer", &BufferPoolNative::buffer, py::arg("size"),
             py::arg("block") = py::none(), py::arg("timeout") = py::none())
        .def("prewarm", &BufferPoolNative::prewarm, py::arg("size"),
             py::arg("count"))
        .def("close", &BufferPoolNative::close);

    m.attr("RegisteredBufferPool") = m.attr("BufferPool");
    m.attr("RegisteredBufferLease") = m.attr("BufferLease");
    m.attr("RegisteredBufferLeaseView") = m.attr("BufferLeaseView");
}

}  // namespace mooncake
