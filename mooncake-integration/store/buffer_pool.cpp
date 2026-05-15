#include "buffer_pool.h"

#include <pybind11/gil.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "pyclient.h"
#include "real_client.h"
#include "types.h"

namespace py = pybind11;

namespace mooncake {

class RegisteredBufferPoolNative;
class RegisteredBufferLeaseNative;

class RegisteredBufferLeaseViewNative {
   public:
    RegisteredBufferLeaseViewNative(
        std::shared_ptr<RegisteredBufferLeaseNative> lease, size_t size);
    ~RegisteredBufferLeaseViewNative();

    RegisteredBufferLeaseViewNative(const RegisteredBufferLeaseViewNative &) =
        delete;
    RegisteredBufferLeaseViewNative &operator=(
        const RegisteredBufferLeaseViewNative &) = delete;

    py::buffer_info buffer_info();

   private:
    std::shared_ptr<RegisteredBufferLeaseNative> lease_;
    size_t size_ = 0;
};

class RegisteredBufferLeaseNative
    : public std::enable_shared_from_this<RegisteredBufferLeaseNative> {
   public:
    RegisteredBufferLeaseNative(
        std::shared_ptr<RegisteredBufferPoolNative> pool,
        std::shared_ptr<BufferHandle> handle, size_t requested_size);
    ~RegisteredBufferLeaseNative();

    RegisteredBufferLeaseNative(const RegisteredBufferLeaseNative &) = delete;
    RegisteredBufferLeaseNative &operator=(
        const RegisteredBufferLeaseNative &) = delete;

    uintptr_t ptr() const;
    size_t size() const { return requested_size_; }
    py::object buffer();
    py::buffer_info buffer_info(size_t size);
    void add_export();
    void release_export();
    RegisteredBufferLeaseNative &enter() { return *this; }
    void exit(const py::object &, const py::object &, const py::object &) {
        release();
    }
    void release();

   private:
    void release_lease(bool check_exported_views);

    std::shared_ptr<RegisteredBufferPoolNative> pool_;
    std::shared_ptr<BufferHandle> handle_;
    size_t requested_size_ = 0;
    bool closed_ = false;
    std::atomic<size_t> exports_{0};
};

class RegisteredBufferPoolNative
    : public std::enable_shared_from_this<RegisteredBufferPoolNative> {
   public:
    RegisteredBufferPoolNative(const py::object &store, size_t max_bytes,
                               size_t min_size_class, py::object max_size_class,
                               size_t alignment, bool block_on_exhaustion,
                               py::object default_timeout,
                               py::object max_regions);
    ~RegisteredBufferPoolNative() {
        try {
            close();
        } catch (...) {
        }
    }

    std::shared_ptr<RegisteredBufferLeaseNative> acquire(size_t size,
                                                         py::object block,
                                                         py::object timeout);
    std::shared_ptr<RegisteredBufferLeaseNative> buffer(size_t size,
                                                        py::object block,
                                                        py::object timeout) {
        return acquire(size, block, timeout);
    }
    void prewarm(size_t size, size_t count);
    void close();

   private:
    struct Region {
        std::shared_ptr<BufferHandle> handle;
        size_t size = 0;
    };

    friend class RegisteredBufferLeaseNative;

    std::shared_ptr<RegisteredBufferLeaseNative> make_lease_locked(
        Region region, size_t requested_size);
    std::optional<Region> try_acquire_locked(std::unique_lock<std::mutex> &lock,
                                             size_t requested_size);
    Region allocate_region_locked(std::unique_lock<std::mutex> &lock,
                                  size_t size_class);
    void release_internal(const std::shared_ptr<BufferHandle> &handle);
    void unregister_region_unlocked(const Region &region);
    void reserve_locked(size_t size_class);
    void unreserve_locked(size_t size_class);
    std::pair<size_t, bool> allocation_size(size_t size) const;
    size_t align_size(size_t size) const;
    bool has_capacity_for_locked(size_t size_class) const;
    void raise_if_open_locked() const;

    py::object store_obj_;
    size_t max_bytes_ = 0;
    size_t min_size_class_ = 0;
    size_t max_size_class_ = 0;
    size_t alignment_ = 0;
    bool block_on_exhaustion_ = true;
    std::optional<double> default_timeout_;
    std::optional<size_t> max_regions_;

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::unordered_map<size_t, std::deque<Region>> free_;
    std::unordered_map<uintptr_t, Region> regions_;
    std::unordered_set<uintptr_t> in_use_;
    bool closed_ = false;
    bool closing_ = false;
    size_t total_bytes_ = 0;
    size_t reserved_bytes_ = 0;
    size_t reserved_regions_ = 0;
    size_t acquire_count_ = 0;
    size_t reuse_count_ = 0;
    size_t allocate_count_ = 0;
    size_t oversize_allocate_count_ = 0;
    size_t wait_count_ = 0;
    size_t release_count_ = 0;
    double register_s_ = 0.0;
    double unregister_s_ = 0.0;
};

RegisteredBufferLeaseViewNative::RegisteredBufferLeaseViewNative(
    std::shared_ptr<RegisteredBufferLeaseNative> lease, size_t size)
    : lease_(std::move(lease)), size_(size) {
    lease_->add_export();
}

RegisteredBufferLeaseViewNative::~RegisteredBufferLeaseViewNative() {
    lease_->release_export();
}

py::buffer_info RegisteredBufferLeaseViewNative::buffer_info() {
    return lease_->buffer_info(size_);
}

RegisteredBufferLeaseNative::RegisteredBufferLeaseNative(
    std::shared_ptr<RegisteredBufferPoolNative> pool,
    std::shared_ptr<BufferHandle> handle, size_t requested_size)
    : pool_(std::move(pool)),
      handle_(std::move(handle)),
      requested_size_(requested_size) {}

RegisteredBufferLeaseNative::~RegisteredBufferLeaseNative() {
    try {
        release_lease(false);
    } catch (...) {
    }
}

uintptr_t RegisteredBufferLeaseNative::ptr() const {
    if (!handle_) throw std::runtime_error("registered buffer lease is closed");
    return reinterpret_cast<uintptr_t>(handle_->ptr());
}

py::object RegisteredBufferLeaseNative::buffer() {
    auto view = std::make_shared<RegisteredBufferLeaseViewNative>(
        shared_from_this(), requested_size_);
    return py::memoryview(py::cast(view));
}

py::buffer_info RegisteredBufferLeaseNative::buffer_info(size_t size) {
    if (!handle_) throw std::runtime_error("registered buffer lease is closed");
    return py::buffer_info(handle_->ptr(), sizeof(uint8_t),
                           py::format_descriptor<uint8_t>::format(), 1, {size},
                           {sizeof(uint8_t)});
}

void RegisteredBufferLeaseNative::add_export() {
    exports_.fetch_add(1, std::memory_order_relaxed);
}

void RegisteredBufferLeaseNative::release_export() {
    exports_.fetch_sub(1, std::memory_order_relaxed);
}

void RegisteredBufferLeaseNative::release() { release_lease(true); }

void RegisteredBufferLeaseNative::release_lease(bool check_exported_views) {
    if (closed_) return;
    if (check_exported_views && exports_.load(std::memory_order_acquire) != 0) {
        throw std::runtime_error(
            "cannot release registered buffer while exported views exist");
    }
    auto handle = handle_;
    auto pool = pool_;
    if (pool && handle) {
        pool->release_internal(handle);
    }
    handle_.reset();
    pool_.reset();
    closed_ = true;
}

RegisteredBufferPoolNative::RegisteredBufferPoolNative(
    const py::object &store, size_t max_bytes, size_t min_size_class,
    py::object max_size_class, size_t alignment, bool block_on_exhaustion,
    py::object default_timeout, py::object max_regions)
    : store_obj_(store),
      max_bytes_(max_bytes),
      min_size_class_(min_size_class),
      max_size_class_(max_size_class.is_none() ? max_bytes
                                               : max_size_class.cast<size_t>()),
      alignment_(alignment),
      block_on_exhaustion_(block_on_exhaustion) {
    if (store_obj_.is_none()) {
        throw std::runtime_error("MooncakeDistributedStore is not initialized");
    }
    if (max_bytes_ == 0) {
        throw std::runtime_error("max_bytes must be positive");
    }
    if (min_size_class_ == 0 || alignment_ == 0) {
        throw std::runtime_error(
            "min_size_class and alignment must be positive");
    }
    if (alignment_ < sizeof(void *) || (alignment_ & (alignment_ - 1)) != 0) {
        throw std::runtime_error(
            "alignment must be a power of two and at least sizeof(void*)");
    }
    max_size_class_ = std::min(max_size_class_, max_bytes_);
    if (!default_timeout.is_none()) {
        default_timeout_ = default_timeout.cast<double>();
    }
    if (!max_regions.is_none()) {
        max_regions_ = max_regions.cast<size_t>();
    }
}

std::shared_ptr<RegisteredBufferLeaseNative>
RegisteredBufferPoolNative::acquire(size_t size, py::object block,
                                    py::object timeout) {
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
        if (!should_block)
            throw std::runtime_error("registered buffer pool is exhausted");
        ++wait_count_;
        if (timeout_s.has_value()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error(
                    "timed out waiting for registered buffer");
            }
            condition_.wait_until(lock, deadline);
        } else {
            condition_.wait(lock);
        }
    }
}

void RegisteredBufferPoolNative::prewarm(size_t size, size_t count) {
    auto [size_class, oversize] = allocation_size(size);
    if (oversize) {
        throw std::runtime_error("cannot prewarm oversize buffers");
    }
    py::gil_scoped_release release_gil;
    for (size_t i = 0; i < count; ++i) {
        std::unique_lock<std::mutex> lock(mutex_);
        raise_if_open_locked();
        if (!has_capacity_for_locked(size_class)) {
            throw std::runtime_error(
                "registered buffer pool capacity exceeded");
        }
        Region region = allocate_region_locked(lock, size_class);
        if (closed_ || closing_) {
            lock.unlock();
            unregister_region_unlocked(region);
            lock.lock();
            throw std::runtime_error("registered buffer pool is closing");
        }
        try {
            free_[size_class].push_back(std::move(region));
        } catch (...) {
            lock.unlock();
            unregister_region_unlocked(region);
            throw;
        }
        condition_.notify_all();
    }
}

void RegisteredBufferPoolNative::close() {
    std::deque<Region> to_unregister;
    {
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
        size_t active_count = in_use_.size();
        if (active_count) {
            closing_ = false;
            condition_.notify_all();
            throw std::runtime_error(
                "cannot close registered buffer pool with active leases");
        }
        for (auto &entry : free_) {
            while (!entry.second.empty()) {
                to_unregister.push_back(std::move(entry.second.front()));
                entry.second.pop_front();
            }
        }
        free_.clear();
    }
    while (!to_unregister.empty()) {
        Region region = to_unregister.front();
        to_unregister.pop_front();
        try {
            unregister_region_unlocked(region);
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            free_[region.size].push_back(region);
            while (!to_unregister.empty()) {
                free_[to_unregister.front().size].push_back(
                    to_unregister.front());
                to_unregister.pop_front();
            }
            closing_ = false;
            condition_.notify_all();
            throw;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        closing_ = false;
        condition_.notify_all();
    }
}

std::shared_ptr<RegisteredBufferLeaseNative>
RegisteredBufferPoolNative::make_lease_locked(Region region,
                                              size_t requested_size) {
    auto lease = std::make_shared<RegisteredBufferLeaseNative>(
        shared_from_this(), region.handle, requested_size);
    uintptr_t ptr = reinterpret_cast<uintptr_t>(region.handle->ptr());
    in_use_.insert(ptr);
    ++acquire_count_;
    return lease;
}

std::optional<RegisteredBufferPoolNative::Region>
RegisteredBufferPoolNative::try_acquire_locked(
    std::unique_lock<std::mutex> &lock, size_t requested_size) {
    auto [size_class, oversize] = allocation_size(requested_size);
    if (!oversize) {
        auto free_iter = free_.find(size_class);
        if (free_iter != free_.end() && !free_iter->second.empty()) {
            Region region = std::move(free_iter->second.back());
            free_iter->second.pop_back();
            ++reuse_count_;
            return region;
        }
    }
    if (!has_capacity_for_locked(size_class)) {
        return std::nullopt;
    }
    Region region = allocate_region_locked(lock, size_class);
    if (closing_ || closed_) {
        lock.unlock();
        unregister_region_unlocked(region);
        lock.lock();
        throw std::runtime_error("registered buffer pool is closing");
    }
    if (oversize) {
        ++oversize_allocate_count_;
    } else {
        ++allocate_count_;
    }
    return region;
}

RegisteredBufferPoolNative::Region
RegisteredBufferPoolNative::allocate_region_locked(
    std::unique_lock<std::mutex> &lock, size_t size_class) {
    reserve_locked(size_class);
    lock.unlock();
    void *ptr = nullptr;
    std::shared_ptr<BufferHandle> handle;
    int ret = 0;
    auto start = std::chrono::steady_clock::now();
    try {
        if (posix_memalign(&ptr, alignment_, size_class) != 0) {
            throw std::bad_alloc();
        }
        auto data = std::shared_ptr<void>(ptr, std::free);
        handle = std::make_shared<BufferHandle>(
            ptr, size_class,
            [data = std::move(data)]() mutable { data.reset(); });
        py::gil_scoped_acquire acquire_gil;
        ret = py::cast<int>(store_obj_.attr("register_buffer")(
            reinterpret_cast<uintptr_t>(ptr), size_class));
    } catch (...) {
        lock.lock();
        unreserve_locked(size_class);
        throw;
    }
    double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    if (ret != 0) {
        lock.lock();
        register_s_ += elapsed;
        unreserve_locked(size_class);
        lock.unlock();
        throw std::runtime_error("register_buffer failed");
    }
    uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
    Region region{handle, size_class};
    try {
        lock.lock();
        register_s_ += elapsed;
        unreserve_locked(size_class);
        regions_.emplace(addr, region);
        total_bytes_ += size_class;
        return region;
    } catch (...) {
        if (lock.owns_lock()) {
            lock.unlock();
        }
        py::gil_scoped_acquire acquire_gil;
        int unregister_ret = py::cast<int>(store_obj_.attr("unregister_buffer")(
            reinterpret_cast<uintptr_t>(ptr)));
        if (unregister_ret != 0) {
            LOG(ERROR)
                << "unregister_buffer failed while rolling back allocation";
        }
        throw;
    }
}

void RegisteredBufferPoolNative::release_internal(
    const std::shared_ptr<BufferHandle> &handle) {
    if (!handle) throw std::runtime_error("registered buffer lease is closed");
    uintptr_t ptr = reinterpret_cast<uintptr_t>(handle->ptr());
    Region region;
    bool should_unregister = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!in_use_.count(ptr)) {
            throw std::runtime_error("registered buffer lease is not active");
        }
        region = regions_.at(ptr);
        should_unregister =
            closed_ || closing_ || region.size > max_size_class_;
        if (!should_unregister) {
            try {
                free_[region.size].push_back(region);
            } catch (...) {
                condition_.notify_all();
                throw;
            }
            in_use_.erase(ptr);
            ++release_count_;
            condition_.notify_all();
            return;
        }
    }
    unregister_region_unlocked(region);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        in_use_.erase(ptr);
        ++release_count_;
        condition_.notify_all();
    }
}

void RegisteredBufferPoolNative::reserve_locked(size_t size_class) {
    raise_if_open_locked();
    if (!has_capacity_for_locked(size_class)) {
        throw std::runtime_error("registered buffer pool capacity exceeded");
    }
    reserved_bytes_ += size_class;
    ++reserved_regions_;
}

void RegisteredBufferPoolNative::unreserve_locked(size_t size_class) {
    reserved_bytes_ -= size_class;
    --reserved_regions_;
    condition_.notify_all();
}

std::pair<size_t, bool> RegisteredBufferPoolNative::allocation_size(
    size_t size) const {
    size = std::max<size_t>(size, 1);
    if (size > max_size_class_) {
        return {align_size(size), true};
    }
    if (size <= min_size_class_) return {min_size_class_, false};
    if (size <= alignment_) {
        size_t power = 1;
        while (power < size) power <<= 1;
        return {power, false};
    }
    return {align_size(size), false};
}

size_t RegisteredBufferPoolNative::align_size(size_t size) const {
    if (size > std::numeric_limits<size_t>::max() - alignment_ + 1) {
        throw std::runtime_error("buffer size overflow");
    }
    return ((size + alignment_ - 1) / alignment_) * alignment_;
}

bool RegisteredBufferPoolNative::has_capacity_for_locked(
    size_t size_class) const {
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
           regions_.size() + reserved_regions_ < *max_regions_;
}

void RegisteredBufferPoolNative::raise_if_open_locked() const {
    if (closed_) {
        throw std::runtime_error("registered buffer pool is closed");
    }
    if (closing_) {
        throw std::runtime_error("registered buffer pool is closing");
    }
}

void RegisteredBufferPoolNative::unregister_region_unlocked(
    const Region &region) {
    auto start = std::chrono::steady_clock::now();
    int ret;
    py::gil_scoped_acquire acquire_gil;
    ret = py::cast<int>(store_obj_.attr("unregister_buffer")(
        reinterpret_cast<uintptr_t>(region.handle->ptr())));
    double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    std::lock_guard<std::mutex> lock(mutex_);
    unregister_s_ += elapsed;
    if (ret != 0) {
        throw std::runtime_error("unregister_buffer failed");
    }
    uintptr_t ptr = reinterpret_cast<uintptr_t>(region.handle->ptr());
    regions_.erase(ptr);
    if (total_bytes_ >= region.size) total_bytes_ -= region.size;
}

void bind_buffer_pool(py::module &m) {
    py::class_<RegisteredBufferLeaseViewNative,
               std::shared_ptr<RegisteredBufferLeaseViewNative>>(
        m, "RegisteredBufferLeaseView", py::buffer_protocol())
        .def_property_readonly(
            "buffer",
            [](const py::object &self) { return py::memoryview(self); })
        .def_buffer(&RegisteredBufferLeaseViewNative::buffer_info);

    py::class_<RegisteredBufferLeaseNative,
               std::shared_ptr<RegisteredBufferLeaseNative>>(
        m, "RegisteredBufferLease")
        .def_property_readonly("ptr", &RegisteredBufferLeaseNative::ptr)
        .def_property_readonly("size", &RegisteredBufferLeaseNative::size)
        .def_property_readonly("buffer", &RegisteredBufferLeaseNative::buffer)
        .def("release", &RegisteredBufferLeaseNative::release)
        .def("__enter__", &RegisteredBufferLeaseNative::enter,
             py::return_value_policy::reference_internal)
        .def("__exit__", &RegisteredBufferLeaseNative::exit);

    py::class_<RegisteredBufferPoolNative,
               std::shared_ptr<RegisteredBufferPoolNative>>(
        m, "RegisteredBufferPool")
        .def(py::init([](const py::object &store, size_t max_bytes,
                         size_t min_size_class, py::object max_size_class,
                         size_t alignment, bool block_on_exhaustion,
                         py::object default_timeout, py::object max_regions,
                         py::object prewarm_size, size_t prewarm_count) {
                 auto pool = std::make_shared<RegisteredBufferPoolNative>(
                     store, max_bytes, min_size_class, max_size_class,
                     alignment, block_on_exhaustion, default_timeout,
                     max_regions);
                 if (!prewarm_size.is_none() && prewarm_count > 0) {
                     pool->prewarm(prewarm_size.cast<size_t>(), prewarm_count);
                 }
                 return pool;
             }),
             py::arg("store"), py::arg("max_bytes"),
             py::arg("min_size_class") = 64 * 1024,
             py::arg("max_size_class") = py::none(),
             py::arg("alignment") = 8 * 1024 * 1024,
             py::arg("block_on_exhaustion") = true,
             py::arg("default_timeout") = py::none(),
             py::arg("max_regions") = py::none(),
             py::arg("prewarm_size") = py::none(), py::arg("prewarm_count") = 0)
        .def("acquire", &RegisteredBufferPoolNative::acquire, py::arg("size"),
             py::arg("block") = py::none(), py::arg("timeout") = py::none())
        .def("buffer", &RegisteredBufferPoolNative::buffer, py::arg("size"),
             py::arg("block") = py::none(), py::arg("timeout") = py::none())
        .def("prewarm", &RegisteredBufferPoolNative::prewarm, py::arg("size"),
             py::arg("count"))
        .def("close", &RegisteredBufferPoolNative::close);
}

}  // namespace mooncake
