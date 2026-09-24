// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/transport/mpcomm/mpcomm_adapter.h"

#ifdef USE_MPCOMM
#include "mpcomm.h"
#endif

namespace mooncake {
namespace tent {
namespace {

// The provider's numeric status is kept in the message: MPComm logs the same
// code, which is how a user correlates a TENT failure with the library's own
// output.
Status providerMissing(const char *op) {
    return Status::InternalError(
        "MPComm " + std::string(op) +
        " unavailable: this build was compiled without USE_MPCOMM");
}

#ifdef USE_MPCOMM

Status providerFailed(const char *op, int code) {
    return Status::InternalError(
        "MPComm " + std::string(op) +
        " failed, provider status=" + std::to_string(code));
}

// Thin pass-through onto libmpcomm. It adds no locking of its own because
// MPComm is thread-safe, and no policy: every decision that could be made
// differently lives above this boundary so that it can be tested there.
class LibMpcommAdapter final : public MpcommAdapter {
   public:
    bool available() const noexcept override { return true; }

    Status init(const std::string &host_id, const std::string &device_names,
                int tcp_port) override {
        // Constructed here rather than in the constructor so that merely
        // creating the transport does not touch the provider, matching the
        // lifetime the transport had before this boundary existed.
        impl_ = std::make_unique<mpcomm::MPComm>();
        int rc = impl_->init(host_id, device_names, tcp_port);
        if (rc != mpcomm::MPCOMM_SUCCESS) {
            impl_.reset();
            return providerFailed("init", rc);
        }
        return Status::OK();
    }

    int tcpPort() const override { return impl_ ? impl_->getTcpPort() : 0; }

    Status startAcceptThread() override {
        if (!impl_) return notInitialized("startAcceptThread");
        int rc = impl_->startAcceptThread();
        if (rc != mpcomm::MPCOMM_SUCCESS) {
            return providerFailed("startAcceptThread", rc);
        }
        return Status::OK();
    }

    void stopAcceptThread() override {
        if (impl_) impl_->stopAcceptThread();
    }

    void shutdown() override {
        if (!impl_) return;
        impl_->shutdown();
        impl_.reset();
    }

    Status registerMemory(void *addr, size_t length) override {
        if (!impl_) return notInitialized("registerMemory");
        int rc = impl_->registerMemory(addr, length);
        if (rc != mpcomm::MPCOMM_SUCCESS) {
            return providerFailed("registerMemory", rc);
        }
        return Status::OK();
    }

    void unregisterMemory(void *addr) override {
        if (impl_) impl_->unregisterMemory(addr);
    }

    Status publishBuffer(void *addr, size_t length, int numa_node) override {
        if (!impl_) return notInitialized("publishBuffer");
        int rc = impl_->publishBuffer(addr, length, numa_node);
        if (rc != mpcomm::MPCOMM_SUCCESS) {
            return providerFailed("publishBuffer", rc);
        }
        return Status::OK();
    }

    void unpublishBuffer(void *addr) override {
        if (impl_) impl_->unpublishBuffer(addr);
    }

    Status connect(const std::string &host_id, const std::string &tcp_addr,
                   int tcp_port) override {
        if (!impl_) return notInitialized("connect");
        int rc = impl_->connect(host_id, tcp_addr, tcp_port);
        if (rc != mpcomm::MPCOMM_SUCCESS) {
            return providerFailed("connect", rc);
        }
        return Status::OK();
    }

    Status queryRemoteBuffer(const std::string &host_id,
                             const std::string &tcp_addr,
                             int tcp_port) override {
        if (!impl_) return notInitialized("queryRemoteBuffer");
        // The provider stores the keys in its own connection record; the
        // returned description is of no use above this boundary.
        mpcomm::RemoteBufferInfo unused;
        int rc = impl_->queryRemoteBuffer(host_id, tcp_addr, tcp_port, unused);
        if (rc != mpcomm::MPCOMM_SUCCESS) {
            return providerFailed("queryRemoteBuffer", rc);
        }
        return Status::OK();
    }

    MpcommTransferHandle putAsync(uintptr_t local_addr,
                                  const std::string &host_id,
                                  uintptr_t remote_addr,
                                  size_t length) override {
        if (!impl_) return kInvalidMpcommTransferHandle;
        return toHandle(
            impl_->putAsync(local_addr, host_id, remote_addr, length));
    }

    MpcommTransferHandle getAsync(uintptr_t local_addr,
                                  const std::string &host_id,
                                  uintptr_t remote_addr,
                                  size_t length) override {
        if (!impl_) return kInvalidMpcommTransferHandle;
        return toHandle(
            impl_->getAsync(local_addr, host_id, remote_addr, length));
    }

    bool isTransferComplete(MpcommTransferHandle handle) override {
        if (!impl_ || handle == kInvalidMpcommTransferHandle) return false;
        return impl_->isTransferComplete(handle);
    }

    MpcommTransferOutcome getTransferResult(
        MpcommTransferHandle handle) override {
        MpcommTransferOutcome outcome;
        if (!impl_ || handle == kInvalidMpcommTransferHandle) return outcome;
        auto result = impl_->getTransferResult(handle);
        outcome.ok = result.error_code == mpcomm::MPCOMM_SUCCESS;
        outcome.bytes_transferred = result.bytes_transferred;
        outcome.native_status = result.error_code;
        return outcome;
    }

    void releaseTransfer(MpcommTransferHandle handle) override {
        if (impl_ && handle != kInvalidMpcommTransferHandle) {
            impl_->releaseTransfer(handle);
        }
    }

   private:
    static Status notInitialized(const char *op) {
        return Status::InternalError("MPComm " + std::string(op) +
                                     " called before init()");
    }

    // MPComm's invalid handle is normalised to this boundary's own, so that
    // nothing above needs the provider's constant.
    static MpcommTransferHandle toHandle(mpcomm::TransferHandle handle) {
        return handle == mpcomm::INVALID_TRANSFER_HANDLE
                   ? kInvalidMpcommTransferHandle
                   : static_cast<MpcommTransferHandle>(handle);
    }

    std::unique_ptr<mpcomm::MPComm> impl_;
};

#endif  // USE_MPCOMM

// Used when the provider was not compiled in. It keeps a USE_MPCOMM=OFF build
// linkable and lets the transport report a clear reason instead of failing at
// an arbitrary later point.
class UnavailableMpcommAdapter final : public MpcommAdapter {
   public:
    bool available() const noexcept override { return false; }

    Status init(const std::string &, const std::string &, int) override {
        return providerMissing("init");
    }
    int tcpPort() const override { return 0; }
    Status startAcceptThread() override {
        return providerMissing("startAcceptThread");
    }
    void stopAcceptThread() override {}
    void shutdown() override {}

    Status registerMemory(void *, size_t) override {
        return providerMissing("registerMemory");
    }
    void unregisterMemory(void *) override {}
    Status publishBuffer(void *, size_t, int) override {
        return providerMissing("publishBuffer");
    }
    void unpublishBuffer(void *) override {}

    Status connect(const std::string &, const std::string &, int) override {
        return providerMissing("connect");
    }
    Status queryRemoteBuffer(const std::string &, const std::string &,
                             int) override {
        return providerMissing("queryRemoteBuffer");
    }

    MpcommTransferHandle putAsync(uintptr_t, const std::string &, uintptr_t,
                                  size_t) override {
        return kInvalidMpcommTransferHandle;
    }
    MpcommTransferHandle getAsync(uintptr_t, const std::string &, uintptr_t,
                                  size_t) override {
        return kInvalidMpcommTransferHandle;
    }
    bool isTransferComplete(MpcommTransferHandle) override { return false; }
    MpcommTransferOutcome getTransferResult(MpcommTransferHandle) override {
        return MpcommTransferOutcome{};
    }
    void releaseTransfer(MpcommTransferHandle) override {}
};

}  // namespace

std::shared_ptr<MpcommAdapter> createDefaultMpcommAdapter() {
#ifdef USE_MPCOMM
    return std::make_shared<LibMpcommAdapter>();
#else
    return std::make_shared<UnavailableMpcommAdapter>();
#endif
}

}  // namespace tent
}  // namespace mooncake
