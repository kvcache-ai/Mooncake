#include "environ.h"

#include <cstdlib>
#include <map>
#include <mutex>

#if defined(__APPLE__)
#include <crt_externs.h>
#else
#include <unistd.h>
#endif

namespace mooncake {
namespace {

char** ProcessEnvironment() {
#if defined(__APPLE__)
    return *_NSGetEnviron();
#else
    return environ;
#endif
}

// Serves the process environment from a snapshot taken on the first Get().
class ProcessEnvironSource final : public EnvironSource {
   public:
    std::optional<std::string> Get(std::string_view name) const override {
        std::call_once(captured_, [this] { snapshot_ = Capture(); });
        const auto it = snapshot_.find(name);
        if (it == snapshot_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    // Not synchronized with Get(); see Environ::RefreshProcess().
    void Refresh() {
        std::call_once(captured_, [] {});
        snapshot_ = Capture();
    }

   private:
    using Snapshot = std::map<std::string, std::string, std::less<>>;

    static Snapshot Capture() {
        Snapshot snapshot;
        for (char** entry = ProcessEnvironment(); entry != nullptr && *entry;
             ++entry) {
            const std::string_view variable(*entry);
            const size_t separator = variable.find('=');
            if (separator == std::string_view::npos) {
                continue;
            }
            // Like getenv(), keep the first definition of a duplicated name.
            snapshot.emplace(variable.substr(0, separator),
                             variable.substr(separator + 1));
        }
        return snapshot;
    }

    mutable std::once_flag captured_;
    mutable Snapshot snapshot_;
};

// Never destroyed, so Process() stays usable from other static destructors.
ProcessEnvironSource& ProcessSource() {
    static auto* source = new ProcessEnvironSource();
    return *source;
}

}  // namespace

std::optional<std::string> MapEnvironSource::Get(std::string_view name) const {
    const auto it = values_.find(name);
    if (it == values_.end()) {
        return std::nullopt;
    }
    return it->second;
}

void MapEnvironSource::Set(std::string name, std::string value) {
    values_.insert_or_assign(std::move(name), std::move(value));
}

void MapEnvironSource::Unset(std::string_view name) {
    const auto it = values_.find(name);
    if (it != values_.end()) {
        values_.erase(it);
    }
}

const Environ& Environ::Process() {
    static const auto* process_environ = new Environ(ProcessSource());
    return *process_environ;
}

void Environ::RefreshProcess() { ProcessSource().Refresh(); }

}  // namespace mooncake
