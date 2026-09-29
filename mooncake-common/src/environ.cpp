#include "environ.h"

#include <cstdlib>

namespace mooncake {

std::optional<std::string> ProcessEnvironSource::Get(
    std::string_view name) const {
    const char* value = std::getenv(std::string(name).c_str());
    if (value == nullptr) {
        return std::nullopt;
    }
    return std::string(value);
}

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
    static const ProcessEnvironSource source;
    static const Environ process_environ(source);
    return process_environ;
}

}  // namespace mooncake
