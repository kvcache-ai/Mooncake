#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace mooncake::traffic {

// Redaction runs on the writer thread; the hot request path only copies the
// raw payload.
class Redactor {
   public:
    virtual ~Redactor() = default;
    virtual bool Redact(std::string_view input, std::string& output) const = 0;
};

class RedactionRegistry {
   public:
    void Add(std::uint32_t route_key, std::shared_ptr<const Redactor> redactor);
    bool Contains(std::uint32_t route_key) const;
    bool Redact(std::uint32_t route_key, std::string_view input,
                std::string& output) const;

   private:
    std::unordered_map<std::uint32_t, std::shared_ptr<const Redactor>>
        redactors_;
};

// Registers the redactors for the master routes; callers run this once before
// recording starts.
void RegisterMasterRedactors(RedactionRegistry& registry);

}  // namespace mooncake::traffic
