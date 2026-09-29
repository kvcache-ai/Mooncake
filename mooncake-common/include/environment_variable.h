#pragma once

namespace mooncake {

template <typename T>
struct EnvironmentVariable {
    const char* name;
};

#define MC_DEFINE_ENV_VAR(Type, Name) \
    inline static constexpr ::mooncake::EnvironmentVariable<Type> Name { #Name }

}  // namespace mooncake
