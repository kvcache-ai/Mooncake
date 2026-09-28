#pragma once

#include <cstdlib>

#include "environ.h"

namespace mooncake::test {

// The test access boundary for Environ. Environ::Process() captures the process
// environment on its first read, so tests that change the environment between
// cases go through SetEnv()/UnsetEnv() to make code under test observe the new
// value. Not synchronized with concurrent reads.
class EnvironTestPeer {
   public:
    static void RefreshProcessEnvironment() { Environ::RefreshProcess(); }

    static int SetEnv(const char* name, const char* value, int overwrite) {
        const int result = ::setenv(name, value, overwrite);
        RefreshProcessEnvironment();
        return result;
    }

    static int UnsetEnv(const char* name) {
        const int result = ::unsetenv(name);
        RefreshProcessEnvironment();
        return result;
    }
};

}  // namespace mooncake::test
