# NDS public header fixture

`ascend_nds.h` is the unmodified public API header from UBSIO `ubsio-nds`.
Its original copyright notice and Mulan PSL v2 license are retained.
The CPU test runner checks its SHA-256 checksum with LF line endings.

CPU tests build `nds_test_backend.cpp` against this contract into an isolated
mock SDK directory. Production builds use the externally supplied NDS SDK;
neither this header nor the mock library is installed as part of TENT.
