# CUDA driver loading regression tests

This standalone CMake project needs a C++11 compiler, Python 3, and the real
CUDA Toolkit headers. It does not link to CUDA and needs neither a GPU nor an
installed NVIDIA driver. It runs on Linux (`nm` and `readelf`) and macOS (`nm`
and `otool`); macOS is useful for testing loading behavior with Linux Toolkit
headers, although CUDA itself is not supported there.

```sh
cmake -S mooncake-common/tests/cuda_driver -B /tmp/mooncake-cuda-driver-test \
  -DCUDA_DRIVER_INCLUDE_DIR=/usr/local/cuda/include
cmake --build /tmp/mooncake-cuda-driver-test -j
ctest --test-dir /tmp/mooncake-cuda-driver-test --output-on-failure
```

The subprocess scenarios verify lazy shared-library import, driver load failure,
missing symbols, safe error messages, successful calls, concurrent first use,
and CUDA header-selected ABI aliases. The unavailable-driver case places an
invalid library first in the search path so it also works on hosts with a real
driver. The missing-symbol fixture exports the legacy `cuMemGetAddressRange`
while withholding `cuMemGetAddressRange_v2`, ensuring no unsafe ABI fallback.
The executable and consumer shared library are also checked for direct CUDA
driver symbol references and driver library dependencies.

Run against both CUDA 12 and CUDA 13 headers to cover the `cuCtxCreate_v2` and
`cuCtxCreate_v4` signatures. This test does not validate real GPU operations.
