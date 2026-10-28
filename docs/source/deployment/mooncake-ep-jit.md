# JIT Kernel Compilation

Mooncake EP JIT currently supports only the NVIDIA (CUDA) backend.

Instead of shipping every kernel specialization in the library, Mooncake EP
compiles several of its device kernels at runtime. The first call that needs a
given kernel variant runs nvcc, caches the resulting cubin on disk, and reuses
it for the rest of the process and for later runs.

As a result, a deployed Mooncake EP needs more than
`libmooncake_ep_device.so`. It also needs a working nvcc, the kernel headers,
the CUDA headers, a writable cache directory, and, for builds with
`USE_NCCL_DEVICE=ON`, the NCCL device headers. Each of these is found through an
environment variable, with a fallback compiled into the library. A build used
where it was built, or an installed wheel, usually needs no configuration. The
environment variables cover the other cases: relocated installs, containers,
read-only filesystems, and cross-architecture builds.

## Path resolution

Each path comes from the first rule in its list that gives a value. The
compiled-in defaults are set by CMake at build time and point into the build
tree.

**Kernel headers** (the Mooncake EP and transfer engine headers that the JIT
compiles against):

1. The `MOONCAKE_EP_JIT_SOURCE_DIR` environment variable.
2. `$MOONCAKE_EP_HOME/include/mooncake_ep`.
3. `mooncake_ep_jit/include/mooncake_ep` next to `libmooncake_ep_device.so`, if
   that directory exists (the layout of an installed wheel).
4. The compiled-in `MOONCAKE_EP_JIT_SOURCE_DIR`.

**NCCL device headers** (only for builds with `USE_NCCL_DEVICE=ON`):

1. The `MOONCAKE_EP_JIT_BUILD_INCLUDE_DIR` environment variable.
2. `$NCCL_HOME/include`.
3. The compiled-in `MOONCAKE_EP_JIT_BUILD_INCLUDE_DIR`.

**CUDA headers:**

1. The `MOONCAKE_EP_JIT_CUDA_INCLUDE_DIR` environment variable.
2. `$CUDA_HOME/include`, or `$CUDA_PATH/include` if `CUDA_HOME` is not set.
3. The compiled-in `MOONCAKE_EP_JIT_CUDA_INCLUDE_DIR`.

**Compiler:**

1. The `MOONCAKE_EP_JIT_NVCC` environment variable.
2. The `NVCC` environment variable.
3. `$CUDA_HOME/bin/nvcc`, or `$CUDA_PATH/bin/nvcc` if `CUDA_HOME` is not set,
   if that file exists.
4. `nvcc` from `PATH`.

## Cache

Compiled cubins are cached in `MOONCAKE_EP_JIT_CACHE_DIR`, which defaults to
`/tmp/mooncake_ep/jit`.

The cache key combines two fingerprints:

- **The kernel variant**: kernel family, variant name, entry point, generated
  source, target SM and architecture, and runtime key.
- **The compile environment**: compiler identity, the contents of the kernel,
  transfer engine and NCCL header trees, and the full list of compile options.

This has two consequences:

- **The cache invalidates itself.** Editing a kernel header, switching
  compilers, changing architecture flags, or using different include
  directories all change the key, so a new entry is compiled. A stale cubin is
  never loaded, and there is no need to clear the cache after a rebuild.
- **Old entries are never removed.** Superseded cubins stay on disk, so on a
  machine that rebuilds often the cache keeps growing. Deleting the directory is
  always safe; the only cost is recompiling.

Cache writes are atomic. Each cubin is first written to a temporary file named
with the process ID and thread ID, then renamed into place. Ranks on the same
node can therefore fill a shared cache at the same time without corrupting each
other's entries.

Set `MOONCAKE_EP_JIT_CACHE_DIR` to another location when `/tmp` is read-only,
too small, or not shared across containers the way you expect. Putting the
cache on a persistent volume also avoids the first-call compile delay each time
a new container starts.


## Diagnostics

Set `MOONCAKE_EP_JIT_LOG=1` to print JIT logs to stderr. Each line starts with
`[mooncake_ep jit] pid=<pid>`. The log shows cache hits and misses, compile
attempts and failures, and cache write errors (mkdir, open, write, and rename).
It is the quickest way to find a cache directory that is not writable or is
full.

Each kernel variant has its own directory in the cache, containing
`kernel.cu` (the generated source), `kernel.cubin`, `metadata.json`, and, after
a compile, `compile.log` (the nvcc output).

When a variant fails to compile, the failure is recorded in two places:

- **In the process**: the variant is not compiled again, and its warning is
  printed only once.
- **On disk**: a `compile.failed` file is written to the variant's cache
  directory. Later processes that use the same cache skip the compile and
  report the failure without running nvcc again.

To retry after fixing the cause (for example, a missing header), delete that
variant's directory or the whole cache directory. Changes that alter the cache
key, such as editing a header or changing compile options, retry automatically
because they use a new directory.

## Deployment checklist

- **nvcc** can be found through `MOONCAKE_EP_JIT_NVCC`, `NVCC`, `CUDA_HOME`,
  `CUDA_PATH`, or `PATH`.
- **Kernel headers** can be found. Installed wheels and builds used where they
  were built need no setup. If the library was moved after it was built, set
  `MOONCAKE_EP_HOME` or `MOONCAKE_EP_JIT_SOURCE_DIR`.
- **CUDA headers** can be found through `CUDA_HOME`, `CUDA_PATH`, or
  `MOONCAKE_EP_JIT_CUDA_INCLUDE_DIR`.
- **NCCL device headers** (only for `USE_NCCL_DEVICE=ON` builds) can be found
  through `NCCL_HOME` or `MOONCAKE_EP_JIT_BUILD_INCLUDE_DIR`.
- **The cache directory** is writable. Set `MOONCAKE_EP_JIT_CACHE_DIR` if `/tmp`
  is not suitable.
- **The first call** to each kernel variant includes compile time. Later calls,
  and later runs that share the cache, do not.
