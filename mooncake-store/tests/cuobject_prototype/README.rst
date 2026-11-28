cuObject host-buffer adapter experiment
======================================

This standalone benchmark compares Store's existing S3 adapter with an
experimental cuObject payload transport. It reuses S3 configuration, SigV4
signing and key mapping. The caller registers host buffers once and reuses
them; HTTP carries control requests while RDMA transfers the object bytes.
It is built separately and is not enabled in the Store backend factory.

The purpose is to measure client CPU cost per GiB as well as throughput.
Lower storage-client CPU demand could benefit a colocated application under
a shared CPU budget; that application-level benefit has not been measured.

Build and test
--------------

Requires C++20, CMake 3.24+, libcurl, OpenSSL, glog and GTest. CMake downloads
pinned YLT headers and checks the archive's SHA256. Protocol tests simulate
only the RDMA operation and do not require hardware or NVIDIA libraries::

  cmake -S mooncake-store/tests/cuobject_prototype -B build/cuobject \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_CUOBJECT=OFF
  cmake --build build/cuobject -j4
  ctest --test-dir build/cuobject --output-on-failure

For real transfers, install the NVIDIA cuObject client SDK and configure an
active RDMA NIC as required by that SDK. Reconfigure with ``WITH_CUOBJECT=ON``;
``CUOBJECT_INCLUDE_DIR`` and ``CUOBJECT_LIBRARY`` override discovery under
``/usr/local/cuda``. No NVIDIA binaries or server sources are included here::

  cmake -S mooncake-store/tests/cuobject_prototype -B build/cuobject \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo -DWITH_CUOBJECT=ON
  cmake --build build/cuobject -j4
  build/cuobject/cuobject_benchmark --sdk-smoke

Use a disposable bucket on an existing cuObject-capable S3 endpoint. Configure
``MOONCAKE_S3_ENDPOINT``, ``MOONCAKE_S3_BUCKET``, ``MOONCAKE_S3_REGION`` and
``MOONCAKE_S3_PATH_STYLE=true``. Supply credentials through the adapter's
``MOONCAKE_S3_ACCESS_KEY_ID`` / ``MOONCAKE_S3_SECRET_ACCESS_KEY`` variables or
their ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` aliases. SDK configuration
is process-local through ``CUFILE_ENV_PATH_JSON``. Then run, alternating order::

  build/cuobject/cuobject_benchmark --benchmark 4194304 128
  build/cuobject/cuobject_benchmark --benchmark 4194304 128 --reverse
  build/cuobject/cuobject_benchmark --benchmark 16777216 32
  build/cuobject/cuobject_benchmark --benchmark 16777216 32 --reverse

Repeat each size at least three times, alternating TCP-first/cuObject-first.
Each operation above transfers 512 MiB. JSON stdout records process CPU time,
wall time, throughput and latency; buffer registration is reported separately.
Warmups, data preparation and byte verification are outside measured intervals.
Accept results only when the process exits with status 0: records are printed
before the final byte checks, and a later failure invalidates that run.

Correctness and boundaries
--------------------------

The benchmark creates a unique temporary key. It verifies RDMA GET against a
TCP seed before attempting a body-less PUT, then writes different RDMA bytes
and verifies them via TCP. After warmup, GET destinations are poisoned and PUT
objects reset outside the timed loop so skipped transfers cannot pass using
setup data. Independent no-op PUT and no-op measured-GET mutations were both
detected. The object is deleted on normal completion.

GET requires an explicit RDMA success acknowledgment and an exact byte count;
both ``x-amz-rdma-bytes-transferred`` and the older ``x-amz-rdma-bytes`` spelling
are accepted, with conflicting counts rejected. PUT requires HTTP 200 and
``x-amz-rdma-reply: 200``. An ordinary S3 server can store an empty object from
a body-less PUT, which is why the benchmark probes GET first and uses its own
temporary key. There is no automatic retry or TCP fallback after submission.

This experiment accepts contiguous host buffers only. Multi-slice PUT, empty
objects, GPU buffers, registration-lifetime integration and concurrent batch
scheduling remain outside its scope. Batch helpers execute serially.

With the tested SDK 1.2 interface, the wrapper cannot revoke an unresolved
token. It terminates the benchmark process with exit code 2 on ambiguous
completion or cleanup failure, preserving registered allocations until process
teardown. This is unsuitable for a production Store process. Abrupt exit may
leave the temporary object; safely recoverable cancellation and ownership are
requirements for a future integration.

Measurement scope
-----------------

The accompanying measurements are a local experiment with a storage-backed
development MinIO endpoint. They measure this adapter, not a complete Store
workload. Read data fits the server's page cache. Process CPU excludes other
processes and kernel work not charged to the benchmark, and registration is
amortized across reused buffers. A CPU-constrained colocated application and
total-host CPU consumption need separate measurements before claiming those
benefits. Three repetitions with background workloads are exploratory evidence.

Local observations (2026-10-26)
-------------------------------

``measurements.jsonl`` contains all six successful runs (24 operation records
and six registration records). ``environment.json`` records the conditions
and source hashes. ``summary.csv`` contains medians and ratios of medians.
The endpoint was single-drive MinIO, GOMAXPROCS=8, with same-host traffic;
three repeats and 512 MiB per operation were used.

.. csv-table:: Adapter-level observations
   :header: "Object MiB", "Operation", "cuObject/TCP throughput", "Client CPU reduction"

   4, GET, 1.72x, 69.6%
   4, PUT, 1.07x, 48.5%
   16, GET, 2.14x, 83.9%
   16, PUT, 0.99x, 60.5%

Client CPU savings persisted while write throughput remained near TCP.
Small throughput differences should not be treated as established improvements.
