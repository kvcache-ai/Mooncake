#pragma once

// Official DeepEP elastic source import surface for Mooncake.
//
// This umbrella intentionally lives beside, not inside, the legacy EP API.  It
// keeps the imported elastic implementation discoverable while allowing the
// host launch/runtime glue to opt in file-by-file without perturbing legacy
// Buffer dispatch/combine symbols.

#include <mooncake_ep_elastic_comm.cuh>
#include <mooncake_ep_elastic_transport.cuh>
#include <mooncake_ep_elastic_layout.cuh>
#include <mooncake_ep_elastic_combine_utils.cuh>
#include <mooncake_ep_elastic_dispatch_deterministic_prologue.cuh>
#include <mooncake_ep_elastic_dispatch_official.cuh>
#include <mooncake_ep_elastic_hybrid_dispatch_official.cuh>
#include <mooncake_ep_elastic_dispatch_copy_epilogue.cuh>
#include <mooncake_ep_elastic_combine_official.cuh>
#include <mooncake_ep_elastic_hybrid_combine_official.cuh>
#include <mooncake_ep_elastic_combine_reduce_epilogue.cuh>
