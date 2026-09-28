#pragma once

// Official DeepEP elastic source import surface for Mooncake.
//
// This umbrella intentionally lives under include/elastic, not in the legacy EP
// include root.  It keeps the imported elastic implementation discoverable
// while allowing the host launch/runtime glue to opt in file-by-file without
// perturbing legacy Buffer dispatch/combine symbols.

#include <elastic/mooncake_ep_elastic_comm.cuh>
#include <elastic/mooncake_ep_elastic_transport.cuh>
#include <elastic/mooncake_ep_elastic_layout.cuh>
#include <elastic/mooncake_ep_elastic_combine_utils.cuh>
#include <elastic/mooncake_ep_elastic_dispatch_deterministic_prologue.cuh>
#include <elastic/mooncake_ep_elastic_dispatch_official.cuh>
#include <elastic/mooncake_ep_elastic_hybrid_dispatch_official.cuh>
#include <elastic/mooncake_ep_elastic_dispatch_copy_epilogue.cuh>
#include <elastic/mooncake_ep_elastic_combine_official.cuh>
#include <elastic/mooncake_ep_elastic_hybrid_combine_official.cuh>
#include <elastic/mooncake_ep_elastic_combine_reduce_epilogue.cuh>
