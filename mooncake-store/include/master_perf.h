#pragma once
// Ubdiag perf-point integration for the mooncake_master process.
// Include this header (at most once per translation unit) in any .cpp that
// needs to construct UbDiag::PerfPoint with a MASTER_* key.
//
// The program name "mooncake_master" keeps master shards separate from the
// client-side "mooncake_store" shards in the shared-memory region.
#define UBDIAG_PERF_DEF_FILE "mooncake_perf_points.def"
#define UBDIAG_PROGRAM_NAME "mooncake_master"
#include "ubdiag/auto_perf.h"
