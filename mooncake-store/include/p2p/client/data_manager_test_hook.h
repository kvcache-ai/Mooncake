#pragma once

// Test-only escape hatch. Deliberately NOT part of `DataManager`, which
// carries production methods only. Tests obtain it with
// `dynamic_cast<DataManagerTestHook*>(&data_manager)`.

namespace mooncake {

/**
 * @class DataManagerTestHook
 * @brief Lets a test quiesce implementation-internal background work before
 *        comparing observable state.
 *
 * An implementation is free to apply metadata callbacks, data movement and
 * bookkeeping asynchronously, so state read right after the last API call can
 * race those background workers. DrainForTest() defines the point at which
 * every already-triggered background effect has been applied. V1 applies
 * everything inline and so implements it as a no-op.
 */
class DataManagerTestHook {
   public:
    virtual ~DataManagerTestHook() = default;

    /**
     * @brief Block until all background work triggered by already-completed
     *        API calls has been applied. Idempotent; safe after Stop().
     *
     * It does NOT wait on TaskHandles the caller still holds: those only run
     * inside Wait(), which is the caller's responsibility.
     */
    virtual void DrainForTest() = 0;
};

}  // namespace mooncake
