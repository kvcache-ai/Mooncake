#!/usr/bin/env python3
"""Offline evidence checks; invoked by run_oplog_batch_cluster_test.sh."""

from copy import deepcopy

from capacity import pruned, validate


def sample(floor):
    return {
        "time": floor,
        "floor": floor,
        "alarms": [],
        "live_batches": 5,
        "min_batch": floor + 1,
        "max_batch": floor + 5,
        "durable": {"batch_id": floor + 5},
        "retained_durable_batches": 5,
        "latest": {"snapshot_id": str(floor + 1), "last_included_batch_id": floor + 1},
        "fallback": {"snapshot_id": str(floor), "last_included_batch_id": floor},
    }


def fails(samples, message):
    try:
        validate(samples, 20)
    except RuntimeError as error:
        assert message in str(error), error
    else:
        raise AssertionError("invalid evidence accepted: " + message)


def main():
    samples = [sample(i) for i in (1, 2, 3)]
    assert validate(samples, 20)["final_floor"] == 3
    fails(samples[:2], "three capacity samples")
    fails(samples[::-1], "backwards")
    fails([sample(1)] * 3, "distinct positive floors")
    broken = deepcopy(samples)
    broken[1]["alarms"] = [{"alarm": 1}]
    fails(broken, "alarm")
    broken = deepcopy(samples)
    broken[1]["live_batches"] = 21
    fails(broken, "bound")
    broken = deepcopy(samples)
    broken[-1]["min_batch"] = 1
    fails(broken, "deletion")
    broken = deepcopy(samples)
    broken[-1]["fallback"] = None
    assert not pruned(broken[-1])
    broken = deepcopy(samples)
    broken[-1]["max_batch"] = 100
    fails(broken, "writer head")
    broken = deepcopy(samples)
    broken[1]["retained_durable_batches"] = 4
    fails(broken, "missing batch")
    print("PASS: capacity evidence rejects stalled pruning, alarms and unbounded keys")


if __name__ == "__main__":
    main()
