"""C/Python transport identities and ordinary request options; no hardware."""

import sys

import tent

assert int(tent.TransportType.NDS) == 19
assert int(tent.TransportType.MEMFABRIC) == 18
requests = [
    tent.Request(
        tent.OpCode.READ,
        address,
        1,
        index * 4096,
        4096,
        transport_hint=tent.TransportType.NDS,
        policy_name="nds_storage",
        priority=tent.PRIO_LOW,
        deadline_ns=42,
        intent_type=tent.IntentType.WEIGHT_LOADING,
    )
    for index, address in enumerate((4096, 16384))
]
assert [request.source for request in requests] == [4096, 16384]
assert [request.target_offset for request in requests] == [0, 4096]
for request in requests:
    assert request.length == 4096
    assert request.transport_hint == tent.TransportType.NDS
    assert request.policy_name == "nds_storage" and request.priority == tent.PRIO_LOW
    assert request.deadline_ns == 42
    assert request.intent_type == tent.IntentType.WEIGHT_LOADING
print(f"Python binding ({sys.argv[1]}): PASS")
