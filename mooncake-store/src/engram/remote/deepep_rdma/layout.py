"""DeepEP logical slots, row alignment and index limits."""

import operator

from ..rendezvous import fingerprint


class ShardLayout:
    """Table names are opaque; the model supplies the head-to-node assignment."""

    def __init__(self, tables, owners, num_shards):
        self.num_shards = operator.index(num_shards)
        if self.num_shards < 1 or set(tables) != set(owners):
            raise ValueError("Every table needs exactly one storage owner")
        widths = {s["row_bytes"] for s in tables.values()}
        if len(widths) != 1:
            raise ValueError("A remote store requires equal byte-row widths")
        self.width = widths.pop()
        self.stride = (self.width + 7) // 8 * 8
        self.tables = tables
        self.owners = {k: operator.index(v) for k, v in owners.items()}
        self.starts = {}
        counts = [0] * self.num_shards
        for name in sorted(tables):
            owner = self.owners[name]
            if not 0 <= owner < self.num_shards:
                raise ValueError(f"Invalid owner for {name}")
            self.starts[name] = counts[owner]
            counts[owner] += tables[name]["num_rows"]
        # DeepEP addresses equal logical slots; unused tail rows are never read.
        self.slot_rows = max(1, *counts)
        if self.slot_rows * self.num_shards > 2**31 - 1:
            raise ValueError("DeepEP Engram indices exceed signed int32")
        self.identity = fingerprint(
            {"tables": tables, "owners": self.owners, "num_shards": self.num_shards}
        )

    def local_names(self, shard_id):
        return [name for name in sorted(self.tables) if self.owners[name] == shard_id]
