# Architecture overview

## Layers

`GraphDb` is the supported process-level facade. It parses a schema, owns one
node shard and one or more edge shards, assigns topology sequence numbers, and
routes logical property names to a physical shard and column.

Each shard is an `LSMStore`. Its mutable topology is held in MemTables. A flush
creates an SST with topology, indexes, and fixed-width base property columns.
The LSM mode can compact files through multiple levels; the optional CSR mode
rebuilds an immutable read snapshot from visible SSTs.

Property-only updates are deliberately separated from topology insertion:

```text
GraphDb
  +-- edge shard 0 (LSMStore)
  +-- edge shard N (LSMStore)
  +-- node shard   (LSMStore)
  `-- PropertyUpdateManager
        +-- pending-update index
        +-- bounded rotating buffers
        `-- flush worker
              `-- PropertyDeltaStore per shard
                    +-- append-only manifest
                    +-- immutable mmap delta files
                    +-- per-target read views
                    `-- merge worker
```

## Ownership and shutdown

`GraphDb` exclusively owns all shards and the property-update manager. Its
destructor first flushes engine-owned property buffers, then waits for shard
background work, then destroys shards. `LSMStore` stops CSR and compaction
workers before releasing versions, mapped data, locks, and MemTables.

Public callers must stop their own read/write threads before destroying a
`GraphDb`. Destruction is a process-local synchronization boundary, not a
concurrent API operation.

## Read path

For a requested property, `GraphDb` resolves the logical name and locates the
topology record. Resolution order is:

1. the manager's immediately visible pending value;
2. the target SST's delta read view, newest file/version first;
3. a merged replacement property column, when present;
4. the original property column in the SST.

Scans copy matching pending values before capturing immutable delta views. This
ordering closes the buffer-to-delta handoff race: a committed process-local
update remains visible in at least one of the two layers.

## Delta identity

A physical delta chain is identified by object kind, shard, base SST file ID,
base generation, and property-column ID. A logical record adds source,
destination, direction, edge type, and immutable base topology sequence.
Property commit sequences order repeated values independently from topology
sequence numbers.

## Merge and compaction interaction

Publishing the delta that reaches the configured chain threshold schedules a
background merge. The worker materializes a replacement property column,
publishes a new immutable read view, appends the manifest change, and retires
old files only after readers release their shared ownership.

Compaction takes the property-delta preparation barrier, blocks new delta
attachment for its inputs, drains persistent chains into their base columns,
and holds the barrier through SST publication. Compaction therefore never
reads a base property file while an unaccounted delta chain is being attached.

## Compatibility boundary

The old benchmark programs still configure parts of the storage engine through
gflags. `GraphDbOptions` is the public interface and temporarily bridges its
values into those legacy globals while shards are constructed. Only one
independently configured `GraphDb` should therefore be open in a process; this
restriction is tracked in `docs/known-limitations.md`.
