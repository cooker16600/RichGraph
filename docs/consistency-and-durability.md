# Consistency and durability

## Topology and property versions

Topology insertions receive a monotonically increasing topology sequence.
Property updates refer to the immutable sequence of the base node/edge record
and receive a separate monotonically increasing property commit sequence. A
multi-property `UpdateNode` or `UpdateEdge` call assigns one commit sequence to
the entire batch and never splits that batch across rotating buffers.

The current public reads request the latest version. Point reads and scans use
the same pending/delta/base precedence, but the public API does not yet expose
an arbitrary historical read timestamp.

## Visibility

An update is process-locally visible before a successful update call returns:
the update manager first publishes the value to its pending index and then
places the batch in the active buffer. During background flush the value is
removed from the pending index only after the corresponding delta file has
been published.

This visibility guarantee assumes calls do not race with `GraphDb`
destruction. A background flush failure becomes sticky; subsequent barriers
report the failure rather than silently discarding the failed buffer.

## Persistence

A successful `UpdateNode`/`UpdateEdge` is not by itself an fsync boundary. It
may still reside in a property buffer. Applications that require a persistence
barrier must call `db->FlushPropertyUpdates()`.

With `DeltaDurability::kProcessCrashSafe`, flushed delta publication uses a
temporary file, `fdatasync`, atomic rename, parent-directory `fsync`, and a
synchronized manifest edit. `DeltaDurability::kNone` omits the explicit sync
operations and is intended only for experiments where process-crash durability
is not required.

Clean shutdown invokes the same flush barrier and waits for delta merges and
storage background work. Fatal process termination before an explicit flush
can lose buffered updates.

## Recovery

Recovery validates delta magic/version fields, record bounds, and checksums,
replays complete manifest records, truncates a torn manifest tail, reconstructs
visible chains, and removes orphan temporary files. Non-persistent chains that
still refer to a MemTable are discarded unless the MemTable-to-SST handoff was
published before shutdown.

The base SST metadata file remains backward compatible with the research
prototype. New metadata stores the next topology sequence; legacy metadata is
recovered by scanning SST edge bodies once.

## Compaction

Before compaction consumes an input SST, RichGraph prevents new delta
publication against that input and merges its persistent delta chain. The
barrier remains held until the replacement SST version is published. Existing
read views retain shared ownership of old mappings and delta files, so physical
reclamation cannot invalidate an active reader.

## Failure reporting

Public methods return a `Status`. `StatusName` produces a stable diagnostic
name. `GraphDb::OpenFromYaml` and `LoadSchemaFromYaml` also accept an optional
error string for contextual messages. Background errors are surfaced by
`FlushPropertyUpdates`, `WaitBackgroundIdle`, or shutdown diagnostics.
