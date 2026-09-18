# Benchmarking

The FinBench and SNB executables are research drivers, not part of the stable
library API. Enable them explicitly:

```bash
cmake -S . -B build-bench \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DRICHGRAPH_BUILD_BENCHMARKS=ON
cmake --build build-bench -j --target test_graphdb_finbench
```

## Property-update ownership

Both benchmark drivers translate dataset records and then call the public
engine update path (`GraphDb::UpdateNode` / `GraphDb::UpdateEdge`). Buffer
rotation, durable delta publication, chain merge, recovery, and compaction
coordination belong to `GraphDb`; benchmark code must not recreate those
mechanisms.

The old `*_enable_memproperty` and `*_update_delta_dir` flags are retained only
so existing experiment scripts still parse. They do not enable a second
benchmark-local update path. `*_update_node_memproperty_cap` and
`*_update_edge_memproperty_cap` are compatibility inputs; because the public
engine currently owns one shared property-update manager, the larger cap is
used as its record limit. New runs should also record
`*_property_buffer_bytes`, `*_delta_merge_threshold`, and
`*_delta_crash_safe`.

## Reproducibility rules

- Record the Git commit, compiler, CMake options, dataset scale, preprocessed
  dataset manifest, command line, CPU affinity, memory limit, storage device,
  filesystem, and kernel.
- Put input data and the database on separate block devices when reporting
  database I/O. Device counters otherwise combine loader reads and worker I/O.
- Start every independent run with a fresh database directory.
- Keep schema property grouping, MemTable capacities, property-buffer limits,
  thread counts, and LSM/CSR mode in the result metadata.
- Wait through explicit engine barriers instead of fixed sleeps when measuring
  a stable post-update state.
- Report failed/OOM runs; do not silently omit them.

## Timing boundaries

Foreground throughput/latency excludes a deliberate post-stage background
idle barrier. I/O counters may be sampled after that barrier if the experiment
is intended to attribute all deferred compaction I/O to the stage. The report
must state which boundary was used because the two answer different questions.

For HTAP mixes, record TP and AP operation start/end timestamps independently.
Service rate (`1 / mean service time`) is not aggregate throughput; label it as
such. Aggregate TP/AP throughput uses completed operations divided by the same
wall-clock interval.

## Correctness

Performance results are valid only after the driver's result/checksum
validation passes. Cross-system query counts, source parameters, result limits,
and checksum semantics must be identical before comparing QPS.
