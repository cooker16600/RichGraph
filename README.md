# RichGraph

RichGraph is a research graph-storage engine for hybrid transactional and
analytical processing (HTAP). It stores graph topology in an LSM- or CSR-based
layout and routes logical properties into independently configured physical
shards. Property updates use engine-owned memory buffers, durable delta files,
and threshold-triggered merges so readers can observe new values without
waiting for a full SST rewrite.

This repository is the RichGraph research-artifact release. The public API is under
`include/richgraph`; files under `core` are implementation details and are not
installed.

## Supported environment

- Linux (the current storage implementation uses `mmap`, futexes, and POSIX
  file APIs)
- CMake 3.16 or newer
- a C++17 compiler (GCC and Clang are tested)
- oneTBB and gflags development packages
- OpenMP is optional
- tcmalloc is optional and disabled by default

On Debian or Ubuntu, the usual development dependencies are:

```bash
sudo apt-get install build-essential cmake libtbb-dev libgflags-dev
```

## Build and test

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DRICHGRAPH_BUILD_BENCHMARKS=OFF
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Run the minimal example with an empty database directory:

```bash
./build/richgraph_basic_example /tmp/richgraph-example examples/schema.yaml
```

The expected application output is `amount=42`. Internal diagnostics are
disabled by default; set `GraphDbOptions::verbose_logging` to `true` when
investigating the storage layer.

## Use from another CMake project

Install RichGraph to a prefix:

```bash
cmake --install build --prefix /opt/richgraph
```

Then consume the exported target:

```cmake
find_package(RichGraph CONFIG REQUIRED)
target_link_libraries(my_program PRIVATE RichGraph::RichGraph)
```

Only include public headers:

```cpp
#include <richgraph/graph_db.h>
```

See `examples/basic_usage.cpp` for database creation, insertion, and lookup.
`GraphDb::OpenFromYaml` accepts an optional error string; callers should check
both the returned `Status` and that message.

## Schema

The schema maps logical property names to physical edge and node shards. A
minimal schema is available at `examples/schema.yaml`:

```yaml
max_vertex_num: 1024
use_csr_disk: false
sub_property_num: 1
max_property_length: 32
system_threads: 2
property_defs:
  - name: amount
    length: 32
edge_shards:
  - name: edge_Db0
    properties: [amount]
node_db:
  name: node_Db
  properties: [name]
```

Property lengths describe the current fixed-width base-SST layout. Delta
records store the actual value length.

## Property-update lifecycle

```text
UpdateNode / UpdateEdge
  -> bounded PropertyUpdateManager buffers
  -> immediately visible pending-update index
  -> checksummed delta file and append-only manifest
  -> per-SST immutable delta chain
  -> threshold merge into a replacement property column
  -> compaction barrier and version publication
```

Point reads and scans resolve the newest pending or delta value before falling
back to the base SST value. A clean close flushes pending updates and waits for
background work. Use `FlushPropertyUpdates()` when the application needs an
explicit persistence barrier.

More detail is in:

- `docs/architecture/overview.md`
- `docs/consistency-and-durability.md`
- `docs/file-formats.md`
- `docs/benchmarking.md`
- `docs/known-limitations.md`

## Optional targets and instrumentation

FinBench/SNB programs are research benchmarks and are not part of the default
build:

```bash
cmake -S . -B build-bench \
  -DRICHGRAPH_BUILD_BENCHMARKS=ON \
  -DBUILD_TESTING=OFF
cmake --build build-bench -j --target test_graphdb_finbench
```

Sanitizer builds are available through CMake:

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DRICHGRAPH_ENABLE_ASAN=ON \
  -DRICHGRAPH_ENABLE_UBSAN=ON \
  -DRICHGRAPH_BUILD_BENCHMARKS=OFF
cmake --build build-asan -j
ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build-asan --output-on-failure
```

LeakSanitizer availability depends on the host's ptrace/container policy;
disabling leak detection does not disable AddressSanitizer or UBSan checks.

Run the same supported suite under ThreadSanitizer with OpenMP disabled:

```bash
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DRICHGRAPH_ENABLE_TSAN=ON \
  -DRICHGRAPH_USE_OPENMP=OFF \
  -DRICHGRAPH_BUILD_BENCHMARKS=OFF \
  -DRICHGRAPH_BUILD_EXAMPLES=OFF
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

## Project status

RichGraph is still a research system. Read `docs/known-limitations.md` before
using it for production data. The first public release is an experimental
`0.x` paper artifact; its C++ API and on-disk formats may change without
backward compatibility before `1.0`.

The source tree also contains small adapted portions of LevelDB, LiveGraph,
and RocksDB; their provenance is summarized in `THIRD_PARTY_NOTICES.md`.
