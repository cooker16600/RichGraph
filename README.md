# RichGraph

RichGraph is a graph-storage engine designed for hybrid transactional and
analytical processing (HTAP). It separates graph topology from properties and
organizes properties into independently configured groups to support both
transactional updates and analytical graph queries.

## Features

- LSM- and CSR-based graph topology storage.
- Configurable property grouping for different access patterns.
- In-memory property-update buffering with durable delta files.
- Threshold-triggered delta merging to limit read amplification.
- Point reads and scans over the latest visible property values.
- A C++17 `GraphDb` API with YAML-based schema configuration.

## Build and run

RichGraph requires Linux, CMake 3.16 or newer, a C++17 compiler, oneTBB, and
gflags. On Debian or Ubuntu, install the dependencies with:

```bash
sudo apt-get install build-essential cmake libtbb-dev libgflags-dev
```

Build RichGraph and run its tests:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DRICHGRAPH_BUILD_BENCHMARKS=OFF
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Run the included example with an empty database directory:

```bash
./build/richgraph_basic_example /tmp/richgraph-example examples/schema.yaml
```
