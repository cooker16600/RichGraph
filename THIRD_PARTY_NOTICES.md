# Third-party source inventory

This file is an inventory for release review; it is not a substitute for the
license texts required by the respective projects.

| Path | Upstream indicated by source headers | License indicated by source headers | Current action |
|---|---|---|---|
| `util/leveldb/port` | LevelDB Authors | BSD-style; headers refer to upstream `LICENSE` | Add the applicable LevelDB license text and verify provenance/version |
| `util/livegraph` | Guanyu Feng, Tsinghua University / LiveGraph | Apache License 2.0 | Add Apache-2.0 text and verify upstream provenance |
| `util/rocksdb` | Facebook / RocksDB | dual GPLv2 or Apache License 2.0 | Select the compatible option, add its text, and verify provenance/version |

The project also depends on system-provided oneTBB, gflags, pthreads, and
optionally OpenMP/tcmalloc. Their licenses are not copied into this source tree
by the build.

Before publishing a release, a maintainer must:

1. select and add RichGraph's top-level `LICENSE`;
2. verify every vendored file against a known upstream revision;
3. add complete required license/notice texts without removing existing source
   headers;
4. review generated binary-distribution obligations for linked dependencies.
