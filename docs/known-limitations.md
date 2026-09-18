# Known limitations

The following items are explicit boundaries of the first open-source
stabilization branch.

- Linux is required. The engine relies on POSIX file APIs, `mmap`, and Linux
  futex behavior.
- Only one independently configured `GraphDb` should be open in a process.
  Some legacy internals still read process-global gflags during construction
  and operation.
- Property-update overlays currently require `cache_sst_data=true`; option
  validation rejects the unsupported combination rather than returning stale
  reads.
- Public reads expose latest-value semantics. Historical read timestamps and a
  transaction object spanning multiple API calls are not yet public.
- A successful update is immediately visible in the process but is not an
  fsync boundary. Call `FlushPropertyUpdates()` for persistence.
- Multi-property updates share one commit sequence, but a reader does not yet
  acquire a single public batch snapshot across several independent property
  columns.
- Base SST and `file.info` structures use host byte order and native legacy
  layouts. Cross-architecture database portability is not supported.
- The delta manifest is checksummed and torn-tail recoverable. The legacy
  `file.info` checkpoint still needs a fully versioned, atomic replacement
  before power-loss recovery can be claimed for every base-SST publication
  boundary.
- The supported tests cover deterministic reopen, delta thresholds, and a
  concurrent update/read/forced-compaction/reopen path. The short suite passes
  under TSan; broader crash fault injection and long-running stress remain
  release-candidate gates.
- Benchmark executables retain dataset-specific gflags and large translation
  units. They are opt-in and are not stable library interfaces.
