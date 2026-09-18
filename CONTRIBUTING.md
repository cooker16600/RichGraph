# Contributing

## Build before submitting

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug \
  -DRICHGRAPH_BUILD_BENCHMARKS=OFF
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Storage changes should also pass an ASan/UBSan build. Tests must use unique
temporary directories and explicit engine barriers; do not use fixed sleeps to
wait for flush or compaction.

## Change discipline

- Keep observable behavior changes separate from file moves or bulk formatting.
- Add a focused regression test before fixing a storage/recovery bug.
- Preserve point-read/scan parity and restart behavior.
- Never add a new library-level `exit()` or detached background thread.
- Prefer RAII ownership and typed options/status values.
- Do not expose headers from `core`; supported API additions belong under
  `include/richgraph`.
- Do not commit datasets, generated databases, benchmark logs, build trees, or
  developer-specific absolute paths.

Formatting is governed by `.clang-format`. The initial cleanup follows a
touched-code policy: format the lines/files materially changed by a patch, not
the entire legacy tree.

## Commit messages

Use a short subsystem prefix, for example:

```text
storage: recover delta chains after restart
api: reject unsupported cache configuration
docs: document property update durability
```

Decisions that alter persistence, consistency, compatibility, or public API
semantics should be proposed before implementation and recorded as an ADR only
after maintainers approve the decision.
