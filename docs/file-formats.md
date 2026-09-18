# File formats

This document describes compatibility obligations, not a promise that every
legacy structure is portable across architectures. All current files use the
host byte order and therefore must be treated as host-local research formats.

## Base SST and `file.info`

Base SST files contain graph topology, a source index, a header, and one
physical file per configured property column. The exact structures are still
inherited from the original prototype and may contain native-width fields.

Each shard stores `file.info`, which lists the visible file IDs by level. New
files append a magic-tagged next-topology-sequence trailer. Readers accept the
older form without this trailer and reconstruct the next sequence by scanning
SST edge bodies.

`file.info` is still a legacy checkpoint rather than the property-delta
manifest. See `docs/known-limitations.md` for its remaining crash-consistency
restriction.

## Property delta file

A delta file is immutable and consists of:

1. a versioned fixed header;
2. sorted fixed-size record descriptors;
3. variable-length property bytes.

The header records object kind, shard, base file/generation, property ID, file
generation, commit-sequence range, record count, section offsets/sizes, and
checksums. Each record contains the logical graph key, base topology sequence,
property commit sequence, direction/type, and value offset/length.

Recovery rejects unknown versions, invalid object kinds, integer/bounds
overflows, bad header checksums, bad payload checksums, and unsorted or
duplicate physical versions.

## Delta manifest

The manifest is an append-only sequence of checksummed, sequence-numbered
records. Add records publish a delta file and its target metadata. Remove
records retire one or more file basenames. Absolute paths are never stored, so
a database directory remains relocatable.

Recovery replays the longest valid prefix. An incomplete final record is
treated as a torn tail and truncated; corruption inside the valid prefix is an
error. Delta files are written and renamed before the corresponding add record
is synchronized.

## Format evolution policy

New public releases must either read the previous format or fail with a clear
version error. Any incompatible change requires a format-version increment, a
migration tool or documented rebuild path, and recovery tests containing old
fixtures.
