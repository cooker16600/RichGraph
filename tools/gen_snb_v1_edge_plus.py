#!/usr/bin/env python3
"""
Generate SNB Interactive v1 datasets with additional cold edge properties.

The base SNB-v1 edge files have four edge attributes in total:
creationDate, joinDate, classYear, and workFrom. This script appends generated
cold_extra_XX columns to the initial dynamic edge CSV files so the logical edge
property count reaches 16, 32, or 64. Implicit edges that are derived from node
tables are not materialized as CSV files; their cold-property assignment is
recorded in plus_schema.json for loaders to generate deterministically.

Unchanged files, including nodes, update streams, and substitution parameters,
are hard-linked by default. Edge files are transformed in a streaming pass, and
all requested target variants for one scale are generated from the same read
pass.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


DATASET_SUFFIX = "-CsvCompositeMergeForeign-LongDateFormatter"
BASE_EDGE_PROPERTY_COUNT = 4
DEFAULT_SCALES = ("0.1", "30", "100")
DEFAULT_TARGETS = (16, 32, 64)
EXTRA_VALUE_LENGTH = 13
EXTRA_PROPERTY_SPARSITY = 8

INITIAL_EDGE_FILES = {
    "comment_hasTag_tag_0_0.csv",
    "forum_hasMember_person_0_0.csv",
    "forum_hasTag_tag_0_0.csv",
    "person_hasInterest_tag_0_0.csv",
    "person_knows_person_0_0.csv",
    "person_likes_comment_0_0.csv",
    "person_likes_post_0_0.csv",
    "person_studyAt_organisation_0_0.csv",
    "person_workAt_organisation_0_0.csv",
    "post_hasTag_tag_0_0.csv",
}

EXPLICIT_EDGE_KEYS = {
    f"dynamic/{name}"
    for name in INITIAL_EDGE_FILES
}

IMPLICIT_EDGE_TYPES = {
    "comment_hasCreator_person",
    "comment_isLocatedIn_place",
    "comment_replyOf_comment",
    "comment_replyOf_post",
    "forum_hasModerator_person",
    "organisation_isLocatedIn_place",
    "person_isLocatedIn_place",
    "place_isPartOf_place",
    "post_hasCreator_person",
    "post_isLocatedIn_place",
    "post_isPartOf_forum",
    "tag_hasType_tagclass",
    "tagclass_isSubclassOf_tagclass",
}

IMPLICIT_EDGE_KEYS = {
    f"implicit/{name}"
    for name in IMPLICIT_EDGE_TYPES
}

EDGE_KEYS = tuple(sorted(EXPLICIT_EDGE_KEYS | IMPLICIT_EDGE_KEYS))

EDGE_FILE_ORDER = {
    name: idx + 1
    for idx, name in enumerate(sorted(INITIAL_EDGE_FILES))
}


def die(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate SNB-v1 edge-property-plus datasets."
    )
    parser.add_argument(
        "--input-base",
        type=Path,
        default=Path("datasets/Snb-v1"),
        help="Directory containing sf0.1/sf30/sf100.",
    )
    parser.add_argument(
        "--output-base",
        type=Path,
        default=Path("datasets/Snb-v1"),
        help="Directory where sfX+16/sfX+32/sfX+64 are created.",
    )
    parser.add_argument(
        "--scales",
        nargs="+",
        default=list(DEFAULT_SCALES),
        help="Scale factors to process, e.g. 0.1 30 100.",
    )
    parser.add_argument(
        "--targets",
        nargs="+",
        type=int,
        default=list(DEFAULT_TARGETS),
        help="Target total logical edge-property counts.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace existing output directories.",
    )
    parser.add_argument(
        "--copy-unchanged",
        action="store_true",
        help="Copy unchanged files instead of hard-linking them.",
    )
    parser.add_argument(
        "--buffer-mb",
        type=int,
        default=32,
        help="Per-file IO buffer size in MiB.",
    )
    parser.add_argument(
        "--suffix-period",
        type=int,
        default=1024,
        help="Number of precomputed generated-value suffixes per edge file.",
    )
    parser.add_argument(
        "--keep-crc",
        action="store_true",
        help="Keep .crc side files. By default they are skipped because changed edge CSVs invalidate them.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=20260620,
        help="Seed for assigning each cold_extra_XX to explicit and implicit edge candidates.",
    )
    return parser.parse_args()


def normalized_targets(targets: Iterable[int]) -> List[int]:
    out = sorted(set(targets))
    for target in out:
        if target < BASE_EDGE_PROPERTY_COUNT:
            die(
                f"target {target} is smaller than base edge property count "
                f"{BASE_EDGE_PROPERTY_COUNT}"
            )
    return out


def source_dataset_dir(base: Path, scale: str) -> Path:
    return base / f"sf{scale}" / f"social_network-sf{scale}{DATASET_SUFFIX}"


def source_params_dir(base: Path, scale: str) -> Path:
    return base / f"sf{scale}" / f"substitution_parameters-sf{scale}"


def output_root(base: Path, scale: str, target: int) -> Path:
    return base / f"sf{scale}+{target}"


def output_dataset_dir(base: Path, scale: str, target: int) -> Path:
    return (
        output_root(base, scale, target)
        / f"social_network-sf{scale}+{target}{DATASET_SUFFIX}"
    )


def output_params_dir(base: Path, scale: str, target: int) -> Path:
    return output_root(base, scale, target) / f"substitution_parameters-sf{scale}+{target}"


def extra_count(target: int) -> int:
    return target - BASE_EDGE_PROPERTY_COUNT


def extra_names(target: int) -> List[str]:
    extra_count = target - BASE_EDGE_PROPERTY_COUNT
    return [f"cold_extra_{idx:02d}" for idx in range(1, extra_count + 1)]


def build_property_assignment(max_extra_count: int,
                              seed: int) -> Dict[int, List[str]]:
    edge_keys = list(EDGE_KEYS)
    if EXTRA_PROPERTY_SPARSITY > len(edge_keys):
        die(
            f"extra property sparsity {EXTRA_PROPERTY_SPARSITY} is larger "
            f"than SNB edge candidate count {len(edge_keys)}"
        )
    rng = random.Random(seed)
    assignment: Dict[int, List[str]] = {}
    for prop_idx in range(1, max_extra_count + 1):
        assignment[prop_idx] = sorted(
            rng.sample(edge_keys, EXTRA_PROPERTY_SPARSITY)
        )
    return assignment


def assigned_prop_indices(target: int,
                          file_name: str,
                          assignment: Dict[int, List[str]]) -> List[int]:
    edge_key = f"dynamic/{file_name}"
    return [
        prop_idx
        for prop_idx in range(1, extra_count(target) + 1)
        if edge_key in assignment.get(prop_idx, [])
    ]


def is_initial_edge_file(dataset_root: Path, path: Path) -> bool:
    rel = path.relative_to(dataset_root)
    return len(rel.parts) == 2 and rel.parts[0] == "dynamic" and rel.parts[1] in INITIAL_EDGE_FILES


def ensure_clean_output(path: Path, force: bool) -> None:
    if path.exists():
        if not force:
            die(f"output already exists: {path}; pass --force to replace it")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def link_or_copy(src: Path, dst: Path, copy_unchanged: bool) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    if not copy_unchanged:
        try:
            os.link(src, dst)
            return
        except OSError:
            pass
    shutil.copy2(src, dst)


def generated_value(row_mod: int, file_code: int, prop_idx: int) -> str:
    value = (row_mod * 137 + file_code * 191 + prop_idx * 983) % 100000000
    out = f"s{prop_idx:02d}{file_code:02d}{value:08d}"
    assert len(out) == EXTRA_VALUE_LENGTH
    return out


def build_suffix_table(file_code: int,
                       prop_indices: Sequence[int],
                       suffix_period: int) -> List[bytes]:
    if not prop_indices:
        return [b""] * suffix_period
    suffixes: List[bytes] = []
    for row_mod in range(suffix_period):
        values = [
            generated_value(row_mod, file_code, idx)
            for idx in prop_indices
        ]
        suffixes.append(("|" + "|".join(values)).encode("ascii"))
    return suffixes


def strip_newline(line: bytes) -> bytes:
    if line.endswith(b"\n"):
        line = line[:-1]
    if line.endswith(b"\r"):
        line = line[:-1]
    return line


def transform_edge_file(src: Path,
                        dst_by_target: Dict[int, Path],
                        props_by_target: Dict[int, List[int]],
                        buffer_bytes: int,
                        suffix_period: int) -> Tuple[int, int, int]:
    file_code = EDGE_FILE_ORDER.get(src.name, 0)
    suffix_by_target = {
        target: build_suffix_table(file_code, prop_indices, suffix_period)
        for target, prop_indices in props_by_target.items()
    }
    headers = {
        target: ("|" + "|".join(f"cold_extra_{idx:02d}" for idx in prop_indices)).encode("ascii")
        for target, prop_indices in props_by_target.items()
    }

    for dst in dst_by_target.values():
        dst.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    in_bytes = src.stat().st_size
    out_bytes = 0
    outputs = []
    try:
        for target in sorted(props_by_target):
            outputs.append((target, dst_by_target[target].open("wb", buffering=buffer_bytes)))
        with src.open("rb", buffering=buffer_bytes) as inp:
            raw_header = inp.readline()
            if not raw_header:
                return 0, in_bytes, 0
            base_header = strip_newline(raw_header)
            for target, out in outputs:
                line = base_header + headers[target] + b"\n"
                out.write(line)
                out_bytes += len(line)

            for raw in inp:
                if raw in (b"\n", b"\r\n"):
                    for _, out in outputs:
                        out.write(b"\n")
                        out_bytes += 1
                    continue
                base_line = strip_newline(raw)
                row_mod = row_count % suffix_period
                for target, out in outputs:
                    line = base_line + suffix_by_target[target][row_mod] + b"\n"
                    out.write(line)
                    out_bytes += len(line)
                row_count += 1
    finally:
        for _, out in outputs:
            out.close()

    return row_count, in_bytes, out_bytes


def iter_dataset_files(root: Path, keep_crc: bool) -> List[Path]:
    out: List[Path] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if not keep_crc and path.name.endswith(".crc"):
            continue
        out.append(path)
    return out


def link_params(src_params: Path,
                output_base: Path,
                scale: str,
                targets: Sequence[int],
                copy_unchanged: bool,
                keep_crc: bool) -> None:
    if not src_params.is_dir():
        die(f"missing substitution parameters: {src_params}")
    for target in targets:
        dst_root = output_params_dir(output_base, scale, target)
        dst_root.mkdir(parents=True, exist_ok=True)
        for src in sorted(src_params.rglob("*")):
            if not src.is_file():
                continue
            if not keep_crc and src.name.endswith(".crc"):
                continue
            link_or_copy(src, dst_root / src.relative_to(src_params), copy_unchanged)


def generate_scale(input_base: Path,
                   output_base: Path,
                   scale: str,
                   targets: Sequence[int],
                   assignment: Dict[int, List[str]],
                   force: bool,
                   copy_unchanged: bool,
                   buffer_bytes: int,
                   suffix_period: int,
                   keep_crc: bool) -> None:
    src_dataset = source_dataset_dir(input_base, scale)
    src_params = source_params_dir(input_base, scale)
    if not src_dataset.is_dir():
        die(f"missing input dataset: {src_dataset}")

    dst_datasets = {
        target: output_dataset_dir(output_base, scale, target)
        for target in targets
    }
    for target in targets:
        ensure_clean_output(output_root(output_base, scale, target), force)
        dst_datasets[target].mkdir(parents=True, exist_ok=True)
    link_params(src_params, output_base, scale, targets, copy_unchanged, keep_crc)

    files = iter_dataset_files(src_dataset, keep_crc)
    edge_files = [path for path in files if is_initial_edge_file(src_dataset, path)]
    edge_file_set = set(edge_files)
    other_files = [path for path in files if path not in edge_file_set]

    print(
        f"[scale sf{scale}] files={len(files)} initial_edge_files={len(edge_files)} "
        f"targets={','.join(str(t) for t in targets)} "
        f"assignment={EXTRA_PROPERTY_SPARSITY}-edge-candidates-per-property "
        f"candidates={len(EDGE_KEYS)} explicit={len(EXPLICIT_EDGE_KEYS)} "
        f"implicit={len(IMPLICIT_EDGE_KEYS)}",
        flush=True,
    )

    for src in other_files:
        rel = src.relative_to(src_dataset)
        for dst_dataset in dst_datasets.values():
            link_or_copy(src, dst_dataset / rel, copy_unchanged)

    total_rows = 0
    total_in = 0
    total_out = 0
    start = time.time()
    for idx, src in enumerate(edge_files, 1):
        rel = src.relative_to(src_dataset)
        props_by_target = {
            target: assigned_prop_indices(target, src.name, assignment)
            for target in targets
        }
        transform_targets = [
            target for target in targets if props_by_target[target]
        ]
        for target in targets:
            if not props_by_target[target]:
                link_or_copy(src, dst_datasets[target] / rel, copy_unchanged)
        if not transform_targets:
            print(
                f"[scale sf{scale}] edge {idx:02d}/{len(edge_files):02d} "
                f"{rel} assigned_props=0 linked",
                flush=True,
            )
            continue
        dst_by_target = {
            target: dst_datasets[target] / rel
            for target in transform_targets
        }
        t0 = time.time()
        rows, in_bytes, out_bytes = transform_edge_file(
            src,
            dst_by_target,
            {target: props_by_target[target] for target in transform_targets},
            buffer_bytes,
            suffix_period,
        )
        total_rows += rows
        total_in += in_bytes
        total_out += out_bytes
        sec = max(time.time() - t0, 1e-9)
        prop_desc = ",".join(
            f"+{target}:{len(props_by_target[target])}"
            for target in transform_targets
        )
        print(
            f"[scale sf{scale}] edge {idx:02d}/{len(edge_files):02d} "
            f"{rel} assigned_props={prop_desc} rows={rows} in={in_bytes / 1e9:.3f}GB "
            f"out={out_bytes / 1e9:.3f}GB time={sec:.2f}s",
            flush=True,
        )

    elapsed = max(time.time() - start, 1e-9)
    print(
        f"[scale sf{scale}] done rows={total_rows} "
        f"edge_in={total_in / 1e9:.3f}GB edge_out={total_out / 1e9:.3f}GB "
        f"time={elapsed:.2f}s throughput_in={total_in / elapsed / 1e6:.1f}MB/s",
        flush=True,
    )


def write_schema_files(output_base: Path,
                       scale: str,
                       targets: Sequence[int],
                       assignment: Dict[int, List[str]]) -> None:
    for target in targets:
        props = []
        for prop_idx in range(1, extra_count(target) + 1):
            edge_keys = assignment[prop_idx]
            explicit_edge_files = sorted(
                key.removeprefix("dynamic/")
                for key in edge_keys
                if key.startswith("dynamic/")
            )
            implicit_edge_types = sorted(
                key.removeprefix("implicit/")
                for key in edge_keys
                if key.startswith("implicit/")
            )
            props.append({
                "name": f"cold_extra_{prop_idx:02d}",
                "length": EXTRA_VALUE_LENGTH,
                "edge_keys": edge_keys,
                "explicit_edge_files": explicit_edge_files,
                "implicit_edge_types": implicit_edge_types,
            })
        schema = {
            "dataset": "SNB Interactive v1",
            "scale": f"sf{scale}",
            "target_edge_property_count": target,
            "base_edge_property_count": BASE_EDGE_PROPERTY_COUNT,
            "extra_edge_property_count": extra_count(target),
            "extra_property_length": EXTRA_VALUE_LENGTH,
            "extra_property_sparsity_edge_candidates": EXTRA_PROPERTY_SPARSITY,
            "edge_candidate_count": len(EDGE_KEYS),
            "explicit_edge_file_count": len(INITIAL_EDGE_FILES),
            "explicit_edge_files": sorted(INITIAL_EDGE_FILES),
            "implicit_edge_type_count": len(IMPLICIT_EDGE_TYPES),
            "implicit_edge_types": sorted(IMPLICIT_EDGE_TYPES),
            "properties": props,
        }
        path = output_root(output_base, scale, target) / "plus_schema.json"
        with path.open("w", encoding="utf-8") as out:
            json.dump(schema, out, indent=2, sort_keys=True)
            out.write("\n")


def main() -> int:
    args = parse_args()
    targets = normalized_targets(args.targets)
    if args.buffer_mb <= 0:
        die("--buffer-mb must be positive")
    if args.suffix_period <= 0:
        die("--suffix-period must be positive")
    buffer_bytes = args.buffer_mb * 1024 * 1024
    assignment = build_property_assignment(
        max(extra_count(target) for target in targets),
        args.seed,
    )

    print(f"[assignment] seed={args.seed}", flush=True)
    for prop_idx in sorted(assignment):
        print(
            f"[assignment] cold_extra_{prop_idx:02d} -> "
            f"{','.join(assignment[prop_idx])}",
            flush=True,
        )

    for scale in args.scales:
        generate_scale(
            args.input_base,
            args.output_base,
            scale,
            targets,
            assignment,
            args.force,
            args.copy_unchanged,
            buffer_bytes,
            args.suffix_period,
            args.keep_crc,
        )
        write_schema_files(args.output_base, scale, targets, assignment)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
