#!/usr/bin/env python3
"""
Generate FinBench datasets with additional cold edge properties.

The original FinBench edge schema used by test_graphdb_finbench has 14 logical
edge properties: two hot properties (createTime, amount) plus 12 cold
properties. This script appends generated cold_extra_XX columns to every edge
CSV so the logical edge-property count reaches 16, 32, or 64.

Unchanged files are hard-linked by default. Edge files are transformed in a
streaming pass, and all requested target variants for one scale are generated
from the same read pass.
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


BASE_EDGE_PROPERTY_COUNT = 14
DEFAULT_SCALES = ("0.1", "30", "100")
DEFAULT_TARGETS = (16, 32, 64)
EXTRA_VALUE_LENGTH = 14
EXTRA_PROPERTY_SPARSITY = 5

SNAPSHOT_EDGE_FILES = {
    "AccountTransferAccount.csv",
    "AccountWithdrawAccount.csv",
    "AccountRepayLoan.csv",
    "CompanyApplyLoan.csv",
    "CompanyGuaranteeCompany.csv",
    "CompanyInvestCompany.csv",
    "CompanyOwnAccount.csv",
    "LoanDepositAccount.csv",
    "MediumSignInAccount.csv",
    "PersonApplyLoan.csv",
    "PersonGuaranteePerson.csv",
    "PersonInvestCompany.csv",
    "PersonOwnAccount.csv",
}

EDGE_TYPES = tuple(sorted(path[:-4] for path in SNAPSHOT_EDGE_FILES))

INCREMENTAL_EDGE_FILES = {
    "AddPersonOwnAccountWrite4.csv",
    "AddCompanyOwnAccountWrite5.csv",
    "AddPersonApplyLoanWrite6.csv",
    "AddCompanyApplyLoanWrite7.csv",
    "AddPersonInvestCompanyWrite8.csv",
    "AddCompanyInvestCompanyWrite9.csv",
    "AddPersonGuaranteePersonWrite10.csv",
    "AddPersonGuaranteePersonReadWrite3.csv",
    "AddCompanyGuaranteeCompanyWrite11.csv",
    "AddAccountTransferAccountWrite12.csv",
    "AddAccountTransferAccountReadWrite1.csv",
    "AddAccountTransferAccountReadWrite2.csv",
    "AddAccountWithdrawAccountWrite13.csv",
    "AddAccountRepayLoanWrite14.csv",
    "AddLoanDepositAccountWrite15.csv",
    "AddMediumSigninAccountWrite16.csv",
}

INCREMENTAL_EDGE_FILE_RELATION = {
    "AddPersonOwnAccountWrite4.csv": "PersonOwnAccount",
    "AddCompanyOwnAccountWrite5.csv": "CompanyOwnAccount",
    "AddPersonApplyLoanWrite6.csv": "PersonApplyLoan",
    "AddCompanyApplyLoanWrite7.csv": "CompanyApplyLoan",
    "AddPersonInvestCompanyWrite8.csv": "PersonInvestCompany",
    "AddCompanyInvestCompanyWrite9.csv": "CompanyInvestCompany",
    "AddPersonGuaranteePersonWrite10.csv": "PersonGuaranteePerson",
    "AddPersonGuaranteePersonReadWrite3.csv": "PersonGuaranteePerson",
    "AddCompanyGuaranteeCompanyWrite11.csv": "CompanyGuaranteeCompany",
    "AddAccountTransferAccountWrite12.csv": "AccountTransferAccount",
    "AddAccountTransferAccountReadWrite1.csv": "AccountTransferAccount",
    "AddAccountTransferAccountReadWrite2.csv": "AccountTransferAccount",
    "AddAccountWithdrawAccountWrite13.csv": "AccountWithdrawAccount",
    "AddAccountRepayLoanWrite14.csv": "AccountRepayLoan",
    "AddLoanDepositAccountWrite15.csv": "LoanDepositAccount",
    "AddMediumSigninAccountWrite16.csv": "MediumSignInAccount",
}

EDGE_FILE_ORDER = {
    name: idx + 1
    for idx, name in enumerate(sorted(SNAPSHOT_EDGE_FILES | INCREMENTAL_EDGE_FILES))
}


def die(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate FinBench edge-property-plus datasets."
    )
    parser.add_argument(
        "--input-base",
        type=Path,
        default=Path("datasets/FinBench"),
        help="Directory containing sf0.1/sf30/sf100.",
    )
    parser.add_argument(
        "--output-base",
        type=Path,
        default=Path("datasets/FinBench"),
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
        "--seed",
        type=int,
        default=20260623,
        help="Seed for assigning each cold_extra_XX to edge types.",
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


def extra_names(target: int) -> List[str]:
    extra_count = target - BASE_EDGE_PROPERTY_COUNT
    return [f"cold_extra_{idx:02d}" for idx in range(1, extra_count + 1)]


def build_property_assignment(max_extra_count: int,
                              seed: int) -> Dict[int, List[str]]:
    if EXTRA_PROPERTY_SPARSITY > len(EDGE_TYPES):
        die(
            f"extra property sparsity {EXTRA_PROPERTY_SPARSITY} is larger "
            f"than FinBench edge type count {len(EDGE_TYPES)}"
        )
    rng = random.Random(seed)
    edge_types = list(EDGE_TYPES)
    assignment: Dict[int, List[str]] = {}
    for prop_idx in range(1, max_extra_count + 1):
        assignment[prop_idx] = sorted(
            rng.sample(edge_types, EXTRA_PROPERTY_SPARSITY)
        )
    return assignment


def edge_type_for_file(root: Path, path: Path) -> str | None:
    rel = path.relative_to(root)
    if len(rel.parts) != 2:
        return None
    section, name = rel.parts
    if section == "snapshot":
        return name[:-4] if name in SNAPSHOT_EDGE_FILES else None
    if section == "incremental":
        return INCREMENTAL_EDGE_FILE_RELATION.get(name)
    return None


def is_edge_file(root: Path, path: Path) -> bool:
    return edge_type_for_file(root, path) is not None


def assigned_prop_indices(target: int,
                          edge_type: str,
                          assignment: Dict[int, List[str]]) -> List[int]:
    return [
        prop_idx
        for prop_idx in range(1, target - BASE_EDGE_PROPERTY_COUNT + 1)
        if edge_type in assignment.get(prop_idx, [])
    ]


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


def two_char_property_code(prop_idx: int) -> str:
    if prop_idx < 100:
        return f"{prop_idx:02d}"
    offset = prop_idx - 100
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    first = offset // len(alphabet)
    second = offset % len(alphabet)
    if first >= len(alphabet):
        die(f"property index {prop_idx} is too large for 2-char encoding")
    return alphabet[first] + alphabet[second]


def generated_value(row_mod: int, file_code: int, prop_idx: int) -> str:
    value = (row_mod * 131 + file_code * 173 + prop_idx * 977) % 100000000
    out = f"fb{two_char_property_code(prop_idx)}{file_code:02d}{value:08d}"
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


def iter_files(root: Path) -> List[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file())


def generate_scale(input_base: Path,
                   output_base: Path,
                   scale: str,
                   targets: Sequence[int],
                   assignment: Dict[int, List[str]],
                   force: bool,
                   copy_unchanged: bool,
                   buffer_bytes: int,
                   suffix_period: int) -> None:
    src_root = input_base / f"sf{scale}"
    if not src_root.is_dir():
        die(f"missing input dataset: {src_root}")

    dst_roots = {target: output_base / f"sf{scale}+{target}" for target in targets}
    for dst_root in dst_roots.values():
        ensure_clean_output(dst_root, force)

    files = iter_files(src_root)
    edge_files = [path for path in files if is_edge_file(src_root, path)]
    edge_file_set = set(edge_files)
    other_files = [path for path in files if path not in edge_file_set]

    print(
        f"[scale sf{scale}] files={len(files)} edge_files={len(edge_files)} "
        f"targets={','.join(str(t) for t in targets)} "
        f"assignment={EXTRA_PROPERTY_SPARSITY}-edge-types-per-property",
        flush=True,
    )

    for src in other_files:
        rel = src.relative_to(src_root)
        for dst_root in dst_roots.values():
            link_or_copy(src, dst_root / rel, copy_unchanged)

    total_rows = 0
    total_in = 0
    total_out = 0
    start = time.time()
    for idx, src in enumerate(edge_files, 1):
        rel = src.relative_to(src_root)
        edge_type = edge_type_for_file(src_root, src)
        assert edge_type is not None
        props_by_target = {
            target: assigned_prop_indices(target, edge_type, assignment)
            for target in targets
        }
        transform_targets = [
            target for target in targets if props_by_target[target]
        ]
        for target in targets:
            if not props_by_target[target]:
                link_or_copy(src, dst_roots[target] / rel, copy_unchanged)
        if not transform_targets:
            print(
                f"[scale sf{scale}] edge {idx:02d}/{len(edge_files):02d} "
                f"{rel} edge_type={edge_type} assigned_props=0 linked",
                flush=True,
            )
            continue
        dst_by_target = {
            target: dst_roots[target] / rel
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
            f"{rel} edge_type={edge_type} assigned_props={prop_desc} "
            f"rows={rows} in={in_bytes / 1e9:.3f}GB "
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
        for prop_idx in range(1, target - BASE_EDGE_PROPERTY_COUNT + 1):
            props.append({
                "name": f"cold_extra_{prop_idx:02d}",
                "length": EXTRA_VALUE_LENGTH,
                "edge_types": assignment[prop_idx],
            })
        schema = {
            "dataset": "FinBench",
            "scale": f"sf{scale}",
            "target_edge_property_count": target,
            "base_edge_property_count": BASE_EDGE_PROPERTY_COUNT,
            "extra_edge_property_count": target - BASE_EDGE_PROPERTY_COUNT,
            "extra_property_length": EXTRA_VALUE_LENGTH,
            "extra_property_sparsity_edge_types": EXTRA_PROPERTY_SPARSITY,
            "edge_type_count": len(EDGE_TYPES),
            "edge_types": list(EDGE_TYPES),
            "properties": props,
        }
        path = output_base / f"sf{scale}+{target}" / "plus_schema.json"
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
        max(target - BASE_EDGE_PROPERTY_COUNT for target in targets),
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
        )
        write_schema_files(args.output_base, scale, targets, assignment)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
