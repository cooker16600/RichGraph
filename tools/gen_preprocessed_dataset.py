#!/usr/bin/env python3
"""Generate system-neutral preprocessed binary chunks for FinBench and SNB-v1.

The output deliberately preserves logical operations only. It does not encode
Rich-specific hot/cold shards, blob offsets, memtable ids, or any physical
layout decision. Test programs can map these records to their own storage
format.

Chunk file format, little endian:
  header:
    char[8] magic = "RPRECH1\0"
    uint16  version = 1
    uint16  stage_id
    uint32  record_count
    uint64  reserved = 0
  record:
    uint8   op_code
    uint32  schema_id
    uint64  row_index
    uint64  event_time_ms
    uint16  field_count
    repeated field_count times:
      uint32 value_len
      byte[value_len] utf8 value

Mixed-sort run files are private temporary files under the output dataset
directory and are removed after merge. They never use /tmp.
"""

from __future__ import annotations

import argparse
import csv
import heapq
import json
import os
import random
import shutil
import struct
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


MAGIC = b"RPRECH1\0"
VERSION = 1
HEADER_STRUCT = struct.Struct("<8sHHIQ")
RECORD_FIXED_STRUCT = struct.Struct("<BIQQH")
RUN_FIXED_STRUCT = struct.Struct("<QQQBIQH")

OP_NODE = 1
OP_EDGE = 2
OP_QUERY = 3
OP_SINGLE_NODE_READ = 4
OP_SINGLE_EDGE_READ = 5
OP_NODE_UPDATE = 6
OP_EDGE_UPDATE = 7

STAGE_IDS = {
    "snapshot_nodes": 1,
    "snapshot_edges": 2,
    "remaining_nodes": 3,
    "remaining_edges": 4,
    "mixed_ops": 5,
    "final_queries": 6,
    "single_node_read": 7,
    "single_edge_read": 8,
    "finbench_node_update": 9,
    "finbench_edge_update": 10,
}

NODE_KIND_FINBENCH = {
    "Person": "personId",
    "Company": "companyId",
    "Account": "accountId",
    "Loan": "loanId",
    "Medium": "mediumId",
}

FINBENCH_RELATIONS = {
    "AccountTransferAccount": ("Account", "Account", "fromId", "toId"),
    "AccountWithdrawAccount": ("Account", "Account", "fromId", "toId"),
    "AccountRepayLoan": ("Account", "Loan", "accountId", "loanId"),
    "CompanyApplyLoan": ("Company", "Loan", "companyId", "loanId"),
    "CompanyGuaranteeCompany": ("Company", "Company", "fromId", "toId"),
    "CompanyInvestCompany": ("Company", "Company", "investorId", "companyId"),
    "CompanyOwnAccount": ("Company", "Account", "companyId", "accountId"),
    "LoanDepositAccount": ("Loan", "Account", "loanId", "accountId"),
    "MediumSignInAccount": ("Medium", "Account", "mediumId", "accountId"),
    "PersonApplyLoan": ("Person", "Loan", "personId", "loanId"),
    "PersonGuaranteePerson": ("Person", "Person", "fromId", "toId"),
    "PersonInvestCompany": ("Person", "Company", "investorId", "companyId"),
    "PersonOwnAccount": ("Person", "Account", "personId", "accountId"),
}

FINBENCH_INCREMENTAL_SKIP_TRAILING_OPS = {17, 18, 19}

FINBENCH_DIRECT_NODE = {
    "AddPersonWrite1.csv": ("Person", "personId"),
    "AddCompanyWrite2.csv": ("Company", "companyId"),
    "AddMediumWrite3.csv": ("Medium", "mediumId"),
}

FINBENCH_OWN_ACCOUNT = {
    "AddPersonOwnAccountWrite4.csv": ("PersonOwnAccount", "Person", "personId"),
    "AddCompanyOwnAccountWrite5.csv": ("CompanyOwnAccount", "Company", "companyId"),
}

FINBENCH_APPLY_LOAN = {
    "AddPersonApplyLoanWrite6.csv": ("PersonApplyLoan", "Person", "personId"),
    "AddCompanyApplyLoanWrite7.csv": ("CompanyApplyLoan", "Company", "companyId"),
}

FINBENCH_SIMPLE_RELATION = {
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

FINBENCH_SOURCE_OVERRIDE = {
    "AddAccountRepayLoanWrite14.csv": ("account", None),
    "AddLoanDepositAccountWrite15.csv": ("loanId", "accountId"),
}

FINBENCH_HOT_EDGE_PROPS = {"createTime", "amount"}
FINBENCH_HOT_NODE_PROPS = {
    "rawId",
    "personId",
    "companyId",
    "accountId",
    "loanId",
    "mediumId",
    "isBlocked",
    "createTime",
}

SNB_NODE_TABLES = {
    "place_0_0.csv": ("Place", "id"),
    "organisation_0_0.csv": ("Organisation", "id"),
    "tag_0_0.csv": ("Tag", "id"),
    "tagclass_0_0.csv": ("TagClass", "id"),
    "person_0_0.csv": ("Person", "id"),
    "forum_0_0.csv": ("Forum", "id"),
    "post_0_0.csv": ("Post", "id"),
    "comment_0_0.csv": ("Comment", "id"),
}

SNB_EDGE_TABLES = {
    "person_knows_person_0_0.csv": ("PersonKnowsPerson", "Person", "Person", 0, 1),
    "forum_hasMember_person_0_0.csv": ("ForumHasMemberPerson", "Forum", "Person", "Forum.id", "Person.id"),
    "forum_hasTag_tag_0_0.csv": ("ForumHasTagTag", "Forum", "Tag", "Forum.id", "Tag.id"),
    "person_hasInterest_tag_0_0.csv": ("PersonHasInterestTag", "Person", "Tag", "Person.id", "Tag.id"),
    "person_likes_post_0_0.csv": ("PersonLikesPost", "Person", "Post", "Person.id", "Post.id"),
    "person_likes_comment_0_0.csv": ("PersonLikesComment", "Person", "Comment", "Person.id", "Comment.id"),
    "person_studyAt_organisation_0_0.csv": ("PersonStudyAtOrganisation", "Person", "Organisation", "Person.id", "Organisation.id"),
    "person_workAt_organisation_0_0.csv": ("PersonWorkAtOrganisation", "Person", "Organisation", "Person.id", "Organisation.id"),
    "post_hasTag_tag_0_0.csv": ("PostHasTagTag", "Post", "Tag", "Post.id", "Tag.id"),
    "comment_hasTag_tag_0_0.csv": ("CommentHasTagTag", "Comment", "Tag", "Comment.id", "Tag.id"),
}

SNB_HOT_EDGE_PROPS = {"creationDate", "joinDate", "workFrom", "edgeExists"}
SNB_HOT_NODE_PROPS = {"id", "firstName", "lastName", "name", "gender", "birthday", "creationDate", "place"}
SNB_UPDATABLE_NODE_PROPS = {
    "name",
    "url",
    "type",
    "firstName",
    "lastName",
    "gender",
    "birthday",
    "creationDate",
    "locationIP",
    "browserUsed",
    "place",
    "language",
    "email",
    "imageFile",
    "content",
    "length",
    "title",
}
SNB_UPDATABLE_EDGE_PROPS = {"creationDate", "joinDate", "workFrom", "classYear"}


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def parse_time_ms(value: str) -> int:
    value = (value or "").strip()
    if not value or value == "-1":
        return 0
    if value.isdigit():
        return int(value)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    return 0


def extract_trailing_number(name: str) -> int:
    stem = Path(name).stem
    digits = []
    for ch in reversed(stem):
        if ch.isdigit():
            digits.append(ch)
        elif digits:
            break
    if not digits:
        return 0
    return int("".join(reversed(digits)))


def read_header(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        line = f.readline()
    return line.rstrip("\n\r").split("|") if line else []


def iter_pipe_rows(path: Path, has_header: bool = True) -> Iterator[Tuple[List[str], List[str], int]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter="|")
        header: List[str]
        if has_header:
            try:
                header = next(reader)
            except StopIteration:
                return
        else:
            header = []
        for row_index, row in enumerate(reader):
            if not row:
                continue
            yield header, row, row_index


def row_dict(header: Sequence[str], row: Sequence[str]) -> Dict[str, str]:
    return {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}


def get_col(header: Sequence[str], row: Sequence[str], key: object) -> str:
    if isinstance(key, int):
        return row[key] if key < len(row) else ""
    try:
        idx = header.index(str(key))
    except ValueError:
        return ""
    return row[idx] if idx < len(row) else ""


def split_semicolon(value: str) -> List[str]:
    out = []
    for item in (value or "").split(";"):
        item = item.strip()
        if item and item != "-1":
            out.append(item)
    return out


def split_pairs(value: str) -> List[Tuple[str, str]]:
    out = []
    for item in split_semicolon(value):
        if "," not in item:
            continue
        a, b = item.split(",", 1)
        a = a.strip()
        b = b.strip()
        if a and b:
            out.append((a, b))
    return out


def length_bucket(value: str) -> str:
    try:
        n = int(value or "0")
    except ValueError:
        n = 0
    if n < 40:
        return "0"
    if n < 80:
        return "1"
    if n < 160:
        return "2"
    return "3"


def make_new_value(prop: str, i: int) -> str:
    return f"upd_{prop}_{i}"


@dataclass
class Schema:
    id: int
    name: str
    kind: str
    columns: List[str]
    metadata: Dict[str, object] = field(default_factory=dict)


class SchemaRegistry:
    def __init__(self) -> None:
        self._schemas: List[Schema] = []
        self._by_key: Dict[Tuple[object, ...], int] = {}

    def get(self, name: str, kind: str, columns: Sequence[str], **metadata: object) -> int:
        key = (name, kind, tuple(columns), tuple(sorted(metadata.items())))
        if key in self._by_key:
            return self._by_key[key]
        sid = len(self._schemas)
        self._schemas.append(Schema(sid, name, kind, list(columns), dict(metadata)))
        self._by_key[key] = sid
        return sid

    def to_json(self) -> List[Dict[str, object]]:
        return [
            {
                "id": s.id,
                "name": s.name,
                "kind": s.kind,
                "columns": s.columns,
                "metadata": s.metadata,
            }
            for s in self._schemas
        ]


class ChunkWriter:
    def __init__(self, root: Path, stage: str, chunk_size: int) -> None:
        self.root = root
        self.stage = stage
        self.stage_id = STAGE_IDS[stage]
        self.chunk_size = chunk_size
        self.dir = root / stage
        self.dir.mkdir(parents=True, exist_ok=True)
        self.index = 0
        self.f = None
        self.count = 0
        self.total = 0
        self.max_count = 0

    def _open(self) -> None:
        if self.f is not None:
            return
        path = self.dir / f"chunk_{self.index:06d}.bin"
        self.f = path.open("wb")
        self.f.write(HEADER_STRUCT.pack(MAGIC, VERSION, self.stage_id, 0, 0))
        self.count = 0

    def write(self, op_code: int, schema_id: int, row_index: int, event_time_ms: int, fields: Sequence[str]) -> None:
        if self.f is None:
            self._open()
        assert self.f is not None
        record = bytearray()
        record.extend(RECORD_FIXED_STRUCT.pack(op_code, schema_id, row_index, event_time_ms, len(fields)))
        for value in fields:
            data = str(value if value is not None else "").encode("utf-8")
            record.extend(struct.pack("<I", len(data)))
            record.extend(data)
        self.f.write(record)
        self.count += 1
        self.total += 1
        self.max_count = max(self.max_count, self.count)
        if self.count >= self.chunk_size:
            self.close_current()

    def close_current(self) -> None:
        if self.f is None:
            return
        self.f.seek(0)
        self.f.write(HEADER_STRUCT.pack(MAGIC, VERSION, self.stage_id, self.count, 0))
        self.f.close()
        self.f = None
        self.index += 1

    def close(self) -> None:
        self.close_current()


class DatasetOutput:
    def __init__(self, out_root: Path, chunk_size: int) -> None:
        self.out_root = out_root
        self.chunk_size = chunk_size
        self.schemas = SchemaRegistry()
        self.writers = {stage: ChunkWriter(out_root, stage, chunk_size) for stage in STAGE_IDS}
        self.stage_counts: Dict[str, int] = {stage: 0 for stage in STAGE_IDS}

    def schema(self, name: str, kind: str, columns: Sequence[str], **metadata: object) -> int:
        return self.schemas.get(name, kind, columns, **metadata)

    def write(self, stage: str, op_code: int, schema_id: int, row_index: int, event_time_ms: int, fields: Sequence[str]) -> None:
        self.writers[stage].write(op_code, schema_id, row_index, event_time_ms, fields)
        self.stage_counts[stage] += 1

    def close(self) -> None:
        for writer in self.writers.values():
            writer.close()


class Reservoir:
    def __init__(self, cap: int, seed: int) -> None:
        self.cap = max(0, cap)
        self.rng = random.Random(seed)
        self.items: List[List[str]] = []
        self.seen = 0

    def add(self, item: Sequence[str]) -> Tuple[Optional[List[str]], Optional[List[str]]]:
        if self.cap <= 0:
            return None, None
        self.seen += 1
        new_item = list(item)
        if len(self.items) < self.cap:
            self.items.append(new_item)
            return new_item, None
        j = self.rng.randrange(self.seen)
        if j < self.cap:
            evicted = self.items[j]
            self.items[j] = new_item
            return new_item, evicted
        return None, None

    def choose(self, rng: random.Random) -> Optional[List[str]]:
        if not self.items:
            return None
        return self.items[rng.randrange(len(self.items))]


class CandidateSets:
    def __init__(self, cap: int, seed: int, track_node_versions: bool = False) -> None:
        per = max(1, cap // 2)
        self.node_hot = Reservoir(per, seed ^ 0x101)
        self.node_cold = Reservoir(per, seed ^ 0x102)
        self.edge_hot = Reservoir(per, seed ^ 0x201)
        self.edge_cold = Reservoir(per, seed ^ 0x202)
        self.track_node_versions = track_node_versions
        self.node_versions: Dict[Tuple[str, str, str], int] = {}
        self.node_collection_enabled = True
        self.edge_watch_counts: Counter[Tuple[str, str, str, str, str, str]] = Counter()
        self.edge_current_props: Dict[Tuple[str, str, str, str, str, str], Dict[str, str]] = {}

    def set_node_collection_enabled(self, enabled: bool) -> None:
        self.node_collection_enabled = enabled

    def add_node_props(self, dataset: str, node_type: str, node_id: str, props: Dict[str, str], hot_props: set) -> None:
        if not self.node_collection_enabled or not node_id:
            return
        if self.track_node_versions:
            key = (dataset, node_type, node_id)
            if key in self.node_versions:
                return
            self.node_versions[key] = 1
        for name, value in props.items():
            if not value or name in {"dependencyTime"}:
                continue
            hot = name in hot_props
            item = [dataset, node_type, node_id, name, "1" if hot else "0"]
            (self.node_hot if hot else self.node_cold).add(item)

    def node_candidate_current(self, item: Sequence[str]) -> bool:
        return True

    def add_edge_props(
        self,
        dataset: str,
        edge_type: str,
        src_type: str,
        src_id: str,
        dst_type: str,
        dst_id: str,
        props: Dict[str, str],
        hot_props: set,
    ) -> None:
        if not src_id or not dst_id:
            return
        key = (dataset, edge_type, src_type, src_id, dst_type, dst_id)
        clean_props = {
            name: value for name, value in props.items()
            if value and name not in {"dependencyTime"}
        }
        if self.edge_watch_counts.get(key, 0) > 0:
            self._merge_edge_visible_props(key, clean_props, hot_props)
        for name, value in props.items():
            if not value or name in {"dependencyTime"}:
                continue
            hot = name in hot_props
            item = [dataset, edge_type, src_type, src_id, dst_type, dst_id, name, "1" if hot else "0"]
            inserted, evicted = (self.edge_hot if hot else self.edge_cold).add(item)
            if evicted is not None:
                self._unwatch_edge(evicted)
            if inserted is not None:
                self._watch_edge(inserted, clean_props, hot_props)

    @staticmethod
    def _edge_key_from_item(item: Sequence[str]) -> Tuple[str, str, str, str, str, str]:
        return (item[0], item[1], item[2], item[3], item[4], item[5])

    def _watch_edge(self, item: Sequence[str], props: Dict[str, str], hot_props: set) -> None:
        key = self._edge_key_from_item(item)
        self.edge_watch_counts[key] += 1
        if key not in self.edge_current_props:
            self.edge_current_props[key] = {}
        self._merge_edge_visible_props(key, props, hot_props)

    def _unwatch_edge(self, item: Sequence[str]) -> None:
        key = self._edge_key_from_item(item)
        if self.edge_watch_counts.get(key, 0) <= 1:
            self.edge_watch_counts.pop(key, None)
            self.edge_current_props.pop(key, None)
            return
        self.edge_watch_counts[key] -= 1

    def _merge_edge_visible_props(self, key: Tuple[str, str, str, str, str, str], props: Dict[str, str], hot_props: set) -> None:
        state = self.edge_current_props.setdefault(key, {})
        cold_props = {name: value for name, value in props.items() if name not in hot_props}
        if cold_props:
            for name in list(state):
                if name not in hot_props:
                    del state[name]
            state.update(cold_props)
        for name, value in props.items():
            if name in hot_props:
                state[name] = value

    def edge_candidate_current(self, item: Sequence[str]) -> bool:
        if len(item) < 8:
            return False
        state = self.edge_current_props.get(self._edge_key_from_item(item))
        return state is not None and bool(state.get(item[6]))


@dataclass
class RunRecord:
    event_time_ms: int
    logical_id: int
    intra_id: int
    op_code: int
    schema_id: int
    row_index: int
    fields: List[str]


class MixedRunBuffer:
    def __init__(self, work_dir: Path, max_records: int = 200_000) -> None:
        self.work_dir = work_dir
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.max_records = max_records
        self.buffer: List[RunRecord] = []
        self.run_paths: List[Path] = []
        self.next_run = 0

    def add(self, rec: RunRecord) -> None:
        self.buffer.append(rec)
        if len(self.buffer) >= self.max_records:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        self.buffer.sort(key=lambda r: (r.event_time_ms, r.logical_id, r.intra_id))
        path = self.work_dir / f"run_{self.next_run:06d}.bin"
        self.next_run += 1
        with path.open("wb") as f:
            for rec in self.buffer:
                record = bytearray()
                record.extend(RUN_FIXED_STRUCT.pack(
                    rec.event_time_ms,
                    rec.logical_id,
                    rec.intra_id,
                    rec.op_code,
                    rec.schema_id,
                    rec.row_index,
                    len(rec.fields),
                ))
                for value in rec.fields:
                    data = str(value if value is not None else "").encode("utf-8")
                    record.extend(struct.pack("<I", len(data)))
                    record.extend(data)
                f.write(record)
        self.run_paths.append(path)
        self.buffer.clear()

    def finish(self) -> List[Path]:
        self.flush()
        return self.run_paths

    @staticmethod
    def iter_run(path: Path) -> Iterator[RunRecord]:
        with path.open("rb") as f:
            while True:
                fixed = f.read(RUN_FIXED_STRUCT.size)
                if not fixed:
                    break
                event_time_ms, logical_id, intra_id, op_code, schema_id, row_index, field_count = RUN_FIXED_STRUCT.unpack(fixed)
                fields = []
                for _ in range(field_count):
                    raw_len = f.read(4)
                    if len(raw_len) != 4:
                        raise RuntimeError(f"truncated run field length in {path}")
                    (n,) = struct.unpack("<I", raw_len)
                    data = f.read(n)
                    if len(data) != n:
                        raise RuntimeError(f"truncated run field payload in {path}")
                    fields.append(data.decode("utf-8", errors="replace"))
                yield RunRecord(event_time_ms, logical_id, intra_id, op_code, schema_id, row_index, fields)

    def cleanup(self) -> None:
        if self.work_dir.exists():
            shutil.rmtree(self.work_dir)


class QuerySampler:
    def __init__(self) -> None:
        self.rows_by_qid: Dict[int, List[Tuple[int, int, List[str]]]] = {}
        self.emitted_by_qid: Dict[int, int] = {}

    def add_rows(self, qid: int, schema_id: int, rows: List[Tuple[int, List[str]]]) -> None:
        self.rows_by_qid[qid] = [(schema_id, row_index, values) for row_index, values in rows]
        self.emitted_by_qid[qid] = 0

    def emit_progress(self, out: DatasetOutput, cumulative_updates: int, total_updates: int) -> None:
        if total_updates <= 0:
            return
        for qid in sorted(self.rows_by_qid):
            rows = self.rows_by_qid[qid]
            target = min(len(rows), (cumulative_updates * len(rows)) // total_updates)
            emitted = self.emitted_by_qid[qid]
            while emitted < target:
                schema_id, row_index, values = rows[emitted]
                out.write("mixed_ops", OP_QUERY, schema_id, row_index, 0, values)
                emitted += 1
            self.emitted_by_qid[qid] = emitted

    def emit_all_remaining(self, out: DatasetOutput) -> None:
        for qid in sorted(self.rows_by_qid):
            rows = self.rows_by_qid[qid]
            emitted = self.emitted_by_qid[qid]
            while emitted < len(rows):
                schema_id, row_index, values = rows[emitted]
                out.write("mixed_ops", OP_QUERY, schema_id, row_index, 0, values)
                emitted += 1
            self.emitted_by_qid[qid] = emitted


def selected(row_index: int, mod: int, remainder: int) -> bool:
    return mod <= 1 or row_index % mod == remainder


def ensure_clean_dir(path: Path, force: bool) -> None:
    if path.exists():
        if not force:
            raise RuntimeError(f"output exists, use --force: {path}")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def write_meta(out: DatasetOutput, source_root: Path, dataset_type: str, dataset_name: str, args: argparse.Namespace, manifest: Dict[str, object]) -> None:
    out.close()
    meta_dir = out.out_root / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    schema = {
        "format": "rich_preprocessed_chunks",
        "magic": MAGIC.decode("latin1"),
        "version": VERSION,
        "dataset_type": dataset_type,
        "dataset_name": dataset_name,
        "source_root": str(source_root),
        "chunk_size": args.chunk_size,
        "sample_mod": args.sample_mod,
        "sample_remainder": args.sample_remainder,
        "op_codes": {
            "node": OP_NODE,
            "edge": OP_EDGE,
            "query": OP_QUERY,
            "single_node_read": OP_SINGLE_NODE_READ,
            "single_edge_read": OP_SINGLE_EDGE_READ,
            "node_update": OP_NODE_UPDATE,
            "edge_update": OP_EDGE_UPDATE,
        },
        "stages": STAGE_IDS,
        "schemas": out.schemas.to_json(),
    }
    (meta_dir / "schema.json").write_text(json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8")
    manifest = dict(manifest)
    manifest["stage_counts"] = out.stage_counts
    manifest["generated_at"] = datetime.now().isoformat(timespec="seconds")
    (meta_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


def validate_output(root: Path, chunk_size: int) -> Dict[str, object]:
    stage_counts: Dict[str, int] = {}
    max_records = 0
    chunks = 0
    errors: List[str] = []
    for stage, stage_id in STAGE_IDS.items():
        total = 0
        stage_dir = root / stage
        if not stage_dir.exists():
            stage_counts[stage] = 0
            continue
        for path in sorted(stage_dir.glob("chunk_*.bin")):
            chunks += 1
            with path.open("rb") as f:
                header = f.read(HEADER_STRUCT.size)
            if len(header) != HEADER_STRUCT.size:
                errors.append(f"{path}: short header")
                continue
            magic, version, got_stage, count, _ = HEADER_STRUCT.unpack(header)
            if magic != MAGIC or version != VERSION or got_stage != stage_id:
                errors.append(f"{path}: bad header magic/version/stage")
            if count > chunk_size:
                errors.append(f"{path}: count {count} > chunk_size {chunk_size}")
            total += count
            max_records = max(max_records, count)
        stage_counts[stage] = total
    return {
        "chunks": chunks,
        "max_chunk_records": max_records,
        "stage_counts": stage_counts,
        "errors": errors,
    }


def emit_read_requests(
    out: DatasetOutput,
    candidates: CandidateSets,
    dataset: str,
    single_read_ops: int,
    hot_weight: int,
    cold_weight: int,
    seed: int,
) -> Dict[str, object]:
    rng = random.Random(seed)
    node_schema = out.schema(
        "single_node_read",
        "single_node_read",
        ["dataset", "node_type", "node_id", "property", "is_hot"],
    )
    edge_schema = out.schema(
        "single_edge_read",
        "single_edge_read",
        ["dataset", "edge_type", "src_type", "src_id", "dst_type", "dst_id", "property", "is_hot"],
    )

    def choose_pair(hot_res: Reservoir, cold_res: Reservoir) -> Tuple[Optional[List[str]], bool]:
        total = max(0, hot_weight) + max(0, cold_weight)
        want_hot = total <= 0 or rng.randrange(total) < hot_weight
        first = hot_res if want_hot else cold_res
        second = cold_res if want_hot else hot_res
        item = first.choose(rng)
        if item is None:
            item = second.choose(rng)
        return item, want_hot

    def choose_current_node() -> Optional[List[str]]:
        for _ in range(64):
            item, _ = choose_pair(candidates.node_hot, candidates.node_cold)
            if item is None or candidates.node_candidate_current(item):
                return item
        return None

    def choose_current_edge() -> Optional[List[str]]:
        for _ in range(128):
            item, _ = choose_pair(candidates.edge_hot, candidates.edge_cold)
            if item is None or candidates.edge_candidate_current(item):
                return item
        return None

    node_written = 0
    edge_written = 0
    for _ in range(single_read_ops):
        item = choose_current_node()
        if item is not None:
            out.write("single_node_read", OP_SINGLE_NODE_READ, node_schema, node_written, 0, item[:5])
            node_written += 1
        item = choose_current_edge()
        if item is not None:
            out.write("single_edge_read", OP_SINGLE_EDGE_READ, edge_schema, edge_written, 0, item[:8])
            edge_written += 1
    return {
        "single_node_read_ops": node_written,
        "single_edge_read_ops": edge_written,
        "node_hot_candidates_seen": candidates.node_hot.seen,
        "node_cold_candidates_seen": candidates.node_cold.seen,
        "edge_hot_candidates_seen": candidates.edge_hot.seen,
        "edge_cold_candidates_seen": candidates.edge_cold.seen,
        "node_hot_candidates_retained": len(candidates.node_hot.items),
        "node_cold_candidates_retained": len(candidates.node_cold.items),
        "edge_hot_candidates_retained": len(candidates.edge_hot.items),
        "edge_cold_candidates_retained": len(candidates.edge_cold.items),
    }


def emit_property_update_requests(
    out: DatasetOutput,
    candidates: CandidateSets,
    node_stage: str,
    edge_stage: str,
    node_schema_name: str,
    edge_schema_name: str,
    update_ops: int,
    hot_weight: int,
    cold_weight: int,
    seed: int,
    node_filter: Optional[Callable[[Sequence[str]], bool]] = None,
    edge_filter: Optional[Callable[[Sequence[str]], bool]] = None,
) -> Dict[str, object]:
    rng = random.Random(seed ^ 0x5555)
    node_schema = out.schema(
        node_schema_name,
        "node_update",
        ["dataset", "node_type", "node_id", "property", "is_hot", "new_value"],
    )
    edge_schema = out.schema(
        edge_schema_name,
        "edge_update",
        ["dataset", "edge_type", "src_type", "src_id", "dst_type", "dst_id", "property", "is_hot", "new_value"],
    )

    def choose_pair(hot_res: Reservoir, cold_res: Reservoir) -> Optional[List[str]]:
        total = max(0, hot_weight) + max(0, cold_weight)
        want_hot = total <= 0 or rng.randrange(total) < hot_weight
        first = hot_res if want_hot else cold_res
        second = cold_res if want_hot else hot_res
        item = first.choose(rng)
        return item if item is not None else second.choose(rng)

    def choose_current_node() -> Optional[List[str]]:
        for _ in range(64):
            item = choose_pair(candidates.node_hot, candidates.node_cold)
            if item is None or candidates.node_candidate_current(item):
                return item
        return None

    def choose_valid_node() -> Optional[List[str]]:
        for _ in range(256):
            item = choose_current_node()
            if item is None:
                return None
            if node_filter is None or node_filter(item):
                return item
        return None

    def choose_valid_edge() -> Optional[List[str]]:
        for _ in range(256):
            item = choose_pair(candidates.edge_hot, candidates.edge_cold)
            if item is None:
                return None
            if edge_filter is None or edge_filter(item):
                return item
        return None

    node_written = 0
    edge_written = 0
    for i in range(update_ops):
        item = choose_valid_node()
        if item is not None:
            out.write(node_stage, OP_NODE_UPDATE, node_schema, node_written, 0, item[:5] + [make_new_value(item[3], i)])
            node_written += 1
        item = choose_valid_edge()
        if item is not None:
            out.write(edge_stage, OP_EDGE_UPDATE, edge_schema, edge_written, 0, item[:8] + [make_new_value(item[6], i)])
            edge_written += 1
    return {"node_update_ops": node_written, "edge_update_ops": edge_written}


def emit_finbench_update_requests(
    out: DatasetOutput,
    candidates: CandidateSets,
    update_ops: int,
    hot_weight: int,
    cold_weight: int,
    seed: int,
) -> Dict[str, object]:
    return emit_property_update_requests(
        out,
        candidates,
        "finbench_node_update",
        "finbench_edge_update",
        "finbench_node_update",
        "finbench_edge_update",
        update_ops,
        hot_weight,
        cold_weight,
        seed,
    )


def emit_snb_update_requests(
    out: DatasetOutput,
    candidates: CandidateSets,
    update_ops: int,
    hot_weight: int,
    cold_weight: int,
    seed: int,
) -> Dict[str, object]:
    # Reuse the existing update stages so all consumers see the same op codes
    # and record schema. The schema names distinguish SNB from FinBench.
    def valid_node(item: Sequence[str]) -> bool:
        return len(item) >= 4 and item[3] in SNB_UPDATABLE_NODE_PROPS

    def valid_edge(item: Sequence[str]) -> bool:
        if len(item) < 7:
            return False
        if item[1] == "PersonKnowsPerson":
            return False
        prop = item[6]
        return prop in SNB_UPDATABLE_EDGE_PROPS or prop.startswith("cold_extra_")

    return emit_property_update_requests(
        out,
        candidates,
        "finbench_node_update",
        "finbench_edge_update",
        "snb_node_update",
        "snb_edge_update",
        update_ops,
        hot_weight,
        cold_weight,
        seed,
        node_filter=valid_node,
        edge_filter=valid_edge,
    )


def maybe_candidate_node(candidates: CandidateSets, dataset: str, node_type: str, node_id: str, header: Sequence[str], row: Sequence[str], hot_props: set, id_col: str) -> None:
    props = row_dict(header, row)
    props.pop(id_col, None)
    candidates.add_node_props(dataset, node_type, node_id, props, hot_props)


def maybe_candidate_edge(
    candidates: CandidateSets,
    dataset: str,
    edge_type: str,
    src_type: str,
    src_id: str,
    dst_type: str,
    dst_id: str,
    props: Dict[str, str],
    hot_props: set,
    exclude_cols: Sequence[str] = (),
) -> None:
    filtered = {k: v for k, v in props.items() if k not in set(exclude_cols)}
    candidates.add_edge_props(dataset, edge_type, src_type, src_id, dst_type, dst_id, filtered, hot_props)


def finbench_snapshot_files(root: Path) -> Tuple[List[Path], List[Path]]:
    snap = root / "snapshot"
    nodes = []
    edges = []
    for path in sorted(snap.glob("*.csv")):
        if path.stem in NODE_KIND_FINBENCH:
            nodes.append(path)
        elif path.stem in FINBENCH_RELATIONS:
            edges.append(path)
    return nodes, edges


def finbench_relation_for_incremental(file_name: str) -> Optional[str]:
    return FINBENCH_SIMPLE_RELATION.get(file_name)


def finbench_emit_node_from_row(
    out: DatasetOutput,
    stage: str,
    schema_name: str,
    node_type: str,
    id_column: str,
    header: Sequence[str],
    row: Sequence[str],
    row_index: int,
    event_time: int,
    candidates: CandidateSets,
    dataset_name: str,
    op_code: int = OP_NODE,
    run: Optional[MixedRunBuffer] = None,
    logical_id: int = 0,
    intra_id: int = 0,
) -> int:
    sid = out.schema(
        schema_name,
        "node",
        header,
        node_type=node_type,
        id_column=id_column,
    )
    if run is None:
        out.write(stage, op_code, sid, row_index, event_time, row)
    else:
        run.add(RunRecord(event_time, logical_id, intra_id, op_code, sid, row_index, list(row)))
    maybe_candidate_node(candidates, dataset_name, node_type, get_col(header, row, id_column), header, row, FINBENCH_HOT_NODE_PROPS, id_column)
    return 1


def finbench_emit_edge_from_row(
    out: DatasetOutput,
    stage: str,
    schema_name: str,
    edge_type: str,
    header: Sequence[str],
    row: Sequence[str],
    row_index: int,
    event_time: int,
    candidates: CandidateSets,
    dataset_name: str,
    run: Optional[MixedRunBuffer] = None,
    logical_id: int = 0,
    intra_id: int = 0,
    src_override: Optional[str] = None,
    dst_override: Optional[str] = None,
    canonical_header: Optional[Sequence[str]] = None,
) -> int:
    src_type, dst_type, rel_src_col, rel_dst_col = FINBENCH_RELATIONS[edge_type]
    src_col = src_override or rel_src_col
    dst_col = dst_override or rel_dst_col
    emit_header = list(header)
    emit_row = list(row)
    if canonical_header is not None:
        emit_header = [
            src_col if col == rel_src_col else dst_col if col == rel_dst_col else col
            for col in canonical_header
        ]
        src = row_dict(header, row)
        emit_row = [src.get(col, "") for col in emit_header]
    sid = out.schema(
        schema_name,
        "edge",
        emit_header,
        edge_type=edge_type,
        src_type=src_type,
        dst_type=dst_type,
        src_column=src_col,
        dst_column=dst_col,
    )
    if run is None:
        out.write(stage, OP_EDGE, sid, row_index, event_time, emit_row)
    else:
        run.add(RunRecord(event_time, logical_id, intra_id, OP_EDGE, sid, row_index, list(emit_row)))
    props = row_dict(emit_header, emit_row)
    maybe_candidate_edge(
        candidates,
        dataset_name,
        edge_type,
        src_type,
        get_col(emit_header, emit_row, src_col),
        dst_type,
        get_col(emit_header, emit_row, dst_col),
        props,
        FINBENCH_HOT_EDGE_PROPS,
        exclude_cols=(src_col, dst_col),
    )
    return 1


def process_finbench_incremental_row(
    out: DatasetOutput,
    file_name: str,
    header: Sequence[str],
    row: Sequence[str],
    row_index: int,
    stage_nodes: str,
    stage_edges: str,
    candidates: CandidateSets,
    dataset_name: str,
    edge_columns_by_type: Dict[str, Sequence[str]],
    run: Optional[MixedRunBuffer] = None,
    logical_id: int = 0,
) -> Tuple[int, int]:
    event_time = parse_time_ms(get_col(header, row, "createTime"))
    nodes = 0
    edges = 0
    intra = 0
    if file_name in FINBENCH_DIRECT_NODE:
        node_type, id_col = FINBENCH_DIRECT_NODE[file_name]
        nodes += finbench_emit_node_from_row(out, stage_nodes, file_name, node_type, id_col, header, row, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra)
        return nodes, edges
    if file_name in FINBENCH_OWN_ACCOUNT:
        edge_type, owner_type, owner_id = FINBENCH_OWN_ACCOUNT[file_name]
        account_header = ["accountId", "accountType", "isBlocked", "nickname", "phonenum", "email", "freqLoginType", "lastLoginTime", "accountLevel", "createTime"]
        d = row_dict(header, row)
        account_row = [
            d.get("accountId", ""),
            d.get("accountType", ""),
            d.get("accountBlocked", ""),
            d.get("nickname", ""),
            d.get("phonenum", ""),
            d.get("email", ""),
            d.get("freqLoginType", ""),
            d.get("lastLoginTime", ""),
            d.get("accountLevel", ""),
            d.get("createTime", ""),
        ]
        nodes += finbench_emit_node_from_row(out, stage_nodes, file_name + ":Account", "Account", "accountId", account_header, account_row, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra)
        intra += 1
        edges += finbench_emit_edge_from_row(out, stage_edges, file_name, edge_type, header, row, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra, canonical_header=edge_columns_by_type.get(edge_type))
        return nodes, edges
    if file_name in FINBENCH_APPLY_LOAN:
        edge_type, applicant_type, applicant_id = FINBENCH_APPLY_LOAN[file_name]
        loan_header = ["loanId", "loanAmount", "balance", "loanUsage", "interestRate", "createTime"]
        d = row_dict(header, row)
        loan_row = [
            d.get("loanId", ""),
            d.get("loanAmount", ""),
            d.get("balance", ""),
            d.get("loanUsage", ""),
            d.get("interestRate", ""),
            d.get("createTime", ""),
        ]
        nodes += finbench_emit_node_from_row(out, stage_nodes, file_name + ":Loan", "Loan", "loanId", loan_header, loan_row, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra)
        intra += 1
        edges += finbench_emit_edge_from_row(out, stage_edges, file_name, edge_type, header, row, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra, canonical_header=edge_columns_by_type.get(edge_type))
        return nodes, edges
    edge_type = finbench_relation_for_incremental(file_name)
    if edge_type:
        src_override, dst_override = FINBENCH_SOURCE_OVERRIDE.get(file_name, (None, None))
        edges += finbench_emit_edge_from_row(out, stage_edges, file_name, edge_type, header, row, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra, src_override=src_override, dst_override=dst_override, canonical_header=edge_columns_by_type.get(edge_type))
        return nodes, edges
    return nodes, edges


def load_finbench_queries(out: DatasetOutput, root: Path, sample_mod: int, sample_remainder: int) -> Tuple[QuerySampler, Dict[int, int]]:
    sampler = QuerySampler()
    counts: Dict[int, int] = {}
    params = root / "params"
    for path in sorted(params.glob("complex_*_param.csv"), key=lambda p: extract_trailing_number(p.name)):
        qid = extract_trailing_number(path.name)
        header = read_header(path)
        sid = out.schema(path.name, "query", header, query_id=qid)
        rows = []
        for _, row, row_index in iter_pipe_rows(path, True):
            if selected(row_index, sample_mod, sample_remainder):
                rows.append((row_index, row))
                out.write("final_queries", OP_QUERY, sid, row_index, 0, row)
        sampler.add_rows(qid, sid, rows)
        counts[qid] = len(rows)
    return sampler, counts


def merge_mixed_runs(out: DatasetOutput, run: MixedRunBuffer, query_sampler: QuerySampler, total_sampled_rows: int) -> int:
    run_paths = run.finish()
    heap: List[Tuple[int, int, int, int, RunRecord, Iterator[RunRecord]]] = []
    for rid, path in enumerate(run_paths):
        it = MixedRunBuffer.iter_run(path)
        try:
            rec = next(it)
        except StopIteration:
            continue
        heapq.heappush(heap, (rec.event_time_ms, rec.logical_id, rec.intra_id, rid, rec, it))

    last_logical = None
    cumulative = 0
    write_records = 0
    while heap:
        _, _, _, rid, rec, it = heapq.heappop(heap)
        if last_logical is None or rec.logical_id != last_logical:
            if last_logical is not None:
                query_sampler.emit_progress(out, cumulative, total_sampled_rows)
            cumulative += 1
            last_logical = rec.logical_id
        out.write("mixed_ops", rec.op_code, rec.schema_id, rec.row_index, rec.event_time_ms, rec.fields)
        write_records += 1
        try:
            nxt = next(it)
            heapq.heappush(heap, (nxt.event_time_ms, nxt.logical_id, nxt.intra_id, rid, nxt, it))
        except StopIteration:
            pass
    if last_logical is not None:
        query_sampler.emit_progress(out, cumulative, total_sampled_rows)
    query_sampler.emit_all_remaining(out)
    run.cleanup()
    return write_records


def process_finbench(root: Path, out_root: Path, args: argparse.Namespace) -> Dict[str, object]:
    ensure_clean_dir(out_root, args.force)
    out = DatasetOutput(out_root, args.chunk_size)
    candidates = CandidateSets(args.candidate_cap, args.seed, track_node_versions=True)
    manifest: Dict[str, object] = {
        "dataset_type": "Finbench",
        "dataset_name": root.name,
        "source": str(root),
        "source_counts": {},
        "skipped_files": [],
    }
    start = time.time()

    node_files, edge_files = finbench_snapshot_files(root)
    edge_columns_by_type: Dict[str, Sequence[str]] = {}
    snapshot_node_rows = 0
    snapshot_edge_rows = 0
    for path in node_files:
        header = read_header(path)
        node_type = path.stem
        id_col = NODE_KIND_FINBENCH[node_type]
        for _, row, row_index in iter_pipe_rows(path, True):
            event_time = 0
            finbench_emit_node_from_row(out, "snapshot_nodes", path.name, node_type, id_col, header, row, row_index, event_time, candidates, root.name)
            snapshot_node_rows += 1
    for path in edge_files:
        header = read_header(path)
        edge_columns_by_type[path.stem] = list(header)
        for _, row, row_index in iter_pipe_rows(path, True):
            event_time = 0
            finbench_emit_edge_from_row(out, "snapshot_edges", path.name, path.stem, header, row, row_index, event_time, candidates, root.name)
            snapshot_edge_rows += 1
    # Rich stores node properties through repeated writes with system-specific
    # visibility semantics.  Restrict node point-read/update candidates to
    # snapshot nodes so generated workloads always target properties visible in
    # the full graph, while edge candidates still cover snapshot + incremental.
    candidates.set_node_collection_enabled(False)
    manifest["source_counts"]["snapshot_node_rows"] = snapshot_node_rows
    manifest["source_counts"]["snapshot_edge_rows"] = snapshot_edge_rows

    query_sampler, query_counts = load_finbench_queries(out, root, args.sample_mod, args.sample_remainder)
    manifest["sampled_query_rows"] = query_counts

    run = MixedRunBuffer(out_root / ".work_mixed_runs")
    sampled_update_rows = 0
    remaining_update_rows = 0
    remaining_node_records = 0
    remaining_edge_records = 0
    mixed_write_records = 0
    logical_id = 0
    inc_dir = root / "incremental"
    inc_files = sorted(inc_dir.glob("*.csv"), key=lambda p: (extract_trailing_number(p.name), p.name))
    for path in inc_files:
        op_num = extract_trailing_number(path.name)
        if op_num in FINBENCH_INCREMENTAL_SKIP_TRAILING_OPS:
            manifest["skipped_files"].append(path.name)
            continue
        header = read_header(path)
        for _, row, row_index in iter_pipe_rows(path, True):
            if selected(row_index, args.sample_mod, args.sample_remainder):
                sampled_update_rows += 1
                n, e = process_finbench_incremental_row(out, path.name, header, row, row_index, "mixed_ops", "mixed_ops", candidates, root.name, edge_columns_by_type, run=run, logical_id=logical_id)
                mixed_write_records += n + e
                logical_id += 1
            else:
                remaining_update_rows += 1
                n, e = process_finbench_incremental_row(out, path.name, header, row, row_index, "remaining_nodes", "remaining_edges", candidates, root.name, edge_columns_by_type)
                remaining_node_records += n
                remaining_edge_records += e
    manifest["source_counts"]["incremental_sampled_source_rows"] = sampled_update_rows
    manifest["source_counts"]["incremental_remaining_source_rows"] = remaining_update_rows
    manifest["source_counts"]["remaining_node_records"] = remaining_node_records
    manifest["source_counts"]["remaining_edge_records"] = remaining_edge_records
    manifest["source_counts"]["mixed_write_records_before_queries"] = mixed_write_records
    merge_mixed_runs(out, run, query_sampler, sampled_update_rows)

    read_stats = emit_read_requests(
        out,
        candidates,
        root.name,
        args.single_read_ops,
        args.hot_weight,
        args.cold_weight,
        args.seed,
    )
    manifest.update(read_stats)
    update_stats = emit_finbench_update_requests(
        out,
        candidates,
        args.update_ops,
        args.hot_weight,
        args.cold_weight,
        args.seed,
    )
    manifest.update(update_stats)
    manifest["elapsed_sec"] = round(time.time() - start, 3)
    write_meta(out, root, "Finbench", root.name, args, manifest)
    validation = validate_output(out_root, args.chunk_size)
    (out_root / "meta" / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    if validation["errors"]:
        raise RuntimeError(f"validation failed for {out_root}: {validation['errors'][:3]}")
    return {"manifest": manifest, "validation": validation}


def snb_find_data_dirs(root: Path) -> Tuple[Path, Path]:
    data_dirs = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("social_network-")]
    param_dirs = [p for p in root.iterdir() if p.is_dir() and p.name.startswith("substitution_parameters-")]
    if not data_dirs:
        raise RuntimeError(f"missing SNB social_network dir under {root}")
    if not param_dirs:
        raise RuntimeError(f"missing SNB substitution_parameters dir under {root}")
    return data_dirs[0], param_dirs[0]


def snb_emit_node(
    out: DatasetOutput,
    stage: str,
    schema_name: str,
    node_type: str,
    id_col: str,
    header: Sequence[str],
    row: Sequence[str],
    row_index: int,
    event_time: int,
    candidates: CandidateSets,
    dataset_name: str,
    run: Optional[MixedRunBuffer] = None,
    logical_id: int = 0,
    intra_id: int = 0,
) -> int:
    sid = out.schema(schema_name, "node", header, node_type=node_type, id_column=id_col)
    if run is None:
        out.write(stage, OP_NODE, sid, row_index, event_time, row)
    else:
        run.add(RunRecord(event_time, logical_id, intra_id, OP_NODE, sid, row_index, list(row)))
    maybe_candidate_node(candidates, dataset_name, node_type, get_col(header, row, id_col), header, row, SNB_HOT_NODE_PROPS, id_col)
    return 1


def snb_emit_edge(
    out: DatasetOutput,
    stage: str,
    schema_name: str,
    edge_type: str,
    src_type: str,
    src_id: str,
    dst_type: str,
    dst_id: str,
    props: Dict[str, str],
    row_index: int,
    event_time: int,
    candidates: CandidateSets,
    dataset_name: str,
    run: Optional[MixedRunBuffer] = None,
    logical_id: int = 0,
    intra_id: int = 0,
) -> int:
    cols = ["src_type", "src_id", "dst_type", "dst_id"] + sorted(props)
    values = [src_type, src_id, dst_type, dst_id] + [props[k] for k in sorted(props)]
    sid = out.schema(schema_name, "edge", cols, edge_type=edge_type, src_type=src_type, dst_type=dst_type, src_column="src_id", dst_column="dst_id")
    if run is None:
        out.write(stage, OP_EDGE, sid, row_index, event_time, values)
    else:
        run.add(RunRecord(event_time, logical_id, intra_id, OP_EDGE, sid, row_index, values))
    maybe_candidate_edge(candidates, dataset_name, edge_type, src_type, src_id, dst_type, dst_id, props, SNB_HOT_EDGE_PROPS)
    return 1


def snb_emit_explicit_edge_from_row(
    out: DatasetOutput,
    stage: str,
    path_name: str,
    header: Sequence[str],
    row: Sequence[str],
    row_index: int,
    candidates: CandidateSets,
    dataset_name: str,
) -> int:
    edge_type, src_type, dst_type, src_key, dst_key = SNB_EDGE_TABLES[path_name]
    src_id = get_col(header, row, src_key)
    dst_id = get_col(header, row, dst_key)
    def key_index(key: object) -> int:
        if isinstance(key, int):
            return key
        return list(header).index(str(key))
    src_idx = key_index(src_key)
    dst_idx = key_index(dst_key)
    cols = list(header)
    if src_idx < len(cols):
        cols[src_idx] = "src_id"
    if dst_idx < len(cols):
        cols[dst_idx] = "dst_id"
    event_time = parse_time_ms(get_col(header, row, "creationDate") or get_col(header, row, "joinDate"))
    props = {cols[i]: row[i] for i in range(min(len(cols), len(row)))}
    sid = out.schema(path_name, "edge", cols, edge_type=edge_type, src_type=src_type, dst_type=dst_type, src_column="src_id", dst_column="dst_id")
    out.write(stage, OP_EDGE, sid, row_index, event_time, row)
    maybe_candidate_edge(
        candidates,
        dataset_name,
        edge_type,
        src_type,
        src_id,
        dst_type,
        dst_id,
        props,
        SNB_HOT_EDGE_PROPS,
        exclude_cols=("src_id", "dst_id"),
    )
    return 1


def snb_implicit_edges_for_node(header: Sequence[str], row: Sequence[str], node_type: str) -> List[Tuple[str, str, str, str, str, Dict[str, str], int]]:
    d = row_dict(header, row)
    base = {"edgeExists": "1"}
    out: List[Tuple[str, str, str, str, str, Dict[str, str], int]] = []
    def add(edge_type: str, src_t: str, src_id: str, dst_t: str, dst_id: str, props: Optional[Dict[str, str]] = None) -> None:
        if not src_id or not dst_id or dst_id == "-1":
            return
        p = dict(base)
        if props:
            p.update({k: v for k, v in props.items() if v and v != "-1"})
        out.append((edge_type, src_t, src_id, dst_t, dst_id, p, parse_time_ms(p.get("creationDate", ""))))
    if node_type == "Place":
        add("PlacePartOfPlace", "Place", d.get("id", ""), "Place", d.get("isPartOf", ""))
    elif node_type == "Organisation":
        add("OrganisationLocationPlace", "Organisation", d.get("id", ""), "Place", d.get("place", ""))
    elif node_type == "Tag":
        add("TagTypeTagClass", "Tag", d.get("id", ""), "TagClass", d.get("hasType", ""))
    elif node_type == "TagClass":
        add("TagClassSubclassTagClass", "TagClass", d.get("id", ""), "TagClass", d.get("isSubclassOf", ""))
    elif node_type == "Person":
        add("PersonLocationPlace", "Person", d.get("id", ""), "Place", d.get("place", ""))
    elif node_type == "Forum":
        add("ForumModeratorPerson", "Forum", d.get("id", ""), "Person", d.get("moderator", ""), {"creationDate": d.get("creationDate", "")})
    elif node_type == "Post":
        props = {"creationDate": d.get("creationDate", "")}
        add("PostCreatorPerson", "Post", d.get("id", ""), "Person", d.get("creator", ""), props)
        add("PostContainerForum", "Post", d.get("id", ""), "Forum", d.get("Forum.id", ""), props)
        add("PostLocationPlace", "Post", d.get("id", ""), "Place", d.get("place", ""), props)
    elif node_type == "Comment":
        props = {"creationDate": d.get("creationDate", ""), "messageLengthBucket": length_bucket(d.get("length", ""))}
        add("CommentCreatorPerson", "Comment", d.get("id", ""), "Person", d.get("creator", ""), props)
        add("CommentLocationPlace", "Comment", d.get("id", ""), "Place", d.get("place", ""), props)
        add("CommentParentPost", "Comment", d.get("id", ""), "Post", d.get("replyOfPost", ""), props)
        add("CommentParentComment", "Comment", d.get("id", ""), "Comment", d.get("replyOfComment", ""), props)
    return out


def load_snb_queries(out: DatasetOutput, params: Path, sample_mod: int, sample_remainder: int) -> Tuple[QuerySampler, Dict[int, int]]:
    sampler = QuerySampler()
    counts: Dict[int, int] = {}
    for path in sorted(params.glob("interactive_*_param.txt"), key=lambda p: extract_trailing_number(p.name)):
        qid = extract_trailing_number(path.name)
        header = read_header(path)
        sid = out.schema(path.name, "query", header, query_id=qid)
        rows = []
        for _, row, row_index in iter_pipe_rows(path, True):
            if selected(row_index, sample_mod, sample_remainder):
                rows.append((row_index, row))
                out.write("final_queries", OP_QUERY, sid, row_index, 0, row)
        sampler.add_rows(qid, sid, rows)
        counts[qid] = len(rows)
    return sampler, counts


def snb_update_person_ops(fields: Sequence[str]) -> Tuple[List[Tuple[str, List[str], str, str]], List[Tuple[str, str, str, str, str, Dict[str, str]]], int]:
    if len(fields) < 3 or fields[2] != "1":
        return [], [], 0
    t = parse_time_ms(fields[0] if fields else "")
    person_id = fields[3] if len(fields) > 3 else ""
    if not person_id:
        return [], [], t
    node_header = ["id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed", "place", "language", "email"]
    def f(i: int) -> str:
        return fields[i] if i < len(fields) else ""
    node = ("Person", [person_id, f(4), f(5), f(6), f(7), f(8), f(9), f(10), f(11), f(12), f(13)], "id", "updateStream_person:Person")
    edges = [("PersonLocationPlace", "Person", person_id, "Place", f(11), {"edgeExists": "1"})]
    for tag in split_semicolon(f(14)):
        edges.append(("PersonHasInterestTag", "Person", person_id, "Tag", tag, {"edgeExists": "1", "edgeWeight": str(1 + ((int(person_id or "0") ^ int(tag or "0")) % 100)) if person_id.isdigit() and tag.isdigit() else "1"}))
    for org, year in split_pairs(f(15)):
        edges.append(("PersonStudyAtOrganisation", "Person", person_id, "Organisation", org, {"edgeExists": "1", "classYear": year}))
    for org, year in split_pairs(f(16)):
        edges.append(("PersonWorkAtOrganisation", "Person", person_id, "Organisation", org, {"edgeExists": "1", "workFrom": year}))
    return [node], edges, t


def snb_update_forum_ops(fields: Sequence[str]) -> Tuple[List[Tuple[str, List[str], str, str]], List[Tuple[str, str, str, str, str, Dict[str, str]]], int]:
    def f(i: int) -> str:
        return fields[i] if i < len(fields) else ""
    t = parse_time_ms(f(0))
    op = f(2)
    nodes: List[Tuple[str, List[str], str, str]] = []
    edges: List[Tuple[str, str, str, str, str, Dict[str, str]]] = []
    if op in {"2", "3"}:
        props = {"edgeExists": "1", "creationDate": f(5)}
        for idx, name in enumerate(["edgeTypeCode", "eventMonth", "eventDow", "srcCountryId", "srcCityId", "dstCountryId", "dstCityId", "srcActivityBucket", "dstActivityBucket", "edgeWeight", "messageLengthBucket"], start=6):
            if f(idx):
                props[name] = f(idx)
        edges.append(("PersonLikesPost" if op == "2" else "PersonLikesComment", "Person", f(3), "Post" if op == "2" else "Comment", f(4), props))
    elif op == "4":
        nodes.append(("Forum", [f(3), f(4), f(5)], "id", "updateStream_forum:Forum"))
        edges.append(("ForumModeratorPerson", "Forum", f(3), "Person", f(6), {"edgeExists": "1", "creationDate": f(5)}))
        for tag in split_semicolon(f(7)):
            edges.append(("ForumHasTagTag", "Forum", f(3), "Tag", tag, {"edgeExists": "1", "srcActivityBucket": f(8)}))
    elif op == "5":
        props = {"edgeExists": "1", "joinDate": f(5)}
        for idx, name in enumerate(["edgeTypeCode", "eventMonth", "eventDow", "srcCountryId", "srcCityId", "dstCountryId", "dstCityId", "srcActivityBucket", "dstActivityBucket", "sameCountry", "sameCity"], start=6):
            if f(idx):
                props[name] = f(idx)
        edges.append(("ForumHasMemberPerson", "Forum", f(3), "Person", f(4), props))
    elif op == "6":
        nodes.append(("Post", [f(3), f(4), f(5), f(6), f(7), f(8), f(9), f(10), f(13)], "id", "updateStream_forum:Post"))
        props = {"edgeExists": "1", "creationDate": f(5)}
        edges.append(("PostCreatorPerson", "Post", f(3), "Person", f(11), props))
        edges.append(("PostContainerForum", "Post", f(3), "Forum", f(12), props))
        edges.append(("PostLocationPlace", "Post", f(3), "Place", f(13), props))
        lb = f(15) or length_bucket(f(10))
        for tag in split_semicolon(f(14)):
            edges.append(("PostHasTagTag", "Post", f(3), "Tag", tag, {"edgeExists": "1", "messageLengthBucket": lb}))
    elif op == "7":
        nodes.append(("Comment", [f(3), f(4), f(5), f(6), f(7), f(8), f(10)], "id", "updateStream_forum:Comment"))
        props = {"edgeExists": "1", "creationDate": f(4)}
        edges.append(("CommentCreatorPerson", "Comment", f(3), "Person", f(9), props))
        edges.append(("CommentLocationPlace", "Comment", f(3), "Place", f(10), props))
        parent_props = dict(props)
        parent_props["messageLengthBucket"] = f(14) or length_bucket(f(8))
        if f(11) and f(11) != "-1":
            edges.append(("CommentParentPost", "Comment", f(3), "Post", f(11), parent_props))
        if f(12) and f(12) != "-1":
            edges.append(("CommentParentComment", "Comment", f(3), "Comment", f(12), parent_props))
        for tag in split_semicolon(f(13)):
            edges.append(("CommentHasTagTag", "Comment", f(3), "Tag", tag, {"edgeExists": "1", "messageLengthBucket": parent_props["messageLengthBucket"]}))
    elif op == "8":
        props = {"edgeExists": "1", "creationDate": f(5)}
        for idx, name in enumerate(["edgeTypeCode", "eventMonth", "eventDow", "srcCountryId", "srcCityId", "dstCountryId", "dstCityId", "srcActivityBucket", "dstActivityBucket", "edgeWeight", "interactionCnt", "sameCountry", "sameCity"], start=6):
            if f(idx):
                props[name] = f(idx)
        edges.append(("PersonKnowsPerson", "Person", f(3), "Person", f(4), props))
    return nodes, edges, t


def process_snb_update_row(
    out: DatasetOutput,
    source_name: str,
    fields: Sequence[str],
    row_index: int,
    stage_nodes: str,
    stage_edges: str,
    candidates: CandidateSets,
    dataset_name: str,
    run: Optional[MixedRunBuffer] = None,
    logical_id: int = 0,
) -> Tuple[int, int]:
    if "person" in source_name:
        node_ops, edge_ops, event_time = snb_update_person_ops(fields)
    else:
        node_ops, edge_ops, event_time = snb_update_forum_ops(fields)
    nodes = 0
    edges = 0
    intra = 0
    for node_type, values, id_col, schema_name in node_ops:
        if node_type == "Person":
            header = ["id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed", "place", "language", "email"]
        elif node_type == "Forum":
            header = ["id", "title", "creationDate"]
        elif node_type == "Post":
            header = ["id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length", "place"]
        else:
            header = ["id", "creationDate", "locationIP", "browserUsed", "content", "length", "place"]
        nodes += snb_emit_node(out, stage_nodes, schema_name, node_type, id_col, header, values, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra)
        intra += 1
    for edge_type, src_type, src_id, dst_type, dst_id, props in edge_ops:
        edges += snb_emit_edge(out, stage_edges, source_name + ":" + edge_type, edge_type, src_type, src_id, dst_type, dst_id, props, row_index, event_time, candidates, dataset_name, run=run, logical_id=logical_id, intra_id=intra)
        intra += 1
    return nodes, edges


def process_snb(root: Path, out_root: Path, args: argparse.Namespace) -> Dict[str, object]:
    ensure_clean_dir(out_root, args.force)
    out = DatasetOutput(out_root, args.chunk_size)
    candidates = CandidateSets(args.candidate_cap, args.seed)
    data_dir, params_dir = snb_find_data_dirs(root)
    manifest: Dict[str, object] = {
        "dataset_type": "Snb-v1",
        "dataset_name": root.name,
        "source": str(root),
        "data_dir": str(data_dir),
        "params_dir": str(params_dir),
        "source_counts": {},
    }
    start = time.time()
    snapshot_node_rows = 0
    snapshot_edge_rows = 0
    implicit_edge_rows = 0
    for section in ("static", "dynamic"):
        sec_dir = data_dir / section
        if not sec_dir.exists():
            continue
        for path in sorted(sec_dir.glob("*.csv")):
            if path.name not in SNB_NODE_TABLES:
                continue
            node_type, id_col = SNB_NODE_TABLES[path.name]
            header = read_header(path)
            for _, row, row_index in iter_pipe_rows(path, True):
                event_time = parse_time_ms(get_col(header, row, "creationDate"))
                snb_emit_node(out, "snapshot_nodes", path.name, node_type, id_col, header, row, row_index, event_time, candidates, root.name)
                snapshot_node_rows += 1
                for edge_type, src_t, src_id, dst_t, dst_id, props, etime in snb_implicit_edges_for_node(header, row, node_type):
                    snb_emit_edge(out, "snapshot_edges", path.name + ":implicit:" + edge_type, edge_type, src_t, src_id, dst_t, dst_id, props, row_index, etime, candidates, root.name)
                    implicit_edge_rows += 1
    for path in sorted((data_dir / "dynamic").glob("*.csv")):
        if path.name not in SNB_EDGE_TABLES:
            continue
        header = read_header(path)
        for _, row, row_index in iter_pipe_rows(path, True):
            snb_emit_explicit_edge_from_row(out, "snapshot_edges", path.name, header, row, row_index, candidates, root.name)
            snapshot_edge_rows += 1
    manifest["source_counts"]["snapshot_node_rows"] = snapshot_node_rows
    manifest["source_counts"]["snapshot_explicit_edge_rows"] = snapshot_edge_rows
    manifest["source_counts"]["snapshot_implicit_edge_rows"] = implicit_edge_rows

    query_sampler, query_counts = load_snb_queries(out, params_dir, args.sample_mod, args.sample_remainder)
    manifest["sampled_query_rows"] = query_counts

    run = MixedRunBuffer(out_root / ".work_mixed_runs")
    sampled_update_rows = 0
    remaining_update_rows = 0
    remaining_node_records = 0
    remaining_edge_records = 0
    mixed_write_records = 0
    logical_id = 0
    for path in sorted(data_dir.glob("updateStream_*.csv")):
        for _, row, row_index in iter_pipe_rows(path, False):
            if selected(row_index, args.sample_mod, args.sample_remainder):
                sampled_update_rows += 1
                n, e = process_snb_update_row(out, path.name, row, row_index, "mixed_ops", "mixed_ops", candidates, root.name, run=run, logical_id=logical_id)
                mixed_write_records += n + e
                logical_id += 1
            else:
                remaining_update_rows += 1
                n, e = process_snb_update_row(out, path.name, row, row_index, "remaining_nodes", "remaining_edges", candidates, root.name)
                remaining_node_records += n
                remaining_edge_records += e
    manifest["source_counts"]["update_sampled_source_rows"] = sampled_update_rows
    manifest["source_counts"]["update_remaining_source_rows"] = remaining_update_rows
    manifest["source_counts"]["remaining_node_records"] = remaining_node_records
    manifest["source_counts"]["remaining_edge_records"] = remaining_edge_records
    manifest["source_counts"]["mixed_write_records_before_queries"] = mixed_write_records
    merge_mixed_runs(out, run, query_sampler, sampled_update_rows)

    manifest.update(emit_read_requests(out, candidates, root.name, args.single_read_ops, args.hot_weight, args.cold_weight, args.seed))
    update_stats = emit_snb_update_requests(
        out,
        candidates,
        args.update_ops,
        args.hot_weight,
        args.cold_weight,
        args.seed,
    )
    manifest.update(update_stats)
    manifest["elapsed_sec"] = round(time.time() - start, 3)
    write_meta(out, root, "Snb-v1", root.name, args, manifest)
    validation = validate_output(out_root, args.chunk_size)
    (out_root / "meta" / "validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    if validation["errors"]:
        raise RuntimeError(f"validation failed for {out_root}: {validation['errors'][:3]}")
    return {"manifest": manifest, "validation": validation}


def infer_dataset_type(path: Path) -> str:
    parts = {p.lower() for p in path.parts}
    if "finbench" in parts:
        return "finbench"
    if "snb-v1" in parts or "snb" in parts:
        return "snb"
    if (path / "snapshot").exists() and (path / "incremental").exists():
        return "finbench"
    if any(p.name.startswith("social_network-") for p in path.iterdir() if p.is_dir()):
        return "snb"
    raise RuntimeError(f"cannot infer dataset type for {path}")


def discover_existing(input_root: Path) -> Tuple[List[Path], List[Path], List[str]]:
    fin = []
    snb = []
    missing = []
    fin_root = input_root / "Finbench"
    if fin_root.exists():
        for p in sorted(fin_root.iterdir()):
            if p.is_dir() and (p / "snapshot").exists():
                fin.append(p)
        for name in ["sf3+16", "sf100+32", "sf100+64"]:
            if not (fin_root / name).exists():
                missing.append(f"Finbench/{name}")
    else:
        missing.append("Finbench")
    snb_root = input_root / "Snb-v1"
    if snb_root.exists():
        for p in sorted(snb_root.iterdir()):
            if p.is_dir() and any(c.name.startswith("social_network-") for c in p.iterdir() if c.is_dir()):
                snb.append(p)
    else:
        missing.append("Snb-v1")
    return fin, snb, missing


def output_path_for(input_root: Path, output_root: Path, dataset: Path, dataset_type: str) -> Path:
    if dataset_type == "finbench":
        return output_root / "Finbench" / dataset.name
    return output_root / "Snb-v1" / dataset.name


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=Path("datasets/raw"))
    parser.add_argument("--output-root", type=Path, default=Path("datasets/preprocessed"))
    parser.add_argument("--dataset-root", type=Path, default=None, help="Process one dataset directory instead of all visible datasets.")
    parser.add_argument("--dataset-type", choices=["finbench", "snb"], default=None)
    parser.add_argument("--all-existing", action="store_true", help="Process all visible datasets under --input-root.")
    parser.add_argument("--chunk-size", type=int, default=200_000)
    parser.add_argument("--sample-mod", type=int, default=20)
    parser.add_argument("--sample-remainder", type=int, default=0)
    parser.add_argument("--single-read-ops", type=int, default=1_000_000)
    parser.add_argument("--update-ops", type=int, default=10_000_000)
    parser.add_argument("--candidate-cap", type=int, default=2_000_000)
    parser.add_argument("--hot-weight", type=int, default=9)
    parser.add_argument("--cold-weight", type=int, default=1)
    parser.add_argument("--seed", type=int, default=20260624)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if str(args.output_root).startswith("/tmp") or (args.dataset_root and str(args.dataset_root).startswith("/tmp")):
        raise RuntimeError("refusing to use /tmp")

    tasks: List[Tuple[str, Path, Path]] = []
    skipped: List[str] = []
    if args.dataset_root is not None:
        dtype = args.dataset_type or infer_dataset_type(args.dataset_root)
        tasks.append((dtype, args.dataset_root, output_path_for(args.input_root, args.output_root, args.dataset_root, dtype)))
    else:
        fin, snb, missing = discover_existing(args.input_root)
        skipped.extend(missing)
        if not args.all_existing:
            fin = [p for p in fin if p.name.startswith("sf0.1")]
            snb = [p for p in snb if p.name.startswith("sf0.1")]
        for p in fin:
            tasks.append(("finbench", p, output_path_for(args.input_root, args.output_root, p, "finbench")))
        for p in snb:
            tasks.append(("snb", p, output_path_for(args.input_root, args.output_root, p, "snb")))

    args.output_root.mkdir(parents=True, exist_ok=True)
    results = []
    for dtype, src, dst in tasks:
        eprint(f"[PREPROCESS] start {dtype} {src} -> {dst}")
        if dtype == "finbench":
            result = process_finbench(src, dst, args)
        else:
            result = process_snb(src, dst, args)
        validation = result["validation"]
        eprint(f"[PREPROCESS] done {src.name}: chunks={validation['chunks']} max_chunk_records={validation['max_chunk_records']}")
        results.append({"dataset_type": dtype, "source": str(src), "output": str(dst), "validation": validation})

    summary = {
        "input_root": str(args.input_root),
        "output_root": str(args.output_root),
        "processed": results,
        "skipped_missing": skipped,
    }
    summary_path = args.output_root / "preprocess_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
