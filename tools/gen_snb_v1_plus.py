#!/usr/bin/env python3
"""
Generate an SNB Interactive v1 "plus" dataset with richer edge properties.

The input is an LDBC SNB Interactive v1 dataset in
CsvCompositeMergeForeign + LongDateFormatter format. The generator creates a
    sibling dataset that keeps original node files intact, adds derived attributes
    to existing hot edge files, and emits extended query parameter files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


DATASET_SUFFIX = "-CsvCompositeMergeForeign-LongDateFormatter"
DAY_MS = 24 * 60 * 60 * 1000
YEAR_MS = 365 * DAY_MS


@dataclass
class PersonInfo:
    city: str = ""
    country: str = ""
    birthday: int = 0
    creation: int = 0
    languages: Set[str] = field(default_factory=set)
    interests: Set[str] = field(default_factory=set)


@dataclass
class MessageInfo:
    message_type: str = ""
    creation: int = 0
    length: int = 0
    language: str = ""
    creator: str = ""
    country: str = ""
    forum: str = ""
    tags: List[str] = field(default_factory=list)
    reply_of_post: str = ""
    reply_of_comment: str = ""


@dataclass
class ForumInfo:
    creation: int = 0
    moderator: str = ""
    tag_count: int = 0


@dataclass
class GraphIndex:
    place_country: Dict[str, str] = field(default_factory=dict)
    organisation_country: Dict[str, str] = field(default_factory=dict)
    tag_class: Dict[str, str] = field(default_factory=dict)
    tag_popularity: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    people: Dict[str, PersonInfo] = field(default_factory=dict)
    messages: Dict[str, MessageInfo] = field(default_factory=dict)
    forums: Dict[str, ForumInfo] = field(default_factory=dict)
    knows_pairs: Set[Tuple[str, str]] = field(default_factory=set)
    person_forums: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    forum_person_posts: Dict[Tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))
    forum_person_comments: Dict[Tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))
    pair_interactions: Dict[Tuple[str, str], List[int]] = field(default_factory=lambda: defaultdict(lambda: [0, 0]))


def die(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def stable_int(*parts: object) -> int:
    h = hashlib.blake2b(digest_size=8)
    for part in parts:
        h.update(str(part).encode("utf-8"))
        h.update(b"\x1f")
    return int.from_bytes(h.digest(), "big")


def read_rows(path: Path) -> Iterable[List[str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            yield row


def write_rows(path: Path, rows: Iterable[Sequence[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="|", lineterminator="\n")
        for row in rows:
            writer.writerow(["" if v is None else str(v) for v in row])


def parse_int(value: str, default: int = 0) -> int:
    if value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def split_multi(value: str) -> Set[str]:
    if not value:
        return set()
    return {v for v in value.split(";") if v}


def length_bucket(length: int) -> int:
    if length <= 0:
        return 0
    if length <= 40:
        return 1
    if length <= 80:
        return 2
    if length <= 160:
        return 3
    return 4


def year_bucket(year: int) -> int:
    if year <= 1980:
        return 0
    if year <= 1990:
        return 1
    if year <= 2000:
        return 2
    if year <= 2010:
        return 3
    return 4


def bool_int(value: bool) -> int:
    return 1 if value else 0


def pair_key(a: str, b: str) -> Tuple[str, str]:
    return (a, b) if a <= b else (b, a)


def infer_scale(input_root: Path, scale: str) -> str:
    if scale:
        return scale
    for child in input_root.iterdir():
        if child.is_dir() and child.name.startswith("social_network-sf") and DATASET_SUFFIX in child.name:
            middle = child.name[len("social_network-sf") :]
            return middle.split(DATASET_SUFFIX, 1)[0]
    if input_root.name.startswith("sf"):
        return input_root.name[2:]
    die("could not infer scale factor; pass --scale")
    return ""


def find_dataset_dir(input_root: Path, scale: str) -> Path:
    expected = input_root / f"social_network-sf{scale}{DATASET_SUFFIX}"
    if expected.is_dir():
        return expected
    matches = sorted(input_root.glob(f"social_network-sf*{DATASET_SUFFIX}"))
    if len(matches) == 1:
        return matches[0]
    die(f"could not find social_network-sf{scale}{DATASET_SUFFIX} under {input_root}")
    return expected


def find_params_dir(input_root: Path, scale: str) -> Optional[Path]:
    expected = input_root / f"substitution_parameters-sf{scale}"
    if expected.is_dir():
        return expected
    matches = sorted(input_root.glob("substitution_parameters-sf*"))
    return matches[0] if len(matches) == 1 else None


def link_or_copy(src: Path, dst: Path, link: bool) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    if link:
        try:
            os.link(src, dst)
            return
        except OSError:
            pass
    shutil.copy2(src, dst)


def remove_output_dir(path: Path, force: bool) -> None:
    if not path.exists():
        return
    if not force:
        die(f"output directory already exists: {path}; pass --force to replace it")
    shutil.rmtree(path)


def load_places(static_dir: Path, index: GraphIndex) -> None:
    place_type: Dict[str, str] = {}
    parent: Dict[str, str] = {}
    for i, row in enumerate(read_rows(static_dir / "place_0_0.csv")):
        if i == 0:
            continue
        pid, _name, _url, typ, is_part_of = row
        place_type[pid] = typ
        parent[pid] = is_part_of

    def country_of(place_id: str) -> str:
        if not place_id:
            return ""
        typ = place_type.get(place_id, "")
        if typ == "country":
            return place_id
        if typ == "city":
            return parent.get(place_id, "")
        if typ == "continent":
            return ""
        return parent.get(place_id, "")

    for pid in place_type:
        index.place_country[pid] = country_of(pid)


def load_static(static_dir: Path, index: GraphIndex) -> None:
    load_places(static_dir, index)

    for i, row in enumerate(read_rows(static_dir / "organisation_0_0.csv")):
        if i == 0:
            continue
        oid, _typ, _name, _url, place = row
        index.organisation_country[oid] = index.place_country.get(place, "")

    for i, row in enumerate(read_rows(static_dir / "tag_0_0.csv")):
        if i == 0:
            continue
        tid, _name, _url, tag_class = row
        index.tag_class[tid] = tag_class


def load_people(dynamic_dir: Path, index: GraphIndex) -> None:
    for i, row in enumerate(read_rows(dynamic_dir / "person_0_0.csv")):
        if i == 0:
            continue
        pid = row[0]
        city = row[8]
        index.people[pid] = PersonInfo(
            city=city,
            country=index.place_country.get(city, ""),
            birthday=parse_int(row[4]),
            creation=parse_int(row[5]),
            languages=split_multi(row[9]),
        )

    for i, row in enumerate(read_rows(dynamic_dir / "person_hasInterest_tag_0_0.csv")):
        if i == 0:
            continue
        pid, tag = row
        index.people.setdefault(pid, PersonInfo()).interests.add(tag)
        index.tag_popularity[tag] += 1


def load_forums(dynamic_dir: Path, index: GraphIndex) -> None:
    for i, row in enumerate(read_rows(dynamic_dir / "forum_0_0.csv")):
        if i == 0:
            continue
        fid, _title, creation, moderator = row
        index.forums[fid] = ForumInfo(creation=parse_int(creation), moderator=moderator)

    for i, row in enumerate(read_rows(dynamic_dir / "forum_hasTag_tag_0_0.csv")):
        if i == 0:
            continue
        fid, tag = row
        index.forums.setdefault(fid, ForumInfo()).tag_count += 1
        index.tag_popularity[tag] += 1

    for i, row in enumerate(read_rows(dynamic_dir / "forum_hasMember_person_0_0.csv")):
        if i == 0:
            continue
        fid, pid, _join = row
        index.person_forums[pid].add(fid)


def load_messages(dynamic_dir: Path, index: GraphIndex) -> None:
    for i, row in enumerate(read_rows(dynamic_dir / "post_0_0.csv")):
        if i == 0:
            continue
        mid = row[0]
        creator = row[8]
        forum = row[9]
        index.messages[mid] = MessageInfo(
            message_type="post",
            creation=parse_int(row[2]),
            length=parse_int(row[7]),
            language=row[5],
            creator=creator,
            country=row[10],
            forum=forum,
        )
        index.forum_person_posts[(forum, creator)] += 1

    for i, row in enumerate(read_rows(dynamic_dir / "comment_0_0.csv")):
        if i == 0:
            continue
        mid = row[0]
        creator = row[6]
        parent_post = row[8]
        parent_comment = row[9]
        forum = ""
        if parent_post:
            forum = index.messages.get(parent_post, MessageInfo()).forum
        elif parent_comment:
            forum = index.messages.get(parent_comment, MessageInfo()).forum
        index.messages[mid] = MessageInfo(
            message_type="comment",
            creation=parse_int(row[1]),
            length=parse_int(row[5]),
            creator=creator,
            country=row[7],
            forum=forum,
            reply_of_post=parent_post,
            reply_of_comment=parent_comment,
        )
        if forum:
            index.forum_person_comments[(forum, creator)] += 1

        parent_id = parent_post or parent_comment
        parent = index.messages.get(parent_id)
        if parent:
            bump_interaction(index, creator, parent.creator, parse_int(row[1]))

    for filename in ("post_hasTag_tag_0_0.csv", "comment_hasTag_tag_0_0.csv"):
        for i, row in enumerate(read_rows(dynamic_dir / filename)):
            if i == 0:
                continue
            mid, tag = row
            if mid in index.messages:
                index.messages[mid].tags.append(tag)
            index.tag_popularity[tag] += 1


def load_edges(dynamic_dir: Path, index: GraphIndex) -> None:
    for i, row in enumerate(read_rows(dynamic_dir / "person_knows_person_0_0.csv")):
        if i == 0:
            continue
        index.knows_pairs.add(pair_key(row[0], row[1]))

    for filename in ("person_likes_post_0_0.csv", "person_likes_comment_0_0.csv"):
        for i, row in enumerate(read_rows(dynamic_dir / filename)):
            if i == 0:
                continue
            liker, mid, creation = row
            msg = index.messages.get(mid)
            if msg:
                bump_interaction(index, liker, msg.creator, parse_int(creation))


def bump_interaction(index: GraphIndex, a: str, b: str, event_time: int) -> None:
    if not a or not b or a == b:
        return
    bucket = index.pair_interactions[pair_key(a, b)]
    bucket[0] += 1
    if event_time > bucket[1]:
        bucket[1] = event_time


def build_index(dataset_dir: Path) -> GraphIndex:
    index = GraphIndex()
    static_dir = dataset_dir / "static"
    dynamic_dir = dataset_dir / "dynamic"
    load_static(static_dir, index)
    load_people(dynamic_dir, index)
    load_forums(dynamic_dir, index)
    load_messages(dynamic_dir, index)
    load_edges(dynamic_dir, index)
    return index


def country_of_person(index: GraphIndex, pid: str) -> str:
    return index.people.get(pid, PersonInfo()).country


def country_of_org(index: GraphIndex, oid: str) -> str:
    return index.organisation_country.get(oid, "")


def common_count(a: Set[str], b: Set[str]) -> int:
    if len(a) > len(b):
        a, b = b, a
    return sum(1 for x in a if x in b)


def knows_features(index: GraphIndex, p1: str, p2: str, creation: int) -> List[object]:
    a = index.people.get(p1, PersonInfo())
    b = index.people.get(p2, PersonInfo())
    same_country = bool_int(a.country != "" and a.country == b.country)
    same_city = bool_int(a.city != "" and a.city == b.city)
    age_diff_years = abs(a.birthday - b.birthday) // YEAR_MS if a.birthday and b.birthday else 0
    common_interest = common_count(a.interests, b.interests)
    common_language = common_count(a.languages, b.languages)
    shared_forums = common_count(index.person_forums.get(p1, set()), index.person_forums.get(p2, set()))
    interaction_count, last_interaction = index.pair_interactions.get(pair_key(p1, p2), [0, 0])
    recency_bonus = 0
    if creation and last_interaction:
        recency_bonus = max(0, 20 - min(20, (last_interaction - creation) // (30 * DAY_MS)))
    trust = (
        25
        + 12 * same_country
        + 8 * same_city
        + 3 * min(common_interest, 8)
        + 5 * min(common_language, 3)
        + 2 * min(shared_forums, 10)
        + min(interaction_count, 20)
        + recency_bonus
        + stable_int("trust", p1, p2) % 8
    )
    trust = max(1, min(100, trust))
    edge_weight = 101 - trust
    return [
        same_country,
        same_city,
        age_diff_years,
        common_interest,
        common_language,
        shared_forums,
        interaction_count,
        last_interaction,
        trust,
        edge_weight,
    ]


def like_features(index: GraphIndex, liker: str, mid: str, like_time: int) -> List[object]:
    msg = index.messages.get(mid, MessageInfo())
    creator = msg.creator
    liker_country = country_of_person(index, liker)
    creator_country = country_of_person(index, creator)
    same_country = bool_int(liker_country != "" and liker_country == creator_country)
    is_friend = bool_int(pair_key(liker, creator) in index.knows_pairs)
    delay = max(0, like_time - msg.creation) if msg.creation and like_time else 0
    reaction_weight = (
        10
        + 25 * is_friend
        + 10 * same_country
        + max(0, 25 - min(25, delay // DAY_MS))
        + stable_int("reaction", liker, mid) % 30
    )
    reaction_weight = max(1, min(100, reaction_weight))
    return [
        msg.creation,
        delay,
        msg.length,
        length_bucket(msg.length),
        msg.language,
        liker_country,
        creator_country,
        same_country,
        is_friend,
        reaction_weight,
    ]


def member_features(index: GraphIndex, fid: str, pid: str) -> List[object]:
    forum = index.forums.get(fid, ForumInfo())
    person_country = country_of_person(index, pid)
    post_count = index.forum_person_posts.get((fid, pid), 0)
    comment_count = index.forum_person_comments.get((fid, pid), 0)
    is_moderator = bool_int(forum.moderator == pid and pid != "")
    active_score = min(100, 10 * is_moderator + 3 * min(post_count, 20) + min(comment_count, 40) + forum.tag_count)
    return [person_country, is_moderator, forum.tag_count, post_count, comment_count, active_score]


def tag_edge_features(index: GraphIndex, mid: str, tag: str, ordinal: int) -> List[object]:
    msg = index.messages.get(mid, MessageInfo())
    return [
        index.tag_class.get(tag, ""),
        index.tag_popularity.get(tag, 0),
        msg.creation,
        length_bucket(msg.length),
        bool_int(ordinal == 0),
    ]


def reply_features(index: GraphIndex, comment_id: str, parent_id: str) -> List[object]:
    child = index.messages.get(comment_id, MessageInfo())
    parent = index.messages.get(parent_id, MessageInfo())
    child_country = country_of_person(index, child.creator)
    parent_country = country_of_person(index, parent.creator)
    latency = max(0, child.creation - parent.creation) if child.creation and parent.creation else 0
    same_country = bool_int(child_country != "" and child_country == parent_country)
    return [
        child.creation,
        parent.creation,
        latency,
        same_country,
        child.length,
        child_country,
    ]


def enhance_edge_files(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex) -> None:
    enhance_knows(src_dynamic, dst_dynamic, index)
    enhance_likes(src_dynamic, dst_dynamic, index, "person_likes_post_0_0.csv")
    enhance_likes(src_dynamic, dst_dynamic, index, "person_likes_comment_0_0.csv")
    enhance_members(src_dynamic, dst_dynamic, index)
    enhance_message_tags(src_dynamic, dst_dynamic, index, "post_hasTag_tag_0_0.csv")
    enhance_message_tags(src_dynamic, dst_dynamic, index, "comment_hasTag_tag_0_0.csv")
    enhance_interests(src_dynamic, dst_dynamic, index)
    enhance_forum_tags(src_dynamic, dst_dynamic, index)
    enhance_study_work(src_dynamic, dst_dynamic, index, "person_studyAt_organisation_0_0.csv", "classYear")
    enhance_study_work(src_dynamic, dst_dynamic, index, "person_workAt_organisation_0_0.csv", "workFrom")


def enhance_knows(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex) -> None:
    src = src_dynamic / "person_knows_person_0_0.csv"
    dst = dst_dynamic / src.name

    def rows() -> Iterable[Sequence[object]]:
        for i, row in enumerate(read_rows(src)):
            if i == 0:
                yield row + [
                    "sameCountry",
                    "sameCity",
                    "ageDiffYears",
                    "commonInterestCnt",
                    "commonLanguageCnt",
                    "sharedForumCnt",
                    "interactionCnt",
                    "lastInteractionTime",
                    "trustScore",
                    "edgeWeight",
                ]
            else:
                yield row + knows_features(index, row[0], row[1], parse_int(row[2]))

    write_rows(dst, rows())


def enhance_likes(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex, filename: str) -> None:
    src = src_dynamic / filename
    dst = dst_dynamic / filename

    def rows() -> Iterable[Sequence[object]]:
        for i, row in enumerate(read_rows(src)):
            if i == 0:
                yield row + [
                    "messageCreationDate",
                    "likeDelayMillis",
                    "messageLength",
                    "messageLengthBucket",
                    "messageLanguage",
                    "likerCountry",
                    "creatorCountry",
                    "sameCountry",
                    "isFriendOfCreator",
                    "reactionWeight",
                ]
            else:
                yield row + like_features(index, row[0], row[1], parse_int(row[2]))

    write_rows(dst, rows())


def enhance_members(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex) -> None:
    src = src_dynamic / "forum_hasMember_person_0_0.csv"
    dst = dst_dynamic / src.name

    def rows() -> Iterable[Sequence[object]]:
        for i, row in enumerate(read_rows(src)):
            if i == 0:
                yield row + [
                    "memberCountry",
                    "isModerator",
                    "forumTagCnt",
                    "memberPostCnt",
                    "memberCommentCnt",
                    "activeScore",
                ]
            else:
                yield row + member_features(index, row[0], row[1])

    write_rows(dst, rows())


def enhance_message_tags(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex, filename: str) -> None:
    src = src_dynamic / filename
    dst = dst_dynamic / filename
    ordinal_by_message: Dict[str, int] = defaultdict(int)

    def rows() -> Iterable[Sequence[object]]:
        for i, row in enumerate(read_rows(src)):
            if i == 0:
                yield row + [
                    "tagClassId",
                    "tagPopularity",
                    "messageCreationDate",
                    "messageLengthBucket",
                    "isPrimaryTag",
                ]
            else:
                ordinal = ordinal_by_message[row[0]]
                ordinal_by_message[row[0]] += 1
                yield row + tag_edge_features(index, row[0], row[1], ordinal)

    write_rows(dst, rows())


def enhance_interests(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex) -> None:
    src = src_dynamic / "person_hasInterest_tag_0_0.csv"
    dst = dst_dynamic / src.name

    def rows() -> Iterable[Sequence[object]]:
        for i, row in enumerate(read_rows(src)):
            if i == 0:
                yield row + ["tagClassId", "tagPopularity", "interestWeight"]
            else:
                weight = 1 + stable_int("interest", row[0], row[1]) % 100
                yield row + [index.tag_class.get(row[1], ""), index.tag_popularity.get(row[1], 0), weight]

    write_rows(dst, rows())


def enhance_forum_tags(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex) -> None:
    src = src_dynamic / "forum_hasTag_tag_0_0.csv"
    dst = dst_dynamic / src.name

    def rows() -> Iterable[Sequence[object]]:
        for i, row in enumerate(read_rows(src)):
            if i == 0:
                yield row + ["tagClassId", "tagPopularity", "forumTagCnt"]
            else:
                yield row + [
                    index.tag_class.get(row[1], ""),
                    index.tag_popularity.get(row[1], 0),
                    index.forums.get(row[0], ForumInfo()).tag_count,
                ]

    write_rows(dst, rows())


def enhance_study_work(src_dynamic: Path, dst_dynamic: Path, index: GraphIndex, filename: str, year_col: str) -> None:
    src = src_dynamic / filename
    dst = dst_dynamic / filename

    def rows() -> Iterable[Sequence[object]]:
        for i, row in enumerate(read_rows(src)):
            if i == 0:
                yield row + ["personCountry", "orgCountry", "sameCountry", "yearBucket"]
            else:
                person_country = country_of_person(index, row[0])
                org_country = country_of_org(index, row[1])
                yield row + [
                    person_country,
                    org_country,
                    bool_int(person_country != "" and person_country == org_country),
                    year_bucket(parse_int(row[2])),
                ]

    write_rows(dst, rows())


def copy_unmodified_initial_files(src_dataset: Path, dst_dataset: Path, link: bool) -> None:
    enhanced_edge_files = {
        "person_knows_person_0_0.csv",
        "person_likes_post_0_0.csv",
        "person_likes_comment_0_0.csv",
        "forum_hasMember_person_0_0.csv",
        "post_hasTag_tag_0_0.csv",
        "comment_hasTag_tag_0_0.csv",
        "person_hasInterest_tag_0_0.csv",
        "forum_hasTag_tag_0_0.csv",
        "person_studyAt_organisation_0_0.csv",
        "person_workAt_organisation_0_0.csv",
    }
    for src in src_dataset.rglob("*"):
        if src.is_dir():
            continue
        rel = src.relative_to(src_dataset)
        if rel.parts and rel.parts[0] == "dynamic" and src.name in enhanced_edge_files:
            continue
        link_or_copy(src, dst_dataset / rel, link)


def generate_plus_params(src_params: Optional[Path], dst_params: Path, index: GraphIndex, link: bool) -> None:
    if src_params is None:
        return
    dst_params.mkdir(parents=True, exist_ok=True)
    for src in src_params.iterdir():
        if src.is_file():
            link_or_copy(src, dst_params / src.name, link)

    create_plus_param_file(src_params / "interactive_1_param.txt", dst_params / "interactive_plus_1_param.txt", index, ["minTrustScore"], plus_values_ic1)
    create_plus_param_file(src_params / "interactive_2_param.txt", dst_params / "interactive_plus_2_param.txt", index, ["minTrustScore", "minLengthBucket"], plus_values_ic2)
    create_plus_param_file(src_params / "interactive_3_param.txt", dst_params / "interactive_plus_3_param.txt", index, ["minTrustScore", "minLengthBucket"], plus_values_ic3)
    create_plus_param_file(src_params / "interactive_4_param.txt", dst_params / "interactive_plus_4_param.txt", index, ["minTagPopularity"], plus_values_ic4)
    create_plus_param_file(src_params / "interactive_5_param.txt", dst_params / "interactive_plus_5_param.txt", index, ["minActiveScore"], plus_values_ic5)
    create_plus_param_file(src_params / "interactive_6_param.txt", dst_params / "interactive_plus_6_param.txt", index, ["minTagPopularity"], plus_values_ic6)
    create_plus_param_file(src_params / "interactive_7_param.txt", dst_params / "interactive_plus_7_param.txt", index, ["minReactionWeight"], plus_values_ic7)
    create_plus_param_file(src_params / "interactive_8_param.txt", dst_params / "interactive_plus_8_param.txt", index, ["maxReplyLatencyMillis"], plus_values_ic8)
    create_plus_param_file(src_params / "interactive_9_param.txt", dst_params / "interactive_plus_9_param.txt", index, ["minTrustScore"], plus_values_ic9)
    create_plus_param_file(src_params / "interactive_10_param.txt", dst_params / "interactive_plus_10_param.txt", index, ["minInterestWeight"], plus_values_ic10)
    create_plus_param_file(src_params / "interactive_11_param.txt", dst_params / "interactive_plus_11_param.txt", index, ["minYearBucket"], plus_values_ic11)
    create_plus_param_file(src_params / "interactive_12_param.txt", dst_params / "interactive_plus_12_param.txt", index, ["minTagPopularity"], plus_values_ic12)
    create_plus_param_file(src_params / "interactive_13_param.txt", dst_params / "interactive_plus_13_param.txt", index, ["maxEdgeWeight"], plus_values_ic13)
    create_plus_param_file(src_params / "interactive_14_param.txt", dst_params / "interactive_plus_14_param.txt", index, ["maxEdgeWeight", "minInteractionCnt"], plus_values_ic14)


def create_plus_param_file(
    src: Path,
    dst: Path,
    index: GraphIndex,
    extra_header: Sequence[str],
    make_values,
) -> None:
    if not src.exists():
        return
    out_rows: List[Sequence[object]] = []
    for i, row in enumerate(read_rows(src)):
        if i == 0:
            out_rows.append(row + list(extra_header))
        else:
            out_rows.append(row + make_values(row, index))
    write_rows(dst, out_rows)


def plus_values_ic1(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [35 + stable_int("ic1", pid) % 35]


def plus_values_ic2(row: Sequence[str], index: GraphIndex) -> List[object]:
    pid = row[0]
    return [30 + stable_int("ic2", pid) % 40, 1 + stable_int("ic2_bucket", pid) % 3]


def plus_values_ic3(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [30 + stable_int("ic3", pid) % 40, 1 + stable_int("ic3_bucket", pid) % 3]


def plus_values_ic4(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [10 + stable_int("ic4_pop", pid) % 500]


def plus_values_ic5(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [20 + stable_int("ic5_active", pid) % 60]


def plus_values_ic6(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [10 + stable_int("ic6_pop", pid) % 500]


def plus_values_ic7(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [20 + stable_int("ic7_react", pid) % 60]


def plus_values_ic8(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    delay_days = [1, 7, 30][stable_int("ic8_delay", pid) % 3]
    return [delay_days * DAY_MS]


def plus_values_ic9(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [30 + stable_int("ic9", pid) % 40]


def plus_values_ic10(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [20 + stable_int("ic10_interest", pid) % 60]


def plus_values_ic11(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [stable_int("ic11_bucket", pid) % 5]


def plus_values_ic12(row: Sequence[str], _index: GraphIndex) -> List[object]:
    pid = row[0]
    return [10 + stable_int("ic12_pop", pid) % 500]


def plus_values_ic13(row: Sequence[str], _index: GraphIndex) -> List[object]:
    p1, p2 = row[0], row[1]
    min_trust = 25 + stable_int("ic13", p1, p2) % 45
    return [101 - min_trust]


def plus_values_ic14(row: Sequence[str], _index: GraphIndex) -> List[object]:
    p1, p2 = row[0], row[1]
    min_trust = 20 + stable_int("ic14", p1, p2) % 45
    max_edge = 101 - min_trust
    return [max_edge, stable_int("ic14_interactions", p1, p2) % 5]


def enhance_update_stream(src_dataset: Path, dst_dataset: Path, index: GraphIndex) -> None:
    for stem in ("person", "forum"):
        src = src_dataset / f"updateStream_0_0_{stem}.csv"
        if not src.exists():
            continue
        dst = dst_dataset / f"updateStream_plus_0_0_{stem}.csv"
        write_rows(dst, enhanced_update_rows(src, index))


def enhanced_update_rows(src: Path, index: GraphIndex) -> Iterable[Sequence[object]]:
    for row in read_rows(src):
        if len(row) < 3:
            yield row
            continue
        op = row[2]
        payload = row[3:]
        if op == "1":
            pid = payload[0]
            city = payload[8] if len(payload) > 8 else ""
            person = PersonInfo(
                city=city,
                country=index.place_country.get(city, ""),
                birthday=parse_int(payload[4]) if len(payload) > 4 else 0,
                creation=parse_int(payload[5]) if len(payload) > 5 else 0,
                languages=split_multi(payload[9] if len(payload) > 9 else ""),
                interests=split_multi(payload[11] if len(payload) > 11 else ""),
            )
            index.people[pid] = person
            yield row + [person.country, len(person.languages), len(person.interests)]
        elif op == "2" or op == "3":
            liker, mid, creation = payload[0], payload[1], parse_int(payload[2])
            yield row + like_features(index, liker, mid, creation)
        elif op == "4":
            fid = payload[0]
            title = payload[1] if len(payload) > 1 else ""
            creation = parse_int(payload[2] if len(payload) > 2 else "0")
            moderator = payload[3] if len(payload) > 3 else ""
            tags = split_multi(payload[4] if len(payload) > 4 else "")
            index.forums[fid] = ForumInfo(creation=creation, moderator=moderator, tag_count=len(tags))
            yield row + [len(tags), country_of_person(index, moderator), stable_int("forum", fid, title) % 100]
        elif op == "5":
            fid, pid = payload[0], payload[1]
            index.person_forums[pid].add(fid)
            yield row + member_features(index, fid, pid)
        elif op == "6":
            mid = payload[0]
            creation = parse_int(payload[2] if len(payload) > 2 else "0")
            length = parse_int(payload[7] if len(payload) > 7 else "0")
            creator = payload[8] if len(payload) > 8 else ""
            forum = payload[9] if len(payload) > 9 else ""
            country = payload[10] if len(payload) > 10 else ""
            tags = [t for t in (payload[11].split(";") if len(payload) > 11 and payload[11] else []) if t]
            index.messages[mid] = MessageInfo(
                message_type="post",
                creation=creation,
                length=length,
                language=payload[5] if len(payload) > 5 else "",
                creator=creator,
                country=country,
                forum=forum,
                tags=tags,
            )
            index.forum_person_posts[(forum, creator)] += 1
            yield row + [len(tags), length_bucket(length), country_of_person(index, creator)]
        elif op == "7":
            mid = payload[0]
            creation = parse_int(payload[1] if len(payload) > 1 else "0")
            length = parse_int(payload[5] if len(payload) > 5 else "0")
            creator = payload[6] if len(payload) > 6 else ""
            country = payload[7] if len(payload) > 7 else ""
            reply_post = "" if len(payload) <= 8 or payload[8] == "-1" else payload[8]
            reply_comment = "" if len(payload) <= 9 or payload[9] == "-1" else payload[9]
            tags = [t for t in (payload[10].split(";") if len(payload) > 10 and payload[10] else []) if t]
            parent_id = reply_post or reply_comment
            parent = index.messages.get(parent_id, MessageInfo())
            forum = parent.forum
            index.messages[mid] = MessageInfo(
                message_type="comment",
                creation=creation,
                length=length,
                creator=creator,
                country=country,
                forum=forum,
                tags=tags,
                reply_of_post=reply_post,
                reply_of_comment=reply_comment,
            )
            if forum:
                index.forum_person_comments[(forum, creator)] += 1
            bump_interaction(index, creator, parent.creator, creation)
            latency = max(0, creation - parent.creation) if parent.creation else 0
            same_country = bool_int(country_of_person(index, creator) == country_of_person(index, parent.creator) and creator and parent.creator)
            yield row + [len(tags), length_bucket(length), parent.creation, latency, same_country]
        elif op == "8":
            p1, p2 = payload[0], payload[1]
            creation = parse_int(payload[2] if len(payload) > 2 else "0")
            index.knows_pairs.add(pair_key(p1, p2))
            yield row + knows_features(index, p1, p2, creation)
        else:
            yield row


def write_manifest(
    output_root: Path,
    src_dataset: Path,
    dst_dataset: Path,
    dst_params: Optional[Path],
    scale: str,
) -> None:
    manifest = {
        "name": f"snb-interactive-v1-sf{scale}-plus",
        "source_dataset": str(src_dataset),
        "dataset": str(dst_dataset),
        "parameters": str(dst_params) if dst_params else "",
        "format": "CsvCompositeMergeForeign + LongDateFormatter + RichEdgePlus",
        "enhanced_edge_files": [
            "dynamic/person_knows_person_0_0.csv",
            "dynamic/person_likes_post_0_0.csv",
            "dynamic/person_likes_comment_0_0.csv",
            "dynamic/forum_hasMember_person_0_0.csv",
            "dynamic/post_hasTag_tag_0_0.csv",
            "dynamic/comment_hasTag_tag_0_0.csv",
            "dynamic/person_hasInterest_tag_0_0.csv",
            "dynamic/forum_hasTag_tag_0_0.csv",
            "dynamic/person_studyAt_organisation_0_0.csv",
            "dynamic/person_workAt_organisation_0_0.csv",
        ],
        "materialized_plus_edges": [],
        "plus_query_parameter_files": [
            "interactive_plus_1_param.txt",
            "interactive_plus_2_param.txt",
            "interactive_plus_3_param.txt",
            "interactive_plus_4_param.txt",
            "interactive_plus_5_param.txt",
            "interactive_plus_6_param.txt",
            "interactive_plus_7_param.txt",
            "interactive_plus_8_param.txt",
            "interactive_plus_9_param.txt",
            "interactive_plus_10_param.txt",
            "interactive_plus_11_param.txt",
            "interactive_plus_12_param.txt",
            "interactive_plus_13_param.txt",
            "interactive_plus_14_param.txt",
        ],
    }
    with (output_root / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")

    schema = output_root / "README_RichEdgePlus.md"
    schema.write_text(
        """# SNB Interactive v1 RichEdgePlus

This directory is generated from the official SNB Interactive v1
CsvCompositeMergeForeign + LongDateFormatter dataset.

The original node files are preserved. Existing hot edge files receive extra
columns. No reverse/index edges are materialized; the plus dataset keeps the
official SNB-v1 edge topology.

## Main Added Edge Attributes

- `trustScore`, `edgeWeight`, `sameCountry`, `commonInterestCnt`,
  `sharedForumCnt` on `Person-knows-Person`.
- `likeDelayMillis`, `reactionWeight`, `isFriendOfCreator`, message length and
  country fields on like edges.
- `activeScore`, `memberPostCnt`, `memberCommentCnt`, `isModerator` on forum
  membership edges.
- `tagClassId`, `tagPopularity`, `isPrimaryTag`, message time/length buckets on
  tag-message edges.
- `personCountry`, `orgCountry`, `sameCountry`, `yearBucket` on study/work edges.

## Extended Query Parameter Files

`interactive_plus_1` through `interactive_plus_14` keep the official parameters
and add one or two edge-property predicates per query, such as trust thresholds,
message length buckets, tag popularity, active-score filters, interest weights,
reply latency, and max edge weight.

The plus dataset is a derived workload, not an official LDBC result format.
""",
        encoding="utf-8",
    )


def generate(args: argparse.Namespace) -> None:
    input_root = Path(args.input_root).expanduser().resolve()
    if not input_root.is_dir():
        die(f"input root does not exist: {input_root}")
    scale = infer_scale(input_root, args.scale)
    src_dataset = find_dataset_dir(input_root, scale)
    src_params = find_params_dir(input_root, scale)
    output_root = Path(args.output_root).expanduser().resolve() if args.output_root else input_root.parent / f"sf{scale}+"
    dst_dataset = output_root / f"social_network-sf{scale}+{DATASET_SUFFIX}"
    dst_params = output_root / f"substitution_parameters-sf{scale}+"

    remove_output_dir(output_root, args.force)
    output_root.mkdir(parents=True, exist_ok=True)

    print(f"[1/7] building in-memory indexes from {src_dataset}")
    index = build_index(src_dataset)

    print(f"[2/7] linking/copying unchanged files to {dst_dataset}")
    copy_unmodified_initial_files(src_dataset, dst_dataset, args.link_unchanged)

    print("[3/7] writing enhanced hot edge files")
    enhance_edge_files(src_dataset / "dynamic", dst_dataset / "dynamic", index)

    print("[4/7] keeping original edge topology; no plus edges materialized")

    print("[5/7] generating extended query parameter files")
    generate_plus_params(src_params, dst_params if src_params else None, index, args.link_unchanged)

    if args.enhance_update_stream:
        print("[6/7] generating plus update stream files")
        enhance_update_stream(src_dataset, dst_dataset, index)
    else:
        print("[6/7] skipping plus update stream files")

    print("[7/7] writing manifest")
    write_manifest(output_root, src_dataset, dst_dataset, dst_params if src_params else None, scale)
    print(f"done: {output_root}")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an SNB-v1 RichEdgePlus dataset.")
    parser.add_argument("--input-root", required=True, help="Directory containing social_network-sfX... and substitution_parameters-sfX.")
    parser.add_argument("--output-root", default="", help="Output directory. Default: sibling sfX+ directory.")
    parser.add_argument("--scale", default="", help="Scale factor, e.g. 0.1, 30, 100. Inferred by default.")
    parser.add_argument("--force", action="store_true", help="Replace output directory if it already exists.")
    parser.add_argument("--no-link-unchanged", dest="link_unchanged", action="store_false", help="Copy unchanged files instead of hard-linking them.")
    parser.add_argument("--skip-update-stream", dest="enhance_update_stream", action="store_false", help="Do not generate updateStream_plus_0_0_*.csv.")
    parser.set_defaults(link_unchanged=True, enhance_update_stream=True)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    generate(parse_args(argv))


if __name__ == "__main__":
    main()
