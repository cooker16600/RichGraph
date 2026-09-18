# SNB-v1 RichEdgePlus 生成脚本说明

脚本位置：

```text
tools/gen_snb_v1_plus.py
```

这个脚本用于从官方 SNB Interactive v1 数据集生成一个派生版本 `sf+`。它的目标是让数据集更适合 Rich 的边属性扫描能力，但保持原始 SNB-v1 的点边拓扑不变。

## 输入与输出

输入目录示例（`$DATA_ROOT` 由使用者自行设置）：

```text
$DATA_ROOT/Snb-v1/sf0.1/
  social_network-sf0.1-CsvCompositeMergeForeign-LongDateFormatter/
  substitution_parameters-sf0.1/
```

输出目录示例：

```text
$DATA_ROOT/Snb-v1/sf0.1+/
  social_network-sf0.1+-CsvCompositeMergeForeign-LongDateFormatter/
  substitution_parameters-sf0.1+/
  manifest.json
  README_RichEdgePlus.md
```

正式生成命令：

```bash
python3 tools/gen_snb_v1_plus.py \
  --input-root "$DATA_ROOT/Snb-v1/sf0.1" \
  --output-root "$DATA_ROOT/Snb-v1/sf0.1+" \
  --force
```

## 不做的事情

当前版本明确不做这些事：

```text
不修改原始数据集
不新增物化边
不新增反向边
不新增索引边
不改变节点类型
不改变边类型
不改变原始 query 参数文件
```

也就是说，`sf+` 仍然使用 SNB-v1 原始拓扑，只是在已有热点边表上追加属性列。

## 建索引阶段

脚本先读取原始 `static/` 和 `dynamic/` CSV，构建用于派生边属性的内存索引：

```text
Person:
  city, country, birthday, creationDate, languages, interests

Message:
  type, creationDate, length, language, creator, country, forum, tags, parent

Forum:
  moderator, tagCount

Tag:
  tagClass, tagPopularity

关系统计:
  person -> joined forums
  forum + person -> post/comment count
  person pair -> interactionCount / lastInteractionTime
  knows pair set
```

这些索引只用于生成派生属性，不会改变原始节点和边。

## 增强的边表

脚本只重写下列已有边表，输出格式是“原始列 + 新增属性列”。

### `person_knows_person_0_0.csv`

原始列：

```text
Person.id | Person.id | creationDate
```

新增列：

```text
sameCountry
sameCity
ageDiffYears
commonInterestCnt
commonLanguageCnt
sharedForumCnt
interactionCnt
lastInteractionTime
trustScore
edgeWeight
```

生成逻辑：

```text
sameCountry / sameCity:
  比较两端 Person 的国家和城市

ageDiffYears:
  根据 birthday 计算年龄差

commonInterestCnt:
  两端 Person 共同兴趣 tag 数

commonLanguageCnt:
  两端 Person 共同语言数

sharedForumCnt:
  两端 Person 共同加入的 forum 数

interactionCnt / lastInteractionTime:
  根据 likes 和 replies 统计两人之间的互动次数和最后互动时间

trustScore:
  基于同国、同城、共同兴趣、共同语言、共同 forum、互动次数、最近互动时间和稳定 hash 计算

edgeWeight:
  101 - trustScore
```

### `person_likes_post_0_0.csv` / `person_likes_comment_0_0.csv`

原始列：

```text
Person.id | Post.id/Comment.id | creationDate
```

新增列：

```text
messageCreationDate
likeDelayMillis
messageLength
messageLengthBucket
messageLanguage
likerCountry
creatorCountry
sameCountry
isFriendOfCreator
reactionWeight
```

生成逻辑：

```text
messageCreationDate / messageLength / messageLanguage:
  从被点赞的 Post/Comment 派生

likeDelayMillis:
  like 时间 - message 创建时间

likerCountry / creatorCountry:
  从 liker 和 message creator 的 Person 信息派生

sameCountry:
  likerCountry == creatorCountry

isFriendOfCreator:
  liker 和 creator 是否存在 knows 边

reactionWeight:
  基于是否好友、是否同国、点赞延迟和稳定 hash 计算
```

### `forum_hasMember_person_0_0.csv`

原始列：

```text
Forum.id | Person.id | joinDate
```

新增列：

```text
memberCountry
isModerator
forumTagCnt
memberPostCnt
memberCommentCnt
activeScore
```

生成逻辑：

```text
memberCountry:
  成员所属国家

isModerator:
  成员是否为 forum moderator

forumTagCnt:
  forum 关联的 tag 数

memberPostCnt / memberCommentCnt:
  成员在该 forum 中创建的 post/comment 数

activeScore:
  基于 moderator 身份、post/comment 数和 forumTagCnt 计算
```

### `post_hasTag_tag_0_0.csv` / `comment_hasTag_tag_0_0.csv`

原始列：

```text
Post.id/Comment.id | Tag.id
```

新增列：

```text
tagClassId
tagPopularity
messageCreationDate
messageLengthBucket
isPrimaryTag
```

生成逻辑：

```text
tagClassId:
  从 Tag 表的 hasType 派生

tagPopularity:
  统计该 tag 在 interest、forum tag、message tag 中的出现次数

messageCreationDate / messageLengthBucket:
  从对应 message 派生

isPrimaryTag:
  同一 message 的第一个 tag 标为 1，其余为 0
```

### `person_hasInterest_tag_0_0.csv`

新增列：

```text
tagClassId
tagPopularity
interestWeight
```

`interestWeight` 使用 `Person.id + Tag.id` 的稳定 hash 生成，范围为 1 到 100。

### `forum_hasTag_tag_0_0.csv`

新增列：

```text
tagClassId
tagPopularity
forumTagCnt
```

### `person_studyAt_organisation_0_0.csv` / `person_workAt_organisation_0_0.csv`

新增列：

```text
personCountry
orgCountry
sameCountry
yearBucket
```

`yearBucket` 根据 `classYear` 或 `workFrom` 分桶。

## Query 参数扩展

脚本会保留官方 `interactive_1_param.txt` 到 `interactive_14_param.txt`，并额外生成：

```text
interactive_plus_1_param.txt
...
interactive_plus_14_param.txt
```

每个 plus query 只追加 1 到 2 个边属性过滤参数。

| Query | 新增参数 | 预期使用的边属性 |
|---|---|---|
| `IC1` | `minTrustScore` | `knows.trustScore` |
| `IC2` | `minTrustScore`, `minLengthBucket` | `knows.trustScore`, message tag/like 边上的 `messageLengthBucket` |
| `IC3` | `minTrustScore`, `minLengthBucket` | `knows.trustScore`, message tag/like 边上的 `messageLengthBucket` |
| `IC4` | `minTagPopularity` | `post_hasTag.tagPopularity` |
| `IC5` | `minActiveScore` | `forum_hasMember.activeScore` |
| `IC6` | `minTagPopularity` | `post_hasTag.tagPopularity` |
| `IC7` | `minReactionWeight` | `person_likes_post/comment.reactionWeight` |
| `IC8` | `maxReplyLatencyMillis` | Rich 导入 Comment 的 replyOf 隐式边时计算该属性 |
| `IC9` | `minTrustScore` | `knows.trustScore` |
| `IC10` | `minInterestWeight` | `person_hasInterest.interestWeight` |
| `IC11` | `minYearBucket` | `person_workAt_organisation.yearBucket` |
| `IC12` | `minTagPopularity` | `comment_hasTag.tagPopularity` |
| `IC13` | `maxEdgeWeight` | `knows.edgeWeight` |
| `IC14` | `maxEdgeWeight`, `minInteractionCnt` | `knows.edgeWeight`, `knows.interactionCnt` |

这些文件只是参数扩展，真正的 query 执行逻辑需要在后续 Rich SNB-v1 测试脚本里读取这些参数并应用到对应边属性上。

## Update Stream 扩展

原始 update stream 保留不变：

```text
updateStream_0_0_person.csv
updateStream_0_0_forum.csv
```

脚本额外生成：

```text
updateStream_plus_0_0_person.csv
updateStream_plus_0_0_forum.csv
```

格式仍然是：

```text
eventDate | dependantDate | opCode | 原始 payload... | 追加属性...
```

例如：

```text
ADD_PERSON:
  追加 country, languageCount, interestCount

ADD_LIKE_POST / ADD_LIKE_COMMENT:
  追加 like 相关属性

ADD_FORUM_MEMBERSHIP:
  追加 member 活跃度属性

ADD_FRIENDSHIP:
  追加 knows/trust 相关属性
```

## 当前规模变化参考

以 SF0.1 smoke 结果为例：

```text
初始图 CSV:
  原始约 53.1 MB
  sf0.1+ 约 66.9 MB
  增加约 26%

包含 updateStream_plus 的完整目录:
  原始约 80.6 MB
  sf0.1+ 约 128.5 MB
  增加约 59%
```

这个规模变化来自边表追加属性列和额外的 update plus stream，不来自新增边。
