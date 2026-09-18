# SNB-v1 RichEdgePlus20 生成脚本说明

脚本：`tools/gen_snb_v1_plus20.py`

这个脚本用于从官方 SNB Interactive v1 `CsvCompositeMergeForeign + LongDateFormatter`
数据集生成一个轻量增强版数据集。它不会新增物化边，不改变原始拓扑，只在已有热点边文件上添加短码化、桶值化的派生属性。查询参数文件保持官方 `interactive_1..14_param.txt` 的原始格式，不再生成额外过滤参数。

默认输出目录是输入目录同级的 `sfX+20/`，例如：

```bash
python3 tools/gen_snb_v1_plus20.py \
  --input-root "$DATA_ROOT/Snb-v1/sf0.1" \
  --scale 0.1 \
  --force
```

生成后目录形态为：

```text
sf0.1+20/
  social_network-sf0.1+20-CsvCompositeMergeForeign-LongDateFormatter/
  substitution_parameters-sf0.1+20/
  manifest.json
  README_RichEdgePlus20.md
```

## 设计目标

这版不再追求把边属性堆得很宽，而是把 4 个原始边属性扩展到 20 个业务属性，并额外保留 Rich 内部使用的 `edgeExists`。新增属性尽量满足三个约束：

```text
1. 大部分是短字段或桶值字段，降低定长空槽浪费。
2. 大部分 query 只读取 Shard0。
3. query 使用官方原始参数，不额外增加过滤条件。
```

## 边属性集合

20 个业务边属性如下：

```text
creationDate
joinDate
classYear
workFrom
edgeTypeCode
eventMonth
eventDow
srcCountryId
srcCityId
dstCountryId
dstCityId
srcActivityBucket
dstActivityBucket
edgeWeight
interactionCnt
sameCountry
sameCity
tagClassId
tagPopularityBucket
messageLengthBucket
```

`edgeExists` 是 Rich 内部存在性槽位，不计入上面的 20 个业务属性。

## 属性分片方案

Shard0 是热分片，当前大多数 query 只需要访问这个分片：

```text
edgeExists
creationDate
joinDate
workFrom
edgeTypeCode
eventMonth
eventDow
srcCountryId
srcCityId
srcActivityBucket
edgeWeight
interactionCnt
sameCountry
sameCity
tagClassId
tagPopularityBucket
messageLengthBucket
```

Shard1 是冷分片，主要放不常被 query 直接读取的目的端属性：

```text
classYear
dstCountryId
dstCityId
dstActivityBucket
```

固定属性长度：

```text
Shard0: 68 bytes  # 含 edgeExists
Shard1: 14 bytes
Total : 82 bytes  # 其中业务属性 81 bytes
```

## 每类边新增的属性

```text
person_knows_person:
  保留 creationDate
  新增 edgeTypeCode, eventMonth, eventDow
  新增 src/dst country-city 短码、src/dst activity bucket
  新增 edgeWeight, interactionCnt, sameCountry, sameCity

person_likes_post / person_likes_comment:
  保留 creationDate
  新增 edgeTypeCode, eventMonth, eventDow
  新增 src/dst country-city 短码、src/dst activity bucket
  新增 edgeWeight, messageLengthBucket

forum_hasMember_person:
  保留 joinDate
  新增 edgeTypeCode, eventMonth, eventDow
  新增 src/dst country-city 短码、src/dst activity bucket
  新增 sameCountry, sameCity

forum_hasTag_tag:
  新增 edgeTypeCode, srcActivityBucket, tagClassId, tagPopularityBucket

person_hasInterest_tag:
  新增 edgeTypeCode, srcCountryId, srcCityId, srcActivityBucket
  新增 tagClassId, tagPopularityBucket, edgeWeight

post_hasTag_tag / comment_hasTag_tag:
  新增 edgeTypeCode, eventMonth, eventDow, srcCountryId, srcCityId
  新增 tagClassId, tagPopularityBucket, messageLengthBucket

person_studyAt_organisation:
  保留 classYear
  新增 edgeTypeCode, srcCountryId, srcCityId, dstCountryId
  新增 srcActivityBucket, dstActivityBucket

person_workAt_organisation:
  保留 workFrom
  新增 edgeTypeCode, srcCountryId, srcCityId, dstCountryId
  新增 srcActivityBucket, dstActivityBucket
```

## 查询参数

plus20 只复制官方 `interactive_1_param.txt` 到 `interactive_14_param.txt`，不生成 `interactive_plus_*` 文件。Rich 测试脚本默认读取原始参数文件，query 不会按新增边属性添加额外过滤条件。

## 和旧版 plus 的关系

`gen_snb_v1_plus.py` 保留不动，继续生成旧的 `sfX+/` 38 属性版本。

`gen_snb_v1_plus20.py` 是新的轻量版本，默认生成 `sfX+20/`，不会覆盖旧数据集。
