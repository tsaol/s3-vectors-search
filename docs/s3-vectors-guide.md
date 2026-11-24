# S3 Vectors 怎么用

> 本文的数字和规则都来自 AWS 官方文档（2026-11-24 查阅），每节末尾有出处。上限和价格会变，用之前请再看一次官方文档。

## 1. 它是什么

S3 Vectors 是 S3 里的一种新桶：**向量桶**（vector bucket）。它专门用来存向量并按相似度查询，不用建集群、不用管机器，按用量付费。

- **向量桶**：放向量的桶，和普通 S3 桶是两种桶。"阻止公有访问"永远打开，关不掉。
- **向量索引**（vector index）：在向量桶里再分的组。查询只在一个索引里做。维度、距离算法在建索引时定死。
- **向量**：一条记录 = 一个 key + 一组浮点数 + 一些元数据（metadata）。

它适合"数据很多、查询不太频繁"的场景：冷查询亚秒级，热查询最快约 100 ms。官方说法是 *"ideal for workloads where queries are less frequent"*。如果要很高的 QPS 和很低的延迟，可以把索引导出到 OpenSearch Serverless。

出处：[Working with S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html)

## 2. 上限（建之前要知道）

- 每个区域每个账号：最多 10,000 个向量桶；每个向量桶最多 10,000 个索引
- 每个索引：最多 **20 亿**个向量
- 维度：1 到 4,096
- 元数据（ENHANCED 索引）：每个向量总共最多 100 KB；其中可过滤的最多 40 KB；最多 50 个 key
- 元数据（CLASSIC 索引）：每个向量总共最多 40 KB；其中可过滤的只有 **2 KB**
- 每个索引的不可过滤 key：最多 10 个；全文检索字段：最多 10 个
- 写入：每次 PutVectors / DeleteVectors 最多 500 个；每个索引每秒最多 1,000 次写请求、2,500 个向量
- 读取：GetVectors 每次最多 100 个；ListVectors 每页最多 1,000 个，可以分成最多 16 段并行列出
- 查询：topK 最多 10,000；每页最多返回 100 个；ENHANCED 索引每次查询最多 100 个过滤条件

出处：[Limitations and restrictions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html)

## 3. 建索引时定死的东西

建索引时要想清楚。下面这几项**以后不能改**：

- `dimension`：维度，必须和 embedding 模型输出一致（例如 Titan Text V2 用 1024）
- `distanceMetric`：`cosine` 或 `euclidean`
- `dataType`：`float32`
- `nonFilterableMetadataKeys`：哪些元数据**只返回、不过滤**。大字段（原文片段、描述）放这里，因为可过滤的元数据有大小上限，而且会算进查询费用（见 [pricing.md](pricing.md)）

以后**可以改**的：

- 索引模式：CLASSIC 可以原地改成 ENHANCED（`UpdateIndexMode`），不用重新导入
- 全文检索字段：可以加，也可以删（`UpdateMetadataConfiguration`）；已有字段的分词器不能改

出处：[Configuring full-text search](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-configuring-full-text-search.html)、[Changing a vector index's mode](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-index-mode.html)

## 4. CLASSIC 和 ENHANCED：一定要用 ENHANCED

- **ENHANCED**：**先过滤，再找最相近的**（pre-filtering）。过滤条件只命中很少的向量时，召回也高；支持 `$startsWith`；可过滤元数据最多 40 KB；支持全文检索和混合检索。
- **CLASSIC**：一边找一边过滤。过滤条件很窄时，可能返回少于 topK 条结果。

**2026-09-30 及以后**新建的向量桶，默认建 ENHANCED 索引；之前的桶默认建 CLASSIC 索引。改成 ENHANCED 不额外收费。

这对**按权限过滤**特别重要：一个人只能看 1% 的文件时，CLASSIC 索引可能只返回很少的结果，甚至一条都没有。

出处：[Metadata filtering](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-metadata-filtering.html)

## 5. 基础 API（对应 `examples/`）

| 你要做的事 | API | 示例 |
|---|---|---|
| 建桶、建索引 | `CreateVectorBucket`、`CreateIndex`、`GetIndex` | [01_create.py](../examples/01_create.py) |
| 写入、查询 | `PutVectors`、`QueryVectors` | [02_put_query.py](../examples/02_put_query.py) |
| 按元数据过滤 | `QueryVectors(filter=...)` | [03_filter.py](../examples/03_filter.py) |
| 全文检索 / 混合检索 | `QueryVectors(queryTexts=...)` | [04_hybrid.py](../examples/04_hybrid.py) |
| 读取、列出、删除 | `GetVectors`、`ListVectors`、`DeleteVectors` | [05_manage.py](../examples/05_manage.py) |
| 删除 | `DeleteIndex`、`DeleteVectorBucket` | [99_cleanup.py](../examples/99_cleanup.py) |

最小例子（boto3，需要较新的版本，例如 1.44 以上才有 `queryTexts`）：

```python
sv = boto3.client('s3vectors', region_name='eu-central-1')
sv.create_vector_bucket(vectorBucketName='my-vectors')
sv.create_index(vectorBucketName='my-vectors', indexName='docs', dataType='float32',
                dimension=1024, distanceMetric='cosine',
                metadataConfiguration={'nonFilterableMetadataKeys': ['text']})
sv.put_vectors(vectorBucketName='my-vectors', indexName='docs', vectors=[
    {'key': 'doc1#0', 'data': {'float32': emb}, 'metadata': {'dept': 'HR', 'text': '年假 12 天'}}])
hits = sv.query_vectors(vectorBucketName='my-vectors', indexName='docs', topK=5,
                        queryVector={'float32': q_emb}, filter={'dept': 'HR'},
                        returnMetadata=True, returnDistance=True)['vectors']
```

要点：

- **写入是强一致的**：PutVectors 返回后马上就能查到。
- **同一个 key 再写一次就是覆盖**。
- **权限**：IAM 的命名空间是 `s3vectors:`，和 `s3:` 分开。最小权限可以精确到某一个索引。查询时如果要过滤或返回元数据，除了 `s3vectors:QueryVectors`，还要 `s3vectors:GetVectors`。

## 6. 过滤语法

- 比较：`$eq` `$ne` `$gt` `$gte` `$lt` `$lte`
- 集合：`$in` `$nin`。列表字段用 `$eq` 时，只要有一个元素匹配就算匹配（文档原文）；用 `$in` 时，只要列表和给定的值有交集就匹配（文档没有单独说明，本 repo 的权限过滤依赖这一点，实测可行，`e2e.py` 第 2 项会检查）
- 其他：`$exists`、`$startsWith`（只在 ENHANCED 上可用）
- 组合：`$and` `$or`
- 不写操作符就是 `$eq`：`{"dept": "HR"}`

**过滤条件怎么数**：`{"region": {"$in": ["a", "b", "c"]}}` 算 3 个条件。ENHANCED 索引每次查询最多 100 个条件。如果一个用户属于很多权限组，要先在业务侧合并（本 repo 的 agent 超过 100 个组会直接报错）。

## 7. 全文检索和混合检索

S3 Vectors 现在除了向量检索，还能做：

- **全文检索**：在指定的文本字段上用 BM25 打分，适合错误码、产品型号、人名、法条编号这种"要精确命中的词"
- **混合检索**：向量检索 + 全文检索一起跑，用 RRF（Reciprocal Rank Fusion）合成一个排名

怎么开：

1. 索引必须是 **ENHANCED**。
2. 在 `textSearchConfigurations` 里列出文本字段，可以指定语言分词器。共有 10 种语言：`en` `fr` `es` `pt` `it` `de` `nl` `zh` `ja` `ko`；不指定就用标准分词器。
3. 查询时，传 `queryTexts=[{"text": "...", "metadataKey": "chunk_text"}]`：只传它就是全文检索；同时也传 `queryVector`，就是混合检索。
4. 在已有数据上新开全文字段，会在后台回填，字段状态从 `BUILDING` 变成 `ACTIVE` 之后才能查。

出处：[Hybrid search](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-hybrid-search.html)、[Querying with full-text and vector search](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-querying-full-text-and-vector-search.html)

## 8. 和其他服务一起用

- **Amazon Bedrock Knowledge Bases**：可以直接选 S3 Vectors 作为向量库，由 Knowledge Bases 负责解析、切分、embedding，最省事。如果你要自己控制抽取规则、权限字段，以及和其他数据源用同一套 embedding 规则（本 repo 的需求），就自己写流水线。
- **Amazon OpenSearch Service**：可以用 S3 Vectors 做底层存储来降低成本；也可以把索引快照导出到 OpenSearch Serverless，满足高 QPS、低延迟的需求。

## 9. 常见坑

- **CLASSIC 索引 + 很窄的过滤** → 结果偏少：换 ENHANCED。
- **大文本放进可过滤元数据** → 超过大小上限，写入报 400；而且算进每次查询的处理量，查询费用变高。原文片段一定要放进 `nonFilterableMetadataKeys`。
- **频繁覆盖或删除同一批 key** → 旧数据在一段时间内仍算进索引大小，查询费用会暂时变高。
- **每次 PUT 最少按 128 KB 计费** → 一次写一个向量很浪费，要批量写，每次最多 500 个。
- **boto3 太旧** → 没有 `queryTexts`、`returnScores`。Lambda 自带的 boto3 可能不够新，要在部署包里带上新版本。
