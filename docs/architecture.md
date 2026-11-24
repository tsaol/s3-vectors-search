# 场景 demo：S3 文件语义检索（按权限过滤）

需求原文见 [requirements.md](requirements.md)。

## 1. 整体流程

```
             写入 / 覆盖 / 删除 / 过期
 业务系统 ─────────────────────────▶  S3 源桶（只读）
                                        │ S3 事件通知
                                        ▼
       backfill.py / reconcile.py ─▶  SQS（events / backfill，各自限并发）──▶ DLQ（失败 8 次）
                                        │
                                        ▼
                               Lambda indexer（同一区域）
       HEAD 对象 → 抽取（文本 / docx / Textract OCR）→ 切分 → Bedrock embedding
                                        │ PutVectors / DeleteVectors
                                        ▼
                      S3 Vectors 向量索引（ENHANCED，cosine，1024 维）
                      元数据：路径 / ETag / 部门 / 上传人 / 时间 / 切片 / acl / 规则版本
                      chunk_text：不可过滤 + 中文全文检索
                                        ▲ QueryVectors（向量 + BM25，filter acl $in 用户组）
                                        │
 用户 ── Cognito 登录 ── JWT ──▶ AgentCore Runtime 检索 agent ──▶（可选）大模型回答 + 标注来源
```

## 2. 设计要点

### 事件只是"提醒"，S3 的当前状态才是"事实"

S3 事件可能重复、可能乱序，极少数情况下会丢失。所以 indexer 不看事件类型，每次都按下面的规则处理（丢失的事件靠 `reconcile.py` 对账补上）：

1. HEAD 对象：对象不在了 → 删除它的所有向量。
2. 对象在，而且 ETag、规则版本、模型都和已写入的一样，上次也写完整了 → 什么都不做（重复事件）；如果只是权限字段变了，只重写元数据。
3. 其他情况 → 抽取、切分、embedding，覆盖第 0..m-1 片，再删掉第 m 片及之后的旧片。
4. 写完再 HEAD 一次。如果对象在处理期间又变了 → 抛异常，由队列重试。重试时会处理新版本。

这样一套逻辑就覆盖了新增、覆盖、删除、重复、乱序、回填和对账（FR-1、FR-2、FR-3）。

### 不扫描整个索引，失败了也能补完

向量 key 的格式是 `<doc_id>#<序号>`，其中 doc_id = sha256(桶/对象 key) 的前 32 位。同一个对象的切片序号总是连续的 0..n-1，所以：

- 找旧切片：用 GetVectors 按 key 直接取，每次 100 个，取到没有为止。
- 判断上次写完整没有：一次 GetVectors 取 3 个 key（第 0 片、最后一片、最后一片的下一片）。

这样处理一个对象的成本和索引大小无关（旧 demo 每处理一个对象都要 ListVectors 整个索引）。

为了让"按 key 往上找"永远找得全，所有写入和删除都遵守一条规则：**一个对象的切片永远是从 0 开始、没有空洞的一段**。

- 写入：第 0 片放在最后一批写。第 0 片就是"提交标记"：它的 ETag、acl 一变，说明其他切片都已经写好了。
- 删除：从最高的序号往下删。删到一半失败时，剩下的还是 0..k 连续的一段，重试时能全部找到。
- 改权限：同样从高往低改，第 0 片最后改。改到一半失败时，第 0 片还是旧权限，下次检查会发现不一致，再改一遍。

### 权限：只信 token

- 运行时（AgentCore Runtime）配置了 **JWT authorizer**（Cognito）：token 不对，请求根本到不了 agent。
- `Authorization` 头在 runtime 的请求头白名单（`requestHeaderAllowlist`）里，agent 从 token 的 `cognito:groups` 里取权限组。
- 请求体里的 `groups` 字段**会被忽略**。
- 查询时带上 `filter={"acl": {"$in": 用户组}}`，在 S3 Vectors 里完成过滤。ENHANCED 索引是先过滤再检索，所以权限很窄时召回也不会下降。
- 对象没有 acl 时，不写 acl 字段，任何过滤条件都匹配不到它，也就是**默认拒绝**。
- 同一个会话里，用户的权限组一变，就清空历史，防止上一轮的内容带进来。

生产环境的权限数据在业务系统里：把 `acl.py` 换成调用业务系统接口的实现；权限变化时，重新索引（或更新元数据）受影响的对象。

### 失败不丢、不阻塞

- S3 → SQS → Lambda。用了 `ReportBatchItemFailures`，一条消息失败只重试这一条。失败的消息按 30 秒、60 秒……最长 15 分钟退避重试（`ChangeMessageVisibility`），不用等整个可见性超时（90 分钟）；失败 8 次进入 DLQ，并触发 CloudWatch 告警。
- 每处理一条消息之前，先看 Lambda 还剩多少时间。不够处理一个慢文件（PDF OCR）时，本批剩下的消息直接还给队列。两个队列都是每次只取 1 条。
- 文件读不了（格式不支持、文件损坏、OCR 失败），记为 `skipped` 并写明原因，不重试，也不阻塞别的文件（FR-4）。
- 限流、超时这类临时错误会抛异常，由队列重试。

### 回填不影响日常增量

日常事件和回填走两个队列，两个 Lambda 触发器分别设最大并发（默认分别是 10 和 4）（NFR-5）。`backfill.py` 可以按前缀和时间分批，Ctrl-C 暂停，`--resume` 续跑。

### 就近，不下载

- 全部组件在同一区域，embedding 用本区域的 Titan Text Embeddings V2。
- 图片和 PDF 交给 Textract，直接读 S3 上的对象（`S3Object`），不经过 Lambda 下载。
- 文本和 docx 在 Lambda 内存里读，不落盘，也不保留副本。
- **注意**：回答用的大模型如果是跨区域推理配置文件（`eu.`、`us.`、`global.` 开头），检索到的原文片段会被送到配置文件覆盖的其他区域。部署时 `--answer-model` 默认留空，也就是只返回检索结果、不调大模型。是否允许跨区域，由合规团队决定（NFR-1）。

## 3. 需求对照

| 编号 | 实现 | 状态 |
|---|---|---|
| FR-1 增量触发 | S3 事件 → SQS → Lambda；ETag 判断重复；写到一半、删到一半、权限改到一半，重试时都会补完（第 0 片最后写，删除从高到低） | ✅ |
| FR-2 删除、覆盖同步 | 以 S3 状态为准；订阅 `ObjectRemoved` 和 `LifecycleExpiration`；`reconcile.py` 对账 | ✅ |
| FR-3 存量回填 | `backfill.py`：前缀、时间、暂停、续跑，走独立队列 | ✅ |
| FR-4 文档抽取 | txt / md / csv / json / html / docx；图片和 PDF 用 Textract（支持中文）；失败记原因 | ✅（doc、xlsx、pptx 未做） |
| FR-5 图片理解 | 只做了图中文字（OCR）；按画面内容检索、以文搜图需要多模态 embedding | ⚠️ 部分 |
| FR-6 视频理解 | 未做（P1） | ❌ |
| FR-7 切分 | 先按段落，再按长度 + 重叠；规则版本写入每个向量 | ✅（按标题切分可替换 `chunk.py`） |
| FR-8 embedding | Titan V2 / Cohere / SageMaker 可选；模型和维度通过配置指定；换模型 = 改配置 + 回填 | ✅ |
| FR-9 元数据 | 路径、ETag（版本）、类型、部门、上传人、时间、切片、切片数、acl、模型、规则版本 | ✅ |
| FR-10 写入、对账 | 每批 500；可重试；`reconcile.py` 报告对象数、向量数、孤儿、过期、缺失 | ✅ |
| FR-11 权限过滤 | JWT → `cognito:groups` → `acl $in`；不信请求体 | ✅ |
| FR-12 问答 | 可选大模型；只根据片段回答；没找到就说没找到；片段不当指令执行 | ✅ |
| FR-13 运行监控 | EMF 指标（按状态计数、耗时）；DLQ 告警；积压超过 15 分钟告警 | ✅ |
| NFR-2 安全 | 源桶只读；最小权限；可部署进私有子网；SQS / S3 / 向量桶都加密 | ✅ |
| NFR-6 版本 | `rule_version`、`model` 写入每个向量；规则一变，再跑一次 `backfill.py`，旧向量会被重新处理（规则相同的对象跳过） | ✅ |
| NFR-7 可替换 | 抽取、切分、embedding、权限、向量存储各是一个模块 | ✅ |

## 4. 放进私有子网（可选）

`deploy.py --subnets ... --security-groups ...` 会把 Lambda 和 AgentCore Runtime 放进你的私有子网。子网需要能访问以下服务（通过 VPC 终端节点，或者 NAT）：

- `s3`（网关终端节点）
- `s3vectors`、`bedrock-runtime`、`textract`、`sqs`、`logs`（接口终端节点；`sqs` 用于失败消息的退避）
- 本 repo 的代码不直接访问 Cognito：JWT 由 AgentCore Runtime 的 authorizer 验证。

安全组只需要出站 443 到这些终端节点。

## 5. 已知限制

- PDF 走 Textract 异步接口，在 Lambda 里轮询，最多等 `OCR_WAIT_SECONDS`（默认 600 秒）；超大 PDF 建议改成 Textract 完成后发 SNS 通知的方式。
- 同一个对象如果被两个 Lambda 同时处理，最终会收敛到 S3 上的最新版本（写完再 HEAD 一次，变了就退避重试，一般几分钟内），但在这之前可能出现新旧切片混在一起的情况。
- 权限变了但对象没变时，需要业务系统发一条回填消息（或者调用 `backfill.py`）。indexer 发现只有 acl / 部门 / 上传人变了，只重写元数据，不重新抽取和 embedding。
