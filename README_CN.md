# s3-vectors-search

[English](README.md) · 讲解页：<https://liucao.me/s3/vectors/>

**用 Amazon S3 Vectors 给 S3 里的文件做语义检索。** 内容包括：S3 Vectors 怎么用、怎么收费、基础 API 示例，以及一个完整的场景 demo（S3 文件 → 向量 → 按权限检索 → 问答）。

> 这是讲解用的示例代码，不是 AWS 官方工具，也不能直接用在生产环境。上限和价格以 AWS 官方文档为准。

## 内容

| 你想知道 | 看这里 |
|---|---|
| S3 Vectors 是什么、怎么用、有哪些上限和坑 | [docs/s3-vectors-guide.md](docs/s3-vectors-guide.md) |
| 怎么收费、怎么估算 | [docs/pricing.md](docs/pricing.md) · `tools/cost_estimate.py` |
| 基础 API，一步一个脚本 | [examples/](examples/) |
| 场景：S3 文件语义检索 + 权限过滤 | [docs/architecture.md](docs/architecture.md) · [demo/](demo/) |
| 这个场景的需求原文 | [docs/requirements.md](docs/requirements.md) |

## 基础 API 示例

需要 Python 3.10+、boto3 1.44+，以及开通了 Bedrock Titan Text Embeddings V2 的区域。

```bash
pip install -r requirements.txt
cd examples
python 01_create.py --region eu-central-1 --bucket my-vectors-demo   # 建向量桶 + ENHANCED 索引（中文全文检索）
python 02_put_query.py --bucket my-vectors-demo   # 写入 + 相似度查询
python 03_filter.py --bucket my-vectors-demo      # 元数据过滤
python 04_hybrid.py --bucket my-vectors-demo      # 场景：错误码工单，向量 / 全文 / 混合检索对比
python 05_manage.py --bucket my-vectors-demo      # 读取、分段列出、删除
python 06_agent_memory.py --bucket my-vectors-demo  # 场景：Agent 长期记忆（按用户 + 时间过滤）
python 99_cleanup.py --bucket my-vectors-demo     # 删除
```

## 场景 demo：S3 文件语义检索

```
S3 源桶 ─事件→ SQS ─→ Lambda（抽取 / OCR / 切分 / embedding）─→ S3 Vectors（ENHANCED，混合检索）
                                                                     ▲
用户 ─Cognito JWT─→ AgentCore Runtime 检索 agent ─按 token 里的权限组过滤─┘
```

要点：

- 以 S3 的当前状态为准，重复、乱序的事件不会留下错误的向量；丢失的事件由 `reconcile.py` 对账找出来。
- 处理一个对象不需要扫描整个索引。
- 权限组只从 JWT 里取，不信请求体。
- 失败的消息进 DLQ 并告警；回填走单独的队列，不影响日常增量处理。

详见 [docs/architecture.md](docs/architecture.md)。

```bash
# 部署（需要能创建 IAM、Lambda、SQS、S3 Vectors、Cognito、AgentCore 的权限）
python demo/deploy/deploy.py --region eu-central-1 --name s3vs --demo-users
#   已有源桶：             --source-bucket <your-bucket>
#   放进私有子网：         --subnets subnet-a subnet-b --security-groups sg-x
#   要大模型回答（可选）：  --answer-model <model id 或推理配置文件>
python demo/deploy/deploy.py --region eu-central-1 --dry-run   # 不调用 AWS，只校验每个 API 参数

# 端到端测试：上传示例文件 → 等待索引完成 → 权限、混合检索、覆盖、删除、对账
python demo/tools/e2e.py

# 日常
python demo/tools/ask.py --user alice "年假有几天？"
python demo/tools/backfill.py --prefix hr/          # 存量回填，可 --resume
python demo/tools/reconcile.py --fix                # 对账

# 删除（保留源桶和向量；加 --delete-vectors 会删掉索引）
python demo/deploy/deploy.py --region eu-central-1 --name s3vs --destroy
```

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q tests
```

- 流水线逻辑用内存里的假 S3、假 S3 Vectors 测试，包括重复、乱序、写到一半、删到一半、权限改到一半、处理中被覆盖等情况（故障注入）。
- `deploy.py`、示例脚本和工具脚本都在 dry-run 模式下运行：每个 boto3 请求都按 AWS 的服务模型校验参数名、类型和必填项。
- 收费计算器用 AWS 定价页的例 1（$11.38）和例 3（$107.79）验证。

## License

MIT-0
