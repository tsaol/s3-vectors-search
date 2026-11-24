#!/usr/bin/env python3
"""Step 4 (scenario: IT tickets with error codes): vector vs full-text vs hybrid.

Vector search understands meaning but is weak on exact codes like "E2071".
Full-text search (BM25) matches exact terms. Hybrid runs both and fuses the two rankings
with Reciprocal Rank Fusion (RRF). Needs an ENHANCED index with `text` configured for
full-text search (01_create.py does that).
"""
from _common import args, clients, embed, show

TICKETS = [
    ('t1', 'VPN 报错 E2071，账号被锁定，输错密码太多次', {'team': 'it'}),
    ('t2', 'VPN 报错 E1234，证书过期，需要更新设备证书', {'team': 'it'}),
    ('t3', '连不上公司网络，提示认证失败，可能是密码问题', {'team': 'it'}),
    ('t4', '打印机卡纸，错误码 P0042', {'team': 'facilities'}),
    ('t5', '邮箱满了，收不到新邮件', {'team': 'it'}),
]

a = args(__doc__.split('\n')[0])
sv, br = clients(a.region)
sv.put_vectors(vectorBucketName=a.bucket, indexName=a.index, vectors=[
    {'key': k, 'data': {'float32': embed(br, t)}, 'metadata': {**m, 'text': t}} for k, t, m in TICKETS])

q = 'E2071'
base = dict(vectorBucketName=a.bucket, indexName=a.index, topK=3, returnMetadata=True,
            filter={'team': 'it'})
print(f'query: {q}\n-- vector only')
show(sv.query_vectors(**base, queryVector={'float32': embed(br, q)}, returnDistance=True)['vectors'])
print('-- full-text only (BM25)')
show(sv.query_vectors(**base, queryTexts=[{'text': q, 'metadataKey': 'text'}], returnScores=True)['vectors'])
print('-- hybrid (RRF)')
show(sv.query_vectors(**base, queryVector={'float32': embed(br, q)},
                      queryTexts=[{'text': q, 'metadataKey': 'text'}], returnScores=True)['vectors'])
