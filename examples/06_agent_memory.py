#!/usr/bin/env python3
"""Scenario: long-term memory for an AI agent.

Each memory is a vector with user_id + ts (epoch seconds) metadata. Recall = similarity
search filtered to one user and a time window. S3 Vectors fits because memories are
many, mostly cold, and read now and then: you pay for storage and queries, no cluster.
"""
import time

from _common import args, clients, embed, show

a = args(__doc__.split('\n')[0])
sv, br = clients(a.region)
now = int(time.time())
MEMS = [
    ('m1', 'u-alice', now - 86400 * 40, '用户说她对花生过敏'),
    ('m2', 'u-alice', now - 86400 * 2, '用户下周去东京出差，住在新宿'),
    ('m3', 'u-alice', now - 3600, '用户喜欢靠窗的座位'),
    ('m4', 'u-bob', now - 86400, '用户在准备 AWS 认证考试'),
]
sv.put_vectors(vectorBucketName=a.bucket, indexName=a.index, vectors=[
    {'key': k, 'data': {'float32': embed(br, t)}, 'metadata': {'user_id': u, 'ts': ts, 'text': t}}
    for k, u, ts, t in MEMS])

q = '帮我订去东京的机票，有什么要注意的？'
for label, f in (('alice, all time', {'user_id': 'u-alice'}),
                 ('alice, last 7 days', {'$and': [{'user_id': 'u-alice'}, {'ts': {'$gte': now - 86400 * 7}}]})):
    r = sv.query_vectors(vectorBucketName=a.bucket, indexName=a.index, topK=3,
                         queryVector={'float32': embed(br, q)}, filter=f, returnMetadata=True, returnDistance=True)
    print(f'{label}:')
    show(r['vectors'])
