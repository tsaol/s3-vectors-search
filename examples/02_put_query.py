#!/usr/bin/env python3
"""Step 2: write vectors with metadata (PutVectors) and run a similarity query (QueryVectors).

PutVectors: up to 500 vectors per call; same key = overwrite. Writes are strongly
consistent, so you can query right after. You pay PUT per GB, minimum 128 KB per call:
batch your writes.
"""
from _common import args, clients, embed, show

DOCS = [
    ('p1', '无线降噪耳机，续航 30 小时，适合通勤', {'category': 'audio', 'price': 899, 'stock': True}),
    ('p2', '头戴式监听耳机，录音棚级别音质', {'category': 'audio', 'price': 1299, 'stock': True}),
    ('p3', '运动蓝牙耳机，防水防汗，跑步不掉', {'category': 'audio', 'price': 399, 'stock': False}),
    ('p4', '机械键盘，红轴，带背光', {'category': 'keyboard', 'price': 499, 'stock': True}),
    ('p5', '人体工学椅，腰部支撑，适合久坐', {'category': 'furniture', 'price': 1999, 'stock': True}),
    ('p6', '便携蓝牙音箱，户外露营用', {'category': 'audio', 'price': 599, 'stock': True}),
]

a = args(__doc__.split('\n')[0])
sv, br = clients(a.region)

vectors = [{'key': k, 'data': {'float32': embed(br, t)}, 'metadata': {**m, 'text': t}} for k, t, m in DOCS]
sv.put_vectors(vectorBucketName=a.bucket, indexName=a.index, vectors=vectors)
print(f'put {len(vectors)} vectors')

q = '坐地铁时听歌用的耳机'
r = sv.query_vectors(vectorBucketName=a.bucket, indexName=a.index, topK=3,
                     queryVector={'float32': embed(br, q)}, returnMetadata=True, returnDistance=True)
print(f'query: {q}')
show(r['vectors'])
