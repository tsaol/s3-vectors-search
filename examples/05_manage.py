#!/usr/bin/env python3
"""Step 5: read, list, delete.

GetVectors: up to 100 keys per call (missing keys are just not returned).
ListVectors: up to 1,000 per page; split into up to 16 segments to list in parallel.
DeleteVectors: up to 500 keys per call.
Deleted or overwritten vectors still count toward storage for a while, so frequent
overwrites of the same keys can raise query cost for a time (see docs/pricing.md).
"""
from concurrent.futures import ThreadPoolExecutor

from _common import args, clients

a = args(__doc__.split('\n')[0])
sv, _ = clients(a.region)
B = dict(vectorBucketName=a.bucket, indexName=a.index)

got = sv.get_vectors(**B, keys=['p1', 'p2', 'nope'], returnMetadata=True)['vectors']
print('get:', [(v['key'], v['metadata'].get('text')) for v in got])


def segment(i, n=4):
    keys, tok = [], None
    while True:
        kw = dict(B, maxResults=1000, segmentCount=n, segmentIndex=i)
        if tok:
            kw['nextToken'] = tok
        r = sv.list_vectors(**kw)
        keys += [v['key'] for v in r.get('vectors', [])]
        tok = r.get('nextToken')
        if not tok:
            return keys


with ThreadPoolExecutor(4) as ex:
    keys = sorted(k for part in ex.map(segment, range(4)) for k in part)
print('list:', keys)

sv.delete_vectors(**B, keys=['p6'])
print('deleted p6; left:', len(keys) - 1)
