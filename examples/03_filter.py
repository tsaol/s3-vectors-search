#!/usr/bin/env python3
"""Step 3: metadata filters. On an ENHANCED index the filter runs BEFORE the vector search
(pre-filtering), so a narrow filter still returns up to topK good matches.

Operators: $eq $ne $gt $gte $lt $lte $in $nin $exists $and $or, and $startsWith (ENHANCED).
On a list field, $eq matches if any element matches (documented); $in matches if the
list and the values overlap (tested; the demo's permission filter relies on it). Up to 100 filter constraints per
query on an ENHANCED index ($in with 3 values = 3 constraints).
Run 02_put_query.py first.
"""
from _common import args, clients, embed, show

a = args(__doc__.split('\n')[0])
sv, br = clients(a.region)
qv = {'float32': embed(br, '耳机')}

FILTERS = {
    'in stock, under 900': {'$and': [{'stock': True}, {'price': {'$lt': 900}}]},
    'audio or keyboard':   {'category': {'$in': ['audio', 'keyboard']}},
    'not audio':           {'category': {'$ne': 'audio'}},
    'category starts with aud': {'category': {'$startsWith': 'aud'}},
}
for name, f in FILTERS.items():
    r = sv.query_vectors(vectorBucketName=a.bucket, indexName=a.index, topK=3, queryVector=qv,
                         filter=f, returnMetadata=True, returnDistance=True)
    print(f'filter {name}: {f}')
    show(r['vectors'])
