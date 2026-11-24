#!/usr/bin/env python3
"""Reconcile S3 and the vector index (FR-2, FR-10).

Lists every vector (ListVectors, parallel segments) and every object, then reports:
  objects in S3, objects with vectors, vectors in total,
  orphans     - vectors whose object is gone (missed delete event),
  stale       - vectors whose ETag differs from the object (missed overwrite),
  missing     - supported objects with no vectors (missed create event).
With --fix, each problem object is sent to the backfill queue; the indexer then deletes
or re-indexes it from the current S3 state.

    python demo/tools/reconcile.py
    python demo/tools/reconcile.py --fix
"""
import argparse
import json
from concurrent.futures import ThreadPoolExecutor

import boto3

from common import outputs
from s3vsearch import extract
from s3vsearch.store import doc_id

SEGMENTS = 4


def list_vectors(sv, vb, index):
    def seg(i):
        docs, n, tok = {}, 0, None
        while True:
            kw = dict(vectorBucketName=vb, indexName=index, maxResults=1000, returnMetadata=True,
                      segmentCount=SEGMENTS, segmentIndex=i)
            if tok:
                kw['nextToken'] = tok
            r = sv.list_vectors(**kw)
            for v in r.get('vectors', []):
                m = v.get('metadata', {})
                n += 1
                d = docs.setdefault(m.get('file_key'), {'etags': set(), 'vectors': 0})
                d['etags'].add(m.get('etag'))
                d['vectors'] += 1
            tok = r.get('nextToken')
            if not tok:
                return docs, n
    docs, total = {}, 0
    with ThreadPoolExecutor(SEGMENTS) as ex:
        for part, n in ex.map(seg, range(SEGMENTS)):
            total += n
            for k, d in part.items():
                cur = docs.setdefault(k, {'etags': set(), 'vectors': 0})
                cur['etags'] |= d['etags']
                cur['vectors'] += d['vectors']
    return docs, total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--prefix', default='')
    ap.add_argument('--fix', action='store_true')
    a = ap.parse_args()
    o = outputs()
    s3 = boto3.client('s3', region_name=o['region'])
    sv = boto3.client('s3vectors', region_name=o['region'])
    objects = {}
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=o['source_bucket'], Prefix=a.prefix):
        for obj in page.get('Contents', []):
            objects[obj['Key']] = obj['ETag'].strip('"')
    docs, total = list_vectors(sv, o['vector_bucket'], o['index'])
    docs = {k: v for k, v in docs.items() if k is not None and k.startswith(a.prefix)}
    orphans = sorted(k for k in docs if k not in objects)
    stale = sorted(k for k, d in docs.items() if k in objects and d['etags'] != {objects[k]})
    missing = sorted(k for k in objects if k not in docs and extract.kind_of(k))
    report = {'objects': len(objects), 'objects_with_vectors': len(docs) - len(orphans),
              'vectors': total, 'orphans': orphans, 'stale': stale, 'missing_or_skipped': missing}
    print(json.dumps(report, ensure_ascii=False, indent=1))
    if a.fix:
        todo = orphans + stale + missing
        sqs = boto3.client('sqs', region_name=o['region'])
        for i in range(0, len(todo), 10):
            sqs.send_message_batch(QueueUrl=o['queues']['backfill'], Entries=[
                {'Id': str(j), 'MessageBody': json.dumps({'bucket': o['source_bucket'], 'key': k})}
                for j, k in enumerate(todo[i:i + 10])])
        print(f'sent {len(todo)} objects to the backfill queue')
    return report


if __name__ == '__main__':
    main()
