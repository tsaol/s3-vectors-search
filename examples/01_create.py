#!/usr/bin/env python3
"""Step 1: create a vector bucket and a vector index.

Things you fix at creation time and can't change later:
  dimension, distance metric (cosine | euclidean), data type (float32),
  nonFilterableMetadataKeys (big fields you only want returned, not filtered on).
Things you can change later: index mode (CLASSIC -> ENHANCED), full-text fields (add/remove).
"""
from botocore.exceptions import ClientError

from _common import DIM, args, clients

a = args(__doc__.split('\n')[0])
sv, _ = clients(a.region)

try:
    sv.create_vector_bucket(vectorBucketName=a.bucket)       # SSE-S3 by default; can use SSE-KMS
    print(f'created vector bucket {a.bucket}')
except ClientError as e:
    if e.response['Error']['Code'] != 'ConflictException':
        raise
    print(f'vector bucket {a.bucket} already exists')

# Buckets created on/after 2026-09-30 make ENHANCED indexes by default; set it anyway.
sv.put_vector_bucket_default_index_mode(vectorBucketName=a.bucket, defaultIndexMode='ENHANCED')

try:
    sv.create_index(
        vectorBucketName=a.bucket, indexName=a.index,
        dataType='float32', dimension=DIM, distanceMetric='cosine',
        metadataConfiguration={
            'nonFilterableMetadataKeys': ['text'],                       # returned, not filterable
            'textSearchConfigurations': [{'metadataKey': 'text', 'language': 'zh'}],   # BM25 on text
        })
    print(f'created index {a.index}')
except ClientError as e:
    if e.response['Error']['Code'] != 'ConflictException':
        raise
    print(f'index {a.index} already exists')

idx = sv.get_index(vectorBucketName=a.bucket, indexName=a.index)['index']
print({k: idx.get(k) for k in ('indexName', 'dimension', 'distanceMetric', 'indexMode', 'metadataConfiguration')})
