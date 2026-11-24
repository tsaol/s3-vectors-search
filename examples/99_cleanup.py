#!/usr/bin/env python3
"""Delete the example index and vector bucket (a bucket must be empty of indexes first)."""
from botocore.exceptions import ClientError

from _common import args, clients

a = args(__doc__.split('\n')[0])
sv, _ = clients(a.region)
for fn, kw in ((sv.delete_index, {'vectorBucketName': a.bucket, 'indexName': a.index}),
               (sv.delete_vector_bucket, {'vectorBucketName': a.bucket})):
    try:
        fn(**kw)
        print('deleted', kw)
    except ClientError as e:
        print('skip', kw, e.response['Error']['Code'])
