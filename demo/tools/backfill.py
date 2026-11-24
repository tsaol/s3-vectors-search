#!/usr/bin/env python3
"""Backfill existing objects (FR-3): list a prefix and send one message per object to the
backfill queue. The backfill queue has its own, lower Lambda concurrency, so live
uploads are not slowed down (NFR-5).

    python demo/tools/backfill.py --prefix hr/
    python demo/tools/backfill.py --prefix hr/ --resume      # continue after Ctrl-C
    python demo/tools/backfill.py --since 2026-01-01         # only objects changed since

Stopping is safe: the last key sent is saved in .backfill-state.json; --resume starts
after it. Sending the same object twice is harmless (unchanged objects are skipped).
"""
import argparse
import datetime
import json
import os

import boto3

from common import HERE, outputs

STATE = os.path.join(HERE, '.backfill-state.json')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--prefix', default='')
    ap.add_argument('--since', help='YYYY-MM-DD: only objects modified on or after this day')
    ap.add_argument('--resume', action='store_true')
    ap.add_argument('--limit', type=int, default=0, help='stop after N objects (0 = no limit)')
    a = ap.parse_args()
    o = outputs()
    s3 = boto3.client('s3', region_name=o['region'])
    sqs = boto3.client('sqs', region_name=o['region'])
    q = o['queues']['backfill']
    since = datetime.datetime.fromisoformat(a.since).replace(tzinfo=datetime.timezone.utc) if a.since else None
    state = json.load(open(STATE)) if a.resume and os.path.exists(STATE) else {}
    if state and state.get('prefix') != a.prefix:
        raise SystemExit(f'saved state is for prefix {state.get("prefix")!r}; use the same --prefix or drop --resume')
    kw = {'Bucket': o['source_bucket'], 'Prefix': a.prefix}
    if state.get('last_key'):
        kw['StartAfter'] = state['last_key']
    sent, batch = state.get('sent', 0), []

    def flush():
        nonlocal sent, batch
        if not batch:
            return
        r = sqs.send_message_batch(QueueUrl=q, Entries=[
            {'Id': str(i), 'MessageBody': json.dumps({'bucket': o['source_bucket'], 'key': k})} for i, k in enumerate(batch)])
        if r.get('Failed'):
            raise SystemExit(f"send failed: {r['Failed']}")
        sent += len(batch)
        json.dump({'prefix': a.prefix, 'last_key': batch[-1], 'sent': sent}, open(STATE, 'w'))
        batch = []

    try:
        for page in s3.get_paginator('list_objects_v2').paginate(**kw):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('/') or (since and obj['LastModified'] < since):
                    continue
                batch.append(obj['Key'])
                if len(batch) == 10:
                    flush()
                    print(f'\rsent {sent}', end='', flush=True)
                if a.limit and sent + len(batch) >= a.limit:
                    flush()
                    print(f'\nlimit reached; --resume continues after {json.load(open(STATE))["last_key"]}')
                    return
        flush()
        print(f'\nsent {sent} objects; done')
        if os.path.exists(STATE):
            os.remove(STATE)
    except KeyboardInterrupt:
        flush()
        print(f'\npaused after {sent}; run again with --resume')


if __name__ == '__main__':
    main()
