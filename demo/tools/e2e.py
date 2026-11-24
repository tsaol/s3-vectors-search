#!/usr/bin/env python3
"""End-to-end test against a deployed demo (needs deploy.py --demo-users first).

Uploads the sample files, waits for the indexer, then checks the whole flow:
  1  new objects get vectors (S3 event -> SQS -> Lambda -> S3 Vectors)
  2  alice (all-staff) can't find payroll; bob (hr-payroll) can
  3  a forged "groups" field in the body is ignored
  4  no token -> rejected by the runtime's JWT authorizer
  5  hybrid search finds an exact error code (E2071)
  6  overwrite -> answer uses the new content, old chunks gone
  7  delete -> vectors gone
  8  reconcile finds no orphans / stale vectors
Cleans up the sample objects at the end (unless --keep).

    python demo/tools/e2e.py
"""
import argparse
import json
import os
import sys
import time

import boto3

from common import DEMO, invoke, login, outputs
from s3vsearch.store import doc_id, vkey

SAMPLES = os.path.join(DEMO, 'sample-data')
PREFIX = 'e2e/'
results = []


def check(name, ok, detail=''):
    results.append((name, bool(ok)))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f'  ({detail})' if detail and not ok else ''), flush=True)
    return ok


def wait(pred, timeout=300, step=5):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(step)
    return pred()


def files(body):
    return [s['file'] for s in body.get('sources', [])]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--keep', action='store_true')
    a = ap.parse_args()
    o = outputs()
    s3 = boto3.client('s3', region_name=o['region'])
    sv = boto3.client('s3vectors', region_name=o['region'])
    B, VB, IX = o['source_bucket'], o['vector_bucket'], o['index']
    manifest = json.load(open(os.path.join(SAMPLES, 'manifest.json')))

    def head(key):
        r = sv.get_vectors(vectorBucketName=VB, indexName=IX, keys=[vkey(doc_id(B, key), 0)], returnMetadata=True)
        return (r.get('vectors') or [{}])[0].get('metadata')

    def put(key, path=None, body=None, meta=None):
        data = body.encode() if body is not None else open(path, 'rb').read()
        return s3.put_object(Bucket=B, Key=key, Body=data, Metadata=meta or {})['ETag'].strip('"')

    # 1 index
    etags = {}
    for rel, meta in manifest.items():
        etags[PREFIX + rel] = put(PREFIX + rel, os.path.join(SAMPLES, rel), meta=meta)
    done = wait(lambda: all((head(k) or {}).get('etag') == e for k, e in etags.items()))
    check('1 new objects indexed', done, json.dumps({k: (head(k) or {}).get('etag') for k in etags}))
    if not done:
        return summary()

    alice, bob = login(o, 'alice'), login(o, 'bob')
    payroll = PREFIX + 'hr/payroll.md'
    # 2 permissions
    _, r = invoke(o, {'question': '工资每个月几号发？'}, alice)
    check('2a alice does not see payroll', payroll not in files(r), json.dumps(r, ensure_ascii=False))
    _, r = invoke(o, {'question': '工资每个月几号发？'}, bob)
    check('2b bob sees payroll', payroll in files(r), json.dumps(r, ensure_ascii=False))
    # 3 forged groups
    _, r = invoke(o, {'question': '工资每个月几号发？', 'groups': ['hr-payroll']}, alice)
    check('3 forged groups ignored', payroll not in files(r), json.dumps(r, ensure_ascii=False))
    # 4 no token
    st, r = invoke(o, {'question': '工资'}, None)
    check('4 no token rejected', st in (401, 403), f'HTTP {st} {r}')
    # 5 exact code via hybrid search
    _, r = invoke(o, {'question': 'E2071'}, alice)
    check('5 hybrid finds E2071', (files(r) or [None])[0] == PREFIX + 'it/vpn-faq.md', json.dumps(r, ensure_ascii=False))
    # 6 overwrite
    leave = PREFIX + 'hr/leave-policy.md'
    new = '# 年假规定（第二版，示例）\n\n从 2027 年起，员工入职满一年后享有 20 天带薪年假。'
    e2 = put(leave, body=new, meta=manifest['hr/leave-policy.md'])
    ok = wait(lambda: (head(leave) or {}).get('etag') == e2)
    did = doc_id(B, leave)
    left = sv.get_vectors(vectorBucketName=VB, indexName=IX, keys=[vkey(did, n) for n in range(1, 20)]).get('vectors', [])
    _, r = invoke(o, {'question': '年假有几天？'}, alice)
    texts = ' '.join(s.get('text', '') for s in r.get('sources', []))
    check('6 overwrite re-indexed', ok and not left and '20 天' in texts, f'left={len(left)} {texts[:120]}')
    # 7 delete
    s3.delete_object(Bucket=B, Key=payroll)
    gone = wait(lambda: head(payroll) is None)
    check('7 delete removes vectors', gone)
    # 8 reconcile
    sys.argv = ['reconcile.py', '--prefix', PREFIX]
    import reconcile
    rep = reconcile.main()
    check('8 reconcile clean', not rep['orphans'] and not rep['stale'] and not rep['missing_or_skipped'], json.dumps(rep))

    if not a.keep:
        for k in etags:
            s3.delete_object(Bucket=B, Key=k)
        wait(lambda: all(head(k) is None for k in etags))
    return summary()


def summary():
    bad = [n for n, ok in results if not ok]
    print(f'\n{len(results) - len(bad)}/{len(results)} passed' + (f'; failed: {bad}' if bad else ''))
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
