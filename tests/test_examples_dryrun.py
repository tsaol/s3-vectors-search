"""Run every example and demo tool against dry-run clients: each boto3 call is checked
against the service model (parameter names, types, required fields)."""
import glob
import io
import json
import os
import runpy
import sys

import boto3
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'demo', 'deploy'))
import dryrun  # noqa: E402


@pytest.fixture
def dry(monkeypatch, tmp_path):
    sess = dryrun.DryRunSession()
    emb = lambda: dryrun._ok({'body': io.BytesIO(json.dumps({'embedding': [0.0] * 1024}).encode()),
                              'contentType': 'application/json'})
    monkeypatch.setitem(dryrun.CANNED, 'InvokeModel', emb)
    monkeypatch.setitem(dryrun.CANNED, 'QueryVectors', dryrun._ok({'vectors': [
        {'key': 'k', 'distance': 0.1, 'metadata': {'text': 'x'}}], 'distanceMetric': 'cosine'}))
    monkeypatch.setitem(dryrun.CANNED, 'GetVectors', dryrun._ok({'vectors': [{'key': 'p1', 'metadata': {'text': 'x'}}]}))
    monkeypatch.setitem(dryrun.CANNED, 'GetIndex', dryrun._ok({'index': {'indexName': 'examples', 'dimension': 1024}}))
    monkeypatch.setitem(dryrun.CANNED, 'ListVectors', dryrun._ok({'vectors': []}))
    monkeypatch.setitem(dryrun.CANNED, 'ListObjectsV2', dryrun._ok({'Contents': [], 'KeyCount': 0}))
    monkeypatch.setitem(dryrun.CANNED, 'SendMessageBatch', dryrun._ok({'Successful': [], 'Failed': []}))
    monkeypatch.setattr(boto3, 'client', lambda name, region_name=None, **kw: sess.client(name, region_name))
    return sess


@pytest.mark.parametrize('path', sorted(glob.glob(os.path.join(ROOT, 'examples', '[0-9]*.py'))))
def test_example(dry, monkeypatch, path):
    monkeypatch.syspath_prepend(os.path.join(ROOT, 'examples'))
    monkeypatch.setattr(sys, 'argv', [path, '--region', 'eu-central-1'])
    runpy.run_path(path, run_name='__main__')
    assert dry.calls, 'example made no API calls'


def test_reconcile_and_backfill(dry, monkeypatch, tmp_path):
    out = {'region': 'eu-central-1', 'source_bucket': 'demo-src', 'vector_bucket': 'demo-vb', 'index': 'demo-idx',
           'queues': {'backfill': 'https://sqs.dry-run/q'}}
    monkeypatch.syspath_prepend(os.path.join(ROOT, 'demo', 'tools'))
    import common
    monkeypatch.setattr(common, 'outputs', lambda: out)
    for tool, argv in (('reconcile', ['--fix']), ('backfill', ['--prefix', 'hr/'])):
        mod = __import__(tool)
        monkeypatch.setattr(mod, 'outputs', lambda: out)
        if tool == 'backfill':
            monkeypatch.setattr(mod, 'STATE', str(tmp_path / 'state.json'))
        monkeypatch.setattr(sys, 'argv', [tool] + argv)
        mod.main()
    assert {op for _, op, _ in dry.calls} >= {'ListVectors', 'ListObjectsV2'}
