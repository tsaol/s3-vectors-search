"""Run deploy.py against the offline dry-run session: every boto3 request is validated
against the service model, so a wrong parameter name or type fails here."""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'demo', 'deploy'))
import deploy  # noqa: E402


def ops(d):
    return [(svc, op) for svc, op, _ in d.s.calls]


def params(d, op):
    return [p for _, o, p in d.s.calls if o == op]


def test_up_validates_all_calls():
    d = deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run', '--demo-users',
                     '--answer-model', 'eu.anthropic.claude-opus-5-5'])
    names = {op for _, op in ops(d)}
    for need in ('CreateVectorBucket', 'CreateIndex', 'CreateQueue', 'CreateFunction',
                 'CreateEventSourceMapping', 'PutBucketNotificationConfiguration',
                 'CreateUserPool', 'CreateAgentRuntime', 'PutMetricAlarm'):
        assert need in names, need
    idx = params(d, 'CreateIndex')[0]
    assert idx['metadataConfiguration'] == {
        'nonFilterableMetadataKeys': ['chunk_text'],
        'textSearchConfigurations': [{'metadataKey': 'chunk_text', 'language': 'zh'}]}
    rt = params(d, 'CreateAgentRuntime')[0]
    assert rt['requestHeaderConfiguration'] == {'requestHeaderAllowlist': ['Authorization']}
    assert rt['authorizerConfiguration']['customJWTAuthorizer']['allowedClients'] == ['dryrunclient']
    esm = params(d, 'CreateEventSourceMapping')
    assert sorted(e['ScalingConfig']['MaximumConcurrency'] for e in esm) == [4, 10]
    assert all(e['FunctionResponseTypes'] == ['ReportBatchItemFailures'] for e in esm)


def test_indexer_policy_is_read_only_and_lists_bucket():
    d = deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run'])
    pol = [json.loads(p['PolicyDocument']) for p in params(d, 'PutRolePolicy') if p['RoleName'] == 'demo-indexer-role'][0]
    acts = {a for s in pol['Statement'] for a in ([s['Action']] if isinstance(s['Action'], str) else s['Action'])}
    assert 's3:ListBucket' in acts and 's3:GetObject' in acts
    assert not any(a.startswith('s3:Put') or a.startswith('s3:Delete') for a in acts)


def test_cross_region_profile_resources():
    d = deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run'])
    r = d.models('eu.anthropic.claude-opus-5-5')
    assert r[0].endswith(':inference-profile/eu.anthropic.claude-opus-5-5')
    assert r[1] == 'arn:aws:bedrock:*::foundation-model/anthropic.claude-opus-5-5'
    assert d.models('amazon.titan-embed-text-v2:0') == [
        'arn:aws:bedrock:eu-central-1::foundation-model/amazon.titan-embed-text-v2:0']


def test_destroy_validates():
    d = deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run', '--destroy', '--delete-vectors'])
    assert ('s3vectors', 'DeleteIndex') in ops(d)


def test_existing_notifications_need_flag(monkeypatch):
    import dryrun
    monkeypatch.setitem(dryrun.CANNED, 'GetBucketNotificationConfiguration', dryrun._ok(
        {'LambdaFunctionConfigurations': [{'LambdaFunctionArn': 'arn:aws:lambda:x:0:function:old',
                                           'Events': ['s3:ObjectCreated:*']}]}))
    with pytest.raises(SystemExit):
        deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run'])
    deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run', '--replace-notifications'])


def test_dry_run_rejects_bad_params():
    from botocore.exceptions import ParamValidationError
    import dryrun
    sv = dryrun.DryRunSession().client('s3vectors', 'eu-central-1')
    with pytest.raises(ParamValidationError):
        sv.create_index(vectorBucketName='b', indexName='i', dataType='float32', dimension='1024',
                        distanceMetric='cosine')
    with pytest.raises(ParamValidationError):
        sv.query_vectors(vectorBucketName='b', indexName='i', topK=3, queryTexts=[{'txt': 'x'}])


def test_existing_index_building_uses_vector_mode(monkeypatch):
    import dryrun
    monkeypatch.setitem(dryrun.CANNED, 'GetVectorBucket', dryrun._ok({'vectorBucket': {
        'vectorBucketName': 'demo-vectors-000000000000', 'vectorBucketArn': 'arn:x', 'creationTime': '2026-01-01T00:00:00Z'}}))
    monkeypatch.setitem(dryrun.CANNED, 'GetIndex', dryrun._ok({'index': {
        'vectorBucketName': 'v', 'indexName': 'docs-titan-v2', 'indexArn': 'arn:x', 'creationTime': '2026-01-01T00:00:00Z',
        'dataType': 'float32', 'dimension': 1024, 'distanceMetric': 'cosine', 'indexMode': 'CLASSIC'}}))
    d = deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run'])
    assert params(d, 'UpdateIndexMode')[0]['indexMode'] == 'ENHANCED'
    assert params(d, 'UpdateMetadataConfiguration')
    env = params(d, 'CreateAgentRuntime')[0]['environmentVariables']
    assert env['SEARCH_MODE'] == 'vector'


def test_existing_index_wrong_dim_stops(monkeypatch):
    import dryrun
    monkeypatch.setitem(dryrun.CANNED, 'GetVectorBucket', dryrun._ok({'vectorBucket': {
        'vectorBucketName': 'v', 'vectorBucketArn': 'arn:x', 'creationTime': '2026-01-01T00:00:00Z'}}))
    monkeypatch.setitem(dryrun.CANNED, 'GetIndex', dryrun._ok({'index': {
        'vectorBucketName': 'v', 'indexName': 'i', 'indexArn': 'arn:x', 'creationTime': '2026-01-01T00:00:00Z',
        'dataType': 'float32', 'dimension': 512, 'distanceMetric': 'cosine'}}))
    with pytest.raises(SystemExit):
        deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run'])


def test_bad_args():
    with pytest.raises(SystemExit):
        deploy.main(['--region', 'eu-central-1', '--dry-run', '--live-concurrency', '1'])
    with pytest.raises(SystemExit):
        deploy.main(['--region', 'eu-central-1', '--dry-run', '--embed-model', 'cohere.embed-multilingual-v3'])


def test_backfill_batch_size_one():
    d = deploy.main(['--region', 'eu-central-1', '--name', 'demo', '--dry-run'])
    assert sorted(e['BatchSize'] for e in params(d, 'CreateEventSourceMapping')) == [1, 1]
