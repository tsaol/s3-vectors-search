"""Offline check for deploy.py: real botocore clients (so every request is validated
against the service model: names, types, required fields), but a 'before-call' hook
answers each call with a canned response instead of sending it. The canned answers
describe an empty account, so the create paths run."""
import botocore.session
from botocore.awsrequest import AWSResponse

ACCOUNT = '000000000000'


class _Http(AWSResponse):
    def __init__(self, status):
        super().__init__('https://dry-run', status, {}, None)


def _err(code, status=404):
    return _Http(status), {'Error': {'Code': code, 'Message': 'dry run'}, 'ResponseMetadata': {'HTTPStatusCode': status}}


def _ok(body=None):
    return _Http(200), {**(body or {}), 'ResponseMetadata': {'HTTPStatusCode': 200}}


CANNED = {
    'GetCallerIdentity': _ok({'Account': ACCOUNT}),
    'HeadBucket': _err('404'),
    'GetVectorBucket': _err('NotFoundException'),
    'GetIndex': _err('NotFoundException'),
    'CreateQueue': _ok({'QueueUrl': 'https://sqs.dry-run/q'}),
    'GetQueueAttributes': _ok({'Attributes': {'QueueArn': f'arn:aws:sqs:dry:{ACCOUNT}:dlq'}}),
    'GetQueueUrl': _err('AWS.SimpleQueueService.NonExistentQueue', 400),
    'GetRole': _err('NoSuchEntity'),
    'CreateRole': _ok({'Role': {'Arn': f'arn:aws:iam::{ACCOUNT}:role/x', 'RoleName': 'x', 'Path': '/',
                                'RoleId': 'AROAXXXXXXXXXXXXXXXXX', 'CreateDate': '2026-01-01T00:00:00Z'}}),
    'ListAttachedRolePolicies': _ok({'AttachedPolicies': []}),
    'GetFunction': _err('ResourceNotFoundException'),
    'ListEventSourceMappings': _ok({'EventSourceMappings': []}),
    'GetBucketNotificationConfiguration': _ok({}),
    'ListUserPools': _ok({'UserPools': []}),
    'CreateUserPool': _ok({'UserPool': {'Id': 'eu-central-1_DRYRUN'}}),
    'ListUserPoolClients': _ok({'UserPoolClients': []}),
    'CreateUserPoolClient': _ok({'UserPoolClient': {'ClientId': 'dryrunclient'}}),
    'ListGroups': _ok({'Groups': []}),
    'ListAgentRuntimes': _ok({'agentRuntimes': []}),
    'CreateAgentRuntime': _ok({'agentRuntimeId': 'rt-1', 'agentRuntimeArn': f'arn:aws:bedrock-agentcore:dry:{ACCOUNT}:runtime/rt-1',
                               'agentRuntimeVersion': '1', 'createdAt': '2026-01-01T00:00:00Z', 'status': 'CREATING'}),
}


class DryRunSession:
    def __init__(self):
        self.calls = []
        self._s = botocore.session.get_session()
        self._s.set_credentials('AKIDRYRUN', 'dry-run-secret')

    def client(self, name, region_name=None):
        c = self._s.create_client(name, region_name=region_name or 'eu-central-1')

        sid = c.meta.service_model.service_id.hyphenize()

        def record(params, model, **kw):          # raw API params, before validation
            self.calls.append((name, model.name, dict(params)))

        def answer(model, **kw):                  # runs after validation; never sends
            r = CANNED.get(model.name, _ok())
            return r() if callable(r) else r

        c.meta.events.register_first(f'before-parameter-build.{sid}', record)
        c.meta.events.register_first(f'before-call.{sid}', answer)
        return c
