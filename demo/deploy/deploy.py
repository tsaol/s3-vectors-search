#!/usr/bin/env python3
"""Deploy (or update, or remove) the S3 semantic search demo with boto3. Safe to run again.

    python demo/deploy/deploy.py --region eu-central-1 --name s3vs
    python demo/deploy/deploy.py --region eu-central-1 --name s3vs --demo-users
    python demo/deploy/deploy.py --region eu-central-1 --name s3vs --destroy

What it creates (all in one Region; names start with --name):
  source bucket (or use --source-bucket), artifacts bucket (code zips; not the data bucket)
  vector bucket + ENHANCED vector index (cosine, chunk_text non-filterable + full-text zh)
  SQS: events queue, backfill queue, dead-letter queue; S3 notifications -> events queue
  Lambda indexer (two SQS triggers: live events get more concurrency than backfill)
  Cognito user pool + app client + groups (+ demo users with --demo-users)
  AgentCore Runtime search agent with a JWT authorizer (Cognito); Authorization header forwarded
  CloudWatch alarms: messages in the DLQ, events waiting too long

Outputs go to demo/deploy/.outputs.json (git-ignored). Demo user passwords go to
demo/deploy/.demo-users.json (git-ignored, mode 600).

--dry-run checks every API call against the boto3 service models without calling AWS.
"""
import argparse
import io
import json
import os
import secrets
import string
import subprocess
import sys
import tempfile
import time
import zipfile

import boto3
from botocore.exceptions import ClientError

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = os.path.dirname(HERE)
OUT = os.path.join(HERE, '.outputs.json')
USERS = os.path.join(HERE, '.demo-users.json')
GROUPS = {'all-staff': '所有员工', 'hr-payroll': '薪酬组'}
DEMO_USERS = {'alice': ['all-staff'], 'bob': ['all-staff', 'hr-payroll']}
LAMBDA_TIMEOUT = 900


def log(msg):
    print(f'· {msg}', flush=True)


def code(e):
    return e.response.get('Error', {}).get('Code', '')


class Deployer:
    def __init__(self, a, session):
        self.a, self.s = a, session
        self.region = a.region
        c = lambda n: session.client(n, region_name=self.region)
        self.sts, self.s3, self.sv, self.sqs = c('sts'), c('s3'), c('s3vectors'), c('sqs')
        self.iam, self.lam, self.cog = c('iam'), c('lambda'), c('cognito-idp')
        self.acc_ctl, self.cw = c('bedrock-agentcore-control'), c('cloudwatch')
        self.account = self.sts.get_caller_identity()['Account']
        n = a.name
        self.src = a.source_bucket or f'{n}-docs-{self.account}-{self.region}'
        self.art = f'{n}-artifacts-{self.account}-{self.region}'
        self.vb = a.vector_bucket or f'{n}-vectors-{self.account}'
        self.index = a.index
        self.fn = f'{n}-indexer'
        self.runtime_name = f'{n}_search_agent'.replace('-', '_')
        self.out = {'region': self.region, 'source_bucket': self.src, 'vector_bucket': self.vb, 'index': self.index}

    # ---------- helpers ----------
    def arn(self, service, res, region=True):
        return f"arn:aws:{service}:{self.region if region else ''}:{self.account}:{res}"

    def bucket(self, name):
        try:
            self.s3.head_bucket(Bucket=name)
            return
        except ClientError as e:
            if code(e) not in ('404', 'NoSuchBucket', 'NotFound'):
                raise
        log(f'create bucket {name}')
        kw = {'Bucket': name}
        if self.region != 'us-east-1':
            kw['CreateBucketConfiguration'] = {'LocationConstraint': self.region}
        self.s3.create_bucket(**kw)
        self.s3.put_public_access_block(Bucket=name, PublicAccessBlockConfiguration={
            'BlockPublicAcls': True, 'IgnorePublicAcls': True, 'BlockPublicPolicy': True, 'RestrictPublicBuckets': True})
        self.s3.put_bucket_encryption(Bucket=name, ServerSideEncryptionConfiguration={
            'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]})

    def role(self, name, service, policy, managed=()):
        trust = {'Version': '2012-10-17', 'Statement': [{
            'Effect': 'Allow', 'Principal': {'Service': service}, 'Action': 'sts:AssumeRole',
            'Condition': {'StringEquals': {'aws:SourceAccount': self.account}}}]}
        try:
            arn = self.iam.get_role(RoleName=name)['Role']['Arn']
            self.iam.update_assume_role_policy(RoleName=name, PolicyDocument=json.dumps(trust))
            fresh = False
        except ClientError as e:
            if code(e) != 'NoSuchEntity':
                raise
            log(f'create role {name}')
            arn = self.iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))['Role']['Arn']
            fresh = True
        self.iam.put_role_policy(RoleName=name, PolicyName='inline', PolicyDocument=json.dumps(policy))
        for m in managed:
            self.iam.attach_role_policy(RoleName=name, PolicyArn=m)
        return arn, fresh

    def package(self, src_dirs, extra_files=(), deps=('boto3',)):
        """Zip code + pure-Python deps (latest boto3: S3 Vectors hybrid search needs a new SDK)."""
        with tempfile.TemporaryDirectory() as d:
            if deps and not self.a.dry_run:
                subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '--target', d, *deps],
                               check=True)
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
                for root in [d] + list(src_dirs):
                    base = root if root == d else os.path.dirname(root)
                    for dp, _, files in os.walk(root):
                        if '__pycache__' in dp:
                            continue
                        for f in files:
                            p = os.path.join(dp, f)
                            z.write(p, os.path.relpath(p, base))
                for p in extra_files:
                    z.write(p, os.path.basename(p))
            return buf.getvalue()

    # ---------- parts ----------
    def buckets(self):
        self.bucket(self.art)
        self.bucket(self.src)

    def vectors(self):
        try:
            self.sv.get_vector_bucket(vectorBucketName=self.vb)
        except ClientError as e:
            if code(e) != 'NotFoundException':
                raise
            log(f'create vector bucket {self.vb}')
            self.sv.create_vector_bucket(vectorBucketName=self.vb)
        # new indexes in this bucket use ENHANCED (pre-filtering, full-text search)
        self.sv.put_vector_bucket_default_index_mode(vectorBucketName=self.vb, defaultIndexMode='ENHANCED')
        self.search_mode = 'hybrid'
        try:
            idx = self.sv.get_index(vectorBucketName=self.vb, indexName=self.index)['index']
            if idx.get('dimension') not in (None, self.a.dim):
                raise SystemExit(f"index {self.index} has {idx['dimension']} dimensions, --dim is {self.a.dim}; "
                                 'use another --index or the matching --dim')
            if idx.get('indexMode') != 'ENHANCED':
                log(f'index {self.index}: CLASSIC -> ENHANCED')
                self.sv.update_index_mode(vectorBucketName=self.vb, indexName=self.index, indexMode='ENHANCED')
            text = {t['metadataKey']: t.get('status', 'ACTIVE')
                    for t in idx.get('metadataConfiguration', {}).get('textSearchConfigurations', [])}
            if 'chunk_text' not in text:
                log('enable full-text search on chunk_text (background backfill, billed at the PUT rate)')
                self.sv.update_metadata_configuration(vectorBucketName=self.vb, indexName=self.index, add={
                    'textSearchConfigurations': [{'metadataKey': 'chunk_text', 'language': 'zh'}]})
                text['chunk_text'] = 'BUILDING'
            if text['chunk_text'] != 'ACTIVE':
                # hybrid queries on a BUILDING field are rejected: search by vector until it is ACTIVE
                self.search_mode = 'vector'
                log('full-text field is BUILDING: agent uses vector search for now; '
                    're-run deploy.py when GetIndex shows ACTIVE to switch to hybrid')
        except ClientError as e:
            if code(e) != 'NotFoundException':
                raise
            log(f'create index {self.index} ({self.a.dim} dims, cosine, full-text zh on chunk_text)')
            self.sv.create_index(
                vectorBucketName=self.vb, indexName=self.index, dataType='float32',
                dimension=self.a.dim, distanceMetric='cosine',
                metadataConfiguration={'nonFilterableMetadataKeys': ['chunk_text'],
                                       'textSearchConfigurations': [{'metadataKey': 'chunk_text', 'language': 'zh'}]})
        self.index_arn = self.arn('s3vectors', f'bucket/{self.vb}/index/{self.index}')

    def queues(self):
        n = self.a.name
        dlq = self.sqs.create_queue(QueueName=f'{n}-dlq', Attributes={
            'MessageRetentionPeriod': '1209600', 'SqsManagedSseEnabled': 'true'})['QueueUrl']
        dlq_arn = self.sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
        redrive = json.dumps({'deadLetterTargetArn': dlq_arn, 'maxReceiveCount': 8})
        attrs = {'VisibilityTimeout': str(LAMBDA_TIMEOUT * 6), 'MessageRetentionPeriod': '1209600',
                 'SqsManagedSseEnabled': 'true', 'RedrivePolicy': redrive}
        self.q = {}
        for kind in ('events', 'backfill'):
            url = self.sqs.create_queue(QueueName=f'{n}-{kind}')['QueueUrl']
            a = dict(attrs)
            if kind == 'events':
                a['Policy'] = json.dumps({'Version': '2012-10-17', 'Statement': [{
                    'Effect': 'Allow', 'Principal': {'Service': 's3.amazonaws.com'}, 'Action': 'sqs:SendMessage',
                    'Resource': self.arn('sqs', f'{n}-events'),
                    'Condition': {'ArnEquals': {'aws:SourceArn': f'arn:aws:s3:::{self.src}'},
                                  'StringEquals': {'aws:SourceAccount': self.account}}}]})
            self.sqs.set_queue_attributes(QueueUrl=url, Attributes=a)
            self.q[kind] = {'url': url, 'arn': self.arn('sqs', f'{n}-{kind}')}
        self.q['dlq'] = {'url': dlq, 'arn': dlq_arn}
        self.out['queues'] = {k: v['url'] for k, v in self.q.items()}

    def notifications(self):
        cur = self.s3.get_bucket_notification_configuration(Bucket=self.src)
        cur.pop('ResponseMetadata', None)
        mine = {'Id': f'{self.a.name}-events', 'QueueArn': self.q['events']['arn'],
                'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*', 's3:LifecycleExpiration:*']}
        others = {k: v for k, v in cur.items() if k in (
            'TopicConfigurations', 'QueueConfigurations', 'LambdaFunctionConfigurations')}
        others['QueueConfigurations'] = [q for q in others.get('QueueConfigurations', []) if q.get('Id') != mine['Id']]
        if any(others.values()) and not self.a.replace_notifications:
            raise SystemExit(f'bucket {self.src} already sends notifications elsewhere; S3 does not allow two '
                             'targets for the same events and prefix. Re-run with --replace-notifications '
                             'to replace them (for example the old demo Lambda trigger).')
        log(f'S3 notifications {self.src} -> {self.a.name}-events')
        conf = {'QueueConfigurations': [mine]}
        if cur.get('EventBridgeConfiguration') is not None:
            conf['EventBridgeConfiguration'] = cur['EventBridgeConfiguration']
        self.s3.put_bucket_notification_configuration(Bucket=self.src, NotificationConfiguration=conf)

    def models(self, model):
        """Resources for bedrock:InvokeModel: a model id, or a cross-Region inference profile."""
        if not model:
            return []
        prefix = model.split('.', 1)[0]
        if prefix in ('eu', 'us', 'apac', 'jp', 'au', 'ca', 'global', 'us-gov'):
            base = model.split('.', 1)[1]
            return [self.arn('bedrock', f'inference-profile/{model}'), f'arn:aws:bedrock:*::foundation-model/{base}']
        return [f'arn:aws:bedrock:{self.region}::foundation-model/{model}']

    def indexer_role(self):
        p = {'Version': '2012-10-17', 'Statement': [
            {'Effect': 'Allow', 'Action': ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
             'Resource': self.arn('logs', f'log-group:/aws/lambda/{self.fn}*')},
            {'Effect': 'Allow', 'Action': ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:GetQueueAttributes',
                                           'sqs:ChangeMessageVisibility'],
             'Resource': [self.q['events']['arn'], self.q['backfill']['arn']]},
            # read only. ListBucket makes HeadObject return 404 (not 403) for a deleted object
            {'Effect': 'Allow', 'Action': 's3:GetObject', 'Resource': f'arn:aws:s3:::{self.src}/*'},
            {'Effect': 'Allow', 'Action': 's3:ListBucket', 'Resource': f'arn:aws:s3:::{self.src}'},
            {'Effect': 'Allow', 'Action': ['textract:DetectDocumentText', 'textract:StartDocumentTextDetection',
                                           'textract:GetDocumentTextDetection'], 'Resource': '*'},
            {'Effect': 'Allow', 'Action': 'bedrock:InvokeModel', 'Resource': self.models(self.a.embed_model)},
            {'Effect': 'Allow', 'Action': ['s3vectors:PutVectors', 's3vectors:GetVectors', 's3vectors:DeleteVectors'],
             'Resource': self.index_arn}]}
        managed = ['arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'] if self.a.subnets else []
        return self.role(f'{self.a.name}-indexer-role', 'lambda.amazonaws.com', p, managed)

    def indexer(self):
        role, fresh = self.indexer_role()
        if fresh and not self.a.dry_run:
            time.sleep(10)                                   # new IAM role needs a moment
        z = self.package([os.path.join(DEMO, 'indexer', 's3vsearch')])
        key = 'indexer/indexer.zip'
        self.s3.put_object(Bucket=self.art, Key=key, Body=z)
        env = {'Variables': {'EMBEDDER': 'bedrock-titan-text', 'MODEL_ID': self.a.embed_model, 'DIM': str(self.a.dim),
                             'VECTOR_BUCKET': self.vb, 'INDEX': self.index, 'RULE_VERSION': self.a.rule_version,
                             'CHUNK_CHARS': '600', 'CHUNK_OVERLAP': '80'}}
        vpc = {'SubnetIds': self.a.subnets, 'SecurityGroupIds': self.a.security_groups} if self.a.subnets else \
            {'SubnetIds': [], 'SecurityGroupIds': []}
        conf = dict(Role=role, Handler='s3vsearch.handler.handler', Runtime='python3.13', Timeout=LAMBDA_TIMEOUT,
                    MemorySize=1024, Environment=env, VpcConfig=vpc)
        try:
            self.lam.get_function(FunctionName=self.fn)
            log(f'update Lambda {self.fn}')
            self.lam.update_function_code(FunctionName=self.fn, S3Bucket=self.art, S3Key=key, Architectures=['arm64'])
            self.wait_lambda()
            self.lam.update_function_configuration(FunctionName=self.fn, **conf)
        except ClientError as e:
            if code(e) != 'ResourceNotFoundException':
                raise
            log(f'create Lambda {self.fn}')
            for i in range(6):
                try:
                    self.lam.create_function(FunctionName=self.fn, Code={'S3Bucket': self.art, 'S3Key': key},
                                             Architectures=['arm64'], **conf)
                    break
                except ClientError as e2:            # role not usable yet
                    if code(e2) != 'InvalidParameterValueException' or i == 5:
                        raise
                    time.sleep(10)
        self.wait_lambda()
        have = {m['EventSourceArn']: m['UUID'] for m in
                self.lam.list_event_source_mappings(FunctionName=self.fn).get('EventSourceMappings', [])}
        for kind, conc in (('events', self.a.live_concurrency), ('backfill', self.a.backfill_concurrency)):
            arn = self.q[kind]['arn']
            # one object per invocation: a slow PDF never holds others back, and no message
            # uses up its receive count waiting behind one
            kw = dict(BatchSize=1, FunctionResponseTypes=['ReportBatchItemFailures'],
                      ScalingConfig={'MaximumConcurrency': conc})
            if arn in have:
                self.lam.update_event_source_mapping(UUID=have[arn], **kw)
            else:
                log(f'trigger {kind} queue -> {self.fn} (max concurrency {conc})')
                self.lam.create_event_source_mapping(EventSourceArn=arn, FunctionName=self.fn, **kw)
        self.out['function'] = self.fn

    def wait_lambda(self):
        if self.a.dry_run:
            return
        self.lam.get_waiter('function_updated_v2').wait(FunctionName=self.fn)

    def cognito(self):
        name = f'{self.a.name}-users'
        pool = None
        for page in self.cog.get_paginator('list_user_pools').paginate(MaxResults=60):
            for p in page['UserPools']:
                if p['Name'] == name:
                    pool = p['Id']
        if not pool:
            log(f'create Cognito user pool {name}')
            pool = self.cog.create_user_pool(PoolName=name, Policies={'PasswordPolicy': {
                'MinimumLength': 12, 'RequireUppercase': True, 'RequireLowercase': True,
                'RequireNumbers': True, 'RequireSymbols': False}},
                AdminCreateUserConfig={'AllowAdminCreateUserOnly': True})['UserPool']['Id']
        clients = self.cog.list_user_pool_clients(UserPoolId=pool, MaxResults=60).get('UserPoolClients', [])
        cid = next((c['ClientId'] for c in clients if c['ClientName'] == 'search-cli'), None)
        if not cid:
            cid = self.cog.create_user_pool_client(
                UserPoolId=pool, ClientName='search-cli', GenerateSecret=False,
                ExplicitAuthFlows=['ALLOW_USER_PASSWORD_AUTH', 'ALLOW_REFRESH_TOKEN_AUTH'],
                AccessTokenValidity=60, IdTokenValidity=60, RefreshTokenValidity=1,
                TokenValidityUnits={'AccessToken': 'minutes', 'IdToken': 'minutes', 'RefreshToken': 'days'},
            )['UserPoolClient']['ClientId']
        have = {g['GroupName'] for g in self.cog.list_groups(UserPoolId=pool).get('Groups', [])}
        for g, desc in GROUPS.items():
            if g not in have:
                self.cog.create_group(UserPoolId=pool, GroupName=g, Description=desc)
        if self.a.demo_users:
            self.demo_users(pool)
        self.pool, self.client_id = pool, cid
        self.out.update({'user_pool': pool, 'client_id': cid,
                         'discovery_url': f'https://cognito-idp.{self.region}.amazonaws.com/{pool}/.well-known/openid-configuration'})

    def demo_users(self, pool):
        saved = json.load(open(USERS)) if os.path.exists(USERS) else {}
        abc = string.ascii_letters + string.digits
        for u, groups in DEMO_USERS.items():
            pw = saved.get(u) or ('Aa1' + ''.join(secrets.choice(abc) for _ in range(17)))
            try:
                self.cog.admin_create_user(UserPoolId=pool, Username=u, MessageAction='SUPPRESS')
            except ClientError as e:
                if code(e) != 'UsernameExistsException':
                    raise
            self.cog.admin_set_user_password(UserPoolId=pool, Username=u, Password=pw, Permanent=True)
            for g in groups:
                self.cog.admin_add_user_to_group(UserPoolId=pool, Username=u, GroupName=g)
            saved[u] = pw
        if not self.a.dry_run:
            fd = os.open(USERS, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, 'w') as f:
                json.dump(saved, f, indent=1)
        log(f'demo users {", ".join(DEMO_USERS)} (passwords in {os.path.relpath(USERS)})')

    def agent(self):
        p = {'Version': '2012-10-17', 'Statement': [
            {'Effect': 'Allow', 'Action': ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents',
                                           'logs:DescribeLogStreams', 'logs:DescribeLogGroups'],
             'Resource': self.arn('logs', 'log-group:/aws/bedrock-agentcore/*')},
            {'Effect': 'Allow', 'Action': 'bedrock:InvokeModel',
             'Resource': self.models(self.a.embed_model) + self.models(self.a.answer_model)},
            {'Effect': 'Allow', 'Action': ['s3vectors:QueryVectors', 's3vectors:GetVectors'], 'Resource': self.index_arn},
            {'Effect': 'Allow', 'Action': 's3:GetObject', 'Resource': f'arn:aws:s3:::{self.art}/agent/*'}]}
        role, fresh = self.role(f'{self.a.name}-agent-role', 'bedrock-agentcore.amazonaws.com', p)
        if fresh and not self.a.dry_run:
            time.sleep(10)
        z = self.package([], extra_files=[os.path.join(DEMO, 'agent', 'main.py')])
        key = 'agent/agent.zip'
        self.s3.put_object(Bucket=self.art, Key=key, Body=z)
        net = {'networkMode': 'VPC', 'networkModeConfig': {'subnets': self.a.subnets,
                                                           'securityGroups': self.a.security_groups}} \
            if self.a.subnets else {'networkMode': 'PUBLIC'}
        conf = dict(
            agentRuntimeArtifact={'codeConfiguration': {'code': {'s3': {'bucket': self.art, 'prefix': key}},
                                                        'runtime': 'PYTHON_3_13', 'entryPoint': ['main.py']}},
            roleArn=role, networkConfiguration=net, protocolConfiguration={'serverProtocol': 'HTTP'},
            authorizerConfiguration={'customJWTAuthorizer': {'discoveryUrl': self.out['discovery_url'],
                                                             'allowedClients': [self.client_id]}},
            requestHeaderConfiguration={'requestHeaderAllowlist': ['Authorization']},
            environmentVariables={'EMBED_MODEL': self.a.embed_model, 'DIM': str(self.a.dim),
                                  'ANSWER_MODEL': self.a.answer_model, 'VECTOR_BUCKET': self.vb, 'INDEX': self.index,
                                  'SEARCH_MODE': self.search_mode, 'TOPK': '5', 'MAX_DISTANCE': '0.75',
                                  'MIN_BM25_RATIO': '0.5'})
        rid = None
        for page in self.acc_ctl.get_paginator('list_agent_runtimes').paginate():
            for r in page.get('agentRuntimes', []):
                if r['agentRuntimeName'] == self.runtime_name:
                    rid, rarn = r['agentRuntimeId'], r['agentRuntimeArn']
        if rid:
            log(f'update AgentCore runtime {self.runtime_name}')
            self.acc_ctl.update_agent_runtime(agentRuntimeId=rid, **conf)
        else:
            log(f'create AgentCore runtime {self.runtime_name}')
            for i in range(6):
                try:
                    r = self.acc_ctl.create_agent_runtime(agentRuntimeName=self.runtime_name, **conf)
                    break
                except ClientError as e:
                    if code(e) not in ('ValidationException', 'AccessDeniedException') or i == 5:
                        raise
                    time.sleep(10)                    # role not usable yet
            rid, rarn = r['agentRuntimeId'], r['agentRuntimeArn']
        if not self.a.dry_run:
            for _ in range(60):
                st = self.acc_ctl.get_agent_runtime(agentRuntimeId=rid)['status']
                if st == 'READY':
                    break
                if st.endswith('FAILED'):
                    raise SystemExit(f'runtime {st}')
                time.sleep(5)
        self.out.update({'runtime_id': rid, 'runtime_arn': rarn})

    def alarms(self):
        n = self.a.name
        common = dict(EvaluationPeriods=1, Period=300, Statistic='Maximum', Namespace='AWS/SQS',
                      TreatMissingData='notBreaching')
        if self.a.alarm_topic:
            common['AlarmActions'] = [self.a.alarm_topic]
        self.cw.put_metric_alarm(AlarmName=f'{n}-dlq-not-empty', MetricName='ApproximateNumberOfMessagesVisible',
                                 Dimensions=[{'Name': 'QueueName', 'Value': f'{n}-dlq'}],
                                 Threshold=0, ComparisonOperator='GreaterThanThreshold',
                                 AlarmDescription='objects that failed 8 times; see the indexer logs', **common)
        self.cw.put_metric_alarm(AlarmName=f'{n}-events-backlog', MetricName='ApproximateAgeOfOldestMessage',
                                 Dimensions=[{'Name': 'QueueName', 'Value': f'{n}-events'}],
                                 Threshold=900, ComparisonOperator='GreaterThanThreshold',
                                 AlarmDescription='new objects wait more than 15 minutes', **common)
        log('alarms: dlq-not-empty, events-backlog')

    # ---------- run ----------
    def up(self):
        self.buckets()
        self.vectors()
        self.queues()
        self.indexer()
        self.notifications()
        self.cognito()
        self.agent()
        self.alarms()
        if not self.a.dry_run:
            with open(OUT, 'w') as f:
                json.dump(self.out, f, indent=1)
        log(f'done. outputs -> {os.path.relpath(OUT)}')

    def destroy(self):
        n = self.a.name
        for page in self.acc_ctl.get_paginator('list_agent_runtimes').paginate():
            for r in page.get('agentRuntimes', []):
                if r['agentRuntimeName'] == self.runtime_name:
                    log(f'delete runtime {self.runtime_name}')
                    self.acc_ctl.delete_agent_runtime(agentRuntimeId=r['agentRuntimeId'])
        try:
            for m in self.lam.list_event_source_mappings(FunctionName=self.fn).get('EventSourceMappings', []):
                self.lam.delete_event_source_mapping(UUID=m['UUID'])
            self.lam.delete_function(FunctionName=self.fn)
            log(f'delete Lambda {self.fn}')
        except ClientError as e:
            if code(e) != 'ResourceNotFoundException':
                raise
        try:
            cur = self.s3.get_bucket_notification_configuration(Bucket=self.src)
            qs = [q for q in cur.get('QueueConfigurations', []) if q.get('Id') != f'{n}-events']
            conf = {k: cur[k] for k in ('TopicConfigurations', 'LambdaFunctionConfigurations', 'EventBridgeConfiguration') if k in cur}
            if qs:
                conf['QueueConfigurations'] = qs
            self.s3.put_bucket_notification_configuration(Bucket=self.src, NotificationConfiguration=conf)
        except ClientError as e:
            if code(e) != 'NoSuchBucket':
                raise
        for kind in ('events', 'backfill', 'dlq'):
            try:
                url = self.sqs.get_queue_url(QueueName=f'{n}-{kind}')['QueueUrl']
                self.sqs.delete_queue(QueueUrl=url)
                log(f'delete queue {n}-{kind}')
            except ClientError as e:
                if 'NonExistentQueue' not in code(e):
                    raise
        self.cw.delete_alarms(AlarmNames=[f'{n}-dlq-not-empty', f'{n}-events-backlog'])
        for r in (f'{n}-indexer-role', f'{n}-agent-role'):
            try:
                for m in self.iam.list_attached_role_policies(RoleName=r)['AttachedPolicies']:
                    self.iam.detach_role_policy(RoleName=r, PolicyArn=m['PolicyArn'])
                try:
                    self.iam.delete_role_policy(RoleName=r, PolicyName='inline')
                except ClientError as e:
                    if code(e) != 'NoSuchEntity':
                        raise
                self.iam.delete_role(RoleName=r)
                log(f'delete role {r}')
            except ClientError as e:
                if code(e) != 'NoSuchEntity':
                    raise
        for page in self.cog.get_paginator('list_user_pools').paginate(MaxResults=60):
            for p in page['UserPools']:
                if p['Name'] == f'{n}-users':
                    self.cog.delete_user_pool(UserPoolId=p['Id'])
                    log(f'delete user pool {p["Id"]}')
        if self.a.delete_vectors:
            try:
                self.sv.delete_index(vectorBucketName=self.vb, indexName=self.index)
                log(f'delete index {self.index}')
            except ClientError as e:
                if code(e) != 'NotFoundException':
                    raise
        log(f'kept: source bucket {self.src}, artifacts bucket {self.art}, vector bucket {self.vb}'
            + ('' if self.a.delete_vectors else f' and index {self.index} (use --delete-vectors)'))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--region', required=True)
    ap.add_argument('--name', default='s3vs', help='prefix for all resource names')
    ap.add_argument('--source-bucket', help='use an existing bucket with your objects')
    ap.add_argument('--vector-bucket')
    ap.add_argument('--index', default='docs-titan-v2')
    ap.add_argument('--dim', type=int, default=1024, choices=[256, 512, 1024], help='Titan V2 dimensions')
    ap.add_argument('--embed-model', default='amazon.titan-embed-text-v2:0', choices=['amazon.titan-embed-text-v2:0'],
                    help='the agent embeds questions with Titan V2; to use another model, change both '
                         'demo/indexer (EMBEDDER) and demo/agent/main.py (embed)')
    ap.add_argument('--answer-model', default='',
                    help='Bedrock model or inference profile for answers; empty = search only. '
                         'A cross-Region profile (eu./us./global.) sends chunk text to other Regions.')
    ap.add_argument('--rule-version', default='r2')
    ap.add_argument('--subnets', nargs='*', default=[], help='private subnets (needs VPC endpoints, see docs)')
    ap.add_argument('--security-groups', nargs='*', default=[])
    ap.add_argument('--live-concurrency', type=int, default=10)
    ap.add_argument('--backfill-concurrency', type=int, default=4)
    ap.add_argument('--alarm-topic', help='SNS topic ARN for alarms')
    ap.add_argument('--demo-users', action='store_true', help='create users alice / bob for the demo')
    ap.add_argument('--replace-notifications', action='store_true')
    ap.add_argument('--destroy', action='store_true')
    ap.add_argument('--delete-vectors', action='store_true', help='with --destroy: also delete the index')
    ap.add_argument('--dry-run', action='store_true', help='validate API calls offline, call nothing')
    a = ap.parse_args(argv)
    if min(a.live_concurrency, a.backfill_concurrency) < 2:
        ap.error('SQS triggers need a maximum concurrency of at least 2')
    if bool(a.subnets) != bool(a.security_groups):
        ap.error('--subnets and --security-groups go together')
    if a.dry_run:
        sys.path.insert(0, HERE)
        from dryrun import DryRunSession
        session = DryRunSession()
    else:
        session = boto3.session.Session()
    d = Deployer(a, session)
    d.destroy() if a.destroy else d.up()
    return d


if __name__ == '__main__':
    main()
