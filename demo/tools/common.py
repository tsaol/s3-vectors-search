"""Shared helpers for the demo tools: read deploy outputs, log in to Cognito, call the agent."""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3

HERE = os.path.dirname(os.path.abspath(__file__))
DEMO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(DEMO, 'indexer'))
OUT = os.path.join(DEMO, 'deploy', '.outputs.json')
USERS = os.path.join(DEMO, 'deploy', '.demo-users.json')


def outputs():
    if not os.path.exists(OUT):
        raise SystemExit(f'{OUT} not found: run demo/deploy/deploy.py first')
    return json.load(open(OUT))


def login(o, user, password=None):
    """Cognito USER_PASSWORD_AUTH -> access token (carries cognito:groups)."""
    if password is None:
        password = json.load(open(USERS))[user]
    cog = boto3.client('cognito-idp', region_name=o['region'])
    r = cog.initiate_auth(ClientId=o['client_id'], AuthFlow='USER_PASSWORD_AUTH',
                          AuthParameters={'USERNAME': user, 'PASSWORD': password})
    return r['AuthenticationResult']['AccessToken']


def invoke(o, payload, token=None, session=None):
    """POST to the runtime with a bearer token (OAuth runtimes don't take SigV4).
    Returns (http_status, json_body)."""
    arn = urllib.parse.quote(o['runtime_arn'], safe='')
    url = f"https://bedrock-agentcore.{o['region']}.amazonaws.com/runtimes/{arn}/invocations?qualifier=DEFAULT"
    headers = {'Content-Type': 'application/json',
               'X-Amzn-Bedrock-AgentCore-Runtime-Session-Id': session or (uuid.uuid4().hex + uuid.uuid4().hex)}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.loads(r.read() or b'{}')
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return e.code, json.loads(body or b'{}')
        except ValueError:
            return e.code, {'raw': body.decode('utf-8', 'replace')[:500]}
