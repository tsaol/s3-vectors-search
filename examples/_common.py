"""Shared bits for the examples: args, clients, Titan V2 embeddings."""
import argparse
import json
import os

import boto3

DIM = 1024
EMBED_MODEL = 'amazon.titan-embed-text-v2:0'


def args(desc, **extra):
    ap = argparse.ArgumentParser(description=desc)
    ap.add_argument('--region', default=os.environ.get('AWS_REGION', 'eu-central-1'))
    ap.add_argument('--bucket', default=os.environ.get('VECTOR_BUCKET', 'demo-vectors-examples'),
                    help='vector bucket name (unique in your account and Region)')
    ap.add_argument('--index', default='examples')
    for k, v in extra.items():
        ap.add_argument(f'--{k}', **v)
    return ap.parse_args()


def clients(region):
    return boto3.client('s3vectors', region_name=region), boto3.client('bedrock-runtime', region_name=region)


def embed(br, text):
    body = json.dumps({'inputText': text, 'dimensions': DIM, 'normalize': True})
    return json.loads(br.invoke_model(modelId=EMBED_MODEL, body=body)['body'].read())['embedding']


def show(hits):
    for h in hits:
        m = h.get('metadata', {})
        d = h.get('distance')
        sc = h.get('scores')
        extra = f"  distance={d:.3f}" if d is not None else ''
        if sc:
            extra += f"  scores={json.dumps(sc, ensure_ascii=False)}"
        print(f"  {h['key']:<10} {m.get('text', '')[:40]:<42}{extra}")
