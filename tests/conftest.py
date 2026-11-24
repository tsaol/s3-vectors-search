import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for p in ('demo/indexer', 'demo/agent', 'tools', 'tests'):
    sys.path.insert(0, os.path.join(ROOT, p))
os.environ.setdefault('AWS_DEFAULT_REGION', 'eu-central-1')
