"""Lambda entry point. S3 events go to an SQS queue; this function reads the queue.

- One message failing does not fail the batch: we return batchItemFailures, so only
  that message is retried, after a short backoff (30 s, 60 s, ... 15 min) instead of the
  full visibility timeout. After maxReceiveCount tries it goes to the dead-letter queue.
- Before each message we check the time left; if a slow file (PDF OCR) might not fit,
  the rest of the batch goes back to the queue untried.
- Backfill and reconcile send messages {"bucket": ..., "key": ...} to the same queue.
- Each result is logged as CloudWatch Embedded Metric Format, so Indexed / Skipped /
  Deleted / Failed counts become metrics without extra API calls (FR-13).
"""
import json
import time
import urllib.parse

import boto3

from . import embed
from .acl import ObjectMetadataAcl
from .config import Config
from .pipeline import Pipeline
from .store import VectorStore

_pipe = None


def pipeline():
    global _pipe
    if _pipe is None:
        cfg = Config()
        s = boto3.session.Session()
        _pipe = Pipeline(cfg, s.client('s3'), s.client('textract'), embed.build(cfg, s),
                         VectorStore(s.client('s3vectors'), cfg.vector_bucket, cfg.index), ObjectMetadataAcl())
    return _pipe


def targets(body):
    """Yield (bucket, key) from an SQS message body."""
    msg = json.loads(body)
    if 'Records' in msg:                                    # S3 event notification
        for r in msg['Records']:
            s3 = r.get('s3') or {}
            if 'object' in s3:
                yield s3['bucket']['name'], urllib.parse.unquote_plus(s3['object']['key'])
    elif msg.get('Event') == 's3:TestEvent':                # sent once when notifications are set up
        return
    else:                                                   # backfill / reconcile
        yield msg['bucket'], msg['key']


def _emit(result, ms):
    status = result.get('status', 'failed')
    print(json.dumps({
        '_aws': {'Timestamp': int(time.time() * 1000), 'CloudWatchMetrics': [{
            'Namespace': 'S3VectorsSearch', 'Dimensions': [['Status']],
            'Metrics': [{'Name': 'Objects', 'Unit': 'Count'}, {'Name': 'DurationMs', 'Unit': 'Milliseconds'}]}]},
        'Status': status, 'Objects': 1, 'DurationMs': ms, **result}, ensure_ascii=False, default=str))


def queue_url(arn):
    _, _, _, region, account, name = arn.split(':', 5)
    return f'https://sqs.{region}.amazonaws.com/{account}/{name}'


def backoff(rec):
    """30 s, 60 s, 120 s ... up to 15 min, by how often SQS has delivered the message."""
    n = int(rec.get('attributes', {}).get('ApproximateReceiveCount', '1'))
    return min(900, 30 * 2 ** max(0, n - 1))


def retry_soon(sqs, recs):
    """A failed message would wait the full visibility timeout (6 x the Lambda timeout)
    before SQS delivers it again; shorten it so retries come in minutes."""
    for rec in recs:
        if 'receiptHandle' not in rec or 'eventSourceARN' not in rec:
            continue                                # not from SQS (local test call)
        try:
            sqs.change_message_visibility(QueueUrl=queue_url(rec['eventSourceARN']),
                                          ReceiptHandle=rec['receiptHandle'], VisibilityTimeout=backoff(rec))
        except Exception as e:                      # best effort: SQS still retries later
            print(json.dumps({'warn': 'change_message_visibility failed', 'error': repr(e)[:200]}))


def handler(event, context, pipe=None, sqs=None):
    pipe = pipe or pipeline()
    # stop taking new messages when there is not enough time left for a slow one (PDF OCR)
    cfg = getattr(pipe, 'cfg', None)
    reserve_ms = ((cfg.ocr_wait_seconds if cfg else 600) + 60) * 1000
    failed, out_of_time = [], []
    for rec in event.get('Records', []):
        if context is not None and context.get_remaining_time_in_millis() < reserve_ms:
            out_of_time.append(rec)
            continue
        try:
            for bucket, key in targets(rec['body']):
                t0 = time.monotonic()
                try:
                    res = pipe.sync(bucket, key)
                except Exception as e:
                    _emit({'status': 'failed', 'bucket': bucket, 'key': key, 'error': repr(e)[:500]},
                          int((time.monotonic() - t0) * 1000))
                    raise
                _emit(res, int((time.monotonic() - t0) * 1000))
        except Exception:
            failed.append(rec)
    if failed or out_of_time:
        sqs = sqs or boto3.client('sqs')
        retry_soon(sqs, failed)
        for rec in out_of_time:                     # not tried: visible again after 30 s
            rec.setdefault('attributes', {})['ApproximateReceiveCount'] = '0'
        retry_soon(sqs, out_of_time)
    return {'batchItemFailures': [{'itemIdentifier': r['messageId']} for r in failed + out_of_time]}
