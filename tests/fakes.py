"""In-memory stand-ins for S3, S3 Vectors, Textract and the embedder, so the pipeline
logic can be tested without AWS. They follow the real API shapes used by the code."""
import datetime
import hashlib
import io
import math

from botocore.exceptions import ClientError


def _err(code, op):
    return ClientError({'Error': {'Code': code, 'Message': code}}, op)


class FakeS3:
    def __init__(self):
        self.objects = {}        # (bucket, key) -> dict(body, meta, etag, mtime)
        self.on_get = None       # hook to simulate a change during processing

    def put(self, bucket, key, body, **meta):
        body = body.encode() if isinstance(body, str) else body
        self.objects[(bucket, key)] = {
            'body': body, 'meta': meta, 'etag': hashlib.md5(body).hexdigest(),
            'mtime': datetime.datetime(2026, 11, 24, tzinfo=datetime.timezone.utc)}

    def delete(self, bucket, key):
        self.objects.pop((bucket, key), None)

    def head_object(self, Bucket, Key):
        o = self.objects.get((Bucket, Key))
        if o is None:
            raise _err('404', 'HeadObject')
        return {'ETag': f'"{o["etag"]}"', 'Metadata': dict(o['meta']), 'LastModified': o['mtime'],
                'ContentLength': len(o['body'])}

    def get_object(self, Bucket, Key, IfMatch=None):
        o = self.objects.get((Bucket, Key))
        if o is None:
            raise _err('NoSuchKey', 'GetObject')
        if IfMatch and IfMatch != o['etag']:
            raise _err('PreconditionFailed', 'GetObject')
        if self.on_get:
            self.on_get()
        return {'Body': io.BytesIO(o['body'])}


def _match(meta, flt):
    for k, cond in flt.items():
        if k == '$and':
            if not all(_match(meta, c) for c in cond):
                return False
            continue
        if k == '$or':
            if not any(_match(meta, c) for c in cond):
                return False
            continue
        v = meta.get(k)
        vals = v if isinstance(v, list) else [v]
        if not isinstance(cond, dict):
            cond = {'$eq': cond}
        for op, arg in cond.items():
            if op == '$eq' and arg not in vals:
                return False
            if op == '$in' and not set(vals) & set(arg):
                return False
            if op == '$exists' and (k in meta) != arg:
                return False
    return True


class FakeVectors:
    def __init__(self):
        self.data = {}           # key -> {'data': [...], 'metadata': {...}}
        self.calls = []

    def put_vectors(self, vectorBucketName, indexName, vectors):
        assert len(vectors) <= 500
        self.calls.append(('put', len(vectors)))
        for v in vectors:
            self.data[v['key']] = {'data': v['data']['float32'], 'metadata': v['metadata']}

    def get_vectors(self, vectorBucketName, indexName, keys, returnMetadata=False, returnData=False):
        assert len(keys) <= 100 and len(set(keys)) == len(keys), 'no duplicate keys'
        self.calls.append(('get', len(keys)))
        out = []
        for k in keys:
            if k in self.data:
                d = {'key': k}
                if returnMetadata:
                    d['metadata'] = dict(self.data[k]['metadata'])
                if returnData:
                    d['data'] = {'float32': list(self.data[k]['data'])}
                out.append(d)
        return {'vectors': out}

    def delete_vectors(self, vectorBucketName, indexName, keys):
        assert len(keys) <= 500
        self.calls.append(('delete', len(keys)))
        for k in keys:
            self.data.pop(k, None)

    def list_vectors(self, **kw):
        raise AssertionError('the pipeline must not list the whole index')

    def query_vectors(self, vectorBucketName, indexName, topK, queryVector=None, filter=None,
                      returnMetadata=False, returnDistance=False, queryTexts=None, returnScores=False, **kw):
        self.last_query = dict(topK=topK, filter=filter, queryTexts=queryTexts)
        q = queryVector['float32'] if queryVector else None
        hits = []
        for k, v in self.data.items():
            if filter and not _match(v['metadata'], filter):
                continue
            h = {'key': k, 'metadata': v['metadata']}
            if q is not None:
                h['distance'] = 1 - sum(a * b for a, b in zip(q, v['data']))
            if queryTexts:                      # real shape: list of {metadataKey, score}
                bm = [{'metadataKey': t['metadataKey'],
                       'score': 3.0 * v['metadata'].get(t['metadataKey'], '').count(t['text'])} for t in queryTexts]
                h['scores'] = {'fused': 0.0, 'bm25Scores': bm}
                if q is not None:
                    h['scores']['vectorDistance'] = h['distance']
            hits.append(h)
        hits.sort(key=lambda h: (-max([b['score'] for b in h.get('scores', {}).get('bm25Scores', [])], default=0),
                                 h.get('distance', 0)))
        return {'vectors': hits[:topK]}


class FakeTextract:
    class exceptions:
        class UnsupportedDocumentException(Exception):
            pass

        class BadDocumentException(Exception):
            pass

    def __init__(self, lines=('第一行', '第二行'), pages=1, status_seq=('IN_PROGRESS', 'SUCCEEDED')):
        self.lines, self.pages, self.status_seq = list(lines), pages, list(status_seq)
        self.polls = 0

    def detect_document_text(self, Document):
        assert 'S3Object' in Document
        return {'Blocks': [{'BlockType': 'LINE', 'Text': t} for t in self.lines]}

    def start_document_text_detection(self, DocumentLocation):
        assert 'S3Object' in DocumentLocation
        return {'JobId': 'job-1'}

    def get_document_text_detection(self, JobId, NextToken=None):
        if NextToken is None:
            status = self.status_seq[min(self.polls, len(self.status_seq) - 1)]
            self.polls += 1
            if status != 'SUCCEEDED':
                return {'JobStatus': status}
            page = 0
        else:
            page = int(NextToken)
        r = {'JobStatus': 'SUCCEEDED',
             'Blocks': [{'BlockType': 'LINE', 'Text': f'p{page + 1}:{t}'} for t in self.lines]}
        if page + 1 < self.pages:
            r['NextToken'] = str(page + 1)
        return r


class FakeEmbedder:
    """Bag-of-characters vector, normalized: similar text -> small cosine distance."""
    dim = 64

    def embed(self, text):
        v = [0.0] * self.dim
        for ch in text:
            v[hash(ch) % self.dim] += 1
        n = math.sqrt(sum(x * x for x in v)) or 1
        return [x / n for x in v]
