import io
import json
import zipfile

import pytest

from fakes import FakeEmbedder, FakeS3, FakeTextract, FakeVectors
from s3vsearch import chunk, extract, handler
from s3vsearch.acl import ObjectMetadataAcl
from s3vsearch.config import Config
from s3vsearch.pipeline import ObjectChanged, Pipeline
from s3vsearch.store import VectorStore, doc_id, vkey

B = 'docs'


@pytest.fixture
def env():
    s3, vec, tx = FakeS3(), FakeVectors(), FakeTextract()
    cfg = Config(chunk_chars=50, chunk_overlap=10, vector_bucket='vb', index='idx')
    pipe = Pipeline(cfg, s3, tx, FakeEmbedder(), VectorStore(vec, 'vb', 'idx'), ObjectMetadataAcl())
    return s3, vec, tx, pipe


def keys_of(vec, key):
    d = doc_id(B, key)
    return sorted(k for k in vec.data if k.startswith(d + '#'))


def long_text(paras):
    return '\n\n'.join(f'第{i}段：' + '年假和工资的规定。' * 3 for i in range(paras))


def test_create_indexes_chunks_with_metadata(env):
    s3, vec, _, pipe = env
    s3.put(B, 'hr/leave.md', long_text(4), acl='all-staff, hr', dept='HR', uploader='alice')
    r = pipe.sync(B, 'hr/leave.md')
    assert r['status'] == 'indexed' and r['chunks'] >= 2
    ks = keys_of(vec, 'hr/leave.md')
    assert ks == [vkey(doc_id(B, 'hr/leave.md'), n) for n in range(r['chunks'])]
    m = vec.data[ks[0]]['metadata']
    assert m['acl'] == ['all-staff', 'hr'] and m['dept'] == 'HR' and m['uploader'] == 'alice'
    assert m['file_key'] == 'hr/leave.md' and m['chunk'] == 0 and m['chunks'] == r['chunks']
    assert m['rule_version'] == 'r2' and m['etag'] and isinstance(m['mtime'], int)


def test_duplicate_event_is_noop(env):
    s3, vec, _, pipe = env
    s3.put(B, 'a.txt', 'hello world')
    pipe.sync(B, 'a.txt')
    vec.calls.clear()
    assert pipe.sync(B, 'a.txt')['status'] == 'unchanged'
    assert not any(c[0] in ('put', 'delete') for c in vec.calls)


def test_overwrite_with_fewer_chunks_removes_stale(env):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', long_text(8))
    first = pipe.sync(B, 'a.md')['chunks']
    s3.put(B, 'a.md', '短文本')
    r = pipe.sync(B, 'a.md')
    assert r['chunks'] == 1 and r['stale_deleted'] == first - 1
    assert len(keys_of(vec, 'a.md')) == 1


def test_delete_removes_vectors(env):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', long_text(3))
    pipe.sync(B, 'a.md')
    s3.delete(B, 'a.md')
    r = pipe.sync(B, 'a.md')
    assert r['status'] == 'deleted' and r['vectors'] > 0 and keys_of(vec, 'a.md') == []


def test_late_delete_event_does_not_remove_new_object(env):
    """Delete then re-create, but the delete event arrives last: S3 state wins."""
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', 'v2 内容')
    pipe.sync(B, 'a.md')            # create event
    r = pipe.sync(B, 'a.md')        # stale delete event processed after
    assert r['status'] == 'unchanged' and len(keys_of(vec, 'a.md')) == 1


def test_change_during_indexing_raises_for_retry(env):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', 'old')
    s3.on_get = lambda: (setattr(s3, 'on_get', None), s3.put(B, 'a.md', 'new version'))
    with pytest.raises(ObjectChanged):
        pipe.sync(B, 'a.md')
    assert pipe.sync(B, 'a.md')['status'] == 'indexed'
    m = vec.data[keys_of(vec, 'a.md')[0]]['metadata']
    assert m['chunk_text'] == 'new version'


def test_unsupported_and_empty_are_skipped(env):
    s3, vec, _, pipe = env
    s3.put(B, 'tool.zip', b'PK...')
    r = pipe.sync(B, 'tool.zip')
    assert r['status'] == 'skipped' and 'zip' in r['reason'] and not vec.data
    s3.put(B, 'blank.txt', '   ')
    assert pipe.sync(B, 'blank.txt')['status'] == 'skipped'


def test_no_acl_means_no_acl_field(env):
    s3, vec, _, pipe = env
    s3.put(B, 'secret.txt', 'nobody can see me')
    pipe.sync(B, 'secret.txt')
    assert 'acl' not in vec.data[keys_of(vec, 'secret.txt')[0]]['metadata']


def test_image_uses_textract_on_s3_object(env):
    s3, vec, _, pipe = env
    s3.put(B, 'scan.png', b'\x89PNG', acl='all-staff')
    r = pipe.sync(B, 'scan.png')
    assert r['status'] == 'indexed' and r['source'] == 'ocr'
    assert '第一行' in vec.data[keys_of(vec, 'scan.png')[0]]['metadata']['chunk_text']


def test_pdf_async_ocr_polls_and_pages():
    tx = FakeTextract(lines=['a'], pages=3, status_seq=('IN_PROGRESS', 'IN_PROGRESS', 'SUCCEEDED'))
    got = extract.ocr_pdf(tx, B, 'x.pdf', wait_seconds=60, sleep=lambda s: None)
    assert got.text.split('\n') == ['p1:a', 'p2:a', 'p3:a'] and tx.polls == 3


def test_pdf_ocr_failed_is_skipped():
    tx = FakeTextract(status_seq=('FAILED',))
    assert isinstance(extract.ocr_pdf(tx, B, 'x.pdf', 60, sleep=lambda s: None), extract.Skipped)


def test_docx_extraction():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr('word/document.xml', '<w:document><w:body>'
                   '<w:p><w:r><w:t>第一段</w:t></w:r><w:r><w:t xml:space="preserve"> 续</w:t></w:r></w:p>'
                   '<w:p><w:r><w:t>A &amp; B</w:t></w:r></w:p></w:body></w:document>')
    got = extract.from_bytes('docx', buf.getvalue())
    assert got.text == '第一段 续\n\nA & B'
    assert isinstance(extract.from_bytes('docx', b'not a zip'), extract.Skipped)


def test_many_chunks_delete_in_batches():
    vec = FakeVectors()
    st = VectorStore(vec, 'vb', 'idx')
    st.put([{'key': vkey('d', n), 'data': {'float32': [0.0]}, 'metadata': {}} for n in range(1234)])
    assert st.delete_from('d', 1000) == 234
    assert st.delete_from('d', 0) == 1000 and not vec.data


def test_chunker_paragraphs_and_overlap():
    assert chunk.split('', 10, 2) == []
    parts = chunk.split('aaaa\n\nbbbb\n\ncccc', 10, 0)
    assert parts == ['aaaa\n\nbbbb', 'cccc']
    long = chunk.split('x' * 25, 10, 3)
    assert all(len(p) <= 10 for p in long) and ''.join(p[:7] for p in long).startswith('x' * 21)
    assert all(len(p) <= 50 for p in chunk.split(long_text(5), 50, 10))


class StubPipe:
    def __init__(self, fail_keys=()):
        self.fail, self.seen = set(fail_keys), []

    def sync(self, bucket, key):
        self.seen.append((bucket, key))
        if key in self.fail:
            raise RuntimeError('boom')
        return {'status': 'indexed', 'bucket': bucket, 'key': key}


def s3_msg(key, mid):
    return {'messageId': mid, 'body': json.dumps({'Records': [
        {'eventName': 'ObjectCreated:Put', 's3': {'bucket': {'name': B}, 'object': {'key': key}}}]})}


def test_handler_partial_batch_failure_and_message_kinds(capsys):
    p = StubPipe(fail_keys={'bad.txt'})
    ev = {'Records': [
        s3_msg('hr/%E5%B7%A5%E8%B5%84+%E8%A1%A8.md', 'm1'),
        s3_msg('bad.txt', 'm2'),
        {'messageId': 'm3', 'body': json.dumps({'Event': 's3:TestEvent'})},
        {'messageId': 'm4', 'body': json.dumps({'bucket': B, 'key': 'backfill.md'})},
    ]}
    r = handler.handler(ev, None, pipe=p, sqs=FakeSQS())
    assert r == {'batchItemFailures': [{'itemIdentifier': 'm2'}]}
    assert (B, 'hr/工资 表.md') in p.seen and (B, 'backfill.md') in p.seen
    logs = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert {l['Status'] for l in logs} == {'indexed', 'failed'} and all('_aws' in l for l in logs)


def test_half_written_run_is_redone(env):
    """A run that wrote chunk 0 but failed before the rest must not look 'unchanged'."""
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', long_text(6))
    n = pipe.sync(B, 'a.md')['chunks']
    last = keys_of(vec, 'a.md')[-1]
    del vec.data[last]                                   # last batch lost
    assert pipe.sync(B, 'a.md')['status'] == 'indexed'
    assert len(keys_of(vec, 'a.md')) == n


def test_leftover_stale_chunk_is_redone(env):
    """A run that wrote new chunks but failed before deleting stale ones is redone."""
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', long_text(6))
    pipe.sync(B, 'a.md')
    did = doc_id(B, 'a.md')
    s3.put(B, 'a.md', '短')
    saved = {k: v for k, v in vec.data.items()}
    pipe.sync(B, 'a.md')
    vec.data[vkey(did, 1)] = saved[vkey(did, 1)]         # stale chunk survived
    r = pipe.sync(B, 'a.md')
    assert r['status'] == 'indexed' and keys_of(vec, 'a.md') == [vkey(did, 0)]


def test_acl_change_rewrites_metadata_only(env):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', long_text(4), acl='hr')
    n = pipe.sync(B, 'a.md')['chunks']
    before = {k: list(v['data']) for k, v in vec.data.items()}
    pipe.acl.resolve = lambda b, k, h: {'acl': ['hr', 'all-staff'], 'dept': 'unknown', 'uploader': 'unknown'}
    calls = []
    pipe.embedder.embed = lambda t: calls.append(t)
    r = pipe.sync(B, 'a.md')
    assert r == {'status': 'acl_updated', 'bucket': B, 'key': 'a.md', 'fields': ['acl']}
    assert not calls                                            # no re-embedding
    ks = keys_of(vec, 'a.md')
    assert len(ks) == n and all(vec.data[k]['metadata']['acl'] == ['hr', 'all-staff'] for k in ks)
    assert {k: v['data'] for k, v in vec.data.items()} == before
    assert pipe.sync(B, 'a.md')['status'] == 'unchanged'


def test_acl_removed_drops_field(env):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', 'x', acl='hr')
    pipe.sync(B, 'a.md')
    pipe.acl.resolve = lambda b, k, h: {'dept': 'unknown', 'uploader': 'unknown'}
    assert pipe.sync(B, 'a.md')['status'] == 'acl_updated'
    assert 'acl' not in vec.data[keys_of(vec, 'a.md')[0]]['metadata']


# ---- fault injection: a call that fails half way must never leave vectors behind ----

class Boom(Exception):
    pass


def fail_call(vec, name, nth):
    orig, cnt = getattr(vec, name), [0]

    def f(**kw):
        cnt[0] += 1
        if cnt[0] == nth:
            raise Boom(f'{name} #{nth} throttled')
        return orig(**kw)
    setattr(vec, name, f)
    return lambda: setattr(vec, name, orig)


BIG = '\n\n'.join(f'第{i}段' + '年假规定。' * 6 for i in range(1200))


@pytest.mark.parametrize('nth', [1, 2, 3])
def test_partial_delete_is_finished_on_retry(env, nth):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', BIG, acl='hr')
    n = pipe.sync(B, 'a.md')['chunks']
    assert n > 1000
    s3.delete(B, 'a.md')
    restore = fail_call(vec, 'delete_vectors', nth)
    with pytest.raises(Boom):
        pipe.sync(B, 'a.md')
    restore()
    pipe.sync(B, 'a.md')
    assert keys_of(vec, 'a.md') == []


@pytest.mark.parametrize('nth', [1, 2])
def test_partial_stale_delete_after_shrink_is_finished(env, nth):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', BIG)
    pipe.sync(B, 'a.md')
    s3.put(B, 'a.md', '短文本')
    restore = fail_call(vec, 'delete_vectors', nth)
    with pytest.raises(Boom):
        pipe.sync(B, 'a.md')
    restore()
    assert pipe.sync(B, 'a.md')['status'] == 'indexed'
    assert keys_of(vec, 'a.md') == [vkey(doc_id(B, 'a.md'), 0)]


@pytest.mark.parametrize('nth', [1, 2, 3])
def test_partial_put_is_redone(env, nth):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', 'old')
    pipe.sync(B, 'a.md')
    s3.put(B, 'a.md', BIG)
    restore = fail_call(vec, 'put_vectors', nth)
    with pytest.raises(Boom):
        pipe.sync(B, 'a.md')
    restore()
    r = pipe.sync(B, 'a.md')
    assert r['status'] == 'indexed'
    etag = s3.objects[(B, 'a.md')]['etag']
    assert all(vec.data[k]['metadata']['etag'] == etag for k in keys_of(vec, 'a.md'))
    assert len(keys_of(vec, 'a.md')) == r['chunks']


@pytest.mark.parametrize('nth', [1, 2, 3])
def test_partial_acl_patch_never_leaves_old_acl(env, nth):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', BIG, acl='all-staff,hr')
    n = pipe.sync(B, 'a.md')['chunks']
    s3.objects[(B, 'a.md')]['meta'] = {'acl': 'hr'}
    restore = fail_call(vec, 'put_vectors', nth)
    with pytest.raises(Boom):
        pipe.sync(B, 'a.md')
    restore()
    assert pipe.sync(B, 'a.md')['status'] == 'acl_updated'
    assert all(vec.data[k]['metadata']['acl'] == ['hr'] for k in keys_of(vec, 'a.md'))
    assert len(keys_of(vec, 'a.md')) == n


def test_chunk_zero_is_written_last(env):
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', BIG)
    seen = []
    orig = vec.put_vectors
    vec.put_vectors = lambda **kw: (seen.append([v['key'] for v in kw['vectors']]), orig(**kw))
    pipe.sync(B, 'a.md')
    assert seen[-1][-1].endswith('#00000') and not any(k.endswith('#00000') for b in seen[:-1] for k in b)


FAKE_ACCOUNT = '0' * 12


class FakeSQS:
    def __init__(self):
        self.calls = []

    def change_message_visibility(self, QueueUrl, ReceiptHandle, VisibilityTimeout):
        self.calls.append((QueueUrl, ReceiptHandle, VisibilityTimeout))


class Ctx:
    def __init__(self, ms):
        self.ms = ms

    def get_remaining_time_in_millis(self):
        return self.ms


def sqs_rec(key, mid, receives='1'):
    r = s3_msg(key, mid)
    r.update({'receiptHandle': 'rh-' + mid, 'attributes': {'ApproximateReceiveCount': receives},
              'eventSourceARN': f'arn:aws:sqs:eu-central-1:{FAKE_ACCOUNT}:demo-events'})
    return r


def test_failed_message_retries_with_backoff():
    sqs = FakeSQS()
    r = handler.handler({'Records': [sqs_rec('ok.md', 'm1'), sqs_rec('bad.txt', 'm2', receives='3')]},
                        Ctx(900_000), pipe=StubPipe(fail_keys={'bad.txt'}), sqs=sqs)
    assert r == {'batchItemFailures': [{'itemIdentifier': 'm2'}]}
    assert sqs.calls == [(f'https://sqs.eu-central-1.amazonaws.com/{FAKE_ACCOUNT}/demo-events', 'rh-m2', 120)]


def test_out_of_time_returns_rest_untried():
    sqs, p = FakeSQS(), StubPipe()
    r = handler.handler({'Records': [sqs_rec('a.md', 'm1'), sqs_rec('b.md', 'm2')]}, Ctx(100_000), pipe=p, sqs=sqs)
    assert p.seen == [] and [f['itemIdentifier'] for f in r['batchItemFailures']] == ['m1', 'm2']
    assert [c[2] for c in sqs.calls] == [30, 30]


def test_backoff_caps_at_15_minutes():
    assert [handler.backoff({'attributes': {'ApproximateReceiveCount': str(n)}}) for n in (1, 2, 5, 9)] == [30, 60, 480, 900]


def test_acl_widened_then_reverted_mid_patch_is_repaired(env):
    """Widen the acl, the patch fails half way, the acl is changed back before the retry:
    the chunks already patched must not stay visible to the wider group."""
    s3, vec, _, pipe = env
    s3.put(B, 'a.md', BIG, acl='hr-payroll')
    pipe.sync(B, 'a.md')
    s3.objects[(B, 'a.md')]['meta'] = {'acl': 'all-staff,hr-payroll'}
    restore = fail_call(vec, 'put_vectors', 2)
    with pytest.raises(Boom):
        pipe.sync(B, 'a.md')
    restore()
    s3.objects[(B, 'a.md')]['meta'] = {'acl': 'hr-payroll'}
    assert pipe.sync(B, 'a.md')['status'] == 'acl_updated'
    assert all(vec.data[k]['metadata']['acl'] == ['hr-payroll'] for k in keys_of(vec, 'a.md'))
