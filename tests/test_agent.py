import base64
import json

import pytest

import main as agent
from fakes import FakeEmbedder, FakeS3, FakeTextract, FakeVectors
from s3vsearch.acl import ObjectMetadataAcl
from s3vsearch.config import Config
from s3vsearch.pipeline import Pipeline
from s3vsearch.store import VectorStore


def token(claims):
    enc = lambda d: base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip('=')
    return 'Bearer ' + enc({'alg': 'RS256'}) + '.' + enc(claims) + '.sig'


@pytest.fixture
def indexed(monkeypatch):
    s3, vec = FakeS3(), FakeVectors()
    pipe = Pipeline(Config(chunk_chars=200, chunk_overlap=0), s3, FakeTextract(), FakeEmbedder(),
                    VectorStore(vec, 'vb', 'idx'), ObjectMetadataAcl())
    s3.put('docs', 'hr/payroll.md', '工资在次月 10 日发放。', acl='hr-payroll')
    s3.put('docs', 'hr/leave.md', '年假 12 天，最多 15 天。', acl='all-staff')
    for k in ('hr/payroll.md', 'hr/leave.md'):
        pipe.sync('docs', k)
    emb = FakeEmbedder()
    monkeypatch.setattr(agent, 'clients', lambda: (None, vec))
    monkeypatch.setattr(agent, 'embed', emb.embed)
    monkeypatch.setattr(agent, 'ANSWER_MODEL', '')
    monkeypatch.setattr(agent, 'MAX_DISTANCE', 2.0)
    return vec


def test_groups_come_from_token_not_payload(indexed):
    r = agent.handle({'question': '工资几号发', 'groups': ['hr-payroll']},
                     token({'sub': 'u1', 'cognito:groups': ['all-staff']}), 's1')
    assert indexed.last_query['filter'] == {'acl': {'$in': ['all-staff']}}
    assert [s['file'] for s in r['sources']] == ['hr/leave.md']


def test_hr_payroll_group_sees_payroll(indexed):
    r = agent.handle({'question': '工资几号发'}, token({'sub': 'u2', 'cognito:groups': ['hr-payroll']}), 's2')
    assert [s['file'] for s in r['sources']] == ['hr/payroll.md']


def test_hybrid_query_sends_text(indexed, monkeypatch):
    monkeypatch.setattr(agent, 'SEARCH_MODE', 'hybrid')
    agent.handle({'question': '年假'}, token({'cognito:groups': ['all-staff']}), 's3')
    assert indexed.last_query['queryTexts'] == [{'text': '年假', 'metadataKey': 'chunk_text'}]


def test_auth_errors():
    with pytest.raises(agent.AuthError):
        agent.groups_from_auth(None)
    with pytest.raises(agent.AuthError):
        agent.groups_from_auth('Bearer abc')
    with pytest.raises(agent.AuthError):
        agent.groups_from_auth(token({'cognito:groups': [f'g{i}' for i in range(101)]}))
    assert agent.groups_from_auth(token({'sub': 'x'})) == ('x', [])


def test_no_groups_no_search(indexed):
    r = agent.handle({'question': '工资'}, token({'sub': 'x'}), 's4')
    assert r['sources'] == [] and '没有任何权限组' in r['answer']


def test_relevance_uses_real_score_shape():
    bm = lambda x: {'bm25Scores': [{'metadataKey': 'chunk_text', 'score': x}]}
    assert agent.relevant({'distance': 0.2})
    assert not agent.relevant({'distance': 0.9})
    assert agent.relevant({'distance': 0.9, 'scores': {**bm(1.1), 'fused': 0.03, 'vectorDistance': 0.9}}, best_bm25=1.1)
    assert not agent.relevant({'distance': 0.9, 'scores': bm(0.4)}, best_bm25=1.1)   # weak compared with the best
    assert not agent.relevant({'scores': {'fused': 0.01, 'bm25Scores': []}})
    assert agent.bm25({'scores': bm(2.5)}) == 2.5 and agent.bm25({}) == 0


def test_hybrid_end_to_end_exact_code(indexed, monkeypatch):
    """A word-only match (far vector) is kept in hybrid mode; real-shape scores don't crash."""
    monkeypatch.setattr(agent, 'SEARCH_MODE', 'hybrid')
    monkeypatch.setattr(agent, 'MAX_DISTANCE', 0.0)
    r = agent.handle({'question': '12 天'}, token({'cognito:groups': ['all-staff']}), 's5')
    assert [s['file'] for s in r['sources']] == ['hr/leave.md']


def test_sessions_reset_when_groups_change():
    s = agent.Sessions(cap=2)
    s.put('a', ('u', ('hr',)), [1, 2])
    assert s.get('a', ('u', ('hr',))) == [1, 2]
    assert s.get('a', ('u', ('all-staff',))) == []
    s.put('b', 'w', [3]); s.put('c', 'w', [4])
    assert s.get('a', ('u', ('hr',))) == []          # evicted, cap = 2


def test_http_401_without_token(monkeypatch):
    import threading, urllib.request, urllib.error
    from http.server import ThreadingHTTPServer
    srv = ThreadingHTTPServer(('127.0.0.1', 0), agent.H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        url = f'http://127.0.0.1:{srv.server_address[1]}'
        assert json.loads(urllib.request.urlopen(url + '/ping').read()) == {'status': 'Healthy'}
        req = urllib.request.Request(url + '/invocations', data=b'{"question":"x","groups":["hr"]}', method='POST')
        with pytest.raises(urllib.error.HTTPError) as e:
            urllib.request.urlopen(req)
        assert e.value.code == 401
    finally:
        srv.shutdown()
