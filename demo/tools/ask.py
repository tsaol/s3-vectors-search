#!/usr/bin/env python3
"""Ask the search agent as a demo user.

    python demo/tools/ask.py --user alice "年假有几天？"
    python demo/tools/ask.py --user bob   "工资几号发？"
"""
import argparse
import json

from common import invoke, login, outputs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('question')
    ap.add_argument('--user', default='alice')
    ap.add_argument('--session', help='reuse a session id for follow-up questions')
    a = ap.parse_args()
    o = outputs()
    status, body = invoke(o, {'question': a.question}, token=login(o, a.user), session=a.session)
    print(f'HTTP {status}')
    if body.get('answer'):
        print(body['answer'])
    for s in body.get('sources', []):
        print(f"  - {s['file']} #{s.get('chunk')}  distance={s.get('distance')}")
    if status != 200:
        print(json.dumps(body, ensure_ascii=False))


if __name__ == '__main__':
    main()
