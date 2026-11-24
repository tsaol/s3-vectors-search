"""One object -> vectors. The event only says "look at this key"; what S3 holds now is
the truth. So the same code handles create, overwrite, delete, retries, duplicates,
events that arrive out of order, and backfill (FR-1, FR-2, FR-3):

    object gone        -> delete its vectors
    same ETag + rules  -> nothing to do (duplicate event), if the last run finished;
                          if only the permissions changed, rewrite metadata only
    otherwise          -> extract, chunk, embed, overwrite chunks 0..m-1, delete chunks >= m

After writing we HEAD the object again. If it changed while we worked, we raise so the
queue retries; the retry indexes the new version.
"""
from concurrent.futures import ThreadPoolExecutor

from botocore.exceptions import ClientError

from . import chunk, extract
from .store import doc_id, vkey

NOT_FOUND = ('404', 'NoSuchKey', 'NotFound')
ACCESS_FIELDS = ('acl', 'dept', 'uploader')        # may change without the object changing


class ObjectChanged(Exception):
    pass


class Pipeline:
    def __init__(self, cfg, s3, textract, embedder, store, acl):
        self.cfg, self.s3, self.textract, self.embedder, self.store, self.acl = cfg, s3, textract, embedder, store, acl

    def _head(self, bucket, key):
        try:
            return self.s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in NOT_FOUND:
                return None
            raise

    def _result(self, status, bucket, key, **kw):
        return {'status': status, 'bucket': bucket, 'key': key, **kw}

    def sync(self, bucket, key):
        did = doc_id(bucket, key)
        head = self._head(bucket, key)
        if head is None:
            return self._result('deleted', bucket, key, vectors=self.store.delete_from(did, 0))

        etag = head['ETag'].strip('"')
        access = self.acl.resolve(bucket, key, head)
        cur = self.store.head(did)
        last = None
        if cur and cur.get('etag') == etag and cur.get('rule_version') == self.cfg.rule_version \
                and cur.get('model') == self.cfg.model_id:
            last = self.store.complete(did, cur)
        if last is not None:
            patch = {k: access.get(k) for k in ACCESS_FIELDS
                     if cur.get(k) != access.get(k) or last.get(k) != access.get(k)}
            if not patch:
                return self._result('unchanged', bucket, key)
            # only the permissions changed: rewrite metadata, no new extraction or embedding
            self.store.patch_metadata(did, int(cur['chunks']), patch)
            return self._result('acl_updated', bucket, key, fields=sorted(patch))

        got = self._extract(bucket, key, head, etag)
        if isinstance(got, extract.Skipped):
            removed = self.store.delete_from(did, 0)        # e.g. a file that became unreadable
            return self._result('skipped', bucket, key, reason=got.reason, removed=removed)

        pieces = chunk.split(got.text, self.cfg.chunk_chars, self.cfg.chunk_overlap)
        if not pieces:
            removed = self.store.delete_from(did, 0)
            return self._result('skipped', bucket, key, reason='没有抽取到文字', removed=removed)

        with ThreadPoolExecutor(max_workers=self.cfg.embed_workers) as ex:
            embeddings = list(ex.map(self.embedder.embed, pieces))

        base = {
            'doc_id': did, 'file_key': key, 'etag': etag,
            'kind': extract.kind_of(key), 'source': got.source,
            'mtime': int(head['LastModified'].timestamp()), 'size': int(head.get('ContentLength', 0)),
            'chunks': len(pieces), 'model': self.cfg.model_id, 'rule_version': self.cfg.rule_version,
            **access,
        }
        vectors = [{'key': vkey(did, n), 'data': {'float32': emb},
                    'metadata': {**base, 'chunk': n, 'chunk_text': piece}}
                   for n, (piece, emb) in enumerate(zip(pieces, embeddings))]
        self.store.put(vectors)
        stale = self.store.delete_from(did, len(vectors))

        after = self._head(bucket, key)
        if after is None or after['ETag'].strip('"') != etag:
            raise ObjectChanged(f'{key} changed while indexing; retry')
        return self._result('indexed', bucket, key, chunks=len(vectors), stale_deleted=stale, source=got.source)

    def _extract(self, bucket, key, head, etag):
        kind = extract.kind_of(key)
        if kind is None:
            return extract.Skipped(f'不支持的文件类型：{key.rsplit(".", 1)[-1] if "." in key else "(无扩展名)"}')
        size = int(head.get('ContentLength', 0))
        if extract.needs_body(kind):
            if size > self.cfg.max_text_bytes:
                return extract.Skipped(f'文件太大（{size} 字节）')
            try:
                body = self.s3.get_object(Bucket=bucket, Key=key, IfMatch=etag)['Body'].read()
            except ClientError as e:
                if e.response.get('Error', {}).get('Code') in ('PreconditionFailed',) + NOT_FOUND:
                    raise ObjectChanged(f'{key} changed before read; retry') from e
                raise
            return extract.from_bytes(kind, body)
        if kind == 'image':
            return extract.ocr_image(self.textract, bucket, key, size)
        return extract.ocr_pdf(self.textract, bucket, key, self.cfg.ocr_wait_seconds)
