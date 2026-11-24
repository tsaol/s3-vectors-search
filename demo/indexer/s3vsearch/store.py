"""Write to S3 Vectors (FR-10).

Vector keys are `<doc_id>#<n>` with n = 0, 1, 2 ... (zero padded). doc_id is a hash of
bucket + object key, so the key is short and fixed length whatever the object key is.

Rule kept by every write and delete: the chunks of one object are always the numbers
0..n-1 with no gap (writes put chunk 0 last, deletes go from the top down). So we never
list the whole index to find an object's vectors, and a call that fails half way leaves
a state the next try can still find and finish:
- re-index: PutVectors overwrites keys 0..m-1, then we delete keys from m upward
  (found with GetVectors, 100 keys per call, until a call finds none);
- delete: the same, starting at 0.
Chunk 0 is written last, so it is the commit marker the "unchanged" check reads.
"""
import hashlib

PUT_BATCH = 500          # PutVectors / DeleteVectors: up to 500 vectors per call
GET_BATCH = 100          # GetVectors: up to 100 keys per call


def doc_id(bucket, key):
    return hashlib.sha256(f'{bucket}/{key}'.encode()).hexdigest()[:32]


def vkey(did, n):
    return f'{did}#{n:05d}'


class VectorStore:
    def __init__(self, client, bucket, index):
        self.c, self.b, self.i = client, bucket, index

    def _get(self, keys, metadata=False):
        r = self.c.get_vectors(vectorBucketName=self.b, indexName=self.i, keys=keys,
                               returnMetadata=metadata, returnData=False)
        return r.get('vectors', [])

    def head(self, did):
        """Metadata of chunk 0, or None if the object has no vectors."""
        got = self._get([vkey(did, 0)], metadata=True)
        return got[0].get('metadata', {}) if got else None

    def complete(self, did, meta):
        """If the last run for this object finished (chunk 0 and the last chunk carry the
        same ETag, and there is no chunk after the last one), return the last chunk's
        metadata; else None. One GetVectors call.

        The last chunk is patched first and chunk 0 last, so comparing the permission
        fields on BOTH of them catches a permission patch that stopped half way."""
        n = int(meta.get('chunks', 0))
        if n < 1:
            return None
        got = {v['key']: v.get('metadata', {}) for v in
               self._get(sorted({vkey(did, 0), vkey(did, n - 1), vkey(did, n)}), metadata=True)}
        last = got.get(vkey(did, n - 1))
        if last is None or last.get('etag') != meta.get('etag') or vkey(did, n) in got:
            return None
        return last

    def patch_metadata(self, did, n, patch):
        """Rewrite the metadata of chunks 0..n-1 (e.g. new acl) without re-embedding:
        read them with their data, then put them back with the patch applied.
        Highest chunks first, chunk 0 last: if a call fails, chunk 0 still has the old
        values, so the next run sees the difference and patches again."""
        for start in reversed(range(0, n, GET_BATCH)):
            keys = [vkey(did, k) for k in range(start, min(n, start + GET_BATCH))]
            r = self.c.get_vectors(vectorBucketName=self.b, indexName=self.i, keys=keys,
                                   returnMetadata=True, returnData=True)
            vecs = []
            for v in r.get('vectors', []):
                meta = {k: val for k, val in v.get('metadata', {}).items() if k not in patch}
                meta.update({k: val for k, val in patch.items() if val is not None})
                vecs.append({'key': v['key'], 'data': v['data'], 'metadata': meta})
            if vecs:
                self.put(vecs)

    def put(self, vectors):
        """Write in batches of 500 with chunk 0 in the LAST batch. Chunk 0 is the commit
        marker: its ETag / acl only change once everything else is written."""
        zero = [v for v in vectors if v['key'].endswith('#00000')]
        ordered = [v for v in vectors if not v['key'].endswith('#00000')] + zero
        for n in range(0, len(ordered), PUT_BATCH):
            self.c.put_vectors(vectorBucketName=self.b, indexName=self.i, vectors=ordered[n:n + PUT_BATCH])

    def delete_from(self, did, start=0):
        """Delete chunks start, start+1, ... of one object. Returns how many were deleted.

        First find them (GetVectors, 100 keys per call, until a call finds none), then
        delete from the HIGHEST number down. So if a call fails, what is left is still a
        contiguous run start..k, and the next try finds all of it again."""
        found, n = [], start
        while True:
            keys = [vkey(did, k) for k in range(n, n + GET_BATCH)]
            got = [v['key'] for v in self._get(keys)]
            if not got:
                break
            found += got
            n += GET_BATCH
        found.sort(reverse=True)
        for m in range(0, len(found), PUT_BATCH):
            self.c.delete_vectors(vectorBucketName=self.b, indexName=self.i, keys=found[m:m + PUT_BATCH])
        return len(found)
