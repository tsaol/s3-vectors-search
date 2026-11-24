"""Who may see an object (FR-9, FR-11).

The demo reads the business fields from the object's user metadata, which the
uploading system sets (x-amz-meta-acl / dept / uploader). In production the permission
data lives in the business system: replace ObjectMetadataAcl with a class that calls
its API, and re-index (or patch metadata) when permissions change.

An object with no acl gets no acl field, so no query filter can match it: deny by default.
"""


class ObjectMetadataAcl:
    def resolve(self, bucket, key, head):
        meta = {k.lower(): v for k, v in head.get('Metadata', {}).items()}
        groups = sorted({g.strip() for g in meta.get('acl', '').split(',') if g.strip()})
        out = {'dept': meta.get('dept', 'unknown'), 'uploader': meta.get('uploader', 'unknown')}
        if groups:
            out['acl'] = groups
        return out
