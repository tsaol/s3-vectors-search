"""Chunking (FR-7): split by paragraph first, then pack paragraphs up to `size` characters
with `overlap` characters carried over. A paragraph longer than `size` is cut by length.
To use another rule (by heading, by sentence), replace `split` and bump RULE_VERSION."""
import re


def _hard_cut(text, size, overlap):
    step = max(size - overlap, 1)
    return [text[i:i + size] for i in range(0, len(text), step) if text[i:i + size].strip()]


def split(text, size, overlap):
    text = (text or '').strip()
    if not text:
        return []
    paras = [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip()]
    out, cur = [], ''
    for p in paras:
        if len(p) > size:
            if cur:
                out.append(cur)
                cur = ''
            out += _hard_cut(p, size, overlap)
            continue
        if cur and len(cur) + 2 + len(p) > size:
            out.append(cur)
            tail = cur[-overlap:] if overlap else ''
            cur = (tail + '\n\n' + p) if tail and len(tail) + 2 + len(p) <= size else p
        else:
            cur = (cur + '\n\n' + p) if cur else p
    if cur:
        out.append(cur)
    return out
