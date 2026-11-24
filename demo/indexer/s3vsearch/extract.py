"""Content extraction (FR-4).

- Plain text, Markdown, CSV, JSON, HTML: read directly.
- docx: read the XML inside the zip with the standard library.
- Images (PNG/JPEG/TIFF): Textract DetectDocumentText, pointed at the S3 object, so the
  object is read inside the Region and is not downloaded by us (NFR-3).
- PDF (any page count): Textract async StartDocumentTextDetection on the S3 object.

Textract text detection supports Chinese (Simplified and Traditional).

Return value: Extracted(text, source) or Skipped(reason). A file we can't read is
recorded with its reason and does not block other files. Errors that may go away on
retry (throttling, timeouts) are raised so the queue retries them.
"""
import html
import io
import re
import time
import zipfile
from dataclasses import dataclass

TEXT_EXT = ('.txt', '.md', '.csv', '.json', '.log')
HTML_EXT = ('.html', '.htm')
DOCX_EXT = ('.docx',)
IMAGE_EXT = ('.png', '.jpg', '.jpeg', '.tif', '.tiff')
PDF_EXT = ('.pdf',)
IMAGE_SYNC_LIMIT = 10 * 1024 * 1024          # Textract sync limit for images


@dataclass
class Extracted:
    text: str
    source: str           # text | html | docx | ocr


@dataclass
class Skipped:
    reason: str


def kind_of(key):
    k = key.lower()
    for kind, exts in (('text', TEXT_EXT), ('html', HTML_EXT), ('docx', DOCX_EXT),
                       ('image', IMAGE_EXT), ('pdf', PDF_EXT)):
        if k.endswith(exts):
            return kind
    return None


def needs_body(kind):
    return kind in ('text', 'html', 'docx')


def from_bytes(kind, body):
    if kind == 'text':
        return Extracted(body.decode('utf-8', errors='replace'), 'text')
    if kind == 'html':
        t = re.sub(r'(?is)<(script|style).*?</\1>', ' ', body.decode('utf-8', errors='replace'))
        t = html.unescape(re.sub(r'<[^>]+>', ' ', t))
        return Extracted(re.sub(r'[ \t]+', ' ', t), 'html')
    if kind == 'docx':
        try:
            xml = zipfile.ZipFile(io.BytesIO(body)).read('word/document.xml').decode('utf-8')
        except (zipfile.BadZipFile, KeyError) as e:
            return Skipped(f'docx 无法解析：{e}')
        paras = []
        for p in re.findall(r'<w:p[ >].*?</w:p>', xml, re.S):
            t = ''.join(re.findall(r'<w:t[^>]*>([^<]*)</w:t>', p))
            if t.strip():
                paras.append(html.unescape(t))
        return Extracted('\n\n'.join(paras), 'docx')
    raise ValueError(kind)


def _lines(blocks):
    return [b['Text'] for b in blocks if b.get('BlockType') == 'LINE']


def ocr_image(textract, bucket, key, size):
    if size > IMAGE_SYNC_LIMIT:
        return Skipped('图片超过 10 MB，Textract 无法识别')
    try:
        r = textract.detect_document_text(Document={'S3Object': {'Bucket': bucket, 'Name': key}})
    except textract.exceptions.UnsupportedDocumentException:
        return Skipped('Textract 不支持这个图片格式')
    except textract.exceptions.BadDocumentException:
        return Skipped('图片损坏或无法读取')
    return Extracted('\n'.join(_lines(r['Blocks'])), 'ocr')


def ocr_pdf(textract, bucket, key, wait_seconds, sleep=time.sleep):
    job = textract.start_document_text_detection(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}})['JobId']
    deadline = time.monotonic() + wait_seconds
    delay = 2
    while True:
        r = textract.get_document_text_detection(JobId=job)
        status = r['JobStatus']
        if status == 'SUCCEEDED' or status == 'PARTIAL_SUCCESS':
            break
        if status == 'FAILED':
            return Skipped(f"Textract 识别失败：{r.get('StatusMessage', '')}")
        if time.monotonic() > deadline:
            raise TimeoutError(f'Textract job {job} still running')   # retried by the queue
        sleep(delay)
        delay = min(delay * 2, 15)
    lines = _lines(r['Blocks'])
    token = r.get('NextToken')
    while token:
        r = textract.get_document_text_detection(JobId=job, NextToken=token)
        lines += _lines(r['Blocks'])
        token = r.get('NextToken')
    return Extracted('\n'.join(lines), 'ocr')
