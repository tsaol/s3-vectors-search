"""All settings come from environment variables, so changing the model, the chunk rule
or the vector index means changing config, not code (NFR-7)."""
import os
from dataclasses import dataclass


def _int(name, default):
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Config:
    # embedding (FR-8)
    embedder: str = os.environ.get('EMBEDDER', 'bedrock-titan-text')      # bedrock-titan-text | bedrock-cohere | sagemaker
    model_id: str = os.environ.get('MODEL_ID', 'amazon.titan-embed-text-v2:0')
    dim: int = _int('DIM', 1024)
    embed_workers: int = _int('EMBED_WORKERS', 4)
    # chunking (FR-7)
    chunk_chars: int = _int('CHUNK_CHARS', 600)
    chunk_overlap: int = _int('CHUNK_OVERLAP', 80)
    # extraction (FR-4)
    max_text_bytes: int = _int('MAX_TEXT_BYTES', 5 * 1024 * 1024)       # bigger text files are skipped
    ocr_wait_seconds: int = _int('OCR_WAIT_SECONDS', 600)
    # S3 Vectors (FR-10)
    vector_bucket: str = os.environ.get('VECTOR_BUCKET', '')
    index: str = os.environ.get('INDEX', '')
    # every vector records the rule version, so a full rebuild can find old vectors (NFR-6)
    rule_version: str = os.environ.get('RULE_VERSION', 'r2')
