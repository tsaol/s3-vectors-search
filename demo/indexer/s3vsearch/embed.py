"""Embedding (FR-8): one interface, embed(text) -> list[float]. To change model or use
your own model, add or pick another class and set EMBEDDER / MODEL_ID / DIM."""
import json


class BedrockTitanText:
    """Titan Text Embeddings V2: 256 / 512 / 1024 dimensions, multilingual (incl. Chinese)."""
    def __init__(self, client, model_id, dim):
        self.c, self.model_id, self.dim = client, model_id, dim

    def embed(self, text):
        body = json.dumps({'inputText': text, 'dimensions': self.dim, 'normalize': True})
        return json.loads(self.c.invoke_model(modelId=self.model_id, body=body)['body'].read())['embedding']


class BedrockCohere:
    """Cohere Embed (multilingual). input_type differs for documents and queries."""
    def __init__(self, client, model_id, dim, input_type='search_document'):
        self.c, self.model_id, self.dim, self.input_type = client, model_id, dim, input_type

    def embed(self, text):
        body = json.dumps({'texts': [text], 'input_type': self.input_type,
                           'output_dimension': self.dim, 'embedding_types': ['float']})
        e = json.loads(self.c.invoke_model(modelId=self.model_id, body=body)['body'].read())['embeddings']
        return (e['float'] if isinstance(e, dict) else e)[0]


class SageMakerEndpoint:
    """Your own model on a SageMaker endpoint: input {"inputs": text}, output a vector."""
    def __init__(self, client, model_id, dim):
        self.c, self.endpoint, self.dim = client, model_id, dim

    def embed(self, text):
        r = self.c.invoke_endpoint(EndpointName=self.endpoint, ContentType='application/json',
                                   Body=json.dumps({'inputs': text}))
        v = json.loads(r['Body'].read())
        return v[0] if v and isinstance(v[0], list) else v


def build(cfg, session):
    if cfg.embedder == 'bedrock-titan-text':
        return BedrockTitanText(session.client('bedrock-runtime'), cfg.model_id, cfg.dim)
    if cfg.embedder == 'bedrock-cohere':
        return BedrockCohere(session.client('bedrock-runtime'), cfg.model_id, cfg.dim)
    if cfg.embedder == 'sagemaker':
        return SageMakerEndpoint(session.client('sagemaker-runtime'), cfg.model_id, cfg.dim)
    raise ValueError(f'unknown EMBEDDER {cfg.embedder}')
