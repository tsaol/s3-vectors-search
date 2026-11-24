#!/usr/bin/env python3
"""Monthly cost estimate for S3 Vectors, following the method of the examples on the
Amazon S3 pricing page. Rates are from the AWS Price List API (publication 2026-11-21);
check https://aws.amazon.com/s3/pricing/ before you quote numbers.

    python tools/cost_estimate.py --vectors 20000000 --queries 1000000 --region eu-central-1 \
        --filterable-kb 1 --text-kb 1.8 --indexes 1

Sizes follow the pricing page: 1 dimension = 4 bytes, key = 1 byte per character,
1 KB = 1024 bytes, 1 GB = 1024^3 bytes, 1 TB = 1024^4 bytes.
"""
import argparse
import json

KB, GB, TB = 1024, 1024 ** 3, 1024 ** 4

RATES = {   # USD
    'us-east-1': {'storage_gb_month': 0.06, 'put_gb': 0.20, 'query_per_million': 2.50,
                  'processed_tb': (0.004, 0.002, 0.0004), 'text_processed_tb': 0.00012, 'returned_gb': 0.01},
    'eu-central-1': {'storage_gb_month': 0.064, 'put_gb': 0.214, 'query_per_million': 2.70,
                     'processed_tb': (0.004285, 0.002143, 0.000429), 'text_processed_tb': 0.00012, 'returned_gb': 0.01},
}
TIER_EDGES = (100_000, 10_000_000)       # per index: first 100K, 100K-10M, above 10M vectors
FREE_RETURNED_PER_QUERY = 512 * KB       # pricing page footnote (its examples use 500 KB)
MIN_RESULT_BYTES = 256


def estimate(vectors, dim=1024, filterable_kb=1.0, nonfilterable_kb=0.0, text_kb=0.0, text_filterable=False,
             key_bytes=174, indexes=1, queries=1_000_000, top_k=10, return_metadata=True,
             uploads_per_month=1.0, text_backfill=False, region='us-east-1'):
    """text_kb: size of the full-text searchable field (stored non-filterable unless text_filterable)."""
    r = RATES[region]
    vec_b, key_b = 4 * dim, key_bytes
    filt_b, nonf_b, text_b = filterable_kb * KB, nonfilterable_kb * KB, text_kb * KB
    stored = vec_b + key_b + filt_b + nonf_b + text_b * (2 if text_filterable else 1)
    storage_gb = vectors * stored / GB
    out = {'storage_gb': round(storage_gb, 2), 'storage': storage_gb * r['storage_gb_month'],
           'put': uploads_per_month * storage_gb * r['put_gb'],
           'text_backfill_one_time': (vectors * text_b / GB * r['put_gb']) if text_backfill else 0.0}

    per_index = vectors / indexes
    processed_b = vec_b + key_b + filt_b + (text_b if text_filterable else 0)
    t1, t2 = TIER_EDGES
    tiers = (min(per_index, t1), max(0, min(per_index, t2) - t1), max(0, per_index - t2))
    out['query_processing'] = [n * processed_b / TB * rate * queries for n, rate in zip(tiers, r['processed_tb'])]
    out['text_processing'] = per_index * text_b / TB * r['text_processed_tb'] * queries if text_kb else 0.0

    result_b = max(MIN_RESULT_BYTES, key_b + ((filt_b + nonf_b + text_b) if return_metadata else 0))
    over = max(0, top_k * result_b - FREE_RETURNED_PER_QUERY)
    out['data_returned'] = over * queries / GB * r['returned_gb']
    out['query_api'] = queries / 1e6 * r['query_per_million']
    out['query_total'] = sum(out['query_processing']) + out['text_processing'] + out['data_returned'] + out['query_api']
    out['monthly_total'] = out['storage'] + out['put'] + out['query_total']
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--vectors', type=int, required=True)
    ap.add_argument('--dim', type=int, default=1024)
    ap.add_argument('--filterable-kb', type=float, default=1.0)
    ap.add_argument('--nonfilterable-kb', type=float, default=0.0)
    ap.add_argument('--text-kb', type=float, default=0.0, help='full-text searchable field size')
    ap.add_argument('--key-bytes', type=int, default=174)
    ap.add_argument('--indexes', type=int, default=1, help='vectors are split evenly over this many indexes')
    ap.add_argument('--queries', type=int, default=1_000_000, help='per month')
    ap.add_argument('--top-k', type=int, default=10)
    ap.add_argument('--uploads-per-month', type=float, default=0.0,
                    help='full re-uploads per month (0.1667 = once every 6 months)')
    ap.add_argument('--region', choices=sorted(RATES), default='us-east-1')
    a = ap.parse_args()
    out = estimate(a.vectors, a.dim, a.filterable_kb, a.nonfilterable_kb, a.text_kb, False, a.key_bytes,
                   a.indexes, a.queries, a.top_k, True, a.uploads_per_month, False, a.region)
    print(json.dumps({k: (round(v, 2) if isinstance(v, float) else [round(x, 2) for x in v] if isinstance(v, list) else v)
                      for k, v in out.items()}, indent=1))


if __name__ == '__main__':
    main()
