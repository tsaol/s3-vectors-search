"""The calculator must reproduce the examples on the Amazon S3 pricing page (us-east-1)."""
from cost_estimate import RATES, estimate


def close(a, b, tol=0.02):
    assert abs(a - b) <= tol, (a, b)


def test_pricing_page_example_1():
    # 10M vectors in 40 indexes x 250K, 1024 dims, 1 KB filterable + 1 KB non-filterable,
    # 0.17 KB key, 1M queries/month, top 100 results with metadata, full upload every 6 months
    e = estimate(10_000_000, filterable_kb=1, nonfilterable_kb=1, key_bytes=174, indexes=40,
                 queries=1_000_000, top_k=100, uploads_per_month=1 / 6)
    close(e['storage'], 3.54, 0.02)        # page rounds 58.8 GB up to 59 GB
    close(e['put'], 1.97, 0.02)
    close(e['query_processing'][0], 1.93)
    close(e['query_processing'][1], 1.44)
    assert e['data_returned'] == 0
    close(e['query_api'], 2.50)
    close(e['monthly_total'], 11.38, 0.03)


def test_pricing_page_example_3_hybrid():
    # 10M vectors in one index, 1 KB filterable, 3 KB full-text searchable chunk (non-filterable),
    # 1M hybrid queries; full-text enabled on existing data -> one-time backfill at PUT rate
    e = estimate(10_000_000, filterable_kb=1, text_kb=3, key_bytes=174, indexes=1, queries=1_000_000,
                 top_k=10, return_metadata=False, uploads_per_month=0, text_backfill=True)
    close(e['storage'], 4.67)
    close(e['text_backfill_one_time'], 5.72)
    close(e['query_processing'][0], 1.93)
    close(e['query_processing'][1], 95.34)
    close(e['text_processing'], 3.35)
    close(e['monthly_total'], 107.79, 0.03)


def test_frankfurt_rates_from_price_list():
    r = RATES['eu-central-1']
    # price list: $0.000004185 / $0.000002093 / $0.000000419 per GB
    for per_tb, per_gb in zip(r['processed_tb'], (0.000004185, 0.000002093, 0.000000419)):
        close(per_tb, per_gb * 1024, 0.000002)
    assert r['storage_gb_month'] == 0.064 and r['put_gb'] == 0.214 and r['query_per_million'] == 2.70
