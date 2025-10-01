"""Fetch a single Goo-net summary page and dump raw HTML + parsed records to a JSON file.

Usage:
  python .\scripts\dump_goonet_page.py --out goonet_page1.json
  python .\scripts\dump_goonet_page.py --params brand_cd=XXXX sort=1 --out sample.json

By default requests the configured summary URL with no params.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys
from datetime import datetime


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from goo_net_scrape.client import GooNetClient  # type: ignore
from goo_net_scrape import parser as gparser  # type: ignore


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', type=str, default='goonet_page1_dump.json')
    ap.add_argument('--params', nargs='*', help='key=value pairs for query params')
    args = ap.parse_args()
    
    params = None
    if args.params:
        params = {}
        for kv in args.params:
            if '=' in kv:
                k, v = kv.split('=', 1)
                params[k] = v
    
    with GooNetClient() as client:
        html = client.get_summary_page(params=params)
        # Prefer parse_cars (structured) else fallback parse_summary
        records = []
        if hasattr(gparser, 'parse_cars'):
            try:
                recs = gparser.parse_cars(html)
                for r in recs:
                    records.append(r.to_db_row())
            except Exception:
                pass
        if not records:
            parsed = gparser.parse_summary(html)
        else:
            parsed = {
                'items': records,
                'meta': {
                    'records': len(records)
                }
            }
    out_path = Path(args.out)
    payload = {
        'fetched_at': datetime.utcnow().isoformat() + 'Z',
        'params': params,
        'url': 'hidden_in_client_config',
        'html_length': len(html),
        'html_snippet': html[:8000],  # first 8KB for inspection
        'parsed': parsed,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"Wrote {out_path} (records={len(records)})")


if __name__ == '__main__':
    main()
