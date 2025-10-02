"""Repair listing rows from per-page dumps.

Reads JSON dumps produced by dump_page and reparses the HTML (or uses stored records)
then upserts corrected records into listing (when --commit is used).

Usage:
  python .\scripts\repair_from_dumps.py --dump-dir dumps --limit 100 --commit

This tool is conservative: default is dry-run. Use --commit to perform DB upserts.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import json
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.encoding import decode_response
from app.db import bulk_insert_listing, init_db  # type: ignore
from goo_net_scrape import parser as gparser  # type: ignore


def process_dump(path: Path):
    try:
        j = json.loads(path.read_text(encoding='utf-8'))
    except Exception as e:
        print(f"skip {path}: json load fail {e}")
        return []
    site = j.get('site')
    if site != 'goo':
        return []
    # prefer chosen_encoding if present
    chosen = j.get('chosen_encoding') or j.get('meta', {}).get('chosen_encoding')
    html = j.get('html_snippet', '')
    # if full html file present, prefer it
    html_path = path.with_suffix('.html')
    if html_path.exists():
        try:
            # files were saved with utf-8; if chosen_encoding is present we can read bytes and re-decode
            raw = html_path.read_bytes()
            if chosen:
                try:
                    html_full = raw.decode(chosen, errors='strict')
                except Exception:
                    html_full = raw.decode('utf-8', errors='replace')
            else:
                # try decode heuristically
                class FakeResp:
                    
                    def __init__(self, content):
                        self._content = content
                        self.headers = {}
                        self.url = str(path)
                    
                    @property
                    def content(self):
                        return self._content
                
                fake = FakeResp(raw)
                html_full = decode_response(fake)
            html = html_full
        except Exception:
            # fallback to snippet
            pass
    # parse cars
    try:
        records = gparser.parse_cars(html)
    except Exception:
        records = []
    # normalize to db-ready list (CarRecord objects or dicts)
    out = []
    for r in records or []:
        out.append(r)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dump-dir', type=str, default='dumps', help='Directory with dumps')
    ap.add_argument('--limit', type=int, default=1000)
    ap.add_argument('--commit', action='store_true')
    args = ap.parse_args()
    dump_dir = Path(args.dump_dir)
    if not dump_dir.exists():
        print('dump dir not found')
        return
    init_db()
    files = sorted(list(dump_dir.glob('goo_page*.json')))
    total = 0
    to_upsert = []
    for p in files[:args.limit]:
        recs = process_dump(p)
        if recs:
            print(f"{p.name}: parsed {len(recs)} records")
            total += len(recs)
            to_upsert.extend(recs)
    print(f"Total parsed records: {total}")
    if args.commit and to_upsert:
        inserted = bulk_insert_listing(to_upsert, site='goo')
        print(f"Inserted/upserted {inserted} records into listing")
    else:
        print('Dry run (no --commit)')


if __name__ == '__main__':
    main()
