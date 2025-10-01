"""
Refresh garbled listings by re-fetching their source pages and updating the listing records.

Usage:
    python scripts\refresh_garbled_listings.py [--site SITE] [--limit N] [--commit] [--delay SECS]

Behavior:
 - Walks `listing` table (optionally filtered by `site`) and selects rows where some columns contain the Unicode replacement character \ufffd or other suspicious patterns.
 - For each candidate, performs HTTP GET using GooNetClient (for site='goo') or a simple fetch for carsensor (if needed).
 - Re-parses the HTML using existing parsers and constructs CarRecord objects.
 - If --commit is passed, writes updated rows back to DB via app.db.mysql.bulk_insert_listing or bulk_upsert_cars.
 - By default runs in dry-run mode and prints summary. Use --confirm or --commit to apply changes.

Note: This script requires network access and may generate many requests; respect robots.txt and rate-limit.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

# ensure project root is on sys.path when running scripts directly
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / 'goo_net_scrape'))
    sys.path.insert(0, str(ROOT / 'app'))
    sys.path.insert(0, str(ROOT / 'core'))
    # keep existing sys.path entries after ours

import re
import re
import time
from typing import List
from pprint import pprint

from core.models import CarRecord
from core.logger import Logger
from core.db import get_connection

# We'll import goo_net_scrape modules lazily inside main() to avoid package-level side-effects
# (goo_net_scrape.__init__ may re-export from core and trigger circular imports)

log = Logger.bind(__name__)

REPLACEMENT_RE = re.compile(r"\ufffd")


def find_candidates(site: str | None = None, limit: int | None = None):
    conn = get_connection()
    try:
        # Use explicit cursor to support both sqlite3 and pymysql Connection objects
        cur = conn.cursor()
        if site:
            cur.execute("SELECT site,id,url,manufacturer,name,mission1,bodytype FROM listing WHERE site=%s ORDER BY site,id LIMIT %s", (site, limit or 1000000))
        else:
            cur.execute("SELECT site,id,url,manufacturer,name,mission1,bodytype FROM listing ORDER BY site,id LIMIT %s", (limit or 1000000, ))
        rows = cur.fetchall()
    finally:
        conn.close()
    
    candidates = []
    for r in rows:
        # check if url present and if suspicious text in fields
        text_blob = ' '.join([str(r.get(k) or '') for k in ('manufacturer', 'name', 'mission1', 'bodytype')])
        if REPLACEMENT_RE.search(text_blob):
            candidates.append(r)
        elif limit and len(candidates) >= limit:
            break
    return candidates


def refresh_for_goo(row, client, commit: bool = False, delay: float = 0.5):
    url = row.get('url')
    if not url:
        return False, 'no url'
    try:
        # Fetch the exact listing URL where possible; client.get_summary_page is meant for summary pages
        # but many listing rows store a direct detail page URL. Use it directly if present.
        if url.startswith('http'):
            # reuse session for headers/timeouts
            resp_html = client.session.get(url, timeout=client.config.timeout).text
        else:
            resp_html = client.get_summary_page(params=None)
        html = resp_html
        # lazy import parser to avoid package import-time side-effects
        from goo_net_scrape.parser import parse_cars
        records = parse_cars(html)
        # try to find matching record by id
        for rec in records:
            if rec.id == row['id']:
                if commit:
                    # write back via bulk_insert_listing with same site
                    from app.db.mysql import bulk_insert_listing
                    bulk_insert_listing([rec], site=row['site'])
                return True, rec
        return False, 'not found in parsed page'
    except Exception as e:
        return False, str(e)
    finally:
        time.sleep(delay)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--site', help='site filter (e.g. goo, carsensor)')
    p.add_argument('--limit', type=int, help='max candidates to process')
    p.add_argument('--commit', action='store_true', help='apply updates to DB')
    p.add_argument('--delay', type=float, default=0.5, help='delay between requests')
    args = p.parse_args()
    
    candidates = find_candidates(site=args.site, limit=args.limit)
    print('Found candidates:', len(candidates))
    if not candidates:
        return
    
    # import client and parser lazily
    try:
        from goo_net_scrape.client import GooNetClient
        from goo_net_scrape.parser import parse_cars
    except Exception as e:
        print('Failed to import goo_net_scrape modules:', e)
        return
    
    client = GooNetClient()
    try:
        processed = 0
        for row in candidates:
            ok, res = refresh_for_goo(row, client, commit=args.commit, delay=args.delay)
            processed += 1
            print(f"[{processed}/{len(candidates)}] id={row['id']} site={row['site']} -> {ok} {res if not ok else ''}")
    finally:
        client.close()


if __name__ == '__main__':
    main()
