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
            cur.execute("SELECT site,id,url,manufacturer,name,mission1,mission2,bodytype,repair,location FROM listing WHERE site=%s ORDER BY site,id LIMIT %s", (site, limit or 1000000))
        else:
            cur.execute("SELECT site,id,url,manufacturer,name,mission1,mission2,bodytype,repair,location FROM listing ORDER BY site,id LIMIT %s", (limit or 1000000, ))
        rows = cur.fetchall()
    finally:
        conn.close()
    
    candidates = []
    for r in rows:
        # Build union of checked columns
        cols = ('manufacturer', 'name', 'mission1', 'mission2', 'bodytype', 'repair', 'location')
        garbled_cols = []
        for c in cols:
            v = r.get(c)
            if isinstance(v, str) and REPLACEMENT_RE.search(v):
                garbled_cols.append(c)
        if garbled_cols:
            r['_garbled_cols'] = garbled_cols
            candidates.append(r)
            if limit and len(candidates) >= limit:
                break
    return candidates


def refresh_for_goo(row, client, commit: bool = False, delay: float = 0.5, verbose: bool = False):
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
                if verbose:
                    before = {
                        k: row.get(k)
                        for k in row.get('_garbled_cols', [])
                    }
                    after = {
                        k: getattr(rec, k if k != 'mission2' else 'mission2', None)
                        for k in row.get('_garbled_cols', [])
                    }
                    print(f"DIFF id={row['id']} cols={row.get('_garbled_cols')} BEFORE={before} AFTER={after}")
                if commit:
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
    p.add_argument('--ids', help='comma-separated explicit listing IDs to refresh (overrides candidate scan)')
    p.add_argument('--verbose', action='store_true', help='print before/after diffs for garbled columns')
    p.add_argument('--list', action='store_true', help='only list garbled candidate IDs and exit')
    args = p.parse_args()
    
    if args.ids:
        id_list = [i.strip() for i in args.ids.split(',') if i.strip()]
        # Fetch rows for given ids
        conn = get_connection()
        try:
            cur = conn.cursor()
            # Use IN clause in chunks
            candidates = []
            chunk = 100
            for i in range(0, len(id_list), chunk):
                part = id_list[i:i + chunk]
                placeholders = ','.join(['%s'] * len(part))
                cur.execute(f"SELECT site,id,url,manufacturer,name,mission1,mission2,bodytype,repair,location FROM listing WHERE id IN ({placeholders})", part)
                for r in cur.fetchall():
                    r['_garbled_cols'] = []  # unknown here; will compute quickly
                    candidates.append(r)
        finally:
            conn.close()
    else:
        candidates = find_candidates(site=args.site, limit=args.limit)
    print('Found candidates:', len(candidates))
    if args.list:
        print('Candidate IDs:')
        for r in candidates:
            print(r['id'], ','.join(r.get('_garbled_cols', [])))
        return
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
            ok, res = refresh_for_goo(row, client, commit=args.commit, delay=args.delay, verbose=args.verbose)
            processed += 1
            print(f"[{processed}/{len(candidates)}] id={row['id']} site={row['site']} -> {ok} {res if not ok else ''}")
    finally:
        client.close()


if __name__ == '__main__':
    main()
