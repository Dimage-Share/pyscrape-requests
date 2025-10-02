"""Refetch and overwrite listing rows containing Unicode replacement char (U+FFFD).

Strategy:
 1. Query listing for rows where any monitored column contains '\ufffd'.
 2. For each site:
    - goo: fetch summary pages progressively until target ids encountered (or use direct URL if stored)
    - carsensor: fetch summary pages via CarSensorClient pages & detail enrichment already handled in core.scrape if needed.
 3. Re-parse and bulk upsert matching records only.

Usage:
  python .\scripts\refresh_replacement_rows.py --site goo --limit 100 --commit
  python .\scripts\refresh_replacement_rows.py --site carsensor --pages 50 --commit

Notes:
 - Direct detail-page refetch isn’t implemented here; we rely on summary parsing where possible.
 - For large counts prefer running a segmented full re-scrape instead.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
import time


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection, bulk_insert_listing, init_db  # type: ignore
from core.logger import Logger


log = Logger.bind(__name__)

MONITOR_COLS = ['manufacturer', 'name', 'mission1', 'mission2', 'bodytype', 'repair', 'location']


def find_replacement_rows(site: str | None, limit: int | None):
    conn = get_connection()
    cur = conn.cursor()
    where_parts = [f"{c} LIKE %s" for c in MONITOR_COLS]
    pattern = '%\ufffd%'
    params = [pattern] * len(MONITOR_COLS)
    where = '(' + ' OR '.join(where_parts) + ')'
    if site:
        where = 'site=%s AND ' + where
        params = [site] + params
    sql = f"SELECT site,id,url FROM listing WHERE {where} LIMIT %s"
    params.append(limit or 1000000)
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def refresh_goonet(target_ids: set[str], pages: int, delay: float):
    from goo_net_scrape.client import GooNetClient
    from goo_net_scrape import parser as gparser
    from goo_net_scrape import models as gmodels
    from core.encoding import decode_response
    refreshed = {}
    with GooNetClient() as client:
        html = client.get_summary_page(params=None)
        cars = gparser.parse_cars(html)
        for c in cars:
            if c.id in target_ids:
                refreshed[c.id] = c
        fetched_pages = 1
        while fetched_pages < pages and target_ids - set(refreshed.keys()):
            time.sleep(delay)
            next_url = gparser.get_next_page_url(html)
            if not next_url:
                break
            resp = client.session.get(next_url, timeout=client.config.timeout)
            html = decode_response(resp)
            cars = gparser.parse_cars(html)
            for c in cars:
                if c.id in target_ids:
                    refreshed[c.id] = c
            fetched_pages += 1
    return list(refreshed.values())


def refresh_carsensor(target_ids: set[str], pages: int, delay: float):
    from core.client import CarSensorClient
    from core.carsensor_parser import parse_cars_carsensor, get_next_page_url_carsensor
    from core.encoding import decode_response
    refreshed = {}
    with CarSensorClient() as client:
        html = client.get_summary_page(params=None)
        cars = parse_cars_carsensor(html)
        for c in cars:
            if c.id in target_ids:
                refreshed[c.id] = c
        fetched_pages = 1
        while fetched_pages < pages and target_ids - set(refreshed.keys()):
            time.sleep(delay)
            next_url = get_next_page_url_carsensor(html, current_url='page')
            if not next_url:
                break
            resp = client.session.get(next_url, timeout=client.config.timeout)
            html = decode_response(resp)
            cars = parse_cars_carsensor(html)
            for c in cars:
                if c.id in target_ids:
                    refreshed[c.id] = c
            fetched_pages += 1
    return list(refreshed.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--site', choices=['goo', 'carsensor'], help='Filter by site')
    ap.add_argument('--limit', type=int, default=500)
    ap.add_argument('--pages', type=int, default=200, help='Max pages to walk while searching for targets')
    ap.add_argument('--delay', type=float, default=1.0)
    ap.add_argument('--commit', action='store_true')
    args = ap.parse_args()
    init_db()
    rows = find_replacement_rows(args.site, args.limit)
    if not rows:
        print('No rows containing replacement character found.')
        return
    targets = {r['id']
               for r in rows}
    print(f"Target rows: {len(targets)} (site={args.site or 'ALL'})")
    if args.site == 'goo':
        refreshed = refresh_goonet(targets, args.pages, args.delay)
    elif args.site == 'carsensor':
        refreshed = refresh_carsensor(targets, args.pages, args.delay)
    else:
        # do both
        refreshed = refresh_goonet({t
                                    for t in targets
                                    if t.startswith('tr_') or t.startswith('td_')}, args.pages, args.delay)
        refreshed += refresh_carsensor({t
                                        for t in targets
                                        if t.startswith('AU')}, args.pages, args.delay)
    print(f"Refetched {len(refreshed)} matching records")
    if args.commit and refreshed:
        inserted = bulk_insert_listing(refreshed, site=args.site or 'goo')  # note: mixed-site runs not recommended
        print(f"Upserted {inserted} rows into listing")
    else:
        print('Dry run (no --commit)')


if __name__ == '__main__':
    main()
