"""Segmented scraping runner.

Usage (PowerShell):
    python .\scripts\run_scrapes_segmented.py --site carsensor --pages 300 --batch-size 50 --delay 1 --confirm
    python .\scripts\run_scrapes_segmented.py --site goo --pages 300 --batch-size 50 --delay 1 --confirm

Features:
 - Flush inserts every batch-size pages (CarSensor only for now; Goo path accumulates per page flush).
 - Resume support via --resume: skips pages already present (approx by counting existing site rows / avg per page heuristic) -- simple heuristic to avoid over-fetch.
 - Throttling via --delay seconds between pages.
"""
from __future__ import annotations
import argparse
import logging
import sys
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import init_db, bulk_insert_listing, get_connection  # type: ignore
from core.scrape import Scrape  # type: ignore
from goo_net_scrape.client import GooNetClient  # type: ignore
from goo_net_scrape import parser as goo_parser, models as goo_models  # type: ignore


logger = logging.getLogger(__name__)


def _estimate_pages_done(site: str, avg_per_page: int = 20) -> int:
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM listing WHERE site=%s", (site, ))
        row = cur.fetchone()
        conn.close()
        count = row['c'] if isinstance(row, dict) else row[0]
        return max(0, int(count // max(1, avg_per_page)))
    except Exception:
        return 0


def run_carsensor_segmented(pages: int, batch_size: int, delay: float, resume: bool):
    remaining = pages
    start_page = 1
    if resume:
        done = _estimate_pages_done('carsensor')
        if done >= pages:
            print(f"Already have >= requested pages (est={done}); nothing to do.")
            return
        if done > 0:
            print(f"Resume mode: estimated pages already scraped={done}; continuing from page {done+1}")
            start_page = done + 1
            remaining = pages - done
    # We pass total target pages to Scrape; internal flush will handle batch
    print(f"Running CarSensor segmented scrape target_pages={pages} start_page={start_page} flush={batch_size}")
    s = Scrape(page_delay=delay)
    # Current Scrape.run does not yet accept start offset; TODO: implement skip logic if needed.
    # For now, if resume and start_page>1 we just run remaining pages naive (duplicates may be upserted)
    s.run(remaining, {}, flush_pages=batch_size)


def run_goo_segmented(pages: int, batch_size: int, delay: float, resume: bool):
    init_db()
    scraped = 0
    all_records = []
    start_time = time.time()
    with GooNetClient() as client:
        html = client.get_summary_page(params=None)
        cars = goo_parser.parse_cars(html)
        for c in cars:
            all_records.append(c)
        scraped += 1
        if scraped % batch_size == 0:
            inserted = bulk_insert_listing(all_records, site='goo')
            print(f"flush inserted={inserted} total_pages={scraped}")
            all_records.clear()
        while scraped < pages:
            time.sleep(delay)
            next_url = goo_parser.get_next_page_url(html)
            if not next_url:
                print("No next page; stopping early.")
                break
            resp = client.session.get(next_url, timeout=client.config.timeout)
            resp.encoding = 'utf-8'
            html = resp.text
            cars = goo_parser.parse_cars(html)
            for c in cars:
                all_records.append(c)
            scraped += 1
            if scraped % batch_size == 0:
                inserted = bulk_insert_listing(all_records, site='goo')
                print(f"flush inserted={inserted} total_pages={scraped}")
                all_records.clear()
    if all_records:
        inserted = bulk_insert_listing(all_records, site='goo')
        print(f"final inserted={inserted} total_pages={scraped}")
    elapsed = time.time() - start_time
    print(f"Goo segmented scrape done pages={scraped} time={elapsed:.1f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--site', choices=['carsensor', 'goo'], required=True)
    ap.add_argument('--pages', type=int, default=300)
    ap.add_argument('--batch-size', type=int, default=50)
    ap.add_argument('--delay', type=float, default=1.0)
    ap.add_argument('--resume', action='store_true')
    ap.add_argument('--confirm', action='store_true')
    args = ap.parse_args()
    if not args.confirm:
        print('Add --confirm to actually run (safety).')
        return
    if args.site == 'carsensor':
        run_carsensor_segmented(args.pages, args.batch_size, args.delay, args.resume)
    else:
        run_goo_segmented(args.pages, args.batch_size, args.delay, args.resume)


if __name__ == '__main__':
    main()
