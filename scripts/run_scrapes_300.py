"""Run 300-page scrapes for Goo-net and CarSensor.

This script orchestrates scraping but DOES NOT run automatically; it requires
explicit confirmation from the operator because scraping 600 pages may be
network- and site-impacting (and may violate robots.txt). Use responsibly.

Usage (PowerShell):
    python .\\scripts\\run_scrapes_300.py --confirm

The script will:
 - instantiate appropriate clients/parsers
 - call Scrape.run for CarSensor with pages=300
 - call GooNet pagination logic to fetch 300 pages, using app.scrapers.goonet
 - write outputs to DB via existing adapters (listing/car/goo tables)

Note: This is a convenience orchestrator. If you prefer to run each provider
separately or throttle more aggressively, run the individual modules.
"""
from __future__ import annotations
import argparse
import time
from pathlib import Path
import logging
import sys

# Ensure project root is on sys.path so imports like `core` and `app` resolve
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger(__name__)


def run_carsensor(pages: int = 300, delay: float = 1.0):
    from core.scrape import Scrape
    s = Scrape(page_delay=delay)
    print(f"Starting CarSensor scrape for {pages} pages (delay={delay})")
    start = time.perf_counter()
    s.run(pages, {})
    elapsed = time.perf_counter() - start
    print(f"CarSensor scrape finished in {elapsed:.1f}s")


def run_goonet(pages: int = 300, delay: float = 1.0):
    # Use GooNetClient and parser under app.scrapers.goonet
    from app.scrapers.goonet.client import GooNetClient
    from app.scrapers.goonet import parser, models
    from app.db import init_db, bulk_insert_listing
    print(f"Starting Goo-net scrape for {pages} pages (delay={delay})")
    init_db()
    all_records = []
    with GooNetClient() as client:
        html = client.get_summary_page(params=None)
        items = parser.parse_cars(html) if hasattr(parser, 'parse_cars') else parser.parse_summary(html)
        # parser.parse_cars returns CarRecord-like objects in the goonet module; adapt
        if isinstance(items, list):
            for it in items:
                # if parse_cars returns models.CarRecord, use as-is
                if hasattr(it, 'to_db_row'):
                    all_records.append(it)
                elif isinstance(it, dict):
                    # convert dict to models.CarRecord if possible
                    try:
                        cr = models.CarRecord(**it)
                        all_records.append(cr)
                    except Exception:
                        continue
        pages_fetched = 1
        while pages_fetched < pages:
            time.sleep(delay)
            next_url = parser.get_next_page_url(html) if hasattr(parser, 'get_next_page_url') else None
            if not next_url:
                logger.debug('No next page; stopping')
                break
            resp = client.session.get(next_url, timeout=client.config.timeout)
            resp.encoding = 'utf-8'
            html = resp.text
            new_items = parser.parse_cars(html) if hasattr(parser, 'parse_cars') else parser.parse_summary(html)
            if new_items:
                for it in new_items:
                    if hasattr(it, 'to_db_row'):
                        all_records.append(it)
                    elif isinstance(it, dict):
                        try:
                            cr = models.CarRecord(**it)
                            all_records.append(cr)
                        except Exception:
                            continue
            pages_fetched += 1
    # Write to listing with site='goo' (pass CarRecord objects - adapters will call to_db_row)
    if all_records:
        inserted = bulk_insert_listing(all_records, site='goo')
        print(f"Inserted {inserted} records into listing (goo)")
    else:
        print('No records fetched from goo-net')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--confirm', action='store_true', help='Confirm before running (required)')
    p.add_argument('--pages', type=int, default=300)
    p.add_argument('--delay', type=float, default=1.0)
    p.add_argument('--force', action='store_true', help='Force-run even if robots.txt disallows')
    args = p.parse_args()
    if not args.confirm:
        print('This script will attempt to fetch many pages. Re-run with --confirm to proceed.')
        raise SystemExit(1)
    
    # robots.txt checks
    try:
        import urllib.parse
        import urllib.robotparser
        import requests
        
        def _robot_allowed(target_url: str, user_agent: str = '*') -> bool:
            try:
                purl = urllib.parse.urlparse(target_url)
                root = f"{purl.scheme}://{purl.netloc}"
                robots_url = urllib.parse.urljoin(root, '/robots.txt')
                rp = urllib.robotparser.RobotFileParser()
                # fetch via requests for timeout
                resp = requests.get(robots_url, timeout=10)
                if resp.status_code >= 400:
                    # missing or error -> treat as allowed
                    return True
                rp.parse(resp.text.splitlines())
                path = purl.path or '/'
                return rp.can_fetch(user_agent, path)
            except Exception:
                return True
        
        import core.client as cc
        goo_url = getattr(cc, '_resolved_summary_url', None)
        cars_url = getattr(cc, '_resolved_carsensor_url', None)
        allowed = True
        if not args.force:
            if goo_url and not _robot_allowed(goo_url):
                print(f'robots.txt disallows crawling {goo_url}; aborting. Use --force to override.')
                allowed = False
            if cars_url and not _robot_allowed(cars_url):
                print(f'robots.txt disallows crawling {cars_url}; aborting. Use --force to override.')
                allowed = False
        else:
            print('Skipping robots.txt checks due to --force')
        
        if not allowed:
            raise SystemExit(1)
    except Exception:
        # best-effort; if anything goes wrong in check, continue
        pass
    
    run_carsensor(pages=args.pages, delay=args.delay)
    run_goonet(pages=args.pages, delay=args.delay)
