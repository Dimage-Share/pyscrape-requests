"""Reset listing table and perform segmented re-scrape in 50-page batches.

Usage (PowerShell):
  python .\scripts\reset_and_rescrape.py --pages 300 --batch 50 --delay 1 --confirm

Steps:
 1. Confirm flag check.
 2. DELETE FROM listing (or TRUNCATE if privileged) + optional VACUUM (sqlite fallback not expected here).
 3. Run segmented scrape for CarSensor then Goo using existing segmented runner functions.
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path
import logging


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import get_connection, init_db  # type: ignore
from scripts.run_scrapes_segmented import run_carsensor_segmented, run_goo_segmented  # type: ignore


log = logging.getLogger(__name__)


def reset_listing() -> None:
    init_db()
    conn = get_connection()
    cur = conn.cursor()
    # Prefer TRUNCATE for speed; fallback to DELETE if insufficient privileges
    try:
        cur.execute("TRUNCATE TABLE listing")
    except Exception:
        cur.execute("DELETE FROM listing")
    conn.commit()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pages', type=int, default=300)
    ap.add_argument('--batch', type=int, default=50)
    ap.add_argument('--delay', type=float, default=1.0)
    ap.add_argument('--dump-dir', type=str, help='Dump directory for per-page JSON/HTML (optional)')
    ap.add_argument('--no-full-html', action='store_true', help='Skip saving full HTML (JSON only)')
    ap.add_argument('--confirm', action='store_true')
    args = ap.parse_args()
    if not args.confirm:
        print('Add --confirm to proceed (safety).')
        return
    print('Resetting listing table...')
    reset_listing()
    full_html = not args.no_full_html
    dump_dir = args.dump_dir
    print('Re-scraping CarSensor...')
    run_carsensor_segmented(args.pages, args.batch, args.delay, resume=False, dump_dir=dump_dir, full_html=full_html)
    print('Re-scraping Goo...')
    run_goo_segmented(args.pages, args.batch, args.delay, resume=False, dump_dir=dump_dir, full_html=full_html)
    print('All done.')


if __name__ == '__main__':
    main()
