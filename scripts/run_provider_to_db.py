"""Run a registered provider, parse N pages, and upsert results into the unified `listing` table.

Usage (examples):
  python scripts/run_provider_to_db.py --provider carsensor --pages 30 --delay 1 --commit
  python scripts/run_provider_to_db.py --provider goonet --pages 10 --dump-dir dumps_goonet --commit
  python scripts/run_provider_to_db.py --provider carsensor --pages 5  # dry-run (no commit)

Features:
  - Uses providers.base registry (ensure provider module is imported: vehicles auto-registers)
  - Decoding via core.encoding.decode_response
  - Optional per-page dump (HTML) for audit / later repair
  - Bulk insert (upsert) using app.db.bulk_insert_listing(site=<provider_key>)
  - Dry run by default (prints counts only) unless --commit passed
  - Resilient: stops on first hard error unless --ignore-errors set

Exit codes:
  0 success
  2 provider not found / invalid args
  3 runtime error (network/parse) unless ignored
"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable
from pathlib import Path
import argparse
import time
import sys

from providers.base import get_provider  # registry
from providers import vehicles  # noqa: F401  (ensure auto-registration)
from core.encoding import decode_response
import requests


def iter_pages(provider_key: str, pages: int, delay: float, dump_dir: str | None, verbose: bool = True) -> list[Any]:
    prov = get_provider(provider_key)
    session = requests.Session()
    html = prov.fetch_first(None)
    all_records = list(prov.parse_list(html))
    if verbose:
        print(f"[page 1] records={len(all_records)}")
    if dump_dir:
        Path(dump_dir).mkdir(parents=True, exist_ok=True)
        (Path(dump_dir) / f"{provider_key}_page1.html").write_text(html, encoding='utf-8', errors='ignore')
    current_html = html
    fetched = 1
    while fetched < pages:
        nxt = prov.next_page_url(current_html)
        if not nxt:
            if verbose:
                print("No further next page URL detected; stopping early.")
            break
        try:
            resp = session.get(nxt, timeout=30)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"HTTP error on page {fetched+1}: {e}") from e
        current_html = decode_response(resp)
        recs = list(prov.parse_list(current_html))
        all_records.extend(recs)
        fetched += 1
        if verbose:
            print(f"[page {fetched}] url={nxt} records(+{len(recs)}) total={len(all_records)}")
        if dump_dir:
            (Path(dump_dir) / f"{provider_key}_page{fetched}.html").write_text(current_html, encoding='utf-8', errors='ignore')
        if delay > 0 and fetched < pages:
            time.sleep(delay)
    return all_records


def to_db_rows(objs: Iterable[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for o in objs:
        if o is None:
            continue
        if hasattr(o, 'to_db_row') and callable(getattr(o, 'to_db_row')):  # domain dataclass style
            try:
                rows.append(o.to_db_row())  # type: ignore[arg-type]
                continue
            except Exception:
                pass
        if hasattr(o, '__dict__'):
            d = dict(vars(o))
            rows.append(d)
            continue
        if isinstance(o, dict):
            rows.append(o)
            continue
        # fallback: try dataclass asdict
        try:
            rows.append(asdict(o))  # type: ignore[arg-type]
        except Exception:
            # ignore unconvertible object
            pass
    return rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--provider', '-p', required=True, help='provider key (e.g. carsensor, goonet)')
    p.add_argument('--pages', type=int, default=1, help='number of pages to fetch')
    p.add_argument('--delay', type=float, default=0.0, help='seconds sleep between pages')
    p.add_argument('--dump-dir', help='if set, save each page HTML into this directory')
    p.add_argument('--commit', action='store_true', help='actually upsert into listing (site=<provider>)')
    p.add_argument('--ignore-errors', action='store_true', help='continue on parse HTTP errors (best-effort)')
    p.add_argument('--verbose', action='store_true', help='verbose progress output')
    args = p.parse_args(argv)
    
    provider_key = args.provider.lower().strip()
    try:
        prov = get_provider(provider_key)
    except Exception as e:  # noqa: BLE001
        print(f"Provider not found: {provider_key} ({e})", file=sys.stderr)
        return 2
    
    try:
        records = iter_pages(provider_key, max(1, args.pages), args.delay, args.dump_dir, verbose=args.verbose or True)
    except Exception as e:  # noqa: BLE001
        if not args.ignore_errors:
            print(f"Fatal error: {e}", file=sys.stderr)
            return 3
        print(f"[WARN] error during fetch: {e}")
        records = []
    
    print(f"Fetched total records: {len(records)}")
    if not args.commit:
        print("Dry-run (no DB write). Use --commit to persist into listing.")
        return 0
    
    # Convert and bulk insert
    rows = to_db_rows(records)
    if not rows:
        print("No convertible rows; nothing to insert.")
        return 0
    try:
        from app.db import bulk_insert_listing, init_db  # type: ignore
    except Exception as e:  # noqa: BLE001
        print(f"DB adapter import error: {e}", file=sys.stderr)
        return 3
    try:
        init_db()
    except Exception:
        pass
    try:
        inserted = bulk_insert_listing(rows, site=provider_key)  # type: ignore[arg-type]
    except TypeError:
        # Some implementations expect CarRecord objects; fallback attempt: pass original objects
        try:
            inserted = bulk_insert_listing(records, site=provider_key)  # type: ignore[arg-type]
        except Exception as e:  # noqa: BLE001
            print(f"Insertion failed: {e}", file=sys.stderr)
            return 3
    except Exception as e:  # noqa: BLE001
        print(f"Insertion failed: {e}", file=sys.stderr)
        return 3
    print(f"Upserted {inserted} rows into listing (site={provider_key}).")
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
