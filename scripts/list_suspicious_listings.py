"""List or export suspicious listings (currently flagged by suspicious_bodytype in raw_json).

Usage:
  python .\scripts\list_suspicious_listings.py --limit 50
  python .\scripts\list_suspicious_listings.py --export suspicious.csv
  python .\scripts\list_suspicious_listings.py --nullify --confirm

Options:
  --nullify : set bodytype = NULL where suspicious flag present (leaves raw_json for traceability)
  --export  : write CSV (site,id,bodytype,url)
"""
from __future__ import annotations
import argparse
import csv
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import get_connection, init_db  # type: ignore


def fetch(sql: str, params=()):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def find_suspicious(limit: int | None):
    sql = "SELECT site,id,bodytype,url,LEFT(raw_json,200) raw_part FROM listing WHERE raw_json LIKE %s ORDER BY created_at DESC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    return fetch(sql, ('%suspicious_bodytype%', ))


def nullify_bodytype():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE listing SET bodytype=NULL WHERE raw_json LIKE %s", ('%suspicious_bodytype%', ))
        affected = cur.rowcount if hasattr(cur, 'rowcount') else None
        conn.commit()
        return affected
    finally:
        conn.close()


def export_csv(rows, path: Path):
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['site', 'id', 'bodytype', 'url', 'raw_part'])
        for r in rows:
            if isinstance(r, dict):
                w.writerow([r.get('site'), r.get('id'), r.get('bodytype'), r.get('url'), r.get('raw_part')])
            else:
                w.writerow(list(r))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=30)
    ap.add_argument('--export', type=str)
    ap.add_argument('--nullify', action='store_true')
    ap.add_argument('--confirm', action='store_true')
    args = ap.parse_args()
    init_db()
    rows = find_suspicious(args.limit)
    print(f"Suspicious listings (showing up to {args.limit}): {len(rows)} rows returned")
    for r in rows:
        if isinstance(r, dict):
            print(f"{r['site']} {r['id']} {r['bodytype']} {r['url']} :: {r['raw_part'][:60]}")
        else:
            print(r)
    if args.export:
        out_path = Path(args.export)
        export_csv(rows, out_path)
        print(f"Exported {len(rows)} rows to {out_path}")
    if args.nullify:
        if not args.confirm:
            print('--nullify requested but missing --confirm (safety). Abort.')
            return
        affected = nullify_bodytype()
        print(f"Nullified bodytype on {affected} rows.")


if __name__ == '__main__':
    main()
