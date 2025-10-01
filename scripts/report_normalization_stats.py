"""Report normalization impact: counts of bodytype variants & suspicious flags.

Usage:
  python .\scripts\report_normalization_stats.py
  python .\scripts\report_normalization_stats.py --limit 30
"""
from __future__ import annotations
import argparse
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import get_connection, init_db  # type: ignore
import json


def fetch_rows(sql: str, params=()):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=50)
    args = ap.parse_args()
    init_db()
    print('== Bodytype frequency ==')
    body_rows = fetch_rows("SELECT bodytype, COUNT(*) c FROM listing GROUP BY bodytype ORDER BY c DESC")
    for bt, c in body_rows[:args.limit]:
        print(f"{bt or '(NULL)'}\t{c}")
    print('\n== Suspicious bodytype flagged (raw_json contains key) ==')
    # MySQL JSON_EXTRACT 使えない場合もあるので LIKE 検索
    susp_rows = fetch_rows("SELECT COUNT(*) c FROM listing WHERE raw_json LIKE %s", ('%suspicious_bodytype%', ))
    print(f"suspicious_bodytype rows: {susp_rows[0]['c'] if isinstance(susp_rows[0], dict) else susp_rows[0][0]}")
    print('\n== Sample suspicious records ==')
    sample = fetch_rows("SELECT site,id,bodytype,LEFT(raw_json,200) raw_part FROM listing WHERE raw_json LIKE %s LIMIT 10", ('%suspicious_bodytype%', ))
    for r in sample:
        if isinstance(r, dict):
            print(f"{r['site']} {r['id']} {r['bodytype']} {r['raw_part']}")
        else:
            print(r)


if __name__ == '__main__':
    main()
