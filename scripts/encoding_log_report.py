"""Daily aggregation report for encoding_log.

Usage:
  python .\scripts\encoding_log_report.py --days 7
  python .\scripts\encoding_log_report.py --days 3 --site goo
"""
from __future__ import annotations
import argparse
from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import get_connection, init_db  # type: ignore


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=7, help='Look back N days including today')
    ap.add_argument('--site', type=str, default=None)
    ap.add_argument('--limit', type=int, default=100)
    args = ap.parse_args()
    init_db()
    conn = get_connection()
    cur = conn.cursor()
    where = "WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)"
    params = [args.days]
    if args.site:
        where += " AND site=%s"
        params.append(args.site)
    sql = f"""
        SELECT DATE(created_at) d, site, chosen_encoding, COUNT(*) cnt, AVG(score) avg_score
        FROM encoding_log
        {where}
        GROUP BY d, site, chosen_encoding
        ORDER BY d DESC, cnt DESC
        LIMIT %s
    """
    params.append(args.limit)
    try:
        cur.execute(sql, params)
    except Exception:
        # SQLite fallback (DATE() may behave differently; treat created_at as text)
        cur.execute(sql.replace('UTC_TIMESTAMP()', 'CURRENT_TIMESTAMP'), params)
    rows = cur.fetchall()
    conn.close()
    print('date\tsite\tencoding\tcount\tavg_score')
    for r in rows:
        if isinstance(r, dict):
            print(f"{r['d']}\t{r['site']}\t{r['chosen_encoding']}\t{r['cnt']}\t{float(r['avg_score']):.3f}")
        else:
            print(r)


if __name__ == '__main__':
    main()
