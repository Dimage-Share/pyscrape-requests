"""Inspect MySQL session character sets and listing table charset/collation.

Run: python .\scripts\inspect_db_charset.py
"""
from __future__ import annotations
import json
from pathlib import Path
import sys

# ensure project root on sys.path like other scripts
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection


def main():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@character_set_client as client, @@character_set_connection as connection, @@character_set_results as results")
            sess = cur.fetchone()
            print('SESSION:', json.dumps(sess, ensure_ascii=False))
            
            # Show create table
            cur.execute("SHOW CREATE TABLE listing")
            row = cur.fetchone()
            if row:
                # pymysql returns dict with 'Table' and 'Create Table' keys
                print('\nSHOW CREATE TABLE listing:')
                for k, v in row.items():
                    print(f"{k}: {v}\n")
            
            # Inspect information_schema for column-level charset
            cur.execute("SELECT COLUMN_NAME, CHARACTER_SET_NAME, COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='listing'")
            cols = cur.fetchall()
            print('\nCOLUMNS CHARSETS:')
            for c in cols:
                print(json.dumps(c, ensure_ascii=False))
    finally:
        conn.close()


if __name__ == '__main__':
    main()
