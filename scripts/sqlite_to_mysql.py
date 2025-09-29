"""SQLite -> MySQL migration helper script.

Reads rows from the local SQLite file (`database.db`) and writes them to MySQL
using the project's adapters and models. Runs incrementally and idempotently.

Usage:
    $env:DB_BACKEND='mysql'
    python scripts\sqlite_to_mysql.py

Notes:
- Ensure MySQL container is running and `python -m pip install -r requirements.txt` has been executed.
- This script migrates `goo` and `car` tables.
"""
from __future__ import annotations
import sqlite3
import os
from importlib import import_module
from core.models import CarRecord

# Use app.db wrapper to select mysql
os.environ['DB_BACKEND'] = os.environ.get('DB_BACKEND', 'mysql')
from app.db import init_db, bulk_insert_goo, bulk_upsert_cars


SQLITE_DB = os.environ.get('SQLITE_DB', 'database.db')


def fetch_sqlite_rows(table: str):
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def row_to_carrecord(row: dict) -> CarRecord:
    # core.models.CarRecord expects fields defined in dataclass; map accordingly
    return CarRecord(
        id=row.get('id'),
        manufacturer=row.get('manufacturer'),
        name=row.get('name'),
        price=row.get('price'),
        year=row.get('year'),
        rd=row.get('rd'),
        engine=row.get('engine'),
        color=row.get('color'),
        mission1=row.get('mission1'),
        mission2=row.get('mission2'),
        bodytype=row.get('bodytype'),
        repair=row.get('repair'),
        location=row.get('location'),
        option=row.get('option'),
        wd=row.get('wd'),
        seat=row.get('seat'),
        door=row.get('door'),
        fuel=row.get('fuel'),
        handle=row.get('handle'),
        jc08=row.get('jc08'),
        category=row.get('category'),
        source=row.get('source'),
        url=row.get('url'),
        raw={
            'raw': row.get('raw_json') or {}
        },
    )


def main():
    print('Initializing target DB schema...')
    init_db()
    print('Fetching sqlite goo rows...')
    goo_rows = fetch_sqlite_rows('goo')
    print('Found', len(goo_rows), 'rows in sqlite goo')
    goo_records = [row_to_carrecord(r) for r in goo_rows]
    if goo_records:
        print('Migrating goo rows...')
        inserted = bulk_insert_goo(goo_records)
        print('Inserted', inserted, 'rows into MySQL goo')
    
    print('Fetching sqlite car rows...')
    car_rows = fetch_sqlite_rows('car')
    print('Found', len(car_rows), 'rows in sqlite car')
    car_records = [row_to_carrecord(r) for r in car_rows]
    if car_records:
        print('Migrating car rows...')
        upserted = bulk_upsert_cars(car_records)
        print('Upserted', upserted, 'rows into MySQL car')
    
    print('Migration complete')


if __name__ == '__main__':
    main()
