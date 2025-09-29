"""Direct migration runner: SQLite -> MySQL without importing project packages.

This script connects to local SQLite `database.db`, reads rows from `goo` and `car`,
and writes them into MySQL (host 127.0.0.1:33062) using PyMySQL.

Run as: python scripts/sqlite_to_mysql_runner.py
"""
import sqlite3
import pymysql
from pymysql.constants import CLIENT
import os


SQLITE_DB = os.environ.get('SQLITE_DB', 'database.db')
MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '33062'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'pyscrape')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'pyscrape_pwd')
MYSQL_DB = os.environ.get('MYSQL_DATABASE', 'pyscrape')

CHUNK = 500


def fetch_rows(table):
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def upsert_rows_mysql(conn, table, rows):
    if not rows:
        return 0
    cols = list(rows[0].keys())
    # Build insert SQL with ON DUPLICATE KEY UPDATE
    col_list = ','.join(f'`{c}`' for c in cols)
    placeholders = ','.join(['%s'] * len(cols))
    update_list = ','.join(f'`{c}`=VALUES(`{c}`)' for c in cols if c != 'id')
    sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_list}"
    # Prepare parameter tuples in order
    params = []
    for r in rows:
        # Ensure values order matches cols
        params.append(tuple(r.get(c) for c in cols))
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(params), CHUNK):
            chunk = params[i:i + CHUNK]
            cur.executemany(sql, chunk)
            total += len(chunk)
    conn.commit()
    return total


def ensure_mysql_columns(conn, table, sqlite_cols):
    """Ensure that MySQL table has all columns present in sqlite_cols; add missing as TEXT/appropriate type."""
    with conn.cursor() as cur:
        cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (MYSQL_DB, table))
        existing = {r['COLUMN_NAME']
                    for r in cur.fetchall()}
        to_add = [c for c in sqlite_cols if c not in existing]
        if not to_add:
            return
        # simple type mapping
        type_map = {
            'id': 'VARCHAR(64)',
            'price': 'INT',
            'year': 'INT',
            'rd': 'INT',
            'engine': 'INT',
            'created_at': 'DATETIME',
            'raw_json': 'LONGTEXT',
        }
        for col in to_add:
            col_type = type_map.get(col, 'TEXT')
            sql = f"ALTER TABLE `{table}` ADD COLUMN `{col}` {col_type} NULL"
            print('Altering table', table, 'ADD', col, col_type)
            cur.execute(sql)
    conn.commit()


def main():
    print('Fetch sqlite rows...')
    for table in ('goo', 'car'):
        rows = fetch_rows(table)
        print(f'Found {len(rows)} rows in sqlite.{table}')
    # connect to mysql
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor, client_flag=CLIENT.MULTI_STATEMENTS)
    print('Connected to MySQL')
    total_all = 0
    for table in ('goo', 'car'):
        rows = fetch_rows(table)
        if not rows:
            print(f'No rows for {table}, skipping')
            continue
        # ensure columns exist in mysql table before upsert
        sqlite_cols = list(rows[0].keys())
        ensure_mysql_columns(conn, table, sqlite_cols)
        n = upsert_rows_mysql(conn, table, rows)
        print(f'Upserted {n} rows into {table}')
        total_all += n
    conn.close()
    print('Migration complete, total rows upserted:', total_all)


if __name__ == '__main__':
    main()
