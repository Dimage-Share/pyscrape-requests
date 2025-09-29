"""Safely copy rows from `goo` into `listing` with site='goo' using INSERT IGNORE.
This script will:
- create a backup table `listing_backup` as a copy of current `listing` (if not exists)
- insert rows from `goo` that don't already exist in `listing` (site='goo') using INSERT IGNORE
- report affected rows and final counts
"""
import pymysql
import os


MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '33062'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'pyscrape')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'pyscrape_pwd')
MYSQL_DB = os.environ.get('MYSQL_DATABASE', 'pyscrape')

conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

with conn.cursor() as cur:
    # ensure listing exists
    cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_NAME='listing'", (MYSQL_DB, ))
    if not cur.fetchone():
        raise SystemExit('listing table not found; run merge script first')
    # backup listing
    cur.execute("CREATE TABLE IF NOT EXISTS listing_backup LIKE listing")
    cur.execute("INSERT IGNORE INTO listing_backup SELECT * FROM listing")
    print('Created/updated listing_backup')
    conn.commit()
    
    # build column list
    cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (MYSQL_DB, 'listing'))
    listing_cols = [r['COLUMN_NAME'] for r in cur.fetchall()]
    # remove site from copy-from selection; we'll add constant 'goo'
    src_cols = [c for c in listing_cols if c != 'site']
    insert_cols = ','.join([f'`{c}`' for c in listing_cols])
    select_cols = ','.join([f'`{c}`' if c in ('id', ) or True else 'NULL' for c in src_cols])
    # but we need to map src_cols to goo's columns; build select expression that picks column or NULL
    cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (MYSQL_DB, 'goo'))
    goo_cols = {r['COLUMN_NAME']
                for r in cur.fetchall()}
    select_exprs = []
    for c in src_cols:
        if c in goo_cols:
            select_exprs.append(f'`{c}`')
        else:
            select_exprs.append('NULL')
    # append site constant
    select_exprs.append("'goo'")
    insert_sql = f"INSERT IGNORE INTO listing ({insert_cols}) SELECT {', '.join(select_exprs)} FROM goo"
    cur.execute(insert_sql)
    inserted = cur.rowcount
    print('Inserted (IGNORED duplicates):', inserted)
    conn.commit()
    
    # report counts
    cur.execute("SELECT COUNT(*) AS cnt FROM listing")
    print('listing total:', cur.fetchone()['cnt'])
    cur.execute("SELECT site, COUNT(*) AS cnt FROM listing GROUP BY site")
    for r in cur.fetchall():
        print('by site:', r['site'], r['cnt'])

conn.close()
print('Done')
