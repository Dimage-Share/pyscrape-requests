"""Merge goo and car (CarSensor) tables into a unified `listing` table with `site` column.

This script will:
- create `listing` table with union of columns + `site` varchar(32)
- copy rows from `goo` as site='goo'
- copy rows from `car` as site='carsensor'
- report counts

It does not drop original tables. Run with:
    python scripts/merge_goo_carsensor.py
"""
import pymysql
import os


MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '33062'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'pyscrape')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'pyscrape_pwd')
MYSQL_DB = os.environ.get('MYSQL_DATABASE', 'pyscrape')

conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# decide union columns from existing tables
with conn.cursor() as cur:
    cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s", (MYSQL_DB, ))
    tables = {r['TABLE_NAME']
              for r in cur.fetchall()}
    print('Existing tables:', tables)
    exist_goo = 'goo' in tables
    exist_car = 'car' in tables
    if not (exist_goo or exist_car):
        print('No source tables found (goo or car). Exiting.')
        conn.close()
        raise SystemExit(1)
    cols = set()
    for t in ('goo', 'car'):
        if t in tables:
            cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (MYSQL_DB, t))
            cols.update([r['COLUMN_NAME'] for r in cur.fetchall()])
    # ensure id present
    if 'id' not in cols:
        raise SystemExit('No id column found in source tables')
    # remove site if present by accident
    cols.discard('site')
    # ordering: keep id first, then rest sorted for determinism
    cols_list = ['id'] + sorted([c for c in cols if c != 'id'])
    print('Unified columns:', cols_list)
    
    # create listing table SQL
    create_cols_sql = []
    for c in cols_list:
        # rough type mapping
        if c in ('id', ):
            # id must be NOT NULL for composite PK (site,id)
            typ = 'VARCHAR(64) NOT NULL'
        elif c in ('price', 'year', 'rd', 'engine'):
            typ = 'INT NULL'
        elif c == 'created_at':
            typ = 'DATETIME NULL'
        elif c == 'raw_json':
            typ = 'LONGTEXT NULL'
        else:
            # use TEXT for most variable-length string columns to avoid row-size limits
            typ = 'TEXT NULL'
        create_cols_sql.append(f'`{c}` {typ}')
    create_cols_sql.append('`site` VARCHAR(32) NOT NULL')
    create_stmt = f"CREATE TABLE IF NOT EXISTS listing ( {', '.join(create_cols_sql)}, PRIMARY KEY (`site`,`id`) ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    print('Creating listing table...')
    cur.execute(create_stmt)
    conn.commit()
    
    # build insert-select from each source
    for t, site_label in (('goo', 'goo'), ('car', 'carsensor')):
        if t not in tables:
            continue
        print(f'Copying from {t} as site={site_label}...')
        # build select expression: select columns in cols_list; if column missing in source, use NULL
        cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", (MYSQL_DB, t))
        src_cols = {r['COLUMN_NAME']
                    for r in cur.fetchall()}
        select_exprs = []
        for c in cols_list:
            if c in src_cols:
                select_exprs.append(f'`{c}`')
            else:
                select_exprs.append('NULL')
        select_exprs.append(f"'{site_label}'")
        insert_cols = ','.join([f'`{c}`' for c in cols_list] + ['`site`'])
        select_sql = f"INSERT INTO listing ({insert_cols}) SELECT {', '.join(select_exprs)} FROM `{t}`"
        # use INSERT IGNORE to avoid duplicate (site,id) conflicts, but we want upsert? Use REPLACE INTO to overwrite
        # We'll use INSERT IGNORE to keep existing if present; user can decide
        # To ensure overwriting, use REPLACE INTO (deletes existing row).
        # Use REPLACE INTO for safety of replacing duplicate
        replace_sql = select_sql.replace('INSERT INTO listing', 'REPLACE INTO listing')
    cur.execute(replace_sql)
    affected = cur.rowcount
    print(f'Copied rows from {t}: approx affected={affected}')
    conn.commit()

# report counts
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) AS cnt FROM listing")
    print('listing total:', cur.fetchone()['cnt'])
    cur.execute("SELECT site, COUNT(*) AS cnt FROM listing GROUP BY site")
    for r in cur.fetchall():
        print('by site:', r['site'], r['cnt'])

conn.close()
print('Merge complete')
