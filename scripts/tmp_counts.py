import pymysql
import os


MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '33062'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'pyscrape')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'pyscrape_pwd')
MYSQL_DB = os.environ.get('MYSQL_DATABASE', 'pyscrape')

conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn.cursor() as cur:
    cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s", (MYSQL_DB, ))
    tables = [r['TABLE_NAME'] for r in cur.fetchall()]
    print('tables:', tables)
    for t in ['listing', 'goo', 'car', 'carsensor']:
        if t in tables:
            cur.execute(f"SELECT COUNT(*) AS cnt FROM `{t}`")
            cnt = cur.fetchone()['cnt']
            print(f'count {t}:', cnt)
        else:
            print(f'count {t}: not present')
conn.close()
