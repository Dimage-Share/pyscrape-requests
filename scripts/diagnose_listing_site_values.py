import pymysql
import os


MYSQL_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '33062'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'pyscrape')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'pyscrape_pwd')
MYSQL_DB = os.environ.get('MYSQL_DATABASE', 'pyscrape')

conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) AS cnt FROM listing WHERE site IS NULL")
    null_cnt = cur.fetchone()['cnt']
    cur.execute("SELECT COUNT(*) AS cnt FROM listing WHERE site = ''")
    empty_cnt = cur.fetchone()['cnt']
    cur.execute("SELECT site, COUNT(*) AS cnt FROM listing GROUP BY site ORDER BY cnt DESC LIMIT 10")
    rows = cur.fetchall()
    print('null site count:', null_cnt)
    print("empty site count:", empty_cnt)
    print('top sites:')
    for r in rows:
        print(r['site'], r['cnt'])
conn.close()
