import pymysql


conn = pymysql.connect(host='127.0.0.1', port=33062, user='pyscrape', password='pyscrape_pwd', database='pyscrape', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
with conn.cursor() as cur:
    cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s", ('pyscrape', ))
    tables = [r['TABLE_NAME'] for r in cur.fetchall()]
    print('tables:', tables)
    for t in ['goo', 'carsensor', 'car']:
        if t in tables:
            cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s", ('pyscrape', t))
            cols = [r['COLUMN_NAME'] for r in cur.fetchall()]
            print(f'cols {t}:', cols)
conn.close()
