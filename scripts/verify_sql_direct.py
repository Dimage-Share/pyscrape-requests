import pymysql, os


host = os.environ.get('MYSQL_HOST', '127.0.0.1')
port = int(os.environ.get('MYSQL_PORT', '33062'))
user = os.environ.get('MYSQL_USER', 'pyscrape')
pwd = os.environ.get('MYSQL_PASSWORD', 'pyscrape_pwd')
db = os.environ.get('MYSQL_DATABASE', 'pyscrape')
conn = pymysql.connect(host=host, port=port, user=user, password=pwd, database=db, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
try:
    cols = ['site', 'id', 'manufacturer', 'name', 'price', 'year', 'rd', 'engine', 'mission1', 'mission2', 'bodytype', 'repair', 'location', 'wd', 'seat', 'door', 'fuel', 'handle', 'jc08', 'option', 'category', 'url']
    cols_sql = ','.join([f'`{c}`' for c in cols])
    sql = f'SELECT {cols_sql} FROM listing ORDER BY `price` ASC, `year` DESC LIMIT 5'
    print('SQL:', sql)
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        print('rows', len(rows))
finally:
    conn.close()
