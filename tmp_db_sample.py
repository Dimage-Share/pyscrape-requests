import sqlite3


conn = sqlite3.connect('database.db')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='car';")
if not cur.fetchone():
    print('car table does not exist')
else:
    cur.execute('SELECT id,name,price,year,rd,engine,repair,url FROM car LIMIT 10')
    rows = cur.fetchall()
    print('sample rows (up to 10):', len(rows))
    for r in rows:
        print(r)
conn.close()
