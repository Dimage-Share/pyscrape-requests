import sqlite3


conn = sqlite3.connect('database.db')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='car';")
t = cur.fetchone()
print('car table exists:', bool(t))
if t:
    cur.execute('SELECT count(*) FROM car')
    print('rows in car:', cur.fetchone()[0])
conn.close()
