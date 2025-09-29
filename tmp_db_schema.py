import sqlite3


conn = sqlite3.connect('database.db')
cur = conn.cursor()
cur.execute("PRAGMA table_info(car)")
cols = cur.fetchall()
print('columns:')
for c in cols:
    print(c)
conn.close()
