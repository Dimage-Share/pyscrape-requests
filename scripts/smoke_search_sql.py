import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.db import get_connection

# Build sample SQL similar to search view
select_cols = ['site', 'id', 'manufacturer', 'name', 'price', 'year', 'rd', 'engine', 'mission1', 'mission2', 'bodytype', 'repair', 'location', 'wd', 'seat', 'door', 'fuel', 'handle', 'jc08', 'option', 'category', 'url']
cols_sql = ','.join([f'`{c}`' for c in select_cols])
sql = f'SELECT {cols_sql} FROM listing ORDER BY `price` ASC, `year` DESC LIMIT 5'
print('SQL:', sql)
conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        print('rows:', len(rows))
finally:
    conn.close()
