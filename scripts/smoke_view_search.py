import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Smoke test of app.views.search DB usage (non-HTTP)
from app.views.search import _list_summary_files, register
from core.db import get_connection

# simulate calling view function (no Flask request context) by directly calling DB logic
conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute('SELECT 1')
        print('ok cursor fetch:', cur.fetchall())
finally:
    conn.close()
print('closed ok')
