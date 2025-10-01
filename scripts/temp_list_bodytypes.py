import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / 'app'))
    sys.path.insert(0, str(ROOT / 'core'))
from app.db import get_connection


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT bodytype FROM listing WHERE bodytype IS NOT NULL AND bodytype != '' ORDER BY bodytype LIMIT 80")
        rows = cur.fetchall()
        print('distinct bodytype count:', len(rows))
        for r in rows:
            val = r['bodytype'] if isinstance(r, dict) else r[0]
            print(repr(val))
    finally:
        conn.close()


if __name__ == '__main__':
    main()
