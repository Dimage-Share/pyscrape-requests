import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / 'app', ROOT / 'core'):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)
from app.db import get_connection


def main():
    conn = get_connection()
    try:
        cur = conn.cursor()
        # pattern: starts with the garbled prefix observed '�ܥǥ'
        cur.execute("SELECT COUNT(*) AS c FROM listing WHERE bodytype LIKE %s", ('%�ܥǥ%', ))
        row = cur.fetchone()
        c = row['c'] if isinstance(row, dict) else row[0]
        print('garbled bodytype rows:', c)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
