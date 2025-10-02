from __future__ import annotations
from flask import render_template, request


def register(bp):
    
    @bp.route('/pivot')
    def pivot():
        return render_template('pivot.html')
    
    @bp.route('/pivot/columns')
    def pivot_columns():
        from core.db import get_connection
        conn = get_connection()
        cols = []
        try:
            with conn.cursor() as cur:
                # MySQL / SQLite 両対応: まず SHOW COLUMNS を試し、失敗なら PRAGMA
                try:
                    cur.execute('SHOW COLUMNS FROM listing')
                    rows = cur.fetchall()
                    for r in rows:
                        # pymysql DictCursor or sqlite tuple fallback
                        if isinstance(r, dict):
                            cols.append(r.get('Field') or r.get('field') or r.get('COLUMN_NAME'))
                        else:
                            cols.append(r[0])
                except Exception:
                    try:
                        cur.execute('PRAGMA table_info(listing)')
                        rows = cur.fetchall()
                        for r in rows:
                            if isinstance(r, dict):
                                cols.append(r.get('name'))
                            else:
                                cols.append(r[1])
                    except Exception:
                        cols = []
            cols = [c for c in cols if c]  # remove None
            return {
                'columns': cols
            } if 'columns' in request.args else cols
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    @bp.route('/pivot/data')
    def pivot_data():
        col = request.args.get('col')
        row = request.args.get('row')
        val = request.args.get('val')
        agg = request.args.get('agg', 'count')
        filters = []
        params = {}
        from core.db import get_connection
        conn = get_connection()
        try:
            with conn:
                for k, v in request.args.items():
                    if k.startswith('filter_') and v:
                        colf = k[7:]
                        filters.append(f'{colf} = :{colf}')
                        params[colf] = v
                where = ('WHERE ' + ' AND '.join(filters)) if filters else ''
                if agg == 'count':
                    sql = f'SELECT {row},{col},COUNT(*) FROM listing {where} GROUP BY {row},{col}'
                elif agg == 'sum':
                    sql = f'SELECT {row},{col},SUM({val}) FROM listing {where} GROUP BY {row},{col}'
                elif agg == 'avg':
                    sql = f'SELECT {row},{col},AVG({val}) FROM listing {where} GROUP BY {row},{col}'
                else:
                    return {
                        'error': 'invalid agg'
                    }
                cur = conn.execute(sql, params)
                data = cur.fetchall()
                rows = sorted(set(r[0] for r in data if r[0] is not None))
                cols = sorted(set(r[1] for r in data if r[1] is not None))
                table = [[0 for _ in cols] for _ in rows]
                maxv = 0
                for r in data:
                    if r[0] is None or r[1] is None:
                        continue
                    i = rows.index(r[0])
                    j = cols.index(r[1])
                    v = r[2] if r[2] is not None else 0
                    table[i][j] = v
                    if isinstance(v, (int, float)) and v > maxv:
                        maxv = v
                return {
                    'rows': rows,
                    'cols': cols,
                    'table': table,
                    'max': maxv
                }
        finally:
            conn.close()
