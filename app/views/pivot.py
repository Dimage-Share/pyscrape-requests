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
        try:
            with conn:
                cur = conn.execute('PRAGMA table_info(car)')
                cols = [r[1] for r in cur.fetchall()]
            return {
                'columns': cols
            } if 'columns' in request.args else cols
        finally:
            conn.close()
    
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
                    sql = f'SELECT {row},{col},COUNT(*) FROM car {where} GROUP BY {row},{col}'
                elif agg == 'sum':
                    sql = f'SELECT {row},{col},SUM({val}) FROM car {where} GROUP BY {row},{col}'
                elif agg == 'avg':
                    sql = f'SELECT {row},{col},AVG({val}) FROM car {where} GROUP BY {row},{col}'
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
