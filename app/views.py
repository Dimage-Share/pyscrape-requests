from __future__ import annotations
from flask import Blueprint, current_app, render_template, redirect, url_for, send_file, request, flash
from pathlib import Path
import os

bp = Blueprint('main', __name__)

SUMMARY_PREFIX = 'summary-'
SUMMARY_SUFFIX = '.md'


def _list_summary_files():
    files = []
    for p in Path('.').glob(f'{SUMMARY_PREFIX}*{SUMMARY_SUFFIX}'):
        files.append(p.name)
    return sorted(files)


@bp.route('/')
def index():
    # Build SQL dynamically
    where = []
    params = {}
    # Collect filter parameters from query string
    filters = {
        'manufacturer': request.args.get('manufacturer') or '',
        'name': request.args.get('name') or '',
        'mission1': request.args.get('mission1') or '',
        'mission2': request.args.get('mission2') or '',
        'bodytype': request.args.get('bodytype') or '',
        'repair': request.args.get('repair') or '',
        'location': request.args.get('location') or '',
        'year': request.args.get('year') or '',
        'price_max': request.args.get('price_max') or '',
        'price_min': request.args.get('price_min') or '',
        'category': request.args.get('category') or '',
        'wd': request.args.get('wd') or '',
        'seat': request.args.get('seat') or '',
        'door': request.args.get('door') or '',
        'fuel': request.args.get('fuel') or '',
        'handle': request.args.get('handle') or '',
        'jc08': request.args.get('jc08') or ''
    }
    print("[DEBUG] filters['door']:", repr(filters['door']))
    print("[DEBUG] filters['door']:", repr(filters['door']))
    # wd, seat, fuel, handle: 部分一致 or null
    if filters['wd']:
        where.append('(wd LIKE :wd OR wd IS NULL OR wd = "")')
        params['wd'] = f"%{filters['wd']}%"
    if filters['seat']:
        where.append('seat = :seat')
        params['seat'] = filters['seat']
    if filters['door'] != '':
        where.append('door = :door')
        params['door'] = filters['door']
    if filters['fuel']:
        where.append('(fuel LIKE :fuel OR fuel IS NULL OR fuel = "")')
        params['fuel'] = f"%{filters['fuel']}%"
    if filters['handle']:
        where.append('(handle LIKE :handle OR handle IS NULL OR handle = "")')
        params['handle'] = f"%{filters['handle']}%"
    # jc08: 入力値以上またはnull
    if filters['jc08']:
        try:
            params['jc08'] = float(filters['jc08'])
            where.append('(CAST(jc08 AS FLOAT) >= :jc08 OR jc08 IS NULL OR jc08 = "")')
        except Exception:
            flash('JC08は数値で入力してください', 'error')
    # Sorting parameters (whitelist columns)
    allowed_sort_cols = ['price', 'year', 'rd', 'engine', 'manufacturer', 'name', 'mission1', 'mission2', 'bodytype', 'repair', 'location']
    sort = request.args.get('sort') or 'price'
    if sort not in allowed_sort_cols:
        sort = 'price'
    dir_ = request.args.get('dir') or 'asc'
    dir_ = dir_.lower()
    if dir_ not in ('asc', 'desc'):
        dir_ = 'asc'
    # Secondary ordering to keep results deterministic (price then year desc by default)
    secondary_order = ''
    if sort != 'price':
        secondary_order = ', price ASC'
    if sort != 'year':
        secondary_order += ', year DESC'
    # Build SQL dynamically
    where = []
    params = {}
    if filters['manufacturer']:
        where.append('manufacturer = :manufacturer')
        params['manufacturer'] = filters['manufacturer']
    if filters['name']:
        where.append('name = :name')
        params['name'] = filters['name']
    if filters['mission1']:
        where.append('mission1 = :mission1')
        params['mission1'] = filters['mission1']
    if filters['mission2']:
        where.append('mission2 = :mission2')
        params['mission2'] = filters['mission2']
    if filters['category']:
        where.append('category = :category')
        params['category'] = filters['category']
    if filters['bodytype']:
        where.append('bodytype = :bodytype')
        params['bodytype'] = filters['bodytype']
    if filters['repair']:
        where.append('repair = :repair')
        params['repair'] = filters['repair']
    if filters['location']:
        where.append('location = :location')
        params['location'] = filters['location']
    if filters['door']:
        where.append('door = :door')
        params['door'] = filters['door']
    if filters['year']:
        try:
            params['year'] = int(filters['year'])
            where.append('(year >= :year OR year IS NULL)')
        except Exception:
            flash('年式は整数で入力してください', 'error')
    if filters['price_min']:
        try:
            params['price_min'] = int(filters['price_min'])
            where.append('price >= :price_min')
        except Exception:
            flash('price_minは整数', 'error')
    if filters['price_max']:
        try:
            params['price_max'] = int(filters['price_max'])
            where.append('price <= :price_max')
        except Exception:
            flash('price_maxは整数', 'error')
    sql = 'SELECT id,manufacturer,name,price,year,rd,engine,mission1,mission2,bodytype,repair,location,wd,seat,door,fuel,handle,jc08,option,category,url FROM car'
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += f' ORDER BY {sort} {dir_.upper()}{secondary_order} LIMIT 300'
    print("[SQL QUERY]", sql)
    print("[SQL PARAMS]", params)
    # Fetch distinct values for dropdowns
    import sqlite3
    from core.db import get_connection
    conn = get_connection()
    try:
        with conn:
            cur = conn.execute(sql, params)
            rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
            
            def _vals(col):
                c2 = conn.execute(f'SELECT DISTINCT {col} FROM goo WHERE {col} IS NOT NULL AND {col} != "" ORDER BY {col} LIMIT 200').fetchall()
                return [v[0] for v in c2]
            
            bodytypes = _vals('bodytype')
            manufacturers = _vals('manufacturer')
            names = _vals('name')
            mission1s = _vals('mission1')
            mission2s = _vals('mission2')
            repairs = _vals('repair')
            locations = _vals('location')
            years = [r[0] for r in conn.execute('SELECT DISTINCT year FROM goo WHERE year IS NOT NULL ORDER BY year DESC LIMIT 50').fetchall()]
            categories = _vals('category')
            wds = _vals('wd')
            seats = _vals('seat')
            doors = _vals('door')
            fuels = _vals('fuel')
            handles = _vals('handle')
    finally:
        conn.close()
    files = _list_summary_files()
    state = current_app.extensions.get('scrape_state') or {}
    return render_template('index.html', files=files, rows=rows, filters=filters, bodytypes=bodytypes, manufacturers=manufacturers, names=names, mission1s=mission1s, mission2s=mission2s, repairs=repairs, locations=locations, years=years, categories=categories, wds=wds, seats=seats, doors=doors, fuels=fuels, handles=handles, sort=sort, dir=dir_, state=state)


@bp.route('/scrape', methods=['POST'])
def scrape():
    pages = request.form.get('pages', '1')
    try:
        pages_i = max(1, int(pages))
    except Exception:
        flash('pages は整数で指定してください', 'error')
        return redirect(url_for('main.index'))
    scraper = current_app.extensions['scraper']
    state = current_app.extensions.setdefault('scrape_state', {})
    from core.background import start_scrape_async
    started = start_scrape_async(scraper, pages_i, state, params={})
    if started:
        flash(f'スクレイプ開始 (pages={pages_i})', 'info')
    else:
        flash('既にスクレイプ実行中です', 'error')
    return redirect(url_for('main.index'))


@bp.route('/files/<name>')
def show_file(name: str):
    if not name.startswith(SUMMARY_PREFIX) or not name.endswith(SUMMARY_SUFFIX):
        flash('不正なファイル名', 'error')
        return redirect(url_for('main.index'))
    path = Path(name)
    if not path.exists():
        flash('存在しません', 'error')
        return redirect(url_for('main.index'))
    # Render markdown inside simple <pre> for now
    content = path.read_text(encoding='utf-8')
    return f"<h1>{name}</h1><pre style='white-space:pre-wrap'>{content}</pre><p><a href='{url_for('main.index')}'>戻る</a></p>"


@bp.route('/download/<name>')
def download_file(name: str):
    path = Path(name)
    if not path.exists():
        flash('存在しません', 'error')
        return redirect(url_for('main.index'))
    return send_file(path, as_attachment=True)


@bp.route('/pivot')
def pivot():
    return render_template('pivot.html')

@bp.route('/pivot/columns')
def pivot_columns():
    import sqlite3
    from core.db import get_connection
    conn = get_connection()
    try:
        with conn:
            cur = conn.execute('PRAGMA table_info(car)')
            cols = [r[1] for r in cur.fetchall()]
        return {'columns': cols} if 'columns' in request.args else cols
    finally:
        conn.close()

@bp.route('/pivot/data')
def pivot_data():
    # パラメータ取得
    col = request.args.get('col')
    row = request.args.get('row')
    val = request.args.get('val')
    agg = request.args.get('agg','count')
    # フィルタ
    filters = []
    params = {}
    import sqlite3
    from core.db import get_connection
    conn = get_connection()
    try:
        with conn:
            # 動的フィルタ
            for k,v in request.args.items():
                if k.startswith('filter_') and v:
                    colf = k[7:]
                    filters.append(f'{colf} = :{colf}')
                    params[colf] = v
            where = ('WHERE ' + ' AND '.join(filters)) if filters else ''
            # SQL生成
            if agg=='count':
                sql = f'SELECT {row},{col},COUNT(*) FROM car {where} GROUP BY {row},{col}'
            elif agg=='sum':
                sql = f'SELECT {row},{col},SUM({val}) FROM car {where} GROUP BY {row},{col}'
            elif agg=='avg':
                sql = f'SELECT {row},{col},AVG({val}) FROM car {where} GROUP BY {row},{col}'
            else:
                return {'error':'invalid agg'}
            cur = conn.execute(sql, params)
            data = cur.fetchall()
            # 軸値抽出
            rows = sorted(set(r[0] for r in data if r[0] is not None))
            cols = sorted(set(r[1] for r in data if r[1] is not None))
            table = [[0 for _ in cols] for _ in rows]
            maxv = 0
            for r in data:
                if r[0] is None or r[1] is None:
                    continue  # Skip rows with None values in row or column fields
                i = rows.index(r[0])
                j = cols.index(r[1])
                v = r[2] if r[2] is not None else 0
                table[i][j] = v
                if isinstance(v, (int, float)) and v > maxv:
                    maxv = v
            return {'rows':rows,'cols':cols,'table':table,'max':maxv}
    finally:
        conn.close()
