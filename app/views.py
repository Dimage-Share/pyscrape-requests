from __future__ import annotations
from flask import Blueprint, current_app, render_template, redirect, url_for, send_file, request, flash
from pathlib import Path
import os
from core import logger


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
    # Build SQL dynamically (既に上で where/params を構築しているため再初期化しない)
    # Collect filter parameters from query string
    filters = {
        'manufacturer': request.args.get('manufacturer') or '',
        'name': request.args.get('name') or '',
        'engine': request.args.get('engine') or '',
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
    # wd, seat, fuel, handle: 部分一致 or null (MySQL named style %(name)s)
    if filters['wd']:
        where.append('(`wd` LIKE %(wd)s OR `wd` IS NULL OR `wd` = "")')
        params['wd'] = f"%{filters['wd']}%"
    if filters['seat']:
        where.append('`seat` = %(seat)s')
        params['seat'] = filters['seat']
    if filters['door'] != '':
        where.append('`door` = %(door)s')
        params['door'] = filters['door']
    if filters['fuel']:
        where.append('(`fuel` LIKE %(fuel)s OR `fuel` IS NULL OR `fuel` = "")')
        params['fuel'] = f"%{filters['fuel']}%"
    if filters['handle']:
        where.append('(`handle` LIKE %(handle)s OR `handle` IS NULL OR `handle` = "")')
        params['handle'] = f"%{filters['handle']}%"
    # jc08: 入力値以上またはnull
    if filters['jc08']:
        try:
            params['jc08'] = float(filters['jc08'])
            where.append('(CAST(`jc08` AS DECIMAL(10,3)) >= %(jc08)s OR `jc08` IS NULL OR `jc08` = "")')
        except Exception:
            flash('JC08は数値で入力してください', 'error')
    # engine: 選択した値以上の排気量
    if filters.get('engine'):
        try:
            params['engine'] = int(filters['engine'])
            where.append('(`engine` >= %(engine)s OR `engine` IS NULL OR `engine` = "")')
        except Exception:
            flash('排気量は整数で入力してください', 'error')
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
        secondary_order = ', `price` ASC'
    if sort != 'year':
        secondary_order += ', `year` DESC'
    # Build SQL dynamically
    where = []
    params = {}
    # Global free-text search across multiple columns
    q = request.args.get('q', '').strip()
    if q:
        # search in text-like columns (use LIKE)
        qcols = ['manufacturer', 'name', 'mission1', 'mission2', 'bodytype', 'repair', 'location', 'option', 'category', 'wd', 'fuel', 'handle', 'url', 'raw_json']
        like_clauses = []
        for c in qcols:
            like_clauses.append(f"`{c}` LIKE %(q)s")
        where.append('(' + ' OR '.join(like_clauses) + ')')
        params['q'] = f"%{q}%"
    if filters['manufacturer']:
        where.append('`manufacturer` = %(manufacturer)s')
        params['manufacturer'] = filters['manufacturer']
    if filters['name']:
        where.append('`name` = %(name)s')
        params['name'] = filters['name']
    if filters['mission1']:
        where.append('`mission1` = %(mission1)s')
        params['mission1'] = filters['mission1']
    if filters['mission2']:
        where.append('`mission2` = %(mission2)s')
        params['mission2'] = filters['mission2']
    if filters['category']:
        where.append('`category` = %(category)s')
        params['category'] = filters['category']
    if filters['bodytype']:
        where.append('`bodytype` = %(bodytype)s')
        params['bodytype'] = filters['bodytype']
    if filters['repair']:
        where.append('`repair` = %(repair)s')
        params['repair'] = filters['repair']
    if filters['location']:
        where.append('`location` = %(location)s')
        params['location'] = filters['location']
    if filters['door']:
        where.append('`door` = %(door)s')
        params['door'] = filters['door']
    if filters['year']:
        try:
            params['year'] = int(filters['year'])
            where.append('(`year` >= %(year)s OR `year` IS NULL)')
        except Exception:
            flash('年式は整数で入力してください', 'error')
    if filters['price_min']:
        try:
            params['price_min'] = int(filters['price_min'])
            where.append('`price` >= %(price_min)s')
        except Exception:
            flash('price_minは整数', 'error')
    if filters['price_max']:
        try:
            params['price_max'] = int(filters['price_max'])
            where.append('`price` <= %(price_max)s')
        except Exception:
            flash('price_maxは整数', 'error')
    # listing テーブルへ統一 (site カラムは今後のフィルタ拡張用に保持可能)
    sql = 'SELECT site,id,manufacturer,name,price,year,rd,engine,mission1,mission2,bodytype,repair,location,wd,seat,door,fuel,handle,jc08,`option`,category,url FROM listing'
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += f' ORDER BY {sort} {dir_.upper()}{secondary_order} LIMIT 1000'
    # クエリログ (INFO)
    try:
        logger.Logger.bind(__name__).info(f"SearchSQL main: {sql} params={params}")
    except Exception:
        pass
    # Fetch distinct values for dropdowns
    from core.db import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Log the SQL and params at DEBUG level for troubleshooting. Do not let logging break the request.
            try:
                logger.Logger.bind(__name__).info(f"SearchSQL exec: {sql} params={params}")
            except Exception:
                pass
            cur.execute(sql, params)
            fetched = cur.fetchall()
            rows = []
            for r in fetched:
                # pymysql DictCursor gives dict
                if isinstance(r, dict):
                    rows.append(r)
                else:
                    # fallback tuple
                    rows.append(dict(zip([c[0] for c in cur.description], r)))
            
            def _vals(col):
                # listing から distinct 取得
                with conn.cursor() as c2:
                    sql_distinct = f'SELECT DISTINCT `{col}` AS val FROM listing WHERE `{col}` IS NOT NULL AND `{col}` != "" ORDER BY `{col}` LIMIT 200'
                    c2.execute(sql_distinct)
                    got = c2.fetchall()
                    vals = []
                    for v in got:
                        if isinstance(v, dict):
                            vals.append(v.get('val'))
                        else:
                            vals.append(v[0])
                    return vals
            
            bodytypes = _vals('bodytype')
            manufacturers = _vals('manufacturer')
            names = _vals('name')
            mission1s = _vals('mission1')
            engines = _vals('engine')
            mission2s = _vals('mission2')
            repairs = _vals('repair')
            locations = _vals('location')
            with conn.cursor() as cy:
                cy.execute('SELECT DISTINCT `year` AS y FROM listing WHERE `year` IS NOT NULL ORDER BY `year` DESC LIMIT 50')
                years = [(r.get('y') if isinstance(r, dict) else r[0]) for r in cy.fetchall()]
            categories = _vals('category')
            wds = _vals('wd')
            seats = _vals('seat')
            doors = _vals('door')
            fuels = _vals('fuel')
            handles = _vals('handle')
            # build fixed price options: 400,000 (40万) up to 10,000,000 (1000万) with 200,000 (20万) steps
            price_options = []
            start = 400000
            step = 200000
            max_price = 10000000
            p = start
            while p <= max_price:
                price_options.append(p)
                p += step
    finally:
        conn.close()
    files = _list_summary_files()
    state = current_app.extensions.get('scrape_state') or {}
    return render_template('index.html', files=files, rows=rows, filters=filters, bodytypes=bodytypes, manufacturers=manufacturers, names=names, mission1s=mission1s, mission2s=mission2s, repairs=repairs, locations=locations, years=years, categories=categories, wds=wds, seats=seats, doors=doors, fuels=fuels, handles=handles, engines=engines, price_options=price_options, sort=sort,
                           dir=dir_, state=state)


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
        return {
            'columns': cols
        } if 'columns' in request.args else cols
    finally:
        conn.close()


@bp.route('/pivot/data')
def pivot_data():
    # パラメータ取得
    col = request.args.get('col')
    row = request.args.get('row')
    val = request.args.get('val')
    agg = request.args.get('agg', 'count')
    # フィルタ
    filters = []
    params = {}
    import sqlite3
    from core.db import get_connection
    conn = get_connection()
    try:
        with conn:
            # 動的フィルタ
            for k, v in request.args.items():
                if k.startswith('filter_') and v:
                    colf = k[7:]
                    filters.append(f'{colf} = :{colf}')
                    params[colf] = v
            where = ('WHERE ' + ' AND '.join(filters)) if filters else ''
            # SQL生成
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
            return {
                'rows': rows,
                'cols': cols,
                'table': table,
                'max': maxv
            }
    finally:
        conn.close()
