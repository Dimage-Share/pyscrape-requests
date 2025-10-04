from __future__ import annotations
from flask import current_app, render_template, redirect, url_for, send_file, request, flash
from pathlib import Path
from typing import List
import re


SUMMARY_PREFIX = 'summary-'
SUMMARY_SUFFIX = '.md'


def _list_summary_files() -> List[str]:
    files = []
    for p in Path('.').glob(f'{SUMMARY_PREFIX}*{SUMMARY_SUFFIX}'):
        files.append(p.name)
    return sorted(files)


def register(bp):
    
    @bp.route('/')
    def index():
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
        # Initialize builder state BEFORE applying any filter clauses (以前は途中でリセットされ engine 条件が消失していた)
        where = []
        params = {}
        # --- First phase: numeric/LIKE 系フィルタを順次積み上げ ---
        debug_applied = []  # collect applied conditions for debug logging
        # wd, seat, fuel, handle: 部分一致 or null
        if filters['wd']:
            where.append('(`wd` LIKE %(wd)s OR `wd` IS NULL OR `wd` = "")')
            params['wd'] = f"%{filters['wd']}%"
            debug_applied.append('wd')
        if filters['seat']:
            where.append('`seat` = %(seat)s')
            params['seat'] = filters['seat']
            debug_applied.append('seat')
        if filters['door'] != '':  # allow '0' 値防止のため != '' 判定
            where.append('`door` = %(door)s')
            params['door'] = filters['door']
            debug_applied.append('door')
        if filters['fuel']:
            where.append('(`fuel` LIKE %(fuel)s OR `fuel` IS NULL OR `fuel` = "")')
            params['fuel'] = f"%{filters['fuel']}%"
            debug_applied.append('fuel')
        if filters['handle']:
            where.append('(`handle` LIKE %(handle)s OR `handle` IS NULL OR `handle` = "")')
            params['handle'] = f"%{filters['handle']}%"
            debug_applied.append('handle')
        # jc08: 入力値以上またはnull
        if filters['jc08']:
            try:
                params['jc08'] = float(filters['jc08'])
                where.append('(CAST(`jc08` AS FLOAT) >= %(jc08)s OR `jc08` IS NULL OR `jc08` = "")')
                debug_applied.append('jc08>=')
            except Exception:
                flash('JC08は数値で入力してください', 'error')
        # engine: 選択した値以上の排気量
        if filters.get('engine'):
            try:
                params['engine'] = int(filters['engine'])
                where.append('(`engine` >= %(engine)s OR `engine` IS NULL OR `engine` = "")')
                debug_applied.append('engine>=')
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
            secondary_order = ', price ASC'
        if sort != 'year':
            secondary_order += ', year DESC'
        
        # ここで second phase の equality 系 / free-text を追加 (初期化しないことが重要)
        # Global free-text search across multiple columns
        q = request.args.get('q', '').strip()
        if q:
            qcols = ['manufacturer', 'name', 'mission1', 'mission2', 'bodytype', 'repair', 'location', 'option', 'category', 'wd', 'fuel', 'handle', 'url', 'raw_json']
            like_clauses = []
            for c in qcols:
                like_clauses.append(f"`{c}` LIKE %(q)s")
            where.append('(' + ' OR '.join(like_clauses) + ')')
            params['q'] = f"%{q}%"
        if filters['manufacturer']:
            where.append('`manufacturer` = %(manufacturer)s')
            params['manufacturer'] = filters['manufacturer']
            debug_applied.append('manufacturer=')
        if filters['name']:
            where.append('`name` = %(name)s')
            params['name'] = filters['name']
            debug_applied.append('name=')
        if filters['mission1']:
            where.append('`mission1` = %(mission1)s')
            params['mission1'] = filters['mission1']
            debug_applied.append('mission1=')
        if filters['mission2']:
            where.append('`mission2` = %(mission2)s')
            params['mission2'] = filters['mission2']
            debug_applied.append('mission2=')
        if filters['category']:
            where.append('`category` = %(category)s')
            params['category'] = filters['category']
            debug_applied.append('category=')
        if filters['bodytype']:
            where.append('`bodytype` = %(bodytype)s')
            params['bodytype'] = filters['bodytype']
            debug_applied.append('bodytype=')
        if filters['repair']:
            where.append('`repair` = %(repair)s')
            params['repair'] = filters['repair']
            debug_applied.append('repair=')
        if filters['location']:
            where.append('`location` = %(location)s')
            params['location'] = filters['location']
            debug_applied.append('location=')
        if filters['year']:
            try:
                params['year'] = int(filters['year'])
                where.append('(`year` >= %(year)s OR `year` IS NULL)')
                debug_applied.append('year>=')
            except Exception:
                flash('年式は整数で入力してください', 'error')
        if filters['price_min']:
            try:
                params['price_min'] = int(filters['price_min'])
                where.append('`price` >= %(price_min)s')
                debug_applied.append('price>=')
            except Exception:
                flash('price_minは整数', 'error')
        if filters['price_max']:
            try:
                params['price_max'] = int(filters['price_max'])
                where.append('`price` <= %(price_max)s')
                debug_applied.append('price<=')
            except Exception:
                flash('price_maxは整数', 'error')
        # listing へ統一 (site カラムも取得)
        select_cols = ['site', 'id', 'manufacturer', 'name', 'price', 'year', 'rd', 'engine', 'mission1', 'mission2', 'bodytype', 'repair', 'location', 'wd', 'seat', 'door', 'fuel', 'handle', 'jc08', 'option', 'category', 'url']
        cols_sql = ','.join([f'`{c}`' for c in select_cols])
        sql = f'SELECT {cols_sql} FROM listing'
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        # Quote order-by columns as well
        secondary_order = ''
        if sort != 'price':
            secondary_order = ', `price` ASC'
        if sort != 'year':
            secondary_order += ', `year` DESC'
        sql += f' ORDER BY `{sort}` {dir_.upper()}{secondary_order} LIMIT 1000'
        # Log query
        try:
            from core import logger as _l
            _l.Logger.bind(__name__).info(f"SearchSQL alt: {sql} params={params} applied={debug_applied} raw_filters={filters}")
        except Exception:
            pass
        
        # Fetch distinct values for dropdowns
        from core.db import get_connection
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                fetched = cur.fetchall()
                # If cursor returns mapping-like rows (dict), use them directly; otherwise zip description with sequence
                rows = []
                if fetched:
                    first = fetched[0]
                    if isinstance(first, dict):
                        rows = [dict(r) for r in fetched]
                    else:
                        cols = [c[0] for c in cur.description]
                        rows = [dict(zip(cols, r)) for r in fetched]
                # Normalize numeric fields so Jinja formatting won't fail when values are strings
                for row in rows:
                    # normalize empty strings to None
                    for k in list(row.keys()):
                        if row[k] == '':
                            row[k] = None
                    # price, rd (走行距離), engine, year, jc08 may be numeric but returned as strings
                    for num_col in ('price', 'rd', 'engine', 'year'):
                        v = row.get(num_col)
                        if v is None:
                            continue
                        try:
                            # decode bytes
                            if isinstance(v, (bytes, bytearray)):
                                v = v.decode('utf-8', errors='ignore')
                            # if string, remove common thousands separators and non-numeric chars
                            if isinstance(v, str):
                                s = v.strip()
                                # replace fullwidth comma, normal comma, spaces
                                s = s.replace('，', '').replace(',', '').replace('\u00A0', '').replace(' ', '')
                                # remove any non-digit, non-dot, non-minus
                                s = re.sub(r"[^0-9\.\-]", '', s)
                                if s == '':
                                    continue
                                v = s
                            row[num_col] = int(float(v))
                        except Exception:
                            # leave as-is (could be non-numeric)
                            pass
                    # jc08 may be float
                    v = row.get('jc08')
                    if v is not None:
                        try:
                            if isinstance(v, (bytes, bytearray)):
                                v = v.decode('utf-8', errors='ignore')
                            if isinstance(v, str):
                                s = v.strip().replace('，', '').replace(',', '').replace('\u00A0', '').replace(' ', '')
                                s = re.sub(r"[^0-9\.\-]", '', s)
                                if s == '':
                                    raise ValueError('empty')
                                v = s
                            row['jc08'] = float(v)
                        except Exception:
                            pass
            
            def _vals(col: str):
                with conn.cursor() as c2:
                    # Use parameter-less f-string; col is from whitelist usage in this file
                    c2.execute(f"SELECT DISTINCT `{col}` FROM listing WHERE `{col}` IS NOT NULL AND `{col}` != '' ORDER BY `{col}` LIMIT 200")
                    fetched = c2.fetchall()
                    vals = []
                    for v in fetched:
                        # v may be a tuple/list (indexable by 0) or a dict (mapping column name -> value)
                        if isinstance(v, dict):
                            # prefer explicit column key when available
                            if col in v:
                                vals.append(v[col])
                            else:
                                # fall back to first value
                                try:
                                    vals.append(next(iter(v.values())))
                                except StopIteration:
                                    vals.append(None)
                        else:
                            # assume sequence-like
                            try:
                                vals.append(v[0])
                            except Exception:
                                vals.append(None)
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
                cy.execute('SELECT DISTINCT year FROM listing WHERE year IS NOT NULL ORDER BY year DESC LIMIT 50')
                fetched_years = cy.fetchall()
                years = []
                for r in fetched_years:
                    if isinstance(r, dict):
                        # try key 'year' then first value
                        if 'year' in r:
                            years.append(r['year'])
                        else:
                            try:
                                years.append(next(iter(r.values())))
                            except StopIteration:
                                years.append(None)
                    else:
                        try:
                            years.append(r[0])
                        except Exception:
                            years.append(None)
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
            try:
                conn.close()
            except Exception:
                pass
        files = _list_summary_files()
        state = current_app.extensions.get('scrape_state') or {}
        # fetch listing counts grouped by site (e.g. 'goo', 'carsensor')
        listing_counts = {
            'total': 0,
            'goo': 0,
            'carsensor': 0
        }
        try:
            from core.db import get_connection as _get_conn
            conn2 = _get_conn()
            try:
                with conn2.cursor() as cc:
                    cc.execute('SELECT `site`, COUNT(*) AS cnt FROM listing GROUP BY `site`')
                    rows_cnt = cc.fetchall()
                    for r in rows_cnt or []:
                        if isinstance(r, dict):
                            site = r.get('site')
                            cnt = int(r.get('cnt') or 0)
                        else:
                            site = r[0]
                            cnt = int(r[1] or 0)
                        listing_counts[site] = cnt
                        listing_counts['total'] += cnt
            finally:
                try:
                    conn2.close()
                except Exception:
                    pass
        except Exception:
            # keep defaults on any error
            pass
        
        # convert listing_counts dict into list of (site, count) pairs sorted by count desc for template
        listing_counts_list = sorted([(k, v) for k, v in listing_counts.items() if k != 'total'], key=lambda x: x[1], reverse=True)
        return render_template('index.html', files=files, rows=rows, filters=filters, bodytypes=bodytypes, manufacturers=manufacturers, names=names, mission1s=mission1s, mission2s=mission2s, repairs=repairs, locations=locations, years=years, categories=categories, wds=wds, seats=seats, doors=doors, fuels=fuels, handles=handles, engines=engines, price_options=price_options,
                               sort=sort, dir=dir_, state=state, listing_counts=listing_counts, listing_counts_list=listing_counts_list)
    
    @bp.route('/files/<name>')
    def show_file(name: str):
        if not name.startswith(SUMMARY_PREFIX) or not name.endswith(SUMMARY_SUFFIX):
            flash('不正なファイル名', 'error')
            return redirect(url_for('main.index'))
        path = Path(name)
        if not path.exists():
            flash('存在しません', 'error')
            return redirect(url_for('main.index'))
        content = path.read_text(encoding='utf-8')
        return f"<h1>{name}</h1><pre style='white-space:pre-wrap'>{content}</pre><p><a href='{url_for('main.index')}'>戻る</a></p>"
    
    @bp.route('/download/<name>')
    def download_file(name: str):
        path = Path(name)
        if not path.exists():
            flash('存在しません', 'error')
            return redirect(url_for('main.index'))
        return send_file(path, as_attachment=True)
