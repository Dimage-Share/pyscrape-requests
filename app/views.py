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
    # Collect filter parameters from query string
    filters = {
        'bodytype': request.args.get('bodytype') or '',
        'mission': request.args.get('mission') or '',
        'repair': request.args.get('repair') or '',
        'location': request.args.get('location') or '',
        'year': request.args.get('year') or '',
        'price_max': request.args.get('price_max') or '',
        'price_min': request.args.get('price_min') or ''
    }
    # Sorting parameters (whitelist columns)
    allowed_sort_cols = ['price', 'year', 'rd', 'engine', 'name', 'mission', 'bodytype', 'repair', 'location']
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
    if filters['bodytype']:
        where.append('bodytype = :bodytype')
        params['bodytype'] = filters['bodytype']
    if filters['mission']:
        where.append('mission = :mission')
        params['mission'] = filters['mission']
    if filters['repair']:
        where.append('repair = :repair')
        params['repair'] = filters['repair']
    if filters['location']:
        where.append('location = :location')
        params['location'] = filters['location']
    if filters['year']:
        try:
            params['year'] = int(filters['year'])
            where.append('year = :year')
        except Exception:
            flash('yearは整数', 'error')
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
    sql = 'SELECT id,name,price,year,rd,engine,mission,bodytype,repair,location,url FROM goo'
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += f' ORDER BY {sort} {dir_.upper()}{secondary_order} LIMIT 300'
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
            missions = _vals('mission')
            repairs = _vals('repair')
            locations = _vals('location')
            years = [r[0] for r in conn.execute('SELECT DISTINCT year FROM goo WHERE year IS NOT NULL ORDER BY year DESC LIMIT 50').fetchall()]
    finally:
        conn.close()
    files = _list_summary_files()
    return render_template('index.html', files=files, rows=rows, filters=filters, bodytypes=bodytypes, missions=missions, repairs=repairs, locations=locations, years=years, sort=sort, dir=dir_)


@bp.route('/scrape', methods=['POST'])
def scrape():
    pages = request.form.get('pages', '1')
    try:
        pages_i = max(1, int(pages))
    except Exception:
        flash('pages は整数で指定してください', 'error')
        return redirect(url_for('main.index'))
    scraper = current_app.extensions['scraper']
    path = scraper.run(pages_i, {})
    flash(f'Scrape done: {path.name}', 'info')
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
