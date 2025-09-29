from __future__ import annotations
from flask import current_app, redirect, url_for, request, flash


def register(bp):
    
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
