from __future__ import annotations
from flask import Flask
from pathlib import Path
from typing import Any, Dict

from core.scrape import Scrape


def create_app(config: Dict[str, Any] | None = None) -> Flask:
    # Explicit template folder path (project root/templates)
    base_dir = Path(__file__).resolve().parent.parent
    template_dir = base_dir / 'templates'
    app = Flask(__name__, template_folder=str(template_dir))
    if not template_dir.exists():
        print(f"[WARN] template dir not found: {template_dir}")
    else:
        print(f"[INFO] template dir: {template_dir}")
    if config:
        app.config.update(config)
    
    # Lazy scraper instance (simple singleton in app context)
    scraper = Scrape()
    
    from .views import bp  # noqa: WPS433 (late import to avoid circular)
    app.register_blueprint(bp)
    
    # Expose scraper + shared scrape state via app extensions for access in views
    app.extensions['scraper'] = scraper
    app.extensions['scrape_state'] = {
        'running': False,
        'last_started': None,
        'last_finished': None,
        'last_error': None,
        'last_pages': None,
        'progress': None,
    }
    
    @app.cli.command('scrape')
    def scrape_command():  # pragma: no cover - CLI helper
        pages = int(app.config.get('SCRAPE_PAGES', 1))
        path = scraper.run(pages, {})
        print(f"Generated: {path}")
    
    return app
