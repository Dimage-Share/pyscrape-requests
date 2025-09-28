from __future__ import annotations
"""Background scraping utilities.

Provides a simple thread-based asynchronous runner so the Flask web app can
trigger a scrape job without blocking the request thread.

State dict (shared via app.extensions['scrape_state']):
    running: bool
    last_started: datetime | None
    last_finished: datetime | None
    last_error: str | None
    last_pages: int | None
    progress: dict | None   # optional future use {current, total}
"""
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Callable

from .logger import Logger
from .scrape import Scrape

log = Logger.bind(__name__)

# Type alias for shared mutable state
def _utc_now():
    return datetime.now(timezone.utc)


def start_scrape_async(scraper: Scrape, pages: int, state: Dict[str, Any], params: Dict[str, Any] | None = None) -> bool:
    """Start a background scrape if not already running.

    Returns True if a new job was started, False if a job is already running.
    """
    if state.get('running'):
        return False
    params = params or {}
    state.update({
        'running': True,
        'last_started': _utc_now(),
        'last_pages': pages,
        'last_error': None,
        'progress': {'current': 0, 'total': pages}
    })

    def _run():  # executed in thread
        log.info(f"background scrape start pages={pages}")
        try:
            # Currently Scrape.run truncates + bulk inserts at end.
            scraper.run(pages, params)
            state['last_error'] = None
            log.info("background scrape done")
        except Exception as e:  # noqa: BLE001
            state['last_error'] = str(e)
            log.exception(e)
        finally:
            state['last_finished'] = _utc_now()
            state['running'] = False
            state['progress'] = None

    t = threading.Thread(target=_run, name='scrape-worker', daemon=True)
    t.start()
    return True
