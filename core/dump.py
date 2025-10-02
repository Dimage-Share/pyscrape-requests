from __future__ import annotations
"""Page dump helper utilities.

Provides a single function `dump_page` used by scraping flows to persist
raw HTML snippet + parsed records for diagnostics and regression tracking.
"""
import json
from pathlib import Path
from datetime import datetime, UTC
from typing import Any, Iterable


def dump_page(*, dump_dir: Path, site: str, page_number: int, url: str, html: str, records: Iterable[Any], meta: dict | None = None, snippet_bytes: int = 8000, write_full_html: bool = True) -> Path:
    dump_dir.mkdir(parents=True, exist_ok=True)
    out_path = dump_dir / f"{site}_page{page_number}.json"
    if write_full_html:
        try:
            (dump_dir / f"{site}_page{page_number}.html").write_text(html, encoding='utf-8', errors='replace')
        except Exception:
            pass
    items = []
    for r in records:
        if hasattr(r, 'to_db_row'):
            try:
                items.append(r.to_db_row())
            except Exception:
                pass
        else:
            items.append(r)
    payload = {
        'site': site,
        'page': page_number,
        'url': url,
        'fetched_at': datetime.now(UTC).isoformat(),
        'html_length': len(html),
        'html_snippet': html[:snippet_bytes],
        'records_count': len(items),
        'records': items,
        'meta': meta or {},
        # optional top-level chosen encoding for easier programmatic access
        'chosen_encoding': (meta or {}).get('chosen_encoding') if meta is not None else None,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return out_path
