from __future__ import annotations
"""Generic provider abstraction layer.

This module introduces a small pluggable provider interface so that the
scraping pipeline can be reused across different verticals (not only cars).

Design goals:
 - Minimal surface: fetch -> parse -> next page
 - Registry by string key for dynamic selection (CLI / config driven)
 - Backward compatibility: existing Goo / CarSensor wrappers will register
   themselves under keys 'goonet' and 'carsensor'.

Extensibility:
 - Providers may return custom record dataclasses; for uniform DB insertion
   they should implement to_db_row() or return dicts.
"""
from dataclasses import dataclass
from typing import Protocol, Iterable, Optional, Dict, Any, Callable


@dataclass
class GenericRecord:
    """A loose generic record structure for non-car domains.

    Downstream code can still prefer domain-specific (e.g. CarRecord), but this
    provides a fallback shape. Additional attributes can be stored in extras.
    """
    id: str
    title: Optional[str] = None
    url: Optional[str] = None
    price: Optional[int] = None
    year: Optional[int] = None
    extras: Dict[str, Any] | None = None
    
    def to_db_row(self) -> Dict[str, Any]:  # pragma: no cover - thin
        return {
            'id': self.id,
            'name': self.title,
            'url': self.url,
            'price': self.price,
            'year': self.year,
            'raw_json': self.extras or {},
        }


class Provider(Protocol):
    """Minimal interface all concrete providers implement."""
    
    key: str  # unique short name
    
    def fetch_first(self, params: Dict[str, Any] | None = None) -> str:
        """Return first listing page HTML (params optional)."""
    
    def parse_list(self, html: str) -> Iterable[Any]:  # list of domain records
        """Parse listing page HTML and yield record objects or dicts."""
    
    def next_page_url(self, html: str) -> Optional[str]:
        """Extract absolute URL of the next page or None."""
    
    # Optional detail expansion hook
    def enrich(self, record: Any) -> Any:  # pragma: no cover - default
        return record


_REGISTRY: Dict[str, Provider] = {}


def register(provider: Provider) -> None:
    k = provider.key.lower()
    if k in _REGISTRY:
        raise ValueError(f"provider key already registered: {k}")
    _REGISTRY[k] = provider


def get_provider(key: str) -> Provider:
    k = key.lower()
    if k not in _REGISTRY:
        raise KeyError(f"provider not found: {k}")
    return _REGISTRY[k]


def available_providers() -> list[str]:  # pragma: no cover - trivial
    return sorted(_REGISTRY.keys())


def provider_command(provider_key: str, pages: int = 1, *, dump: bool = False, dump_dir: str = 'dumps_generic') -> int:
    """High-level helper to run N pages for a provider.

    Returns number of parsed records (not DB writes)."""
    prov = get_provider(provider_key)
    from pathlib import Path
    from datetime import datetime
    from core.encoding import decode_response
    import requests
    
    html = prov.fetch_first(None)
    all_records = list(prov.parse_list(html))
    fetched = 1
    session = requests.Session()
    while fetched < pages:
        nxt = prov.next_page_url(html)
        if not nxt:
            break
        resp = session.get(nxt, timeout=20)
        html = decode_response(resp)
        recs = list(prov.parse_list(html))
        all_records.extend(recs)
        fetched += 1
    if dump:
        Path(dump_dir).mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        (Path(dump_dir) / f"{provider_key}_{stamp}.json").write_text(str(len(all_records)), encoding='utf-8')
    return len(all_records)
