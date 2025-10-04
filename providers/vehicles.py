from __future__ import annotations
"""Vehicle domain provider wrappers for legacy GooNet & CarSensor implementations."""
from typing import Iterable, Any, Optional, Dict
from dataclasses import dataclass

from providers.base import Provider, register
from core.encoding import decode_response
import logging


log = logging.getLogger(__name__)


@dataclass
class _GooWrapper(Provider):
    key: str = 'goonet'
    
    def __post_init__(self):  # lazy imports to avoid circulars
        from goo_net_scrape.client import GooNetClient  # type: ignore
        from goo_net_scrape import parser as gparser  # type: ignore
        self._client_cls = GooNetClient
        self._parser = gparser
    
    def fetch_first(self, params: Dict[str, Any] | None = None) -> str:
        with self._client_cls() as c:  # type: ignore[attr-defined]
            html = c.get_summary_page(params=None)
        return html
    
    def parse_list(self, html: str) -> Iterable[Any]:
        return self._parser.parse_cars(html)
    
    def next_page_url(self, html: str) -> Optional[str]:
        return self._parser.get_next_page_url(html)


@dataclass
class _CarSensorWrapper(Provider):
    key: str = 'carsensor'
    
    def __post_init__(self):
        from core.client import CarSensorClient  # type: ignore
        from core.carsensor_parser import parse_cars_carsensor, get_next_page_url_carsensor
        self._client_cls = CarSensorClient
        self._parse = parse_cars_carsensor
        self._next = get_next_page_url_carsensor
    
    def fetch_first(self, params: Dict[str, Any] | None = None) -> str:
        with self._client_cls() as c:  # type: ignore[attr-defined]
            html = c.get_summary_page(params=None)
        return html
    
    def parse_list(self, html: str) -> Iterable[Any]:
        return self._parse(html)
    
    def next_page_url(self, html: str) -> Optional[str]:
        return self._next(html, '')


def _auto_register():  # pragma: no cover
    try:
        register(_GooWrapper())
        register(_CarSensorWrapper())
    except Exception as e:  # noqa: BLE001
        log.debug(f"provider registration issue: {e}")


_auto_register()
