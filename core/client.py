from __future__ import annotations
"""Migrated from goo_net_scrape.client (transitional)."""

import time
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter, Retry

from .logger import Logger


log = Logger.bind(__name__)

DEFAULT_FULL_URL = "https://www.goo-net.com/php/search/summary.php"
CAR_SENSOR_FIRST_URL = "https://www.carsensor.net/usedcar/search.php?AR=4&SKIND=1"

_CONFIG_JSON_PATH = Path(__file__).resolve().parent.parent / 'config.json'
_resolved_summary_url = DEFAULT_FULL_URL
_resolved_carsensor_url = CAR_SENSOR_FIRST_URL
if _CONFIG_JSON_PATH.exists():
    try:
        with _CONFIG_JSON_PATH.open('r', encoding='utf-8') as f:
            _cfg = json.load(f) or {}
        if isinstance(_cfg, dict):
            # GooNet用
            if 'url' in _cfg and isinstance(_cfg['url'], str) and _cfg['url'].strip():
                _resolved_summary_url = _cfg['url'].strip()
            else:
                base_url = _cfg.get('base_url') or 'https://www.goo-net.com'
                summary_path = _cfg.get('summary_path') or '/php/search/summary.php'
                _resolved_summary_url = base_url.rstrip('/') + summary_path
            # CarSensor用 (carsensor_url キーがあれば上書き)
            if 'carsensor_url' in _cfg and isinstance(_cfg['carsensor_url'], str) and _cfg['carsensor_url'].strip():
                _resolved_carsensor_url = _cfg['carsensor_url'].strip()
    except Exception as e:  # noqa: BLE001
        log.debug(f"config.json load fail error={e}")


@dataclass
class GooNetClientConfig:
    timeout: float = 15.0
    max_retries: int = 3
    backoff_factor: float = 0.5
    user_agent: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
    base_url: str = ''
    respect_robots_txt: bool = True  # placeholder for future


class GooNetClient:
    
    def __init__(self, config: Optional[GooNetClientConfig] = None):
        self.config = config or GooNetClientConfig()
        self.session = requests.Session()
        retries = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", ),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "User-Agent": self.config.user_agent,
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Connection": "keep-alive",
        })
    
    def get_summary_page(self, params: Optional[Dict[str, Any]] = None) -> str:
        url = _resolved_summary_url
        if not getattr(self, '_logged_base', False):
            log.debug(f"config summary url={url}")
            self._logged_base = True
        start = time.time()
        log.debug(f"http get start url={url} params={params}")
        resp = self.session.get(
            url,
            params=params,
            timeout=self.config.timeout,
        )
        elapsed = time.time() - start
        log.debug(f"http get done url={url} final={resp.url} elapsed={elapsed:.2f}s status={resp.status_code}")
        resp.raise_for_status()
        # 明示的にUTF-8扱い (一部サイトで charset未指定時に推測誤り→文字化け防止)
        resp.encoding = 'utf-8'
        return resp.text
    
    def close(self):
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CarSensorClientConfig(GooNetClientConfig):  # reuse base dataclass fields
    pass


class CarSensorClient(GooNetClient):
    """CarSensor向けクライアント。

    初回ページは search.php のクエリ形式、2ページ目以降は index2.html 系リンクを直接取得するため、
    get_summary_page(params) は params を無視し最初のURLを返す。
    追加ページは Scrape 側で next URL を直接 session.get する運用。
    """

    def __init__(self, config: Optional[CarSensorClientConfig] = None):  # type: ignore[override]
        super().__init__(config=config or CarSensorClientConfig())

    def get_summary_page(self, params: Optional[Dict[str, Any]] = None) -> str:  # type: ignore[override]
        url = _resolved_carsensor_url
        if not getattr(self, '_logged_base', False):
            log.debug(f"carsensor base url={url}")
            self._logged_base = True
        resp = self.session.get(url, timeout=self.config.timeout)
        log.debug(f"carsensor get done status={resp.status_code} final={resp.url}")
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        return resp.text
