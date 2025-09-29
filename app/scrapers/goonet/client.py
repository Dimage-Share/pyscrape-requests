from __future__ import annotations
import logging
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any
import json
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry


logger = logging.getLogger(__name__)

DEFAULT_FULL_URL = "https://www.goo-net.com/php/search/summary.php"

# Attempt to load external config.json at project root.
_CONFIG_JSON_PATH = Path(__file__).resolve().parent.parent.parent / 'config.json'
_resolved_summary_url = DEFAULT_FULL_URL
if _CONFIG_JSON_PATH.exists():
    try:
        with _CONFIG_JSON_PATH.open('r', encoding='utf-8') as f:
            _cfg = json.load(f) or {}
        if isinstance(_cfg, dict):
            # Priority 1: 'url'
            if 'url' in _cfg and isinstance(_cfg['url'], str) and _cfg['url'].strip():
                _resolved_summary_url = _cfg['url'].strip()
            else:
                # Backward compatibility: base_url + summary_path
                base_url = _cfg.get('base_url') or 'https://www.goo-net.com'
                summary_path = _cfg.get('summary_path') or '/php/search/summary.php'
                _resolved_summary_url = base_url.rstrip('/') + summary_path
    except Exception as e:  # noqa: BLE001
        logger.debug('Failed to load config.json: %s', e)


@dataclass
class GooNetClientConfig:
    timeout: float = 15.0
    max_retries: int = 3
    backoff_factor: float = 0.5
    user_agent: str = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")
    # Kept for backward compatibility; not strictly needed with single full URL.
    base_url: str = ''
    respect_robots_txt: bool = True  # 今後実装用フラグ


class GooNetClient:
    """Goo-net サイトへ HTTP GET を行う軽量クライアント。

    Selenium を使わず requests + Retry で高速化。JavaScript 依存が強い部分は
    取得できない可能性があるため、必要に応じてヘッドレスブラウザ fallback を検討する。
    """
    
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
        """
        サマリーページ HTML を返す。

        params 例: {'brand_cd': 'XXXX', ...}
        今はパラメータ仕様を把握していないので呼び出し側で自由に指定。
        """
        url = _resolved_summary_url
        if not getattr(self, '_logged_base', False):
            logger.debug('Config summary url=%s', url)
            self._logged_base = True
        start = time.time()
        logger.debug("HTTP GET start url=%s params=%s", url, params)
        resp = self.session.get(
            url,
            params=params,
            timeout=self.config.timeout,
        )
        elapsed = time.time() - start
        logger.debug("HTTP GET done url=%s final=%s time=%.2fs status=%s", url, resp.url, elapsed, resp.status_code)
        resp.raise_for_status()
        return resp.text
    
    def close(self):
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
