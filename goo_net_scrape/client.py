from __future__ import annotations
import logging
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any
import json
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
try:
    # app.db may not always be importable at early bootstrap; guard it
    from app.db import get_connection
except Exception:  # pragma: no cover
    get_connection = None  # type: ignore

logger = logging.getLogger(__name__)

DEFAULT_FULL_URL = "https://www.goo-net.com/php/search/summary.php"

# Attempt to load external config.json at project root.
_CONFIG_JSON_PATH = Path(__file__).resolve().parent.parent / 'config.json'
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
    取得できない可能性があるため、必要に応じて後でヘッドレスブラウザ fallback を検討。
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
        """サマリーページ HTML を返す。

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
        # detailed fetch log only at DEBUG (INFO reserved for page boundaries)
        logger.debug("HTTP GET done url=%s final=%s time=%.2fs status=%s", url, resp.url, elapsed, resp.status_code)
        resp.raise_for_status()
        # Better encoding detection: try a list of candidate encodings one-by-one
        content_bytes = resp.content
        candidates = []
        if resp.encoding:
            candidates.append(resp.encoding)
        # Try BeautifulSoup detection from meta tags
        try:
            from bs4 import BeautifulSoup as _BS
            
            bs = _BS(content_bytes, 'lxml')
            if getattr(bs, 'original_encoding', None):
                candidates.append(bs.original_encoding)
        except Exception:
            pass
        if resp.apparent_encoding:
            candidates.append(resp.apparent_encoding)
        
        # Common Japanese encodings as last resort
        candidates.extend(['utf-8', 'cp932', 'shift_jis', 'euc_jp', 'iso-2022-jp'])
        
        # normalize and dedupe while preserving order
        seen = set()
        cand_list = []
        for c in candidates:
            if not c:
                continue
            cn = str(c).lower()
            if cn in seen:
                continue
            seen.add(cn)
            cand_list.append(c)
        
        def score_text(s: str) -> float:
            if not s:
                return 0.0
            total = len(s)
            if total == 0:
                return 0.0
            jp = 0
            repl = s.count('\ufffd')
            for ch in s:
                code = ord(ch)
                # Hiragana
                if 0x3040 <= code <= 0x309F:
                    jp += 1
                # Katakana (basic + phonetic extensions)
                elif 0x30A0 <= code <= 0x30FF or 0x31F0 <= code <= 0x31FF:
                    jp += 1
                # CJK Unified Ideographs (a broad range; may include Chinese but acceptable heuristic)
                elif 0x4E00 <= code <= 0x9FFF:
                    jp += 1
            ratio = jp / total
            # penalty for replacement chars or too many ASCII only when we expect Japanese
            penalty = 0.0
            if repl:
                penalty += min(0.5, repl / total)
            return max(0.0, ratio - penalty)
        
        best = {
            'encoding': None,
            'text': None,
            'score': -1.0,
        }
        tried = []
        for enc in cand_list:
            try:
                candidate_text = content_bytes.decode(enc, errors='strict')
            except (LookupError, UnicodeDecodeError):
                continue
            s = score_text(candidate_text)
            tried.append((enc, s))
            if s > best['score']:
                best.update({
                    'encoding': enc,
                    'text': candidate_text,
                    'score': s
                })
        
        if best['text'] is None:
            # fallback path identical to prior implementation
            fallback_enc = cand_list[0] if cand_list else 'utf-8'
            logger.warning('Encoding autodetect failed (scoring stage); falling back to %s with replace', fallback_enc)
            try:
                best['text'] = content_bytes.decode(fallback_enc, errors='replace')
                best['encoding'] = fallback_enc
            except Exception:
                best['text'] = content_bytes.decode('utf-8', errors='replace')
                best['encoding'] = 'utf-8'
            best['score'] = score_text(best['text'])
        
        tried_sorted = sorted(tried, key=lambda x: x[1], reverse=True)
        logger.debug('Decoded HTTP response using encoding=%s score=%.4f candidates_scored=%s url=%s', best['encoding'], best['score'], tried_sorted[:5], resp.url)
        # Persist encoding detection (best and top scores) if DB available
        if get_connection and best.get('encoding'):
            try:
                conn = get_connection()
                cur = conn.cursor()
                # create table if not exists (lightweight)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS encoding_log (
                        id INTEGER PRIMARY KEY AUTO_INCREMENT,
                        site VARCHAR(32),
                        url TEXT,
                        chosen_encoding VARCHAR(64),
                        score DOUBLE,
                        top_candidates TEXT,
                        created_at DATETIME,
                        KEY idx_site_created (site, created_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """)
                # SQLite compatibility (AUTO_INCREMENT unsupported) -> fallback create
            except Exception:
                try:
                    # second attempt for sqlite variant
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS encoding_log (
                            id INTEGER PRIMARY KEY,
                            site TEXT,
                            url TEXT,
                            chosen_encoding TEXT,
                            score REAL,
                            top_candidates TEXT,
                            created_at TEXT
                        )
                        """)
                except Exception:  # pragma: no cover
                    pass
            try:
                top_serial = ','.join([f"{e}:{s:.3f}" for e, s in tried_sorted[:5]])
                insert_sql = ("INSERT INTO encoding_log (site,url,chosen_encoding,score,top_candidates,created_at) VALUES (%s,%s,%s,%s,%s,%s)")
                params = ('goo', resp.url, best['encoding'], float(best['score']), top_serial, datetime.utcnow())
                try:
                    cur.execute(insert_sql, params)
                except Exception:
                    # sqlite named style fallback
                    cur.execute(
                        "INSERT INTO encoding_log (site,url,chosen_encoding,score,top_candidates,created_at) VALUES (?,?,?,?,?,?)",
                        params,
                    )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        return best['text']
    
    def close(self):
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def cleanup_encoding_log(retention_days: int = 7) -> int:
    """Delete old rows from encoding_log keeping recent retention_days.
    Returns deleted row count (best effort)."""
    if not get_connection:
        return 0
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM encoding_log WHERE created_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)", (retention_days, ))
        except Exception:
            # sqlite fallback
            cur.execute("DELETE FROM encoding_log WHERE datetime(created_at) < datetime('now','-%d day')" % retention_days)
        deleted = cur.rowcount if hasattr(cur, 'rowcount') else 0
        conn.commit()
        return deleted
    except Exception:
        try:
            conn.rollback()  # type: ignore
        except Exception:
            pass
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
