from __future__ import annotations
from typing import Tuple
import logging


logger = logging.getLogger(__name__)


def _score_text(s: str) -> float:
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
        # Katakana
        elif 0x30A0 <= code <= 0x30FF or 0x31F0 <= code <= 0x31FF:
            jp += 1
        # CJK Unified Ideographs (broad heuristic)
        elif 0x4E00 <= code <= 0x9FFF:
            jp += 1
    ratio = jp / total
    penalty = 0.0
    if repl:
        penalty += min(0.5, repl / total)
    return max(0.0, ratio - penalty)


def decode_response(resp) -> str:
    """Decode a requests.Response robustly and return text.

    Strategy:
      1) Try charset from Content-Type header
      2) Try BeautifulSoup original_encoding (if bs4 available)
      3) Try resp.apparent_encoding
      4) Try common encodings list and pick best by heuristic scoring
      5) Fallback to resp.text
    The function sets resp.encoding to the chosen encoding when possible.
    """
    try:
        content_bytes = resp.content
        candidates = []
        # header charset
        try:
            content_type = resp.headers.get('Content-Type', '') or ''
            if 'charset=' in content_type.lower():
                parts = [p.strip() for p in content_type.split(';')]
                for p in parts:
                    if p.lower().startswith('charset='):
                        m = p.split('=', 1)[1].strip().strip('"')
                        if m:
                            candidates.append(m)
                            break
        except Exception:
            pass
        
        # BeautifulSoup original_encoding if available
        try:
            from bs4 import BeautifulSoup as _BS
            
            bs = _BS(content_bytes, 'lxml')
            if getattr(bs, 'original_encoding', None):
                candidates.append(bs.original_encoding)
        except Exception:
            pass
        
        if getattr(resp, 'apparent_encoding', None):
            candidates.append(resp.apparent_encoding)
        
        # common fallbacks
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
        
        best = {
            'encoding': None,
            'text': None,
            'score': -1.0
        }
        tried = []
        for enc in cand_list:
            try:
                txt = content_bytes.decode(enc, errors='strict')
            except Exception:
                continue
            s = _score_text(txt)
            tried.append((enc, s))
            if s > best['score']:
                best.update({
                    'encoding': enc,
                    'text': txt,
                    'score': s
                })
        
        if best['text'] is None:
            # last resort: try first candidate with replace
            fallback = cand_list[0] if cand_list else 'utf-8'
            try:
                txt = content_bytes.decode(fallback, errors='replace')
                best.update({
                    'encoding': fallback,
                    'text': txt,
                    'score': _score_text(txt)
                })
            except Exception:
                try:
                    txt = content_bytes.decode('utf-8', errors='replace')
                    best.update({
                        'encoding': 'utf-8',
                        'text': txt,
                        'score': _score_text(txt)
                    })
                except Exception:
                    # Give up and return requests' .text
                    return resp.text
        
        # set resp.encoding for callers
        try:
            resp.encoding = best['encoding']
        except Exception:
            pass
        logger.debug('Decoded response using encoding=%s score=%.4f for url=%s', best.get('encoding'), best.get('score') or 0.0, getattr(resp, 'url', ''))
        return best['text']
    except Exception as e:
        logger.debug('decode_response unexpected error=%s; falling back to resp.text', e)
        try:
            return resp.text
        except Exception:
            return ''
