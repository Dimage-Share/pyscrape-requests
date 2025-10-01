from __future__ import annotations
"""Normalization helpers for Goo-net parsed fields.

Responsibilities:
 - Canonicalize bodytype variants (e.g., 'ミニバン・ワンボックス' -> 'ミニバン', 'SUV・クロスカントリー' -> 'SUV').
 - Detect obvious mojibake patterns (leading '�', long sequences of ASCII where Japanese expected) and mark suspicious.
 - Strip zero-width and unusual control characters.
"""
import re
from typing import Tuple, Optional, Dict


_BODYTYPE_MAP = {
    'ミニバン・ワンボックス': 'ミニバン',
    'SUV・クロスカントリー': 'SUV',
}

# Patterns that indicate mojibake (start with replacement char already persisted historically or unexpected Latin sequences before Japanese)
_RE_MOJIBAKE_PREFIX = re.compile(r'^[�\ufffd]+')
_RE_LATIN_BLOCK = re.compile(r'[A-Za-z]{8,}')  # long ascii chunk
_RE_CONTROL = re.compile(r'[\u0000-\u001F\u007F]')


def clean_text(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    t = val.strip()
    if not t:
        return None
    # remove zero-width & control chars
    t = _RE_CONTROL.sub('', t)
    return t or None


def normalize_bodytype(bodytype: Optional[str]) -> Tuple[Optional[str], bool]:
    """Return (normalized_bodytype, suspicious_flag)."""
    b = clean_text(bodytype)
    suspicious = False
    if not b:
        return b, suspicious
    if b in _BODYTYPE_MAP:
        b = _BODYTYPE_MAP[b]
    # replacement char or mojibake prefix
    if _RE_MOJIBAKE_PREFIX.search(b):
        suspicious = True
    # ascii runs
    if _RE_LATIN_BLOCK.search(b):
        suspicious = True
    # length sanity (too long improbable)
    if len(b) > 40:
        suspicious = True
    return b, suspicious


def normalize_record_fields(rec_dict: Dict) -> Dict:
    """Mutate-like helper for CarRecord dict (before dataclass instantiation)."""
    raw = rec_dict.get('raw') or {}
    bodytype = rec_dict.get('bodytype')
    nb, suspicious = normalize_bodytype(bodytype)
    rec_dict['bodytype'] = nb
    if suspicious:
        raw['suspicious_bodytype'] = True
    rec_dict['raw'] = raw
    return rec_dict
