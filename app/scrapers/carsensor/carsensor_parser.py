"""Copy of core/carsensor_parser.py for app/scrapers placement.

This file is created to centralize scrapers under app/scrapers and avoid
import-time side-effects from core package imports.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
import re
from bs4 import BeautifulSoup
from lxml import html as lhtml

from core.models import CarRecord
from core.logger import Logger


log = Logger.bind(__name__)

# ---- 正規化用パターン ----
_re_price_man = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*万")
_re_price_digits = re.compile(r"([0-9]{1,9})")
_re_year_west = re.compile(r"(19|20)[0-9]{2}")
_re_era = re.compile(r"([Rr令和Hh平成])(\d{1,2})")
_re_mileage = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*万?km")
_re_engine_cc = re.compile(r"(\d{2,5})\s*cc", re.IGNORECASE)

_fullwidth_map: dict[int, int] = {}
for i in range(10):
    _fullwidth_map[ord('０') + i] = ord('0') + i


def _fw_to_hw(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    return text.translate(_fullwidth_map)


def _norm_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    t = text.replace(',', '')
    m = _re_price_man.search(t)
    if m:
        try:
            return int(float(m.group(1)))
        except ValueError:
            return None
    return None


def _norm_year(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = _re_year_west.search(text)
    if m:
        return int(m.group(0))
    m2 = _re_era.search(text)
    if m2:
        era, num_s = m2.groups()
        y = int(num_s)
        if era in ('R', 'r', '令和'):
            return 2018 + y
        if era in ('H', 'h', '平成'):
            return 1988 + y
    return None


def _norm_mileage(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    t = text.replace(',', '')
    m = _re_mileage.search(t)
    if not m:
        return None
    num = float(m.group(1))
    if '万' in t:
        num *= 10000
    return int(num)


def _norm_engine(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    t = text.replace(',', '')
    m = _re_engine_cc.search(t)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _clean(t: Optional[str]) -> Optional[str]:
    if t is None:
        return None
    s = t.strip()
    return s or None


def _sanitize_text(t: Optional[str]) -> Optional[str]:
    if t is None:
        return None
    s = t.replace('\r', '').replace('\n', '').replace('\t', '')
    s = s.replace(' ', '').replace('\u3000', '')
    s = s.strip()
    return s or None


@dataclass
class _CardExtract:
    id: str
    name: Optional[str]
    price_text: Optional[str]
    year_text: Optional[str]
    mileage_text: Optional[str]
    engine_text: Optional[str]
    repair_text: Optional[str]
    mission_text: Optional[str]
    bodytype_text: Optional[str]
    location_text: Optional[str]
    url: Optional[str]
    source: Optional[str]


# Copy the same functions as in core.carsensor_parser (kept consistent)
from core.carsensor_parser import parse_cars_carsensor, get_next_page_url_carsensor, parse_car_detail
