from __future__ import annotations
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import re
from lxml import html as lhtml

from .models import CarRecord
import logging

from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


@dataclass
class SummaryItem:
    """サマリーページの一車両分の最小情報 (将来拡張)。"""
    title: str | None
    price_text: str | None
    url: str | None


def parse_summary(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    items: List[SummaryItem] = []
    for a in soup.select("a")[:10]:
        title = (a.get_text(strip=True) or None) if a else None
        href = a.get("href") if a else None
        if not title and not href:
            continue
        items.append(SummaryItem(title=title, price_text=None, url=href))
    
    result = {
        "items": [item.__dict__ for item in items],
        "meta": {
            "extracted_links": len(items),
        },
    }
    logger.debug("parse_summary result counts: %s", result["meta"])
    return result


NEXT_PAGE_XPATH_PRIMARY = '//*[@id="resultArea"]/div/div/div/div[2]/div/div/div[2]/ul/li[6]/a'
NEXT_PAGE_XPATH_FALLBACKS: List[str] = [
    '//*[@id="resultArea"]/div/div/div/div[2]/div/div/div[2]/ul/li[8]/a',
]


def _abs_url(href: str) -> str:
    if href.startswith('/'):
        return 'https://www.goo-net.com' + href
    return href


def get_next_page_url(html: str) -> Optional[str]:
    try:
        tree = lhtml.fromstring(html)
    except Exception as e:  # noqa: BLE001
        logger.warning("get_next_page_url: HTML parse 失敗: %s", e)
        return None
    
    try:
        rel_next = tree.xpath('//a[@rel="next" and @href]')
        if rel_next:
            href = rel_next[0].get('href')
            if href:
                url = _abs_url(href)
                logger.debug("next page strategy=rel_next url=%s", url)
                return url
    except Exception:  # noqa: BLE001
        pass
    
    try:
        txt_candidates = tree.xpath('//a[@href]')
        for a in txt_candidates:
            txt = (a.text_content() or '').strip()
            if any(k in txt for k in ('次', '次へ', '次の')) and len(txt) <= 6:
                href = a.get('href')
                if href:
                    url = _abs_url(href)
                    logger.debug("next page strategy=text_match text=%s url=%s", txt, url)
                    return url
    except Exception:  # noqa: BLE001
        pass
    
    try:
        class_next = tree.xpath('//a[contains(@class, "next") or contains(@class, "pagination-next")][@href]')
        if class_next:
            href = class_next[0].get('href')
            if href:
                url = _abs_url(href)
                logger.debug("next page strategy=class_next url=%s", url)
                return url
    except Exception:  # noqa: BLE001
        pass
    
    try:
        active_nodes = tree.xpath('//*[contains(@class,"active") or contains(@class,"current")]/a | //li[contains(@class,"active") or contains(@class,"current")]')
        cur_num: Optional[int] = None
        for n in active_nodes:
            txt = (n.text_content() or '').strip()
            if txt.isdigit():
                cur_num = int(txt)
                break
        if cur_num is not None:
            target = str(cur_num + 1)
            num_links = tree.xpath(f'//a[@href and normalize-space(text())="{target}"]')
            if num_links:
                href = num_links[0].get('href')
                if href:
                    url = _abs_url(href)
                    logger.debug(
                        "next page strategy=number_follow current=%d target=%s url=%s",
                        cur_num,
                        target,
                        url,
                    )
                    return url
    except Exception:  # noqa: BLE001
        pass
    
    static_xps = [NEXT_PAGE_XPATH_PRIMARY, *NEXT_PAGE_XPATH_FALLBACKS]
    for xp in static_xps:
        try:
            nodes = tree.xpath(xp)
        except Exception as e:  # noqa: BLE001
            logger.debug("XPath 評価失敗 xp=%s err=%s", xp, e)
            continue
        if not nodes:
            continue
        href = nodes[0].get('href') if nodes else None
        if not href:
            continue
        url = _abs_url(href)
        logger.debug("next page strategy=static xp=%s url=%s", xp, url)
        return url
    
    logger.debug("next page not found (all strategies failed)")
    return None


ID_TR_PREFIX = "tr_"
ID_TD_PREFIX = "td_"

re_price = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*万")
re_price_plain = re.compile(r"^([0-9]+(?:\.[0-9]+)?)$")  # 金額数値のみ (万円想定)
re_mileage = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*万?km")
re_year_western = re.compile(r"(20[0-9]{2}|19[0-9]{2})")
re_jp_era = re.compile(r"([Rr令和Hh平成])(\d{1,2})")
re_displacement_cc = re.compile(r"(\d+)\s*cc")
re_displacement_l = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*[lL]")

# ---- 正規化補助: 全角英数字 -> 半角英数字 ----
_FW_ALNUM_TRANS: dict[int, int] = {}
for i in range(10):  # ０-９
    _FW_ALNUM_TRANS[ord('０') + i] = ord('0') + i
for i in range(26):  # Ａ-Ｚ
    _FW_ALNUM_TRANS[ord('Ａ') + i] = ord('A') + i
for i in range(26):  # ａ-ｚ
    _FW_ALNUM_TRANS[ord('ａ') + i] = ord('a') + i


def _halfwidth_alnum(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    return text.translate(_FW_ALNUM_TRANS)


def _norm_price(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.replace(',', '').strip()
    m = re_price.search(t)
    if m:
        try:
            return int(float(m.group(1)))  # 万単位整数 (floor)
        except ValueError:
            return None
    m2 = re_price_plain.match(t)
    if m2:
        try:
            return int(float(m2.group(1)))
        except ValueError:
            return None
    return None


def _norm_mileage(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.replace(',', '')
    m = re_mileage.search(t)
    if not m:
        return None
    num = float(m.group(1))
    if '万' in t:
        num *= 10000
    return int(num)


def _norm_year(text: str) -> Optional[int]:
    if not text:
        return None
    m = re_year_western.search(text)
    if m:
        return int(m.group(1))
    m2 = re_jp_era.search(text)
    if m2:
        era, num_s = m2.groups()
        num = int(num_s)
        if era in ('R', 'r', '令和'):
            return 2018 + num  # R1=2019 => 2018 + 1
        if era in ('H', 'h', '平成'):
            return 1988 + num  # H1=1989 => 1988 + 1
    return None


def _norm_displacement(text: str) -> Optional[int]:
    if not text:
        return None
    t = text.replace(',', '')
    m = re_displacement_cc.search(t)
    if m:
        return int(m.group(1))
    m2 = re_displacement_l.search(t)
    if m2:
        return int(float(m2.group(1)) * 1000)
    return None


def _clean(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    t = text.strip()
    return t or None


def parse_cars(html: str) -> List[CarRecord]:
    # lxml ツリー (XPath 用)
    tree = lhtml.fromstring(html)
    soup = BeautifulSoup(html, 'lxml')  # 既存フォールバック/名称抽出用
    
    records: List[CarRecord] = []
    
    # tr ノード列挙
    tr_nodes = tree.xpath('//*[@id][starts-with(@id, "tr_")]')
    # fallback: if lxml zero, try soup
    if not tr_nodes:
        for tag in soup.select('[id^="tr_"]'):
            try:
                frag = lhtml.fromstring(str(tag))
                tr_nodes.append(frag)
            except Exception:  # noqa: BLE001
                continue
    for tr_node in tr_nodes:
        tr_id = tr_node.get('id')
        if not tr_id or not tr_id.startswith(ID_TR_PREFIX):
            continue
        car_unique_id = tr_id[len(ID_TR_PREFIX):]
        try:
            card_candidates = tr_node.xpath('./div[1]')
            card = card_candidates[0] if card_candidates else tr_node
            
            def xp_first(node, xp: str) -> Optional[str]:
                try:
                    r = node.xpath(xp)
                    if not r:
                        return None
                    if isinstance(r[0], str):
                        return _clean(r[0])
                    return _clean(r[0].text_content())
                except Exception:  # noqa: BLE001
                    return None
            
            name = None
            td_id = f"td_{car_unique_id}"
            td_elem = tree.xpath(f'//*[@id="{td_id}"]')
            car_url = None
            if td_elem:
                anchor = td_elem[0].xpath('.//div/h3/a')
                if anchor:
                    car_url = anchor[0].get('href') or None
                    span_txt = xp_first(td_elem[0], './/div/h3/a/p[2]/span')
                    name = span_txt or xp_first(td_elem[0], './/h3')
            if not name:
                name = xp_first(card, './/h3')
                if not car_url:
                    anchor2 = card.xpath('.//h3//a')
                    if anchor2:
                        car_url = anchor2[0].get('href') or None
            
            name = _halfwidth_alnum(name)
            
            if car_url and car_url.startswith('/'):
                car_url = 'https://www.goo-net.com' + car_url
            
            total_price_text = xp_first(card, './div[2]/div/div[2]/div[1]/div/div[1]/p[2]/em')
            if not total_price_text:
                cand_em = card.xpath('.//em[contains(text(),"万")]')
                if cand_em:
                    total_price_text = _clean(cand_em[0].text_content())
            if not total_price_text:
                cand_num = card.xpath('.//*[self::p or self::span or self::em][normalize-space(text()) and not(*)]')
                for c in cand_num:
                    txt = c.text_content().strip()
                    if re_price_plain.match(txt):
                        total_price_text = txt
                        break
            
            year_text = mileage_text = displacement_text = repair_text = None
            spec_ul = card.xpath('./div[2]/div/div[2]/div[2]/div[1]/ul')
            if spec_ul:
                lis = spec_ul[0].xpath('./li')
                
                def li_text(idx: int) -> Optional[str]:
                    if 0 <= idx < len(lis):
                        return _clean(lis[idx].text_content())
                    return None
                
                year_text = li_text(0)
                mileage_text = li_text(1)
                displacement_text = li_text(3)
                repair_text = li_text(4)
            if not (year_text and mileage_text and displacement_text):
                tr_bs = soup.find(id=tr_id)
                if tr_bs:
                    for li in tr_bs.select('ul li'):
                        t = li.get_text(strip=True)
                        if not t:
                            continue
                        if year_text is None and (re_year_western.search(t) or re_jp_era.search(t)):
                            year_text = t
                            continue
                        if mileage_text is None and 'km' in t:
                            mileage_text = t
                            continue
                        if displacement_text is None and ('cc' in t or re_displacement_l.search(t)):
                            displacement_text = t
                            continue
                        if repair_text is None and '修復' in t:
                            repair_text = t
            
            notes_raw = xp_first(card, './div[2]/div/div[2]/div[2]/div[2]/div[2]/div[1]')
            if not notes_raw:
                notes_raw = xp_first(card, './div[2]/div/div[2]/div[2]/div[2]/div[1]')  # オフセット違い候補
            if not notes_raw:
                long_divs = sorted({e.text_content().strip()
                                    for e in card.xpath('.//div')}, key=len, reverse=True)
                for cand in long_divs:
                    if 30 < len(cand) < 400:
                        notes_raw = _clean(cand)
                        break
            
            notes_list: Optional[List[str]] = None
            color = mission = bodytype = None
            if notes_raw:
                tmp = notes_raw.replace('\r', '\n')
                parts = [re.sub(r'[ \u3000]+', '', p) for p in tmp.split('\n')]
                parts = [p for p in parts if p]
                if parts:
                    notes_list = parts
                    if len(parts) > 0:
                        color = parts[0]
                    if len(parts) > 1:
                        mission = parts[1]
                    if len(parts) > 2:
                        bodytype = parts[2]
            
            location = xp_first(tr_node, './div[3]/div/div[1]/div[1]/div/span')
            if (not location) or len(location) > 25:
                loc_candidates = tr_node.xpath('.//span[contains(text(),"県") or contains(text(),"府") or contains(text(),"都") or contains(text(),"道")]')
                for lc in loc_candidates:
                    txt = lc.text_content().strip()
                    if 3 <= len(txt) <= 30:
                        location = txt
                        break
            if location and len(location) > 30:
                location = None
            
            try:
                card_html = lhtml.tostring(card, encoding='unicode')
            except Exception:  # noqa: BLE001
                card_html = None
            
            if card_html:
                card_html = re.sub(r' {2,}', ' ', card_html)
            
            record = CarRecord(
                id=car_unique_id,
                name=name,
                price=_norm_price(total_price_text or ''),
                year=_norm_year(year_text or ''),
                rd=_norm_mileage(mileage_text or ''),
                engine=_norm_displacement(displacement_text or ''),
                color=color,
                mission=mission,
                bodytype=bodytype,
                repair=_clean(repair_text),
                location=_clean(location),
                source=card_html,
                url=car_url,
                raw={
                    'price_text': total_price_text,
                    'year_text': year_text,
                    'mileage_text': mileage_text,
                    'displacement_text': displacement_text,
                    'notes_source': notes_raw,
                    'notes_array': notes_list,
                    'repair_text': repair_text,
                    'location_source': location,
                    'tr_id': tr_id,
                },
            )
            records.append(record)
        except Exception as e:  # noqa: BLE001
            logger.warning("車両抽出失敗 tr_id=%s error=%s", tr_id, e)
            continue
    
    logger.debug("parse_cars records=%d", len(records))
    return records
