from __future__ import annotations
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import re
from lxml import html as lhtml

from .models import CarRecord
from .normalize import normalize_record_fields
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
    """summary.php の HTML から基本情報を抽出する骨組み関数。

    戻り値:
        dict:
            items: List[SummaryItem as dict]
            raw_count: (将来) 件数
            meta: パースメタ情報
    """
    soup = BeautifulSoup(html, "lxml")
    
    # 現時点では具体的な DOM 構造を未調査なので、暫定で代表的なリンクとテキストを拾う。
    items: List[SummaryItem] = []
    
    # TODO: goo-net の各車両カード DOM を特定し適切なセレクタに置き換える。
    for a in soup.select("a")[:10]:  # 暫定で最初の10リンク
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
    '//*[@id="resultArea"]/div/div/div/div[2]/div/div/div[2]/ul/li[8]/a',  # 旧: ユーザ指定 fallback
]


def _abs_url(href: str) -> str:
    if href.startswith('/'):
        return 'https://www.goo-net.com' + href
    return href


def get_next_page_url(html: str) -> Optional[str]:
    """次ページ URL を動的探索 + 静的フォールバックで取得。

    戦略優先順:
      1. rel="next" を持つ <a>
      2. テキストが『次』『次へ』『次の』を含む <a>
      3. class に next / pagination-next を含む <a>
      4. 現在ページ番号 + 1 を指す数字リンク (周辺の active/current から推測)
      5. 静的 XPath PRIMARY (li[6])
      6. 静的 XPath FALLBACKS (li[8] など)

    Returns:
        str | None
    """
    try:
        tree = lhtml.fromstring(html)
    except Exception as e:  # noqa: BLE001
        logger.warning("get_next_page_url: HTML parse 失敗: %s", e)
        return None
    
    # --- 戦略1: rel="next" ---
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
    
    # --- 戦略2: テキストマッチ ---
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
    
    # --- 戦略3: class 名に next / pagination-next ---
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
    
    # --- 戦略4: 現在ページ+1 数字リンク ---
    try:
        # active/current ページ番号候補
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
    
    # --- 戦略5/6: 静的 XPath 群 ---
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


# ---------------- 新規: 車両カード詳細抽出 -----------------
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
    """全角英数字(A-Z a-z 0-9)のみ半角へ変換。他文字はそのまま。"""
    if not text:
        return text
    return text.translate(_FW_ALNUM_TRANS)


def _norm_price(text: str) -> Optional[int]:
    """価格文字列を『万円単位の整数』に正規化 (小数点以下切り捨て)。

    例: '288.4万円' / '288.4万' / '288.4' -> 288
        '709' -> 709
    """
    if not text:
        return None
    t = text.replace(',', '').strip()
    # '万' が含まれるケース
    m = re_price.search(t)
    if m:
        try:
            return int(float(m.group(1)))  # 万単位整数 (floor)
        except ValueError:
            return None
    # 純数値 -> 万円として解釈
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
    """summary ページ HTML から車両情報を抽出 (カード単位: tr_*/div[1] を起点)。

    各フィールドは『カードルート XPath』を起点とした相対 XPath で取得を試み、
    失敗時にフォールバックのヒューリスティックへ切り替える。
    """
    # lxml ツリー (XPath 用)
    tree = lhtml.fromstring(html)
    soup = BeautifulSoup(html, 'lxml')  # 既存フォールバック/名称抽出用
    
    records: List[CarRecord] = []
    
    # tr ノード列挙
    tr_nodes = tree.xpath('//*[@id][starts-with(@id, "tr_")]')
    # fallback: if lxml zero, try soup
    if not tr_nodes:
        for tag in soup.select('[id^="tr_"]'):
            # convert bs4 Tag to lxml element by parsing its string (costly but rare)
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
            # カードルート: ./div[1]
            card_candidates = tr_node.xpath('./div[1]')
            card = card_candidates[0] if card_candidates else tr_node
            
            def xp_first(node, xp: str) -> Optional[str]:
                try:
                    r = node.xpath(xp)
                    if not r:
                        return None
                    # r が要素/属性/文字列混在の可能性
                    if isinstance(r[0], str):
                        return _clean(r[0])
                    return _clean(r[0].text_content())
                except Exception:  # noqa: BLE001
                    return None
            
            # 名称: td_{id} 由来 or カード内 h3
            name = None
            td_id = f"td_{car_unique_id}"
            td_elem = tree.xpath(f'//*[@id="{td_id}"]')
            car_url = None
            if td_elem:
                # h3 構造下の a
                anchor = td_elem[0].xpath('.//div/h3/a')
                if anchor:
                    car_url = anchor[0].get('href') or None
                    # モデル名 span
                    span_txt = xp_first(td_elem[0], './/div/h3/a/p[2]/span')
                    name = span_txt or xp_first(td_elem[0], './/h3')
            if not name:
                name = xp_first(card, './/h3')
                if not car_url:
                    anchor2 = card.xpath('.//h3//a')
                    if anchor2:
                        car_url = anchor2[0].get('href') or None
            
            # 名称: 全角英数字を半角へ
            name = _halfwidth_alnum(name)
            
            # URL を絶対化 (元が相対の場合のみ)
            if car_url and car_url.startswith('/'):
                car_url = 'https://www.goo-net.com' + car_url
            
            # 支払総額 (相対 XPath パターン候補)
            total_price_text = xp_first(card, './div[2]/div/div[2]/div[1]/div/div[1]/p[2]/em')
            if not total_price_text:
                # フォールバック: 金額らしい em
                cand_em = card.xpath('.//em[contains(text(),"万")]')
                if cand_em:
                    total_price_text = _clean(cand_em[0].text_content())
            if not total_price_text:
                # さらに p, span などから数値のみ抽出 (例: 288.4 だけが separate な場合)
                cand_num = card.xpath('.//*[self::p or self::span or self::em][normalize-space(text()) and not(*)]')
                for c in cand_num:
                    txt = c.text_content().strip()
                    if re_price_plain.match(txt):
                        total_price_text = txt
                        break
            
            # 年式/走行距離/排気量/修復歴: spec list ul/li
            year_text = mileage_text = displacement_text = repair_text = None
            spec_ul = card.xpath('./div[2]/div/div[2]/div[2]/div[1]/ul')
            if spec_ul:
                lis = spec_ul[0].xpath('./li')
                
                # 期待順: 1:年式 2:走行 4:排気量 5:修復歴
                def li_text(idx: int) -> Optional[str]:
                    if 0 <= idx < len(lis):
                        return _clean(lis[idx].text_content())
                    return None
                
                year_text = li_text(0)
                mileage_text = li_text(1)
                displacement_text = li_text(3)
                repair_text = li_text(4)
            # フォールバック (未取得): 既存ヒューリスティック
            if not (year_text and mileage_text and displacement_text):
                # fallback soup (同じ tr id を再利用)
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
            
            # 備考 (後で行配列化)
            notes_raw = xp_first(card, './div[2]/div/div[2]/div[2]/div[2]/div[2]/div[1]')
            if not notes_raw:
                notes_raw = xp_first(card, './div[2]/div/div[2]/div[2]/div[2]/div[1]')  # オフセット違い候補
            if not notes_raw:
                # ヒューリスティック fallback (長文)
                long_divs = sorted({e.text_content().strip()
                                    for e in card.xpath('.//div')}, key=len, reverse=True)
                for cand in long_divs:
                    if 30 < len(cand) < 400:
                        notes_raw = _clean(cand)
                        break
            
            # notes_raw を行配列へ: 改行 split / 全角半角スペース除去 / 空除去
            notes_list: Optional[List[str]] = None
            color = mission = bodytype = None
            if notes_raw:
                # 改行正規化
                tmp = notes_raw.replace('\r', '\n')
                # 連続改行を1に (分割時に空行を除去するので任意)
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
            
            # 所在地: tr 起点で ./div[3]/div/div[1]/div[1]/div/span
            location = xp_first(tr_node, './div[3]/div/div[1]/div[1]/div/span')
            if (not location) or len(location) > 25:
                # 追加候補: 都道府県 + 市区 (例: 岡山県岡山市南区)
                loc_candidates = tr_node.xpath('.//span[contains(text(),"県") or contains(text(),"府") or contains(text(),"都") or contains(text(),"道")]')
                for lc in loc_candidates:
                    txt = lc.text_content().strip()
                    if 3 <= len(txt) <= 30:
                        location = txt
                        break
            if location and len(location) > 30:
                location = None
            
            # HTML フラグメント (Unicode 文字列化)
            try:
                card_html = lhtml.tostring(card, encoding='unicode')
            except Exception:  # noqa: BLE001
                card_html = None
            
            # source: 連続した半角スペースは 1 個に圧縮
            if card_html:
                card_html = re.sub(r' {2,}', ' ', card_html)
            
            rec_dict = dict(
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
            rec_dict = normalize_record_fields(rec_dict)
            record = CarRecord(**rec_dict)
            records.append(record)
        except Exception as e:  # noqa: BLE001
            logger.warning("車両抽出失敗 tr_id=%s error=%s", tr_id, e)
            continue
    
    logger.debug("parse_cars records=%d", len(records))
    return records
