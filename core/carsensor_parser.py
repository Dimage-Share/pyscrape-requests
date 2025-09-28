from __future__ import annotations
"""CarSensorサイト専用のパーサ実装。

Goo向け parser.py から汎用ロジック(正規化関数)を一部簡略コピーし、
CarSensorのDOM構造(2024-2025時点観測)に合わせて抽出する。

設計方針:
- 依存: lxml, BeautifulSoup (既存と揃える)
- id: 詳細ページURL中の AU[0-9]+ を採用
- price: 支払総額(存在すれば) -> 数値(万単位)へ正規化
- year: 西暦 or 和暦 (令和/平成) パターン抽出
- rd: 走行距離(万km対応)
- engine: 排気量 cc (数値)
- mission: 表示テキスト一部そのまま
- bodytype: カード内カテゴリ表示(なければNone)
- repair: 修復歴 なし/あり
- location: 店舗所在地(都道府県+市区町村) 推定 (カード内 address / 店舗情報領域)

耐久性:
- 失敗しても1レコードごとに try/except で継続
- raw フィールドにソース断片を格納(デバッグ用)
"""
from dataclasses import dataclass
from typing import List, Optional
import re
from bs4 import BeautifulSoup
from lxml import html as lhtml

from .models import CarRecord
from .logger import Logger

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
    # CarSensor: 支払総額が "支払総額 123.4万円" / "123.4万円" / "本体価格" ブロックなど
    # 万表記が無い場合は諦める(過剰抽出防止)
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
    """全角/半角スペースおよび改行を除去し、空になれば None。

    要件: 「半角および全角スペースは null 置換」「改行コードは null 置換」
    ここでは null 置換 = 削除 と解釈し、最終的に空文字なら None を返す。
    """
    if t is None:
        return None
    # 改行 / タブ -> 削除
    s = t.replace('\r', '').replace('\n', '').replace('\t', '')
    # 半角/全角スペース削除
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


def parse_cars_carsensor(html: str) -> List[CarRecord]:
    soup = BeautifulSoup(html, 'lxml')
    tree = lhtml.fromstring(html)
    records: List[CarRecord] = []

    # --- XPathベース高精度抽出 (id=AU*_cas コンテナ) ---
    try:
        containers = tree.xpath('//*[starts-with(@id, "AU") and contains(@id, "_cas")]')
        for cont in containers:
            try:
                cid = cont.get('id') or ''
                m_id = re.search(r'(AU\d+)', cid)
                if not m_id:
                    continue
                car_id = m_id.group(1)
                # price
                price_node = cont.xpath('./div[1]/div/div[2]/div[2]/div[1]/div[1]/div[1]/p[2]')
                price_text = price_node[0].text_content().strip() if price_node else None
                # year
                year_node = cont.xpath('./div[1]/div/div[2]/div[2]/div[2]/dl/div[1]/dd/span[1]')
                year_text = year_node[0].text_content().strip() if year_node else None
                # name (スペース区切り → 最初の実数トークン手前まで結合)
                name_node = cont.xpath('./div[1]/div/div[2]/h3/a')
                raw_name = name_node[0].text_content().strip() if name_node else None
                name = None
                if raw_name:
                    parts = [p for p in re.split(r'\s+', raw_name) if p]
                    acc = []
                    for p in parts:
                        if re.search(r'\d', p):  # 数字含むトークンで停止
                            break
                        acc.append(p)
                    name = ' '.join(acc) if acc else raw_name
                # run
                run_node = cont.xpath('./div[1]/div/div[2]/div[2]/div[2]/dl/div[2]/dd')
                mileage_text = run_node[0].text_content().strip() if run_node else None
                # engine
                engine_node = cont.xpath('./div[1]/div/div[2]/div[2]/div[2]/dl/div[7]/dd')
                engine_text = engine_node[0].text_content().strip() if engine_node else None
                # mission
                mission_node = cont.xpath('./div[1]/div/div[2]/div[2]/div[2]/dl/div[8]/dd')
                mission_text = mission_node[0].text_content().strip() if mission_node else None
                # body (カテゴリ) ※ 指定例: ./div[1]/div/div[1]/ul/li[1]
                body_node = cont.xpath('./div[1]/div/div[1]/ul/li[1]')
                bodytype_text = body_node[0].text_content().strip() if body_node else None
                # location
                loc_node = cont.xpath('./div[2]/div[1]')
                location_text = loc_node[0].text_content().strip() if loc_node else None
                # repair (明示XPath未提供のため li走査でヒューリスティック)
                repair_text = None
                for cand in cont.xpath('.//li'):  # type: ignore
                    t = (cand.text_content() or '').strip()
                    if '修復' in t:
                        repair_text = t
                        break
                # 詳細URL (既存 a[href*='detail/AU'])
                link = cont.xpath('.//a[contains(@href, "/usedcar/detail/") and contains(@href, car_id)]') if 'car_id' in locals() else []
                href = None
                if link:
                    href = link[0].get('href')
                # HTML断片
                fragment_html = lhtml.tostring(cont, encoding='unicode')
                record = CarRecord(
                    id=_sanitize_text(car_id) or car_id,
                    name=_sanitize_text(_fw_to_hw(name)),
                    price=_norm_price(price_text),
                    year=_norm_year(year_text),
                    rd=_norm_mileage(mileage_text),
                    engine=_norm_engine(engine_text),
                    color=None,
                    mission=_sanitize_text(_clean(mission_text)),
                    bodytype=_sanitize_text(_clean(bodytype_text)),
                    repair=_sanitize_text(_clean(repair_text)),
                    location=_sanitize_text(_clean(location_text)),
                    source=fragment_html,
                    url=_sanitize_text(href if (href or '').startswith('http') else (f"https://www.carsensor.net{href}" if href else None)),
                    raw={
                        'price_text': _sanitize_text(price_text),
                        'year_text': _sanitize_text(year_text),
                        'mileage_text': _sanitize_text(mileage_text),
                        'engine_text': _sanitize_text(engine_text),
                        'mission_text': _sanitize_text(mission_text),
                        'bodytype_text': _sanitize_text(bodytype_text),
                        'repair_text': _sanitize_text(repair_text),
                        'location_text': _sanitize_text(location_text),
                        'extraction': 'xpath_primary'
                    }
                )
                records.append(record)
            except Exception as e:  # noqa: BLE001
                log.debug(f"xpath primary extract fail error={e}")
    except Exception:
        pass

    # 既にXPathで一定件数取得できた場合はヒューリスティック追加抽出をスキップ(重複防止)
    existing_ids = {r.id for r in records}

    # 想定カード: article タグ or li要素 内に /usedcar/detail/AUxxxxx/index.html への a
    detail_links = tree.xpath('//a[contains(@href, "/usedcar/detail/AU") and contains(@href, "index.html")]')
    seen_ids = set()
    for a in detail_links:
        href = a.get('href') or ''
        m_id = re.search(r'/usedcar/detail/(AU[0-9]+)/', href)
        if not m_id:
            continue
        car_id = m_id.group(1)
        if car_id in seen_ids or car_id in existing_ids:
            continue
        seen_ids.add(car_id)
        # カード領域探索: 先祖方向に article/li/div などを走査して適度な container を取得
        card_node = a
        for _ in range(5):  # 深さ制限
            parent = card_node.getparent()
            if parent is None:
                break
            # article or li などで複数子を保持する塊を採用
            tag = parent.tag.lower()
            if tag in ('article', 'li', 'div') and len(parent) >= 2:
                card_node = parent
            else:
                card_node = parent
        try:
            card_html = lhtml.tostring(card_node, encoding='unicode')
        except Exception:
            card_html = None

        # BeautifulSoup再解析で柔軟抽出
        card_bs = BeautifulSoup(card_html, 'lxml') if card_html else None
        name = None
        if card_bs:
            # CarSensor: クルマ名は strong / h2 / h3 内テキストになることが多い
            for sel in ['h2', 'h3', 'p', 'strong']:
                el = card_bs.select_one(sel)
                if el and el.get_text(strip=True):
                    cand = el.get_text(strip=True)
                    # 過剰に長い or 短いタイトルは除外
                    if 2 <= len(cand) <= 120:
                        name = cand
                        break
        name = _fw_to_hw(name)

        # 価格: 支払総額 を優先。『支払総額』の近くの数値
        price_text = None
        if card_bs:
            price_label = card_bs.find(string=re.compile('支払総額'))
            if price_label:
                # 近傍の親要素テキスト
                parent_text = price_label.parent.get_text(" ", strip=True) if price_label.parent else ''
                m_price = _re_price_man.search(parent_text)
                if m_price:
                    price_text = m_price.group(0)
            if not price_text:
                # フォールバック: 万円を含む em/span
                for em in card_bs.select('em, span'):
                    txt = em.get_text(strip=True)
                    if '万' in txt and _re_price_man.search(txt):
                        price_text = txt
                        break

        # スペック行(年式/走行距離/修復歴/排気量/ミッション)を li 群から抽出
        year_text = mileage_text = repair_text = engine_text = mission_text = bodytype_text = None
        if card_bs:
            # li 集合
            li_texts = [li.get_text(strip=True) for li in card_bs.select('li') if li.get_text(strip=True)]
            for t in li_texts:
                if year_text is None and (_re_year_west.search(t) or _re_era.search(t)):
                    year_text = t
                elif mileage_text is None and 'km' in t:
                    mileage_text = t
                elif engine_text is None and ('cc' in t.lower()):
                    engine_text = t
                elif repair_text is None and '修復' in t:
                    repair_text = t
                elif mission_text is None and any(k in t for k in ('AT', 'MT', 'CVT', 'ミッション')):
                    mission_text = t
                elif bodytype_text is None and any(k in t for k in ('SUV', 'バン', 'ワゴン', 'セダン', 'ハッチバック', 'ミニバン', 'トラック', 'クーペ')):
                    bodytype_text = t

        # ロケーション: 店舗所在地 (都/道/府/県 を含む部分)
        location_text = None
        if card_bs:
            for cand in card_bs.find_all(string=re.compile(r'(都|道|府|県)')):
                txt = cand.strip()
                if 2 <= len(txt) <= 40 and any(p in txt for p in ('県', '府', '都', '道')):
                    location_text = txt
                    break

        record = CarRecord(
            id=_sanitize_text(car_id) or car_id,
            name=_sanitize_text(name),
            price=_norm_price(price_text),
            year=_norm_year(year_text),
            rd=_norm_mileage(mileage_text),
            engine=_norm_engine(engine_text),
            color=None,
            mission=_sanitize_text(_clean(mission_text)),
            bodytype=_sanitize_text(_clean(bodytype_text)),
            repair=_sanitize_text(_clean(repair_text)),
            location=_sanitize_text(_clean(location_text)),
            source=card_html,
            url=_sanitize_text(href if href.startswith('http') else f"https://www.carsensor.net{href}"),
            raw={
                'price_text': _sanitize_text(price_text),
                'year_text': _sanitize_text(year_text),
                'mileage_text': _sanitize_text(mileage_text),
                'engine_text': _sanitize_text(engine_text),
                'mission_text': _sanitize_text(mission_text),
                'repair_text': _sanitize_text(repair_text),
                'bodytype_text': _sanitize_text(bodytype_text),
                'location_text': _sanitize_text(location_text),
                'href': _sanitize_text(href),
                'extraction': 'heuristic'
            }
        )
        records.append(record)
    log.debug(f"parse_cars_carsensor records={len(records)}")
    return records


def get_next_page_url_carsensor(html: str, current_url: str) -> Optional[str]:
    """CarSensorのページャから次ページURLを決定。

    ページャ例: <a href="/usedcar/hokkaido/index2.html">2</a>
    現在URLが search.php ... の場合 -> 最初の index2.html へのリンクを採用。
    以降は index{N+1}.html を a[href*="index{n+1}.html"] から探索。
    フォールバック: rel="next" ある a
    """
    try:
        tree = lhtml.fromstring(html)
    except Exception:
        return None
    # rel=next 優先
    rel_next = tree.xpath('//a[@rel="next" and @href]')
    if rel_next:
        href = rel_next[0].get('href')
        if href:
            return href if href.startswith('http') else f"https://www.carsensor.net{href}"
    # ボタン (提供XPath) フォールバック: //*[@id="js-resultBar"]/div[2]/div/div[2]/button[2]
    try:
        btn = tree.xpath('//*[@id="js-resultBar"]/div[2]/div/div[2]/button[2]')
        if btn:
            # 可能性: data-url / data-href / onclick 内に遷移先
            cand_attrs = []
            for k in ('data-url', 'data-href', 'data-link', 'onclick'):
                v = btn[0].get(k)
                if v:
                    cand_attrs.append(v)
            # onclick 例から URL 抽出 ( 'location.href="/usedcar/..."' )
            import re as _re
            for val in cand_attrs:
                # /usedcar/ 以下で indexN.html を含むパターンを抽出
                m = _re.search(r'/usedcar/[^"]+index\d+\.html', val)
                if m:
                    href = m.group(0)
                    return href if href.startswith('http') else f"https://www.carsensor.net{href}"
    except Exception:
        pass

    # indexN.html リンク一覧
    links = tree.xpath('//a[@href]')
    indexes = []
    for a in links:
        h = a.get('href') or ''
        m = re.search(r'/usedcar/(?:[a-z0-9_-]+/)?index(\d+)\.html', h)
        if m:
            try:
                indexes.append((int(m.group(1)), h))
            except ValueError:
                continue
    if not indexes:
        return None
    indexes.sort()
    # current の次を推測
    cur_page = 1
    m_cur = re.search(r'index(\d+)\.html', current_url)
    if m_cur:
        try:
            cur_page = int(m_cur.group(1))
        except ValueError:
            cur_page = 1
    target = cur_page + 1
    for num, h in indexes:
        if num == target:
            return h if h.startswith('http') else f"https://www.carsensor.net{h}"
    return None
