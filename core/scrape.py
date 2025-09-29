from __future__ import annotations
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .client import CarSensorClient
from .carsensor_parser import parse_cars_carsensor, get_next_page_url_carsensor, parse_car_detail
from app.db import init_db, bulk_upsert_cars, truncate_goo, bulk_insert_goo
from .logger import Logger


log = Logger.bind(__name__)

logger = logging.getLogger(__name__)


class Scrape:
    """High-level scraping orchestrator.

    This wraps the multi-page fetch, parsing, persistence, and summary generation.
    Migration note: currently still depends on goo_net_scrape.* modules; after full
    refactor those modules will be moved under core/ and imports updated.
    """
    
    SUMMARY_FIELDS: Sequence[str] = ('price', 'year', 'name', 'rd', 'engine', 'mission', 'bodytype', 'location', 'url')
    
    def __init__(self, page_delay: float = 1.0):
        self.page_delay = page_delay
    
    # ---- Public API ----
    def run(self, pages: int, params: Dict[str, Any]) -> Path:
        """Execute scraping for a given number of pages and return summary file path."""
        t_total_start = time.perf_counter()
        all_cars: List[Any] = []
        init_db()
        # Truncate goo table at start per requirement
        truncate_goo()
        pages_fetched = 0
        with CarSensorClient() as client:
            # CarSensor初回ページ (paramsは現状未使用)
            t_page_start = time.perf_counter()
            log.info("Page start page=1 (carsensor)")
            html = client.get_summary_page(params=None)
            cars = parse_cars_carsensor(html)
            if cars:
                # Collect cars, insert later into goo
                all_cars.extend(cars)
                # --- 詳細ページ取得 ---
                self._enrich_with_details(client, cars)
            elapsed_ms = (time.perf_counter() - t_page_start) * 1000.0
            log.info(f"Page done page=1 records={len(cars)} total={len(all_cars)} {elapsed_ms:.1f}ms")
            pages_fetched = 1
            while pages_fetched < pages:
                next_url = get_next_page_url_carsensor(html, current_url='page1')
                if not next_url:
                    log.debug(f"No next page (pages_fetched={pages_fetched})")
                    break
                target_page = pages_fetched + 1
                # Safety: if target_page exceeds requested pages break
                if target_page > pages:
                    break
                time.sleep(max(0.0, self.page_delay))
                t_page_start = time.perf_counter()
                log.info(f"Page start page={target_page} url={next_url}")
                try:
                    resp = client.session.get(next_url, timeout=client.config.timeout)
                    resp.raise_for_status()
                    # 念のため charset 明示 (ページ遷移後も UTF-8 だが推測誤りや一部環境差異防止)
                    resp.encoding = 'utf-8'
                    html = resp.text
                except Exception as e:  # noqa: BLE001
                    log.warn(f"Page fetch fail page={target_page} error={e}")
                    break
                cars = parse_cars_carsensor(html)
                if cars:
                    all_cars.extend(cars)
                    self._enrich_with_details(client, cars)
                else:
                    log.debug(f"page={target_page} records=0")
                # Increment pages_fetched only once per full page retrieval
                pages_fetched += 1
                elapsed_ms = (time.perf_counter() - t_page_start) * 1000.0
                log.info(f"Page done page={target_page} records={len(cars)} total={len(all_cars)} {elapsed_ms:.1f}ms")
        
        # If loop exited without logging last page (e.g., break before increment), adjust pages_fetched
        if pages_fetched == 1 and pages > 1:
            log.debug("Only first page fetched; stopping.")
        
        # --- 取得後: name分割・missionリネーム・jc08整形 ---
        for car in all_cars:
            # name分割
            n = getattr(car, 'name', None)
            items = n.split('\xa0')
            print(items)
            if len(items) >= 1:
                car.manufacturer = items[0]
                car.name = ' '.join(items[1:])
            elif len(items) == 1:
                car.manufacturer = items[0]
                car.name = ''
            # engine→category自動判定
            try:
                eng = getattr(car, 'engine', None)
                if eng is not None:
                    eng_val = int(eng)
                    car.category = '軽' if eng_val <= 660 else '普通'
                else:
                    car.category = None
            except Exception:
                car.category = None
            
            # mission→mission2リネーム
            if hasattr(car, 'mission2') or hasattr(car, 'mission'):
                mval = getattr(car, 'mission', None)
                if mval is not None:
                    car.mission2 = mval
                    if hasattr(car, 'mission'):
                        delattr(car, 'mission')
            # mission1抽出
            m2 = getattr(car, 'mission2', None)
            if m2 and isinstance(m2, str):
                for key in ['CVT', 'MT', 'AT']:
                    if key in m2:
                        car.mission1 = key
                        break
            # jc08 '(km/L)'除去
            jc = getattr(car, 'jc08', None)
            if jc and isinstance(jc, str):
                car.jc08 = jc.replace('（km/L）', '').replace('(km/L)', '').strip() or None
        inserted = bulk_insert_goo(all_cars)
        log.info(f"goo insert records={inserted}")
        # car テーブルにも同内容を upsert (分析/将来差分用途)
        try:
            from .db import bulk_upsert_cars
            upserted = bulk_upsert_cars(all_cars)
            log.info(f"car upsert records={upserted}")
        except Exception as e:  # noqa: BLE001
            log.warn(f"car upsert failed error={e}")
        # Build per-bodytype summaries (price ascending)
        db_rows = [c.to_db_row() for c in all_cars]
        body_files = self.write_grouped_summaries(db_rows)
        total_rows_written = sum(info['rows'] for info in body_files.values())
        log.info(f"generated_files={len(body_files)} total_rows={total_rows_written}")
        total_elapsed_ms = (time.perf_counter() - t_total_start) * 1000.0
        log.info(f"All done pages={pages_fetched} total_records={len(all_cars)} {total_elapsed_ms:.1f}ms")
        # Return first file path (arbitrary) for backward compatibility
        return next(iter(body_files.values()))['path'] if body_files else Path('summary-none.md')
    
    # ---- Formatting helpers (duplicated temporarily; will be unified later) ----
    @staticmethod
    def _display_width(text: str) -> int:
        import unicodedata
        width = 0
        for ch in text:
            eaw = unicodedata.east_asian_width(ch)
            width += 2 if eaw in ("F", "W", "A") else 1
        return width
    
    @classmethod
    def _field_align(cls, field: str) -> str:
        if field in ('price', 'rd', 'engine'):
            return 'right'
        if field == 'mission':
            return 'center'
        return 'left'
    
    @classmethod
    def build_summary_rows(cls, cars: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        rows: List[Dict[str, str]] = []
        for car in cars:
            price = car.get('price')
            year = car.get('year')
            rd = car.get('rd')
            engine = car.get('engine')
            mission = (car.get('mission') or '').replace('ミッション', '')
            bodytype_raw = (car.get('bodytype') or '').replace('ボディタイプ', '')
            if 'ミニバン・ワンボックス' in bodytype_raw:
                bodytype_raw = bodytype_raw.replace('ミニバン・ワンボックス', 'ミニバン')
            if 'SUV・クロスカントリー' in bodytype_raw:
                bodytype_raw = bodytype_raw.replace('SUV・クロスカントリー', 'SUV')
            bodytype = bodytype_raw
            location = car.get('location') or ''
            rd_fmt = f"{rd:,}km" if isinstance(rd, int) else ''
            engine_fmt = f"{engine:,}cc" if isinstance(engine, int) else ''
            row = {
                'price': f"{price}万" if price is not None else '',
                'year': f"{year}年式" if year is not None else '',
                'name': car.get('name') or '',
                'rd': rd_fmt,
                'engine': engine_fmt,
                'mission': mission,
                'bodytype': bodytype,
                'location': location,
                'url': car.get('url') or '',
            }
            rows.append(row)
        return rows
    
    # ---- New grouped summary (bodytype) ----
    @staticmethod
    def _slug(text: str) -> str:
        if not text:
            return 'unknown'
        import re
        t = text.strip()
        t = re.sub(r"[\s　]+", "_", t)
        t = re.sub(r"[^0-9A-Za-z一-龥ぁ-んァ-ンー_]+", "-", t)
        return t[:80]
    
    @classmethod
    def _sort_rows_price_asc(cls, rows: List[Dict[str, str]]) -> None:
        
        def _int_from_suffix(val: str, suffix: str) -> Optional[int]:
            if not val or not val.endswith(suffix):
                return None
            core = val[:-len(suffix)]
            try:
                return int(core.replace(',', ''))
            except Exception:
                return None
        
        def _key(r: Dict[str, str]):
            price_v = _int_from_suffix(r.get('price', ''), '万')
            year_v = _int_from_suffix(r.get('year', ''), '年式')
            run_raw = r.get('rd', '')
            run_v: Optional[int] = None
            if run_raw.endswith('km'):
                try:
                    run_v = int(run_raw[:-2].replace(',', ''))
                except Exception:
                    run_v = None
            price_key = price_v if price_v is not None else 10**9
            # For tie-breaking prefer newer (desc) then lower run then name
            year_key = -(year_v if year_v is not None else -1)
            run_key = run_v if run_v is not None else 10**12
            name_key = r.get('name', '')
            return (price_key, year_key, run_key, name_key)
        
        rows.sort(key=_key)
    
    @classmethod
    def _lines_from_rows(cls, rows: List[Dict[str, str]]) -> List[str]:
        col_width: Dict[str, int] = {
            f: 0
            for f in cls.SUMMARY_FIELDS
        }
        for r in rows:
            for f in cls.SUMMARY_FIELDS:
                w = cls._display_width(r.get(f, ''))
                if w > col_width[f]:
                    col_width[f] = w
        header_map = {
            'price': 'PRICE',
            'year': 'YEAR',
            'name': 'NAME',
            'rd': 'RUN',
            'engine': 'ENGINE',
            'mission': 'MISSION',
            'bodytype': 'BODY',
            'location': 'LOCATION',
            'url': 'URL'
        }
        parts = []
        for f in cls.SUMMARY_FIELDS:
            title = header_map[f]
            align = cls._field_align(f)
            width_needed = max(col_width[f], cls._display_width(title))
            parts.append(cls._pad(title, width_needed, align))
            if col_width[f] < cls._display_width(title):
                col_width[f] = cls._display_width(title)
        header_line = ' | '.join(parts)
        sep_line = '-+-'.join('-' * cls._display_width(p) for p in parts)
        lines = [header_line, sep_line]
        for r in rows:
            row_parts = []
            for f in cls.SUMMARY_FIELDS:
                align = cls._field_align(f)
                row_parts.append(cls._pad(r.get(f, ''), col_width[f], align))
            lines.append(' | '.join(row_parts))
        return lines
    
    @classmethod
    def write_grouped_summaries(cls, cars: List[Dict[str, Any]]):
        rows = cls.build_summary_rows(cars)
        groups: Dict[str, List[Dict[str, str]]] = {}
        for r in rows:
            bt = r.get('bodytype') or ''
            groups.setdefault(bt, []).append(r)
        result: Dict[str, Dict[str, Any]] = {}
        for body, rlist in groups.items():
            cls._sort_rows_price_asc(rlist)
            lines = cls._lines_from_rows(rlist)
            slug = cls._slug(body or 'unknown')
            path = Path(f"summary-{slug}.md")
            cls.write_summary_file(lines, path)
            result[body] = {
                'path': path,
                'rows': max(0,
                            len(lines) - 2)
            }
        return result
    
    @classmethod
    def build_summary_lines(cls, cars: List[Dict[str, Any]]) -> List[str]:
        # Keep legacy single-file builder (price ascending now) if still used elsewhere
        rows = cls.build_summary_rows(cars)
        cls._sort_rows_price_asc(rows)
        return cls._lines_from_rows(rows)
    
    @classmethod
    def _pad(cls, text: str, target: int, align: str) -> str:
        cur = cls._display_width(text)
        if cur >= target:
            return text
        pad = target - cur
        if align == 'right':
            return ' ' * pad + text
        if align == 'center':
            left = pad // 2
            right = pad - left
            return ' ' * left + text + ' ' * right
        return text + ' ' * pad
    
    @staticmethod
    def write_summary_file(lines: List[str], path: Path) -> None:
        if path.exists():
            try:
                path.unlink()
            except Exception as e:  # noqa: BLE001
                Logger.warn(f"summary remove fail error={e}")
        content = '# Summary\n' + '\n'.join(['````', *lines, '````', ''])
        # Windowsメモ帳互換のため BOM 付きUTF-8で保存
        path.write_text(content, encoding='utf-8-sig')
    
    # ---- 詳細ページ統合 ----
    @staticmethod
    def _enrich_with_details(client: CarSensorClient, cars: List[Any]) -> None:
        """各 CarRecord の url 先 (詳細ページ) を開き追加フィールドを埋める。

        仕様:
          - 404/エラー時はスキップ (警告ログのみ)
          - 過剰アクセス防止のため現在は逐次 (将来並列化可)
          - 既に name があり詳細 name が取れた場合は詳細側を優先
        """
        for car in cars:
            detail_url = getattr(car, 'url', None)
            if not detail_url:
                continue
            try:
                resp = client.session.get(detail_url, timeout=client.config.timeout)
                if resp.status_code != 200:
                    log.debug(f"detail skip status={resp.status_code} url={detail_url}")
                    continue
                resp.encoding = 'utf-8'
                data = parse_car_detail(resp.text)
                if not data:
                    continue
                # 上書き (None を無理に書かない)
                if data.get('name'): car.name = data['name']  # type: ignore[attr-defined]
                if data.get('option'): car.option = data['option']  # type: ignore[attr-defined]
                if data.get('repair'): car.repair = data['repair']  # type: ignore[attr-defined]
                if data.get('wd'): car.wd = data['wd']  # type: ignore[attr-defined]
                if data.get('seat'): car.seat = data['seat']  # type: ignore[attr-defined]
                if data.get('door'): car.door = data['door']  # type: ignore[attr-defined]
                if data.get('fuel'): car.fuel = data['fuel']  # type: ignore[attr-defined]
                if data.get('handle'): car.handle = data['handle']  # type: ignore[attr-defined]
                if data.get('jc08'): car.jc08 = data['jc08']  # type: ignore[attr-defined]
            except Exception as e:  # noqa: BLE001
                log.debug(f"detail fetch fail url={detail_url} error={e}")
