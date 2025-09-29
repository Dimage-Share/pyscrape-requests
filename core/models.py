from __future__ import annotations
"""Migrated from goo_net_scrape.models (transitional)."""
from dataclasses import dataclass, asdict
from typing import Optional, Any, Dict
import json


@dataclass
class CarRecord:
    id: str
    manufacturer: Optional[str] = None  # 新規: name分割 item[0]
    name: Optional[str] = None          # 新仕様: name分割 item[1]以降
    price: Optional[int] = None
    year: Optional[int] = None
    rd: Optional[int] = None
    engine: Optional[int] = None
    repair: Optional[str] = None
    location: Optional[str] = None
    raw: Dict[str, Any] = None
    color: Optional[str] = None
    mission1: Optional[str] = None      # 新規: mission2から抽出
    mission2: Optional[str] = None      # 旧missionをリネーム
    bodytype: Optional[str] = None
    source: Optional[str] = None
    url: Optional[str] = None
    option: Optional[str] = None
    wd: Optional[str] = None
    seat: Optional[str] = None
    door: Optional[str] = None
    fuel: Optional[str] = None
    handle: Optional[str] = None
    jc08: Optional[str] = None
    category: Optional[str] = None  # engine<=660: "軽"、それ以外: "普通"
    
    def to_db_row(self) -> Dict[str, Any]:
        d = asdict(self)
        raw_data = d.pop("raw", {})
        d["raw_json"] = json.dumps(raw_data, ensure_ascii=False)
        return d
