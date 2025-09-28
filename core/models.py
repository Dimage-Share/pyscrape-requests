from __future__ import annotations
"""Migrated from goo_net_scrape.models (transitional)."""
from dataclasses import dataclass, asdict
from typing import Optional, Any, Dict
import json


@dataclass
class CarRecord:
    id: str
    name: Optional[str]
    price: Optional[int]
    year: Optional[int]
    rd: Optional[int]
    engine: Optional[int]
    repair: Optional[str]
    location: Optional[str]
    raw: Dict[str, Any]
    color: Optional[str] = None
    mission: Optional[str] = None
    bodytype: Optional[str] = None
    source: Optional[str] = None
    url: Optional[str] = None
    # 新規追加フィールド (詳細ページ由来)
    option: Optional[str] = None  # h1 内 <span> 部分
    wd: Optional[str] = None      # ホイールベース等 (?) 指定セル
    seat: Optional[str] = None
    door: Optional[str] = None
    fuel: Optional[str] = None
    handle: Optional[str] = None
    jc08: Optional[str] = None    # 燃費関連指標 (JC08)
    
    def to_db_row(self) -> Dict[str, Any]:
        d = asdict(self)
        raw_data = d.pop("raw", {})
        d["raw_json"] = json.dumps(raw_data, ensure_ascii=False)
        return d
