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
    
    def to_db_row(self) -> Dict[str, Any]:
        d = asdict(self)
        raw_data = d.pop("raw", {})
        d["raw_json"] = json.dumps(raw_data, ensure_ascii=False)
        return d
