from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional, Any, Dict, List
import json


@dataclass
class CarSummary:
    title: Optional[str]
    price: Optional[int]
    link: Optional[str]


@dataclass
class CarRecord:
    """
    構造化された 1 車両分の情報。DB 保存対象。
    """
    id: str
    name: Optional[str]
    price: Optional[int]
    year: Optional[int]
    rd: Optional[int]  # running distance (km)
    engine: Optional[int]  # displacement cc
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
        # Ensure DB adapter bindings exist for expected columns even if None.
        for col in (
                "manufacturer",
                "mission1",
                "mission2",
                "option",
                "wd",
                "seat",
                "door",
                "fuel",
                "handle",
                "jc08",
                "category",
        ):
            d.setdefault(col, None)
        # Some code paths use 'mission' and DB expects 'mission1'/'mission2'.
        if "mission" in d and d.get("mission") is not None:
            # populate mission1 if empty
            if not d.get("mission1"):
                d["mission1"] = d.get("mission")
        return d
