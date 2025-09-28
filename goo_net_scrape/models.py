from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional, Any, Dict, List
import json


@dataclass
class CarSummary:
    """(従来) 簡易リンク取得用のプレースホルダ。今後廃止予定。"""
    title: Optional[str]
    price: Optional[int]
    link: Optional[str]


@dataclass
class CarRecord:
    """構造化された 1 車両分の情報。DB 保存対象。

    total_price_yen: 支払総額 (万円単位・整数 / 小数点以下切り捨て)。不明は None。
    year: 西暦 (和暦/Hxx/Rxx 表記を正規化)。不明は None。
    mileage_km: 走行距離 km。"3.4万km" 等は数値化。 不明は None。
    displacement_cc: 排気量 cc。"2.0L" -> 2000。 不明は None。
    notes: 加工後の備考行配列 (色, ミッション, ボディタイプ 等)。
    repair_history: 修復歴表記 (原文保持)。
    location: 所在地。
    raw: 元の抽出中間データ (デバッグ用)。
    """
    id: str
    name: Optional[str]
    # price: 支払総額 (万円単位整数)。例: 288.4万円 -> 288
    price: Optional[int]
    year: Optional[int]
    rd: Optional[int]  # running distance (km)
    engine: Optional[int]  # displacement cc
    repair: Optional[str]
    location: Optional[str]
    raw: Dict[str, Any]
    # 派生フィールド (notes から抽出) - デフォルト None
    color: Optional[str] = None
    mission: Optional[str] = None
    bodytype: Optional[str] = None
    # カードHTMLソース全文
    source: Optional[str] = None
    url: Optional[str] = None  # 車名リンク先 (相対 or 絶対)

    def to_db_row(self) -> Dict[str, Any]:
        d = asdict(self)
        # raw は外部出力(JSON)では不要: DB 用の raw_json だけ付加
        raw_data = d.pop("raw", {})
        # 呼び出し元 (DB 保存) 用に raw_json を付与。run.py の JSON 出力では使用しないため
        d["raw_json"] = json.dumps(raw_data, ensure_ascii=False)
        return d
