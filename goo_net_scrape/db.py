from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Iterable, Optional
import logging
from datetime import datetime, timezone

from .models import CarRecord

logger = logging.getLogger(__name__)

DB_FILENAME = "database.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS car (
    id TEXT PRIMARY KEY,
    name TEXT,
    price INTEGER,
    year INTEGER,
    rd INTEGER,
    engine INTEGER,
    color TEXT,
    mission TEXT,
    bodytype TEXT,
    repair TEXT,
    location TEXT,
    source TEXT,
    url TEXT,
    created_at TEXT NOT NULL,
    raw_json TEXT
);
"""


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else Path(DB_FILENAME)
    conn = sqlite3.connect(path)
    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    conn = get_connection(db_path)
    try:
        with conn:
            conn.executescript(SCHEMA_SQL)
            # 既存 DB への簡易マイグレーション (ALTER 応急処置)
            # 旧→新カラム移行用の簡易 ALTER (存在しない場合のみ追加)
            for col in ("price", "rd", "engine", "color", "mission",
                        "bodytype", "repair", "source", "url"):
                try:
                    conn.execute(f"ALTER TABLE car ADD COLUMN {col} TEXT")
                except Exception:
                    pass
        logger.debug("DB init complete path=%s", db_path or DB_FILENAME)
    finally:
        conn.close()


def upsert_car(record: CarRecord, db_path: Optional[Path] = None) -> None:
    conn = get_connection(db_path)
    try:
        row = record.to_db_row()
        row["created_at"] = datetime.now(timezone.utc).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO car (
                    id,name,price,year,rd,engine,color,mission,bodytype,repair,location,source,url,created_at,raw_json
                ) VALUES (
                    :id,:name,:price,:year,:rd,:engine,:color,:mission,:bodytype,:repair,:location,:source,:url,:created_at,:raw_json
                )
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    price=excluded.price,
                    year=excluded.year,
                    rd=excluded.rd,
                    engine=excluded.engine,
                    color=excluded.color,
                    mission=excluded.mission,
                    bodytype=excluded.bodytype,
                    repair=excluded.repair,
                    location=excluded.location,
                    source=excluded.source,
                    url=excluded.url,
                    raw_json=excluded.raw_json
                ;
                """,
                row,
            )
    finally:
        conn.close()


def bulk_upsert_cars(records: Iterable[CarRecord],
                     db_path: Optional[Path] = None) -> int:
    count = 0
    conn = get_connection(db_path)
    try:
        with conn:
            for rec in records:
                row = rec.to_db_row()
                row["created_at"] = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """
                    INSERT INTO car (
                        id,name,price,year,rd,engine,color,mission,bodytype,repair,location,source,url,created_at,raw_json
                    ) VALUES (
                        :id,:name,:price,:year,:rd,:engine,:color,:mission,:bodytype,:repair,:location,:source,:url,:created_at,:raw_json
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name,
                        price=excluded.price,
                        year=excluded.year,
                        rd=excluded.rd,
                        engine=excluded.engine,
                        color=excluded.color,
                        mission=excluded.mission,
                        bodytype=excluded.bodytype,
                        repair=excluded.repair,
                        location=excluded.location,
                        source=excluded.source,
                        url=excluded.url,
                        raw_json=excluded.raw_json
                    ;
                    """,
                    row,
                )
                count += 1
        return count
    finally:
        conn.close()
