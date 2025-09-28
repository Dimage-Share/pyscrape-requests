from __future__ import annotations
"""Migrated from goo_net_scrape.db (transitional)."""
import sqlite3
from pathlib import Path
from typing import Iterable, Optional
from .logger import Logger


log = Logger.bind(__name__)
from datetime import datetime, timezone

from .models import CarRecord


logger = None  # legacy placeholder

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
CREATE TABLE IF NOT EXISTS goo (
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
            for col in ("price", "rd", "engine", "color", "mission", "bodytype", "repair", "source", "url"):
                try:
                    conn.execute(f"ALTER TABLE car ADD COLUMN {col} TEXT")
                except Exception:
                    pass
            # ensure goo table columns exist (same as car)
            for col in ("price", "rd", "engine", "color", "mission", "bodytype", "repair", "source", "url"):
                try:
                    conn.execute(f"ALTER TABLE goo ADD COLUMN {col} TEXT")
                except Exception:
                    pass
        log.debug(f"db init path={db_path or DB_FILENAME}")
    finally:
        conn.close()


def truncate_goo(db_path: Optional[Path] = None) -> None:
    """Delete all rows from goo table (truncate semantics)."""
    conn = get_connection(db_path)
    try:
        with conn:
            conn.execute("DELETE FROM goo;")
        log.debug("goo truncate done")
    finally:
        conn.close()


def bulk_insert_goo(records: Iterable[CarRecord], db_path: Optional[Path] = None) -> int:
    """Insert (not upsert) all records into goo table after truncate."""
    count = 0
    conn = get_connection(db_path)
    try:
        with conn:
            for rec in records:
                row = rec.to_db_row()
                row["created_at"] = datetime.now(timezone.utc).isoformat()
                # Sanitize: mission/bodytype label words -> NULL
                m = row.get("mission")
                if isinstance(m, str) and m.strip() == "ミッション":
                    row["mission"] = None
                b = row.get("bodytype")
                if isinstance(b, str) and b.strip() == "ボディタイプ":
                    row["bodytype"] = None
                conn.execute(
                    """
                    INSERT OR REPLACE INTO goo (
                        id,name,price,year,rd,engine,color,mission,bodytype,repair,location,source,url,created_at,raw_json
                    ) VALUES (
                        :id,:name,:price,:year,:rd,:engine,:color,:mission,:bodytype,:repair,:location,:source,:url,:created_at,:raw_json
                    );
                    """,
                    row,
                )
                count += 1
        return count
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


def bulk_upsert_cars(records: Iterable[CarRecord], db_path: Optional[Path] = None) -> int:
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
