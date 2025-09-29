from __future__ import annotations
"""SQLite adapter for app.db (migrated from core/db.py)."""
import sqlite3
from pathlib import Path
from typing import Iterable, Optional
from core.logger import Logger


log = Logger.bind(__name__)
from datetime import datetime, timezone

from core.models import CarRecord


DB_FILENAME = "database.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS car (
    id TEXT PRIMARY KEY,
    manufacturer TEXT,
    name TEXT,
    price INTEGER,
    year INTEGER,
    rd INTEGER,
    engine INTEGER,
    color TEXT,
    mission1 TEXT,
    mission2 TEXT,
    bodytype TEXT,
    repair TEXT,
    location TEXT,
    option TEXT,
    wd TEXT,
    seat TEXT,
    door TEXT,
    fuel TEXT,
    handle TEXT,
    jc08 TEXT,
    category TEXT,
    source TEXT,
    url TEXT,
    created_at TEXT NOT NULL,
    raw_json TEXT
);
CREATE TABLE IF NOT EXISTS goo (
    id TEXT PRIMARY KEY,
    manufacturer TEXT,
    name TEXT,
    price INTEGER,
    year INTEGER,
    rd INTEGER,
    engine INTEGER,
    color TEXT,
    mission1 TEXT,
    mission2 TEXT,
    bodytype TEXT,
    repair TEXT,
    location TEXT,
    option TEXT,
    wd TEXT,
    seat TEXT,
    door TEXT,
    fuel TEXT,
    handle TEXT,
    jc08 TEXT,
    source TEXT,
    url TEXT,
    created_at TEXT NOT NULL,
    raw_json TEXT
);
"""


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = Path(db_path) if db_path else Path(DB_FILENAME)
    conn = sqlite3.connect(
        path,
        timeout=30.0,
        check_same_thread=False,
        isolation_level=None,
    )
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
    except Exception as e:  # noqa: BLE001
        log.debug(f"pragma setup fail error={e}")
    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    conn = get_connection(db_path)
    try:
        with conn:
            conn.executescript(SCHEMA_SQL)
            for col in ("manufacturer", "price", "rd", "engine", "color", "mission1", "mission2", "bodytype", "repair", "source", "url", "option", "wd", "seat", "door", "fuel", "handle", "jc08", "category"):
                try:
                    conn.execute(f"ALTER TABLE car ADD COLUMN {col} TEXT")
                except Exception:
                    pass
            for col in ("manufacturer", "price", "rd", "engine", "color", "mission1", "mission2", "bodytype", "repair", "source", "url", "option", "wd", "seat", "door", "fuel", "handle", "jc08", "category"):
                try:
                    conn.execute(f"ALTER TABLE goo ADD COLUMN {col} TEXT")
                except Exception:
                    pass
        log.debug(f"db init path={db_path or DB_FILENAME}")
    finally:
        conn.close()


def truncate_goo(db_path: Optional[Path] = None) -> None:
    conn = get_connection(db_path)
    try:
        with conn:
            conn.execute("DELETE FROM goo;")
        log.debug("goo truncate done")
    finally:
        conn.close()


def bulk_insert_goo(records: Iterable[CarRecord], db_path: Optional[Path] = None) -> int:
    conn = get_connection(db_path)
    try:
        params = []
        for rec in records:
            row = rec.to_db_row()
            row["created_at"] = datetime.now(timezone.utc).isoformat()
            m = row.get("mission")
            if isinstance(m, str) and m.strip() == "ミッション":
                row["mission"] = None
            b = row.get("bodytype")
            if isinstance(b, str) and b.strip() == "ボディタイプ":
                row["bodytype"] = None
            params.append(row)
        with conn:
            if params:
                chunk_size = 500
                for i in range(0, len(params), chunk_size):
                    chunk = params[i:i + chunk_size]
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO goo (
                            id,manufacturer,name,price,year,rd,engine,color,mission1,mission2,bodytype,repair,location,option,wd,seat,door,fuel,handle,jc08,source,url,created_at,raw_json
                        ) VALUES (
                            :id,:manufacturer,:name,:price,:year,:rd,:engine,:color,:mission1,:mission2,:bodytype,:repair,:location,:option,:wd,:seat,:door,:fuel,:handle,:jc08,:source,:url,:created_at,:raw_json
                        );
                        """,
                        chunk,
                    )
        return len(params)
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
                    id,manufacturer,name,price,year,rd,engine,color,mission1,mission2,bodytype,repair,location,option,wd,seat,door,fuel,handle,jc08,category,source,url,created_at,raw_json
                ) VALUES (
                    :id,:manufacturer,:name,:price,:year,:rd,:engine,:color,:mission1,:mission2,:bodytype,:repair,:location,:option,:wd,:seat,:door,:fuel,:handle,:jc08,:category,:source,:url,:created_at,:raw_json
                )
                ON CONFLICT(id) DO UPDATE SET
                    manufacturer=excluded.manufacturer,
                    name=excluded.name,
                    price=excluded.price,
                    year=excluded.year,
                    rd=excluded.rd,
                    engine=excluded.engine,
                    color=excluded.color,
                    mission1=excluded.mission1,
                    mission2=excluded.mission2,
                    bodytype=excluded.bodytype,
                    repair=excluded.repair,
                    location=excluded.location,
                    option=excluded.option,
                    wd=excluded.wd,
                    seat=excluded.seat,
                    door=excluded.door,
                    fuel=excluded.fuel,
                    handle=excluded.handle,
                    jc08=excluded.jc08,
                    category=excluded.category,
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
    conn = get_connection(db_path)
    try:
        params = []
        for rec in records:
            row = rec.to_db_row()
            row["created_at"] = datetime.now(timezone.utc).isoformat()
            params.append(row)
        with conn:
            if params:
                chunk_size = 500
                for i in range(0, len(params), chunk_size):
                    chunk = params[i:i + chunk_size]
                    conn.executemany(
                        """
                        INSERT INTO car (
                            id,manufacturer,name,price,year,rd,engine,color,mission1,mission2,bodytype,repair,location,option,wd,seat,door,fuel,handle,jc08,category,source,url,created_at,raw_json
                        ) VALUES (
                            :id,:manufacturer,:name,:price,:year,:rd,:engine,:color,:mission1,:mission2,:bodytype,:repair,:location,:option,:wd,:seat,:door,:fuel,:handle,:jc08,:category,:source,:url,:created_at,:raw_json
                        )
                        ON CONFLICT(id) DO UPDATE SET
                            manufacturer=excluded.manufacturer,
                            name=excluded.name,
                            price=excluded.price,
                            year=excluded.year,
                            rd=excluded.rd,
                            engine=excluded.engine,
                            color=excluded.color,
                            mission1=excluded.mission1,
                            mission2=excluded.mission2,
                            bodytype=excluded.bodytype,
                            repair=excluded.repair,
                            location=excluded.location,
                            option=excluded.option,
                            wd=excluded.wd,
                            seat=excluded.seat,
                            door=excluded.door,
                            fuel=excluded.fuel,
                            handle=excluded.handle,
                            jc08=excluded.jc08,
                            category=excluded.category,
                            source=excluded.source,
                            url=excluded.url,
                            raw_json=excluded.raw_json
                        ;
                        """,
                        chunk,
                    )
        return len(params)
    finally:
        conn.close()
