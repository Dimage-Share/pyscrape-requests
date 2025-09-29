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
    """Create a new sqlite3 connection with WAL + busy timeout for concurrency.

    Notes:
        - check_same_thread=False を指定しスレッド間共有は *しない* 方針。
          (毎回新規接続を返すため安全) ただしFlask側から並列要求が来ても
          各リクエストで独立コネクションになる。
        - isolation_level=None により autocommit モード。明示的に with conn: を
          使う箇所はトランザクション開始される (sqlite3 は __enter__ で begin)。
        - WAL モードにより同時読取 (複数) + 単一書込がブロック短縮される。
    """
    path = Path(db_path) if db_path else Path(DB_FILENAME)
    conn = sqlite3.connect(
        path,
        timeout=30.0,  # busy timeout for writer contention
        check_same_thread=False,
        isolation_level=None,  # autocommit style
    )
    try:
        # Set WAL journal mode & reasonable synchronous for durability vs speed
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=30000;")  # 30s
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
            # ensure goo table columns exist (same as car)
            for col in ("manufacturer", "price", "rd", "engine", "color", "mission1", "mission2", "bodytype", "repair", "source", "url", "option", "wd", "seat", "door", "fuel", "handle", "jc08", "category"):
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
                        id,manufacturer,name,price,year,rd,engine,color,mission1,mission2,bodytype,repair,location,option,wd,seat,door,fuel,handle,jc08,source,url,created_at,raw_json
                    ) VALUES (
                        :id,:manufacturer,:name,:price,:year,:rd,:engine,:color,:mission1,:mission2,:bodytype,:repair,:location,:option,:wd,:seat,:door,:fuel,:handle,:jc08,:source,:url,:created_at,:raw_json
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
                count += 1
        return count
    finally:
        conn.close()
