"""MySQL adapter for app.db.

Provides the same public API as app.db.sqlite adapter so code can switch
between SQLite and MySQL by importing the appropriate module.

Connection parameters are read from environment variables with sensible
defaults that match the `docker-compose.yml` we added earlier.
"""
from __future__ import annotations
import os
from typing import Iterable, Optional, Dict, Any
from datetime import datetime
import pymysql

from core.logger import Logger
from core.models import CarRecord


log = Logger.bind(__name__)

# Read connection params from environment (defaults to docker-compose values)
DB_HOST = os.environ.get('MYSQL_HOST', '127.0.0.1')
DB_PORT = int(os.environ.get('MYSQL_PORT', '33062'))
DB_USER = os.environ.get('MYSQL_USER', 'pyscrape')
DB_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'pyscrape_pwd')
DB_NAME = os.environ.get('MYSQL_DATABASE', 'pyscrape')

# Note: pymysql.connect auto-commits only when autocommit True; we'll manage commits

SCHEMA_SQL_MYSQL = """
CREATE TABLE IF NOT EXISTS car (
    id VARCHAR(64) PRIMARY KEY,
    manufacturer VARCHAR(255),
    name VARCHAR(1024),
    price INT,
    year INT,
    rd INT,
    engine INT,
    color VARCHAR(255),
    mission1 VARCHAR(64),
    mission2 VARCHAR(255),
    bodytype VARCHAR(255),
    repair VARCHAR(255),
    location VARCHAR(255),
    `option` VARCHAR(255),
    wd VARCHAR(255),
    seat VARCHAR(255),
    door VARCHAR(255),
    fuel VARCHAR(255),
    handle VARCHAR(255),
    jc08 VARCHAR(255),
    category VARCHAR(64),
    source TEXT,
    url TEXT,
    created_at DATETIME NOT NULL,
    raw_json LONGTEXT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS goo (
    id VARCHAR(64) PRIMARY KEY,
    manufacturer VARCHAR(255),
    name VARCHAR(1024),
    price INT,
    year INT,
    rd INT,
    engine INT,
    color VARCHAR(255),
    mission1 VARCHAR(64),
    mission2 VARCHAR(255),
    bodytype VARCHAR(255),
    repair VARCHAR(255),
    location VARCHAR(255),
    `option` VARCHAR(255),
    wd VARCHAR(255),
    seat VARCHAR(255),
    door VARCHAR(255),
    fuel VARCHAR(255),
    handle VARCHAR(255),
    jc08 VARCHAR(255),
    source TEXT,
    url TEXT,
    created_at DATETIME NOT NULL,
    raw_json LONGTEXT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS listing (
    site VARCHAR(32) NOT NULL,
    id VARCHAR(64) NOT NULL,
    manufacturer TEXT NULL,
    name TEXT NULL,
    price INT NULL,
    year INT NULL,
    rd INT NULL,
    engine INT NULL,
    color TEXT NULL,
    mission1 TEXT NULL,
    mission2 TEXT NULL,
    bodytype TEXT NULL,
    repair TEXT NULL,
    location TEXT NULL,
    `option` TEXT NULL,
    wd TEXT NULL,
    seat TEXT NULL,
    door TEXT NULL,
    fuel TEXT NULL,
    handle TEXT NULL,
    jc08 TEXT NULL,
    category TEXT NULL,
    source LONGTEXT NULL,
    url LONGTEXT NULL,
    created_at DATETIME NULL,
    raw_json LONGTEXT NULL,
    PRIMARY KEY (site, id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""


def get_connection() -> pymysql.connections.Connection:
    conn = pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    return conn


def init_db() -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for stmt in SCHEMA_SQL_MYSQL.split(';'):
                s = stmt.strip()
                if not s:
                    continue
                cur.execute(s)
        conn.commit()
        log.debug('mysql init done')
    finally:
        conn.close()


def truncate_goo() -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('DELETE FROM goo')
        conn.commit()
    finally:
        conn.close()


def _to_mysql_row(record: CarRecord) -> Dict[str, Any]:
    row = record.to_db_row()
    # convert created_at / raw_json handled by callers
    return row


LISTING_COLUMNS = ['id', 'manufacturer', 'name', 'price', 'year', 'rd', 'engine', 'color', 'mission1', 'mission2', 'bodytype', 'repair', 'location', 'option', 'wd', 'seat', 'door', 'fuel', 'handle', 'jc08', 'category', 'source', 'url', 'created_at', 'raw_json']


def bulk_insert_listing(records: Iterable[CarRecord], site: str) -> int:
    """Insert many records into listing with given site. Uses ON DUPLICATE KEY UPDATE."""
    conn = get_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            cols = ','.join([f'`{c}`' for c in LISTING_COLUMNS] + ['`site`'])
            vals = ','.join([f'%({c})s' for c in LISTING_COLUMNS] + ['%(site)s'])
            update_expr = ','.join([f"`{c}`=VALUES(`{c}`)" for c in LISTING_COLUMNS if c != 'id'])
            insert_sql = f"INSERT INTO listing ({cols}) VALUES ({vals}) ON DUPLICATE KEY UPDATE {update_expr}"
            params = []
            for rec in records:
                row = _to_mysql_row(rec)
                # ensure all keys
                for c in LISTING_COLUMNS:
                    row.setdefault(c, None)
                row['created_at'] = datetime.utcnow().isoformat()
                row['site'] = site
                params.append(row)
            chunk_size = 500
            for i in range(0, len(params), chunk_size):
                chunk = params[i:i + chunk_size]
                cur.executemany(insert_sql, chunk)
                count += len(chunk)
        conn.commit()
        return count
    finally:
        conn.close()


def bulk_insert_goo(records: Iterable[CarRecord]) -> int:
    # Backwards compatibility: write goo-specific rows into unified listing with site='goo'
    return bulk_insert_listing(records, site='goo')


def upsert_car(record: CarRecord) -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            insert_sql = (
                "INSERT INTO car (id,manufacturer,name,price,year,rd,engine,color,mission1,mission2,bodytype,repair,location,`option`,wd,seat,door,fuel,handle,jc08,category,source,url,created_at,raw_json) "
                "VALUES (%(id)s,%(manufacturer)s,%(name)s,%(price)s,%(year)s,%(rd)s,%(engine)s,%(color)s,%(mission1)s,%(mission2)s,%(bodytype)s,%(repair)s,%(location)s,%(option)s,%(wd)s,%(seat)s,%(door)s,%(fuel)s,%(handle)s,%(jc08)s,%(category)s,%(source)s,%(url)s,%(created_at)s,%(raw_json)s) "
                "ON DUPLICATE KEY UPDATE manufacturer=VALUES(manufacturer),name=VALUES(name),price=VALUES(price),year=VALUES(year),rd=VALUES(rd),engine=VALUES(engine),color=VALUES(color),mission1=VALUES(mission1),mission2=VALUES(mission2),bodytype=VALUES(bodytype),repair=VALUES(repair),location=VALUES(location),`option`=VALUES(`option`),wd=VALUES(wd),seat=VALUES(seat),door=VALUES(door),fuel=VALUES(fuel),handle=VALUES(handle),jc08=VALUES(jc08),category=VALUES(category),source=VALUES(source),url=VALUES(url),raw_json=VALUES(raw_json)"
            )
            row = _to_mysql_row(record)
            row['created_at'] = datetime.utcnow().isoformat()
            cur.execute(insert_sql, row)
        conn.commit()
    finally:
        conn.close()


def bulk_upsert_cars(records: Iterable[CarRecord]) -> int:
    conn = get_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            insert_sql = (
                "INSERT INTO car (id,manufacturer,name,price,year,rd,engine,color,mission1,mission2,bodytype,repair,location,`option`,wd,seat,door,fuel,handle,jc08,category,source,url,created_at,raw_json) "
                "VALUES (%(id)s,%(manufacturer)s,%(name)s,%(price)s,%(year)s,%(rd)s,%(engine)s,%(color)s,%(mission1)s,%(mission2)s,%(bodytype)s,%(repair)s,%(location)s,%(option)s,%(wd)s,%(seat)s,%(door)s,%(fuel)s,%(handle)s,%(jc08)s,%(category)s,%(source)s,%(url)s,%(created_at)s,%(raw_json)s) "
                "ON DUPLICATE KEY UPDATE manufacturer=VALUES(manufacturer),name=VALUES(name),price=VALUES(price),year=VALUES(year),rd=VALUES(rd),engine=VALUES(engine),color=VALUES(color),mission1=VALUES(mission1),mission2=VALUES(mission2),bodytype=VALUES(bodytype),repair=VALUES(repair),location=VALUES(location),`option`=VALUES(`option`),wd=VALUES(wd),seat=VALUES(seat),door=VALUES(door),fuel=VALUES(fuel),handle=VALUES(handle),jc08=VALUES(jc08),category=VALUES(category),source=VALUES(source),url=VALUES(url),raw_json=VALUES(raw_json)"
            )
            params = []
            for rec in records:
                row = _to_mysql_row(rec)
                row['created_at'] = datetime.utcnow().isoformat()
                params.append(row)
            chunk_size = 500
            for i in range(0, len(params), chunk_size):
                chunk = params[i:i + chunk_size]
                cur.executemany(insert_sql, chunk)
                count += len(chunk)
        conn.commit()
        return count
    finally:
        conn.close()
