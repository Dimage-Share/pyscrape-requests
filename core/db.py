"""Compatibility shim re-exporting the app.db sqlite adapter.

This module preserves the historical imports from `core.db` while delegating
implementation to `app.db` to keep a single source of truth.
"""

from app.db import (
    get_connection,
    init_db,
    truncate_goo,
    bulk_insert_goo,
    upsert_car,
    bulk_upsert_cars,
)


__all__ = [
    "get_connection",
    "init_db",
    "truncate_goo",
    "bulk_insert_goo",
    "upsert_car",
    "bulk_upsert_cars",
]
