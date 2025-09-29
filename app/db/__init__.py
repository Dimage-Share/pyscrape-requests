import os
from importlib import import_module


_backend = os.environ.get('DB_BACKEND', 'mysql').lower()
if _backend == 'mysql':
    _mod = import_module('app.db.mysql')
else:
    _mod = import_module('app.db.sqlite')

# Re-export the adapter's public functions used by the app
get_connection = _mod.get_connection
init_db = _mod.init_db
truncate_goo = _mod.truncate_goo
bulk_insert_goo = _mod.bulk_insert_goo
upsert_car = _mod.upsert_car
bulk_upsert_cars = _mod.bulk_upsert_cars

__all__ = [
    "get_connection",
    "init_db",
    "truncate_goo",
    "bulk_insert_goo",
    "upsert_car",
    "bulk_upsert_cars",
]
