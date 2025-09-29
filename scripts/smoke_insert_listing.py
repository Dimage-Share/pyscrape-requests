import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import json
from types import SimpleNamespace
from app.db import bulk_insert_listing


class Dummy:
    
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
    
    def to_db_row(self):
        d = self.__dict__.copy()
        raw = d.pop('raw', {})
        d['raw_json'] = json.dumps(raw, ensure_ascii=False)
        return d


rec = Dummy(id='smoke-1', manufacturer='Test', name='Test Car', price=100, year=2020, rd=10000, engine=1500, raw={})

n = bulk_insert_listing([rec], site='smoke')
print('inserted', n)
