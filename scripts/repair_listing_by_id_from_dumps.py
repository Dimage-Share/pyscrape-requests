"""Repair listing rows by locating their original dump page and reparsing that page.

Dry-run by default. Use --commit to apply updates.

Workflow:
 - find candidate listing rows that look mojibake (heuristic)
 - build an index of dump json files mapping record id -> dump file
 - for each candidate, load dump html bytes and re-decode using chosen_encoding or heuristics
 - reparse page and locate the record by id, then upsert via bulk_insert_listing
"""
from __future__ import annotations
import argparse
from pathlib import Path
import sys
import json
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection, bulk_insert_listing
from core.encoding import decode_response, _score_text
from goo_net_scrape import parser as gparser


def looks_mojibake(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    # obvious replacement character
    if '\ufffd' in s:
        return True
    # some common mojibake artifacts like Ã, â, etc
    if any(ch in s for ch in ['Ã', 'â', '�']):
        return True
    # low japanese score but nonempty (likely not Japanese) - be conservative
    sc = _score_text(s)
    if sc < 0.05 and any(ord(c) > 127 for c in s):
        return True
    return False


def build_dump_index(dump_dir: Path) -> Dict[str, Path]:
    idx: Dict[str, Path] = {}
    for p in sorted(dump_dir.glob('goo_page*.json')):
        try:
            j = json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            continue
        for rec in j.get('records', []) or []:
            rid = rec.get('id')
            if rid:
                idx[rid] = p
    return idx


def reparse_record_from_dump(dump_path: Path, target_id: str):
    # read chosen encoding from json metadata or meta
    try:
        j = json.loads(dump_path.read_text(encoding='utf-8'))
    except Exception:
        return None
    chosen = j.get('chosen_encoding') or j.get('meta', {}).get('chosen_encoding')
    html_path = dump_path.with_suffix('.html')
    html = j.get('html_snippet', '')
    if html_path.exists():
        raw = html_path.read_bytes()
        if chosen:
            try:
                html_full = raw.decode(chosen, errors='strict')
            except Exception:
                html_full = raw.decode('utf-8', errors='replace')
        else:
            
            class FakeResp:
                
                def __init__(self, content):
                    self._content = content
                    self.headers = {}
                    self.url = str(dump_path)
                
                @property
                def content(self):
                    return self._content
            
            fake = FakeResp(raw)
            html_full = decode_response(fake)
        html = html_full
    
    try:
        records = gparser.parse_cars(html)
    except Exception:
        records = []
    
    for r in records or []:
        rid = getattr(r, 'id', None) or (r.get('id') if isinstance(r, dict) else None)
        if rid == target_id:
            return r
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dump-dir', type=str, default='dumps')
    ap.add_argument('--limit', type=int, default=10000)
    ap.add_argument('--commit', action='store_true')
    args = ap.parse_args()
    
    dump_dir = Path(args.dump_dir)
    if not dump_dir.exists():
        print('dump dir not found')
        return
    
    idx = build_dump_index(dump_dir)
    print(f'dump index size: {len(idx)}')
    
    conn = get_connection()
    candidates: List[Dict] = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT site,id,name FROM listing LIMIT %s", (args.limit, ))
            rows = cur.fetchall()
            for r in rows:
                name = r.get('name') or ''
                if looks_mojibake(name):
                    candidates.append(r)
    finally:
        conn.close()
    
    print(f'Found {len(candidates)} candidate rows that look garbled')
    
    to_upsert = []
    fixes = []
    for c in candidates:
        site = c['site']
        rid = c['id']
        dump_path = idx.get(rid)
        if not dump_path:
            continue
        rec = reparse_record_from_dump(dump_path, rid)
        if rec:
            to_upsert.append(rec)
            fixes.append((rid, dump_path))
    
    print(f'Parsed {len(to_upsert)} records from dumps matching garbled rows')
    
    if not args.commit:
        print('Dry run (no --commit). Use --commit to apply fixes.')
        for rid, dp in fixes[:50]:
            print(f'{rid} -> {dp.name}')
        return
    
    if to_upsert:
        inserted = bulk_insert_listing(to_upsert, site='goo')
        print(f'Inserted/upserted {inserted} records into listing')
    else:
        print('No records to upsert')


if __name__ == '__main__':
    main()
