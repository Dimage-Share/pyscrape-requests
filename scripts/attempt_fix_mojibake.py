"""Attempt to repair mojibake in listing textual columns using encoding heuristics.

Dry-run by default. Use --commit to apply updates.

This script tries reinterpreting stored Python strings as bytes (utf-8 and latin-1)
and then decoding those bytes with common Japanese encodings, scoring results
by a Japanese-character heuristic. If a decoded candidate looks significantly
more Japanese than the stored value, it is proposed (or applied with --commit).
"""
from __future__ import annotations
import argparse
from pathlib import Path
import sys
from typing import List


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import get_connection
from core.encoding import _score_text


TEXT_COLUMNS: List[str] = ['manufacturer', 'name', 'color', 'bodytype', 'repair', 'location', 'option', 'wd', 'seat', 'door', 'fuel', 'handle', 'jc08', 'category', 'source', 'url']


def best_redecode(s: str):
    """Return (best_text, best_score, method) or (None, None, None)"""
    if not s:
        return None, 0.0, None
    orig_score = _score_text(s)
    
    candidates = []
    try:
        b_utf8 = s.encode('utf-8', errors='replace')
        candidates.append(('utf8->', b_utf8))
    except Exception:
        pass
    try:
        b_l1 = s.encode('latin-1', errors='replace')
        candidates.append(('latin1->', b_l1))
    except Exception:
        pass
    
    best = (None, orig_score, None)
    enc_targets = ['utf-8', 'cp932', 'shift_jis', 'euc_jp', 'iso-2022-jp']
    for tag, b in candidates:
        for enc in enc_targets:
            try:
                txt = b.decode(enc, errors='strict')
            except Exception:
                continue
            sc = _score_text(txt)
            if sc > best[1]:
                best = (txt, sc, f"{tag}{enc}")
    return best


def scan_and_fix(limit: int, commit: bool):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cols = ','.join([f"`{c}`" for c in TEXT_COLUMNS])
            sql = f"SELECT `site`,`id`,{cols} FROM listing LIMIT %s"
            cur.execute(sql, (limit, ))
            rows = cur.fetchall()
            
            total_rows = 0
            total_updates = 0
            samples = []
            for r in rows:
                total_rows += 1
                site = r['site']
                id_ = r['id']
                updates = {}
                for col in TEXT_COLUMNS:
                    val = r.get(col)
                    if not isinstance(val, str) or not val:
                        continue
                    orig_score = _score_text(val)
                    best_txt, best_score, method = best_redecode(val)
                    # require substantial improvement
                    if best_txt and best_score > max(0.25, orig_score + 0.35):
                        updates[col] = best_txt
                        samples.append((site, id_, col, val[:60], best_txt[:60], orig_score, best_score, method))
                if updates:
                    total_updates += 1
                    if commit:
                        set_expr = ",".join([f"`{c}`=%s" for c in updates.keys()])
                        params = list(updates.values()) + [site, id_]
                        sql = f"UPDATE listing SET {set_expr} WHERE site=%s AND id=%s"
                        # note: params order should match set values then where
                        # but since we appended site,id at end, adjust
                        # build params accordingly
                        with conn.cursor() as cur2:
                            cur2.execute(sql, params)
            if commit:
                conn.commit()
            return total_rows, total_updates, samples
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=10000)
    ap.add_argument('--commit', action='store_true')
    args = ap.parse_args()
    total_rows, total_updates, samples = scan_and_fix(args.limit, args.commit)
    print(f"Scanned rows: {total_rows}")
    print(f"Rows with proposed updates: {total_updates}")
    if samples:
        print('\nSample proposed fixes:')
        for s in samples[:50]:
            site, id_, col, orig, new, oscore, nscore, method = s
            print(f"{site}/{id_} {col} orig_score={oscore:.3f} new_score={nscore:.3f} method={method}")
            print(f"  - orig: {orig}")
            print(f"  - new : {new}\n")
    if not args.commit:
        print('\nDry run (no --commit). Use --commit to apply updates.')


if __name__ == '__main__':
    main()
