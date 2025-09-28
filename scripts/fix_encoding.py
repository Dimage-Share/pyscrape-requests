#!/usr/bin/env python
"""SQLite 内の mojibake (UTF-8 を latin-1 誤デコード) を自動検出/修復するワンショットツール。

使用例:
  (プレビュー) python scripts/fix_encoding.py --db database.db
  (適用)       python scripts/fix_encoding.py --db database.db --apply

検出ロジック (保守的):
 1) 対象カラム (name, mission, bodytype, repair, location) の文字列に日本語([一-龥ぁ-んァ-ン])が含まれない
 2) かつ latin-1 バイト往復 -> utf-8 decode で日本語文字が出現
 3) かつ 復元後の日本語文字数 > 0

復元文字列が既存値と全く同一 or 条件未充足ならスキップ。
--apply 無し: 変更候補を一覧表示 (最大 500)。
--apply 有り: UPDATE 実行。コミット後に件数表示。

安全性: 変換で UnicodeDecodeError が出た場合は無視します。
"""
from __future__ import annotations
import argparse
import re
import sqlite3
from pathlib import Path
from typing import List, Tuple

TARGET_COLUMNS = ["name", "mission", "bodytype", "repair", "location"]
KANJI_RE = re.compile(r"[一-龥ぁ-んァ-ン]")


def detect_and_fix(text: str) -> str | None:
    if not text:
        return None
    # 既に日本語を含むなら触らない
    if KANJI_RE.search(text):
        return None
    try:
        # latin-1 でバイト化 → UTF-8 再デコード
        repaired = text.encode("latin-1", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return None
    if repaired == text:
        return None
    if not KANJI_RE.search(repaired):
        return None
    return repaired


def process(db_path: Path, apply: bool) -> Tuple[int, int, List[Tuple[str, str, str]]]:
    conn = sqlite3.connect(db_path)
    patches: List[Tuple[str, str, str]] = []  # (id, col, new_value)
    examined = 0
    try:
        cur = conn.execute("SELECT id, {} FROM goo".format(
            ",".join(TARGET_COLUMNS)
        ))
        for row in cur.fetchall():
            car_id = row[0]
            examined += 1
            for idx, col in enumerate(TARGET_COLUMNS, start=1):
                val = row[idx]
                if not isinstance(val, str):
                    continue
                new_val = detect_and_fix(val)
                if new_val is not None:
                    patches.append((car_id, col, new_val))
        if apply and patches:
            with conn:
                for car_id, col, new_val in patches:
                    conn.execute(f"UPDATE goo SET {col}=? WHERE id=?", (new_val, car_id))
        return examined, len(patches), patches[:500]
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='database.db', help='ターゲット SQLite DB パス')
    ap.add_argument('--apply', action='store_true', help='検出結果を実際に UPDATE する')
    args = ap.parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERR] DB not found: {db_path}")
        return 2
    examined, patched, sample = process(db_path, args.apply)
    mode = 'APPLY' if args.apply else 'DRY-RUN'
    print(f"[{mode}] examined_rows={examined} repair_candidates={patched}")
    if not args.apply:
        for car_id, col, new_val in sample:
            print(f"  id={car_id} col={col} -> {new_val}")
        if patched > len(sample):
            print(f"  ... ({patched - len(sample)} more)")
    else:
        print("[DONE] Updated.")
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
