# Repository Mapping for pyscrape-requests

このドキュメントは、現状のリポジトリを先に提案した新しいアーキテクチャに移行するためのマッピングを示します。

## 目的
- 機能ごとにコードを整理して保守性を高める
- SQLite / MySQL (Docker) の切替を容易にする
- スクレイパーを独立モジュール化し、再利用とテストを容易にする
- Flask のルーティングを Blueprint 単位で分離する

---

## 既存トップレベルファイル
- `web.py` -> `app/__init__.py` を使って起動する（現状のまま）
- `README.md`, `LICENSE`, `requirements.txt` などはそのまま維持

## 既存ディレクトリ & ファイル（現状）
- `app/`
  - `__init__.py` (現在) -> `app/__init__.py`（保持、create_app を調整）
  - `views.py` -> 分割して `app/views/search.py`, `app/views/pivot.py`, `app/views/admin.py` に移動
  - `__pycache__/` -> 無視
- `core/`
  - `client.py`, `console.py`, `db.py`, `logger.py`, `models.py`, `parser.py`, `scrape.py`
    - `core/db.py` -> 新しい `app/db/sqlite.py` の内容に移植（adapter にする）
    - `core/scrape.py`, `core/client.py`, `core/parser.py` -> `app/scrapers/` 以下に移行（分割）
    - `core/models.py` -> `app/models/`（必要なら SQLAlchemy モデルへ変換）
- `goo_net_scrape/` -> `app/scrapers/goo_net/` に移動（既存の `client.py`, `parser.py`, `models.py`, `db.py` を整理）
- `templates/` -> `app/templates/`（現状 `create_app` はプロジェクトルートの templates を使うため、移動は任意。現状維持でもOK）
- `templates/index.html`, `templates/pivot.html` -> `app/templates/index.html`, `app/templates/pivot.html`（もしくは templates/ のまま）

## 新規追加/移動先（推奨）
```
app/
  __init__.py
  views/
    search.py        # index() 等
    pivot.py         # pivot() と /pivot/data
    admin.py         # scrape(), 管理インタフェース
  templates/
    index.html
    pivot.html
  static/
    css/
    js/
  scrapers/
    base.py
    carsensor.py
    goonet.py
  db/
    base.py          # adapter interface
    sqlite.py        # 現在の実装を移植
    mysql.py         # MySQL 実装（接続ファクトリ）
  models/
    ...              # optional: SQLAlchemy models
  tasks/
    background.py    # 起動用 helper

core/                # 一時的に残すが、徐々に app/ に移動
goo_net_scrape/      # 移行先: app/scrapers/goonet/
templates/           # 変えない場合はこのまま
web.py               # 起動スクリプト（変更なし）
requirements.txt
Docker/              # docker compose, mysql init のためのファイル
```

## 具体的なファイルマッピング（例）
- `app/views.py` ->
  - `app/views/search.py` : `index()` と関連の SQL/フィルタ処理
  - `app/views/pivot.py` : `pivot()` と `pivot_data()`
  - `app/views/admin.py` : `scrape()` と管理用フラグ操作

- `core/db.py` -> `app/db/sqlite.py`（関数名を `get_connection()` に揃える）
- `core/scrape.py` -> `app/scrapers/base.py` + `app/scrapers/<provider>.py` に分割
- `goo_net_scrape/*` -> `app/scrapers/goonet/*` に整理

## マイグレーション方針
1. まずは「リファクタ（ファイルを移す）」のみを実施し、API は既存と互換に保つ
2. 次に DB adapter を導入し、SQLite と MySQL の切替をサポート
3. 最終的に SQLAlchemy 等を導入するか検討（必要に応じて段階的に実施）

## 備考
- すぐに MySQL に切り替える必要が無ければ、まずは adapter の導入と Docker Compose の用意を推奨。
- スクレイパーは provider ごとに名前空間を分け、テストしやすいデザイン（HTTP モック可能）にします。

---

次のアクション候補（どれを実行しますか）:
- 実際に `REPO_MAPPING.md` に基づいてファイルを移動するパッチを作る
- DB adapter の雛形（`app/db/base.py`, `app/db/sqlite.py`, `app/db/mysql.py`）を追加する
- docker/docker-compose.yml と mysql 初期化 SQL を作成する
- scrapers フォルダを作り、`core/scrape.py` の分割を始める

希望を教えてください。
