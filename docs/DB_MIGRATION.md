# DB マイグレーション手順 (SQLite -> MySQL)

このドキュメントは、ローカル環境でプロジェクトのデータストアを SQLite から MySQL に移行する手順をまとめたものです。手順は安全性（バックアップ、検証、ロールバック）を重視しており、Docker を使って MySQL を立ち上げ、アプリの MySQL アダプタにデータを移行します。

前提
- Docker がインストールされており起動していること
- Python 3.11 以上（ワークスペースに合わせる）
- リポジトリのルートで作業していること

概要
1. SQLite のバックアップ
2. MySQL コンテナを立ち上げ
3. requirements をインストール
4. MySQL アダプタでスキーマを作成
5. データの移行（付属スクリプトを利用）
6. 検証
7. ロールバック手順

作業手順

1) SQLite のバックアップ

- DB ファイルをコピーしてバックアップを作成します。通常は `database.db` が SQLite の DB ファイルです。

PowerShell:

```powershell
cp .\database.db .\database.db.bak
```

2) MySQL を起動

- ルートで `docker-compose.yml` を用いて MySQL を起動します（既に用意済み）。

```powershell
docker compose up -d
```

- 起動を確認するには:

```powershell
docker compose ps
```

3) Python 依存をインストール

```powershell
python -m pip install -r requirements.txt
```

4) MySQL スキーマ作成

- 環境変数 `DB_BACKEND` を `mysql` にして `app.db.init_db()` を呼び出します。簡単な方法は付属のサニティスクリプトを実行することです。

```powershell
$env:DB_BACKEND='mysql'
python tmp_mysql_sanity.py
```

- これで `car` と `goo` のテーブルが作成されます。

5) データ移行

- `scripts/sqlite_to_mysql.py` を使って SQLite からデータを読み取り、MySQL に書き込みます。

```powershell
$env:DB_BACKEND='mysql'
python scripts\sqlite_to_mysql.py
```

- スクリプトはインクリメンタルで実行するように作っています（失敗時に再実行可能）。

6) 検証

- MySQL に移行された行数と SQLite の行数を比較します。簡易コマンド例:

```powershell
python - <<'PY'
import sqlite3, pymysql
scon=sqlite3.connect('database.db')
print('sqlite goo:', scon.execute('select count(*) from goo').fetchone())
print('sqlite car:', scon.execute('select count(*) from car').fetchone())
import pymysql
mcon=pymysql.connect(host='127.0.0.1',port=33062,user='pyscrape',password='pyscrape_pwd',database='pyscrape')
with mcon.cursor() as cur:
    cur.execute('select count(*) from goo')
    print('mysql goo:', cur.fetchone())
    cur.execute('select count(*) from car')
    print('mysql car:', cur.fetchone())
mcon.close()
scon.close()
PY
```

7) ロールバック

- もし移行に問題があれば、MySQL のデータは削除して再移行するか、SQLite のバックアップに戻すことができます。

- MySQL データ削除（迅速な方法）:

```powershell
docker compose down -v
# 再作成する場合は docker compose up -d
```

- SQLite バックアップから復元:

```powershell
mv .\database.db.bak .\database.db
```

注意事項
- 本番環境への移行は段階的に計画してください。データ整合性、ダウンタイム、接続設定（パスワード、ホスト）などを十分に検討してください。
- 本ドキュメントはローカル開発向けです。本番環境向けには別途詳細な移行手順（ダンプ、インポート、DB パラメータ調整、インデックス・パーティショニング等）を設計してください。

---
更新履歴
- 2025-09-29: 初版（自動スクリプトとサニティチェックを追加）
