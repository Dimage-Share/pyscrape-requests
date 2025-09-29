# ローカル開発ガイド

このドキュメントはローカルでリポジトリをセットアップして開発を始めるための手順です。Flask Web アプリやスクレイパ、テストをローカルで動かすための最小手順を記載しています。

前提
- Python 3.11 以上がインストールされていること
- Docker がインストールされていること（MySQL を使う場合）
- Git リポジトリをチェックアウト済み

1) 仮想環境の作成

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
```

2) 依存インストール

```powershell
python -m pip install -r requirements.txt
```

3) MySQL（ローカル）を使う場合

- docker-compose.yml が用意されています。MySQL を起動:

```powershell
docker compose up -d
```

- 環境変数 `DB_BACKEND` を `mysql` にしてアプリを実行すると MySQL を使います。

```powershell
$env:DB_BACKEND='mysql'
python web.py
```

- もし sqlite を使う場合は `DB_BACKEND` を 'sqlite' に設定、もしくは環境変数を設定しないで実行してください。

4) アプリの実行

- Web UI を起動する:

```powershell
python web.py
```

- スクレイプを CLI で試す（簡易）:

```powershell
python tmp_e2e_run.py
```

5) テストの実行

```powershell
python -m pytest -q
```

6) よくあるトラブルと対処

- MySQL の認証エラー: MySQL 8 は caching_sha2_password をデフォルトで使うため、クライアントに `cryptography` が必要になる場合があります。docker compose に `--default-authentication-plugin=mysql_native_password` を追加済みです。
- 依存が足りない場合: `pip install -r requirements.txt` を実行してください。

---
更新履歴
- 2025-09-29: 初版（MySQL サニティスクリプトとローカル実行手順を追加）
