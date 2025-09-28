# CarSensor 中古車一覧 スクレイパー (Goo-net版から移行済み)

`https://www.carsensor.net/usedcar/search.php?AR=4&SKIND=1` (例: 北海道エリア) の検索結果ページおよび 2 ページ目以降の `.../hokkaido/index2.html` 形式ページから、各車両カードの主要項目（名称 / 支払総額 / 年式 / 走行距離 / 排気量 / 修復歴 / 所在地 / ミッション / ボディタイプ / 詳細ページURL(絶対) / HTML断片ソース）を抽出し、SQLite に保存します。抽出ロジックはヒューリスティックであり、DOM 構造変更で失敗する可能性があります。旧 Goo-net 版のコードは一部残存しますが現在のデフォルトターゲットは CarSensor です。

## 特徴
- Selenium 非使用 (requests + Retry) による軽量高速取得
- 任意のクエリパラメータを `--param key=value` で付与可能
- lxml (XPath) + BeautifulSoup フォールバックの冗長パーサー
- 走行距離・年式 (和暦/西暦)・排気量・価格等を正規化
- 支払総額は 万円 単位の整数 (小数切り捨て) として保持 (`price`)
- 各カード HTML 断片 (`source`) を保存 (監査/再解析用)
- 中間抽出テキスト (raw) は DB 内 `raw_json` にのみ保持 (外部 JSON 出力からは除外)
- DB は不足カラムを自動 `ALTER TABLE ADD COLUMN` で逐次拡張 (簡易マイグレーション)
- JSON 標準出力 / ファイル出力・ログ出力サポート
- 詳細ページへの URL (`url`) は絶対形式 (https://www.carsensor.net/...)

## インストール
仮想環境(venv)は使用しない方針です。グローバル (もしくは --user) で依存を入れます。

Windows (PowerShell):
```powershell
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
```

macOS / Linux:
```bash
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

権限エラーが出る場合は `--user` を付与してください:
```powershell
py -m pip install --user -r requirements.txt
```

アンインストール例:
```powershell
py -m pip uninstall requests beautifulsoup4 lxml urllib3 colorama Flask
```

注意:
1. グローバル環境は他プロジェクトと依存衝突する可能性があります。
2. 既存バージョンと競合した場合は `pip install パッケージ==バージョン` で固定してください。
3. 将来的に依存数が増える / 衝突が発生した場合には再度仮想環境導入を検討してください。
4. CI 環境など再現性が重要な場面では `requirements.txt` のバージョンピン止めを推奨します。

## 使い方
```powershell
# シンプル実行 (標準出力に JSON)
python run.py

# クエリパラメータを付与 (例: ブランドコード仮定)
python run.py --param brand_cd=XXXX --param sort=1

# ファイル保存
python run.py --param brand_cd=XXXX --out result.json

# 複数ページ取得 (1〜3ページ, 各ページ1秒ウェイト)
python run.py --param brand_cd=XXXX --pages 3 --page-delay 1.0 --out result.json
```

### Web UI + 非同期スクレイピング (並行稼働)

Flask ベースの簡易検索 UI を起動し、画面からスクレイプを非同期実行できます。

```powershell
python web.py
```

ブラウザで `http://127.0.0.1:5000/` を開き、左メニュー下部「スクレイプ」フォームで Pages を指定して Run を押すとバックグラウンドスレッドが起動します。処理中も検索結果フィルタは利用できます。

内部仕様:
- SQLite を WAL モードでオープンし読み込みと書き込みを並行
- 実行開始時に `goo` テーブルを全削除後、新データを一括投入
- 実行中はフォーム入力が無効化され二重起動を防止
- 完了時刻 / エラーは左メニューに表示

注意:
- 実行中は一時的に結果 0 件になることがあります (更新中のため)
- 差分更新を行いたい場合は `truncate_goo()` をやめ UPSERT 方針に書き換える必要があります

拡張案 (未実装): 進捗API `/status`, スケジュール実行, 差分同期, インデックス最適化など。

## 出力例 (一部抜粋)
```json
{
  "items": [
    {"title": "中古車", "price_text": null, "url": "/"}
  ],
  "meta": {"extracted_links": 1, "pages": 3, "car_count": 90},
  "cars": [
    {
      "id": "988025092500602361001",
      "name": "ＲＸ",
      "price": 495,                 // 支払総額 (万円) 小数切り捨て: 495.8 -> 495
      "year": 2021,                // 西暦整数 (和暦は内部で変換)
      "rd": 63000,                 // 走行距離 (km) 3.2万km 等は展開
      "engine": 3500,              // 排気量 cc (1.5L -> 1500 等 正規化)
      "color": "カラーホワイトノーヴァガラスフレーク",
      "mission": "ミッションMTモード付きAT",
      "bodytype": "ボディタイプSUV・クロスカントリー",
      "repair": "修復歴なし",
      "location": null,            // 推測できない場合 null
  "url": "https://www.goo-net.com/usedcar/spread/goo_sort/15/988025092500602361001.html", // 絶対URL
  "source": "<div class=\"section_body clearfix\">..."     // カード HTML 断片
    }
  ]
}
```

## DB (SQLite) スキーマ
初回実行または不足カラム検出時に `database.db` 内 `car` テーブルを作成 / 拡張します (不足分を `ALTER TABLE ADD COLUMN`)。

| カラム     | 説明                                                                     |
| ---------- | ------------------------------------------------------------------------ |
| id         | CarSensor 詳細URL内 AU[数字] ID (AU + 数字) 主キー                       |
| name       | 車両名称 (ブランド + 車種/グレード断片)                                  |
| price      | 支払総額 (万円, 小数切り捨て整数)                                        |
| year       | 西暦年 (和暦記載は内部変換, 不明時 null)                                 |
| rd         | 走行距離 (km, 「X.X万km」表記を展開, 不明時 null)                        |
| engine     | 排気量 (cc, 「1.5L」→1500 等 正規化, 不明時 null)                        |
| color      | 色行 (備考群 or notes_array から抽出)                                    |
| mission    | ミッション行                                                             |
| bodytype   | ボディタイプ行                                                           |
| repair     | 修復歴表記                                                               |
| location   | 所在地 (県/市等をヒューリスティック抽出, 取得不能時 null)                |
| source     | カード HTML 断片 (監査/再パース用。サイズ増大に注意)                     |
| url        | 詳細ページ絶対 URL (例: https://www.carsensor.net/usedcar/detail/AU.../) |
| raw_json   | (外部出力しない) 中間テキスト / notes_array / 元フィールド文字列 JSON    |
| created_at | Upsert 時刻 (UTC ISO8601)                                                |

再取得 (同一 id) は upsert され `created_at` が更新されます。

### フィールド命名/単位ポリシー
- price: 万円整数。表示が 588.8万円 の場合 588 を保存 (精度より安定比較重視)。
- rd: km 整数。`4.5万km` → 45000, `10.8万km` → 108000。
- engine: cc 整数。`1.5L` → 1500。
- url: 取得時点で常に `https://www.carsensor.net` を付与した絶対 URL。
- source: 解析開始直後に取得したカード要素の HTML。後処理で DOM 変化があっても再パース可能。

### 旧 Goo-net バージョンからの主な変更 (CarSensor移行)
| 旧              | 新     | 変更理由                                                  |
| --------------- | ------ | --------------------------------------------------------- |
| total_price_yen | price  | 短縮・単位明確化 (万円固定)                               |
| mileage_km      | rd     | 頻出短縮 (run distance) + 内部指標簡潔化                  |
| displacement_cc | engine | 用語汎用化                                                |
| repair_history  | repair | 短縮                                                      |
| notes (配列)    | (廃止) | 個別カラム抽出 + 元テキストは raw_json.notes_array に保持 |
| (なし)          | source | 監査/再抽出用 HTML 断片                                   |
| (なし)          | url    | 詳細遷移/後続スクレイピング起点                           |

既存 (Goo-net版) DB を保持したままでも不足カラムは自動追加されますが、Goo 特有 ID は再利用されません。クリーンにしたい場合:
```powershell
Remove-Item .\database.db
```
再実行してください。

### (内部) raw_json について
外部 JSON 出力には含めませんが、DB には正規化前のテキスト/配列を `raw_json` として保存しています。正規化ロジックを後で改善 / 差分検証する用途を想定しています。

### 手動確認例
```powershell
sqlite3 .\database.db ".mode markdown" ".headers on" "SELECT id,name,price,year,rd,engine,url FROM car LIMIT 5;"
```

## CarSensor 移行差分サマリ

| 項目        | Goo-net 旧仕様        | CarSensor 現仕様              | 備考                                         |
| ----------- | --------------------- | ----------------------------- | -------------------------------------------- |
| 初回URL     | summary.php (params)  | search.php?AR=4&SKIND=1       | `config.json` で `carsensor_url` 上書き可    |
| 2ページ以降 | rel=next / 数字リンク | /usedcar/hokkaido/indexN.html | `get_next_page_url_carsensor` で indexN 探索 |
| id          | tr_/td_ DOM id        | AU[0-9]+ (URL中)              | 一意性/短縮性向上                            |
| price       | em内 万円テキスト     | 「支払総額」ラベル近傍        | 万円無表記は未対応(None)                     |
| bodytype    | notes配列推測         | li テキストキーワード         | 精度調整余地あり                             |
| location    | span県/府/都/道       | 任意テキスト(県等含む)        | 長過ぎる文字列除外                           |

既知制限: A/B プラン複数価格, 税込/税抜差異, グレード差分など細部は未取得。必要に応じ `source` HTML を二次解析してください。

## 今後の TODO (例)
- [ ] 相対 URL (`url`) をオプションで絶対化する `--abs-url` フラグ
- [ ] 年式が和暦 + 年式不明 ("年式(初度登録年)\n不明") パターンの追加正規化
- [ ] 支払総額の税抜/税込ラベル差異対応
- [ ] ページ終端自動検知 (次ボタン/取得件数減少) 実装
- [ ] 並列 / 非同期取得 (aiohttp など) オプション化
- [ ] HTTP レスポンスキャッシュ (ETag / Last-Modified / ローカルディスク)
- [ ] pytest + 固定 HTML スナップショット回帰テスト
- [ ] robots.txt / 利用規約チェック自動化
- [ ] レート制限・指数バックオフ・ランダムジッター
- [ ] `source` サイズ肥大対策 (圧縮/外部テーブル分離オプション)

## 法的・倫理的注意
- `robots.txt` と 利用規約を必ず確認してください。
- 高頻度アクセスや大量データ取得はサーバー負荷になるため避けてください。
- 取得データの再配布・商用利用は権利確認が必要です。
- 個人情報 (販売店担当者名等) が含まれる場合は二次利用に注意してください。

## ライセンス
内部利用想定 (必要に応じて追記)
