# 次の改善案

## パース精緻化
1. 車両カード要素の CSS セレクタ調査 (例: div.listbox, li.list, article など実 DOM 確認)
2. 各項目:
   - タイトル (車名 + グレード)
   - 価格 (支払総額/車両価格/諸費用) -> 数値抽出 & 単位除去
   - 年式 (西暦/和暦 混在対応) -> 正規化 (YYYY)
   - 走行距離 (万km, km) -> 数値 + 単位
   - 車検有効期限 / 修復歴 / 排気量 / 駆動方式 等
3. ページ総件数・現在ページ・総ページ数

## アーキテクチャ
- parser 層をクリーンアーキテクチャ的に分離 (DOM -> Raw dict -> 正規化 Model)
- dataclass / pydantic で型安全化
- requests-cache 導入 (開発時負荷軽減)
- 失敗時の指数バックオフ + jitter

## 並列・パフォーマンス
- concurrent.futures ThreadPoolExecutor で複数ページ同時フェッチ
- レート制限 Token Bucket 実装

## テスト
- pytest + vcr.py で HTTP レスポンス記録
- BeautifulSoup パース単体テスト

## 運用
- ログローテーション (TimedRotatingFileHandler)
- CLI: --pages N --max-pages N など

## エラーハンドリング
- HTTP 429/5xx リトライポリシー調整
- パース失敗時に問題ある DOM スニペットをログへ

## 法的配慮
- robots.txt 自動取得 & キャッシュ & 解析 (Disallow パス検証)

