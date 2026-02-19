# SINET Stream クライアント（SINDAN 結果送信）

このスクリプトは，SINDAN クライアントの出力 JSON ログを読み込み，Phase ごとに集約した CSV を保存し，SINET Stream（MQTT）経由で JSON をブローカに Publish する．

このプロジェクトは [uv](https://docs.astral.sh/uv/) でパッケージ・仮想環境を管理しています．依存関係は `pyproject.toml` と `uv.lock` で固定されています．

## 概要

- **入力**: `~/log/tmp` 配下のタイムスタンプフォルダ（`YYYYMMDDHHMMSS`）内の SINDAN ログ（`campaign_*.json`, `sindan_*.json`）
- **処理**: 各 Phase（0〜6）の JSON を 1 つにまとめた `allphase.csv` を同じフォルダに保存
- **送信**: SINET Stream の `MessageWriter` でトピック `sindan/{hostname}/phase{0-6}` に JSON を Publish
- **送信後**: 対象フォルダを `~/log/sent` に移動し ZIP 化（30 日より古い ZIP は削除）

## 必要環境

- uv（パッケージ・仮想環境の管理に使用）
- Python 3.13 以上（`.python-version` で 3.13 を指定）

## 設定（main.py 先頭の定数）

| 定数 | 説明 |デフォルト |
|------|------|-----|
| `UNSENT_DIR` | 未送信ログの置き場 | `~/log/tmp` |
| `SENT_DIR` | 送信済みログの置き場（ZIP 保存先） | `~/log/sent` |
| `LOG_FILE` | 本スクリプトのログファイル | `~/log/python.log` |
| `SERVICE` | SINET Stream のサービス名 | `broker1` |
| `MQTT_TOPIC_BASE` | トピックのベース | `sindan` |
| `MQTT_QOS` | MQTT QoS | `0` |
| `MQTT_RETAIN` | メッセージの retain | `False` |

SINET Stream の設定ファイルは，`~/.config/sinetstream/config.yml` から確認することができます．

## 各 Phase とレイヤー

| Phase | layer |
|-------|--------|
| 0 | hardware |
| 1 | datalink |
| 2 | interface |
| 3 | localnet |
| 4 | globalnet |
| 5 | dns |
| 6 | app |

## ディレクトリ・ファイルの動き

1. **処理対象**: `UNSENT_DIR` 直下の，現在時刻の 10 分前 ~ 現在のタイムスタンプ名（14 桁数字）のサブディレクトリのみ
2. **campaign_uuid**: 診断結果ごとに固有の値で，`campaign_*.json` の `log_campaign_uuid` から取得する．ただし，実行が失敗したなどの理由でキャンペーンログが生成されていなければ代わりに `sindan_*.json` から取得する．
3. **Publish 成功時**: そのフォルダを `SENT_DIR` に移動し，中身を ZIP にまとめてから元ディレクトリを削除する
4. **ログ**: 本スクリプトのログは `LOG_FILE` に出力する．10MB でローテーションし、`.1`〜`.4` の 4 ファイルを保持する．

## セットアップ

[SINETStream 送信プログラム実行までのセットアップ手順](https://github.com/labnet-member/CRDED-AIOps-DevDocs/blob/main/docs/sinetstream-client/setup/client.md) を参照

## ログローテーション

- ログファイルは 10MB を超えると次の番号（`.1` → `.2` → `.3` → `.4`）に切り替え
- `.4` まで到達するとローテーションし，新しい `.1` に上書き

## プロジェクト構成（uv）

| ファイル | 説明 |
|----------|------|
| `pyproject.toml` | プロジェクトメタデータと依存関係（uv が参照） |
| `uv.lock` | 依存関係のロックファイル（`uv sync` で更新） |
| `.python-version` | 使用する Python バージョン（3.13） |
