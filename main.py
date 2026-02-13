# ====================================================
# SINET Streamライブラリを用いたデータ送信プログラム
# ====================================================

"""
SINDANクライアントのJSONログファイルを読み込み、
各Phaseの出力を1つにまとめたCSVファイルを保存し、
MQTTブローカにJSON形式でpublishするスクリプト
"""

# ライブラリのインポート
import json
import os
import glob
import shutil
import logging
import re
import socket
import zipfile
from datetime import datetime, timedelta
from sinetstream import MessageWriter

# ==== 設定セクション（環境に応じて修正） =====
# ディレクトリ設定
UNSENT_DIR = os.path.expanduser("~/log/tmp")
SENT_DIR = os.path.expanduser("~/log/sent")
LOG_FILE = os.path.expanduser("~/log/python.log")

# SINETStream Writer 設定
SERVICE = "broker1"        # SINETStream のサービス名
MQTT_TOPIC_BASE = "sindan"  # MQTTトピックのベースパス（実際のトピック形式: sindan/{hostname}/phase{phase}）
MQTT_QOS = 0
MQTT_RETAIN = False

# Phaseとlayerの対応
PHASE_LAYERS = {
    0: "hardware",
    1: "datalink",
    2: "interface",
    3: "localnet",
    4: "globalnet",
    5: "dns",
    6: "app"
}

# =============================================

# ディレクトリが存在しない場合は作成
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


# カスタムログハンドラー（10MBでローテーション、1~4のファイルを使用）
class RotatingLogHandler(logging.Handler):
    def __init__(self, base_filename, max_bytes=10*1024*1024, backup_count=4):
        super().__init__()
        self.base_filename = base_filename
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.current_file = None
        self.current_file_num = None
        self._open_current_file()
    
    def _get_log_filename(self, num):
        """ログファイル名を取得（1~4の番号）"""
        return f"{self.base_filename}.{num}"
    
    def _open_current_file(self):
        """現在のログファイルを開く（必要に応じてローテーション）"""
        # 使用可能なファイル番号を探す（1から順に）
        self.current_file_num = self._find_next_available_file()
        
        # ファイルを開く
        log_filename = self._get_log_filename(self.current_file_num)
        self.current_file = open(log_filename, 'a', encoding='utf-8')
    
    def _find_next_available_file(self):
        """次に使用可能なログファイル番号を探す"""
        for num in range(1, self.backup_count + 1):
            filename = self._get_log_filename(num)
            if not os.path.exists(filename):
                return num
            if os.path.getsize(filename) < self.max_bytes:
                return num
        # すべて満杯の場合は1に戻る（古い1を削除して新しい1を作成）
        return 1
    
    def _rotate(self):
        """ログファイルをローテーション（4→3→2→1の順にシフト、1は削除）"""
        # 4から2まで逆順にシフト（4→3, 3→2, 2→1）
        for num in range(self.backup_count, 1, -1):
            old_file = self._get_log_filename(num)
            new_file = self._get_log_filename(num - 1)
            
            if os.path.exists(old_file):
                # 次の番号のファイルが存在する場合は削除してから移動
                if os.path.exists(new_file):
                    os.remove(new_file)
                os.rename(old_file, new_file)
        
        # 1のファイルは削除（新しい1が作成される）
        file_1 = self._get_log_filename(1)
        if os.path.exists(file_1):
            os.remove(file_1)
    
    def emit(self, record):
        """ログレコードを出力"""
        try:
            # 現在のファイルが満杯かチェック
            if self.current_file:
                current_size = os.path.getsize(self.current_file.name)
                if current_size >= self.max_bytes:
                    self.current_file.close()
                    # 現在のファイルが4の場合はローテーション
                    if self.current_file_num == self.backup_count:
                        self._rotate()
                    # 次のファイルを開く
                    self._open_current_file()
            
            # ファイルが開かれていない場合は開く
            if not self.current_file:
                self._open_current_file()
            
            # 書き込み前に再度サイズチェック（他のプロセスが書き込んだ可能性がある）
            if self.current_file:
                current_size = os.path.getsize(self.current_file.name)
                if current_size >= self.max_bytes:
                    self.current_file.close()
                    # 現在のファイルが4の場合はローテーション
                    if self.current_file_num == self.backup_count:
                        self._rotate()
                    # 次のファイルを開く
                    self._open_current_file()
            
            msg = self.format(record)
            self.current_file.write(msg + '\n')
            self.current_file.flush()
        except Exception:
            self.handleError(record)
    
    def close(self):
        """ハンドラーを閉じる"""
        if self.current_file:
            self.current_file.close()
        super().close()


# ログ設定
log_handler = RotatingLogHandler(LOG_FILE, max_bytes=100*1024*1024, backup_count=4)
log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler]
)


# ホスト名をOSから取得（MQTTトピック・zip名に使用）
def get_hostname():
    try:
        return socket.gethostname() or "unknown-host"
    except Exception:
        return "unknown-host"


# キャンペーンログファイルの有無で実行中かどうかを判断
def is_log_running(log_dir):
    return os.path.exists(os.path.join(log_dir, "campaign_*.json"))


# CSV形式のdetailをJSON配列に変換
def parse_csv_detail_to_json(csv_string):
    if not csv_string or not isinstance(csv_string, str):
        return None
    
    # 改行文字で分割（\n または \r\n の両方に対応）
    lines = csv_string.replace('\r\n', '\n').replace('\r', '\n').strip().split('\n')
    if len(lines) < 2:
        return None
    
    # ヘッダー行を取得
    headers = [h.strip() for h in lines[0].split(',')]
    
    # データ行を処理
    result = []
    for line in lines[1:]:
        if not line.strip():
            continue
        values = [v.strip() for v in line.split(',')]
        # ヘッダーと値を対応付けてオブジェクトを作成
        row_dict = {}
        for i, header in enumerate(headers):
            value = values[i] if i < len(values) else ""
            row_dict[header] = value
        result.append(row_dict)
    
    return result if result else None


# すべてのタイムスタンプサブフォルダを取得（実行時から10分前までのフォルダのみ）
def get_all_timestamp_dirs():
    if not os.path.exists(UNSENT_DIR):
        return []
    
    # 現在時刻と10分前の時刻を取得
    now = datetime.now()
    ten_minutes_ago = now - timedelta(minutes=10)
    
    # タイムスタンプ形式のサブディレクトリを検索（YYYYMMDDHHMMSS形式）
    subdirs = []
    for item in os.listdir(UNSENT_DIR):
        item_path = os.path.join(UNSENT_DIR, item)
        if os.path.isdir(item_path) and len(item) == 14 and item.isdigit():
            try:
                # タイムスタンプをdatetimeオブジェクトに変換
                folder_time = datetime.strptime(item, "%Y%m%d%H%M%S")
                # 10分前から現在時刻までの範囲内かチェック
                if ten_minutes_ago <= folder_time <= now:
                    subdirs.append(os.path.join(UNSENT_DIR, item))
            except ValueError:
                # タイムスタンプのパースに失敗した場合はスキップ
                continue
    
    # 名前でソート（古い順）
    subdirs.sort()
    return subdirs


# 指定されたディレクトリのcampaign JSONファイルからUUIDを取得
def get_campaign_uuid_from_dir(target_dir):
    pattern = os.path.join(target_dir, "campaign_*.json")
    campaign_files = glob.glob(pattern)
    if not campaign_files:
        raise FileNotFoundError(f"campaign JSONファイルが見つかりません: {target_dir}")

    with open(campaign_files[0], 'r', encoding='utf-8') as f:
        campaign_data = json.load(f)
        campaign_uuid = campaign_data.get("log_campaign_uuid")
        return campaign_uuid


# campaign_*.json が無い場合に、sindan_*.json から campaign_uuid を1つ取得（同一ディレクトリ内では1つとみなす）
def get_campaign_uuid_from_sindan(target_dir):
    """ディレクトリ内の sindan_*.json を走査し、campaign_uuid を1つ返す。見つからなければ None。"""
    for layer in PHASE_LAYERS.values():
        pattern = os.path.join(target_dir, f"sindan_{layer}_*.json")
        for json_path in glob.glob(pattern):
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if "wlan_environment" in json_path:
                        pattern_re = r'"detail"\s*:\s*"'
                        match = re.search(pattern_re, content)
                        if match:
                            start_pos = match.end()
                            end_pattern = r'",\s*"occurred_at"'
                            end_match = re.search(end_pattern, content[start_pos:])
                            if end_match:
                                end_pos = start_pos + end_match.start()
                                detail_value = content[start_pos:end_pos]
                                detail_value = detail_value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                                content = content[:start_pos] + detail_value + content[start_pos + len(detail_value):end_pos] + content[end_pos:]
                    data = json.loads(content)
                    uid = data.get("log_campaign_uuid")
                    if uid:
                        return uid
            except (json.JSONDecodeError, IOError):
                continue
    return None


# 指定されたPhaseのJSONファイルを読み込んで1つのJSONオブジェクトにまとめる
def load_phase_json(phase, campaign_uuid, target_dir):
    layer = PHASE_LAYERS.get(phase)
    if not layer:
        return None
    
    # 指定されたディレクトリからJSONファイルを検索
    pattern = os.path.join(target_dir, f"sindan_{layer}_*.json")
    json_files = glob.glob(pattern)
    
    if not json_files:
        return None
    
    # campaign_uuidでフィルタリング
    phase_data = []
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                content = f.read()
                # wlan_environmentファイルの場合、改行をエスケープしてからJSONパース
                if "wlan_environment" in json_file:
                    # detailフィールドの改行を\nに置き換える
                    # detailフィールドの値を探す（"detail" : "から始まり、", "occurred_at"の前で終わる）
                    pattern = r'"detail"\s*:\s*"'
                    match = re.search(pattern, content)
                    if match:
                        start_pos = match.end()
                        # ", "occurred_at"を探す
                        end_pattern = r'",\s*"occurred_at"'
                        end_match = re.search(end_pattern, content[start_pos:])
                        if end_match:
                            end_pos = start_pos + end_match.start()
                            # detailフィールドの値を取得
                            detail_value = content[start_pos:end_pos]
                            # 改行を\nに置き換え、ダブルクォートをエスケープ
                            detail_value = detail_value.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                            # 元のcontentを置き換え
                            content = content[:start_pos] + detail_value + content[start_pos + len(detail_value):end_pos] + content[end_pos:]
                
                data = json.loads(content)
                if data.get("log_campaign_uuid") == campaign_uuid:
                    phase_data.append(data)
        except json.JSONDecodeError:
            continue
        except IOError:
            continue
    
    if not phase_data:
        return None
    
    # タイムスタンプを最新のものに設定
    latest_timestamp = None
    for data in phase_data:
        occurred_at = data.get("occurred_at")
        if occurred_at:
            if latest_timestamp is None or occurred_at > latest_timestamp:
                latest_timestamp = occurred_at
    
    # log_typeとdetailごとに1つのオブジェクトにまとめる
    # Phase 4 (globalnet)、Phase 5 (dns)、Phase 6 (app)の場合はtargetごとに階層化
    # それ以外の場合は、同じlog_typeが複数ある場合は最新のoccurred_atを持つものを優先
    data_dict = {}
    is_target_based = (layer == "globalnet" or layer == "dns" or layer == "app")
    
    for item in phase_data:
        log_type = item.get("log_type")
        detail = item.get("detail")
        occurred_at = item.get("occurred_at", "")
        target = item.get("target", "")
        
        if log_type:
            # wlan_environmentの場合はCSV形式をJSON配列に変換
            if log_type == "wlan_environment" and detail:
                parsed_detail = parse_csv_detail_to_json(detail)
                if parsed_detail is not None:
                    detail = parsed_detail
            
            if is_target_based and target:
                # Phase 4 (globalnet)、Phase 5 (dns)、Phase 6 (app)の場合: targetを最初の階層にし、その中にlog_typeごとの結果を並べる
                if target not in data_dict:
                    data_dict[target] = {}
                
                # detailの値を直接保存（数値の場合は数値に変換）
                if detail:
                    # 数値に変換できる場合は数値として保存
                    try:
                        # 小数点を含む場合はfloat、そうでなければint
                        if '.' in str(detail):
                            value = float(detail)
                        else:
                            value = int(detail)
                    except (ValueError, TypeError):
                        value = detail
                else:
                    value = detail
                
                # 同じtargetで同じlog_typeが複数ある場合は、最新のoccurred_atを持つものを優先
                # occurred_atを追跡するために、一時的にタプルで保存
                if log_type not in data_dict[target]:
                    data_dict[target][log_type] = (value, occurred_at)
                else:
                    existing_occurred_at = data_dict[target][log_type][1] if isinstance(data_dict[target][log_type], tuple) else ""
                    if not existing_occurred_at or occurred_at > existing_occurred_at:
                        data_dict[target][log_type] = (value, occurred_at)
            else:
                # Phase 4以外の場合: 従来通り、最新のoccurred_atを持つものを優先
                if log_type not in data_dict:
                    # 初めて見つけたlog_typeはそのまま保存（値は[detail, occurred_at]のタプル）
                    data_dict[log_type] = (detail, occurred_at)
                else:
                    # 既存のものと比較して、より新しいoccurred_atを持つものを使用
                    existing_time = data_dict[log_type][1]
                    if occurred_at > existing_time:
                        data_dict[log_type] = (detail, occurred_at)
    
    # Phase 4、Phase 5、Phase 6の場合: タプルから値のみを抽出
    if is_target_based:
        for target in data_dict:
            for log_type in data_dict[target]:
                if isinstance(data_dict[target][log_type], tuple):
                    data_dict[target][log_type] = data_dict[target][log_type][0]
    # Phase 4、Phase 5、Phase 6以外の場合: タプルからdetailのみを抽出
    else:
        data_dict = {k: v[0] for k, v in data_dict.items()}
    
    # Phase情報を含むJSONオブジェクトにまとめる
    phase_json = {
        "phase": phase,
        "layer": layer,
        "campaign_uuid": campaign_uuid,
        "timestamp": latest_timestamp or datetime.now().isoformat(),
        "data": data_dict
    }
    
    return phase_json


# 各PhaseのデータをCSVファイルに保存（指定されたディレクトリ内に保存）
def save_csv(phases_data, target_dir):
    # 指定されたディレクトリが存在しない場合は作成
    os.makedirs(target_dir, exist_ok=True)
    fname = "allphase.csv"
    csv_path = os.path.join(target_dir, fname)
    
    with open(csv_path, 'w', encoding='utf-8') as f:
        # ヘッダー行
        f.write("timestamp,phase,layer,campaign_uuid,data_count,data_json\n")
        
        for phase in sorted(phases_data.keys()):
            phase_json = phases_data[phase]
            if phase_json is None:
                continue
            
            timestamp = phase_json.get("timestamp", "")
            phase_num = phase_json.get("phase", "")
            layer = phase_json.get("layer", "")
            campaign_uuid = phase_json.get("campaign_uuid", "")
            data = phase_json.get("data", {})
            data_count = len(data) if isinstance(data, dict) else len(data)
            
            # JSONデータを文字列に変換（CSV内のカンマや改行をエスケープ）
            data_json_str = json.dumps(data, ensure_ascii=False)
            # CSV内の改行やカンマを含む可能性があるため、ダブルクォートで囲む
            data_json_str = data_json_str.replace('"', '""')  # エスケープ
            
            f.write(f'"{timestamp}",{phase_num},"{layer}","{campaign_uuid}",{data_count},"{data_json_str}"\n')
    
    return csv_path

# tmpディレクトリをそのまま~/log/sentに移動してzip化
def move_tmp_to_sent(tmp_dir):
    try:
        # SENT_DIRが存在しない場合は作成を試みる
        if not os.path.exists(SENT_DIR):
            try:
                os.makedirs(SENT_DIR, exist_ok=True)
            except OSError as e:
                logging.warning(f"{SENT_DIR} ディレクトリが作成できませんでした。ログの移動をスキップします。")
                logging.warning(f"  エラー: {e}")
                return None
        
        # tmpディレクトリが存在するか確認
        if not os.path.exists(tmp_dir):
            logging.warning(f"tmpディレクトリが見つかりません: {tmp_dir}")
            return None
        
        # tmpディレクトリをsentディレクトリに移動
        dir_name = os.path.basename(tmp_dir)
        dest_dir = os.path.join(SENT_DIR, dir_name)
        if os.path.exists(dest_dir):
            # 既に存在する場合は、タイムスタンプを追加
            dest_dir = f"{dest_dir}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        shutil.move(tmp_dir, dest_dir)
        
        # zipファイル名を決定
        zip_path = f"{dest_dir}.zip"
        if os.path.exists(zip_path):
            # 既にzipファイルが存在する場合は、タイムスタンプを追加
            zip_path = f"{dest_dir}_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
        
        # ディレクトリをzip化
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(dest_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # zip内のパスは、dest_dirからの相対パスにする（ディレクトリ名を含む）
                    arcname = os.path.relpath(file_path, os.path.dirname(dest_dir))
                    zipf.write(file_path, arcname)
        
        # zip化が成功したら元のディレクトリを削除
        shutil.rmtree(dest_dir)
        logging.info(f"tmpディレクトリをzip化しました: {tmp_dir} -> {zip_path}")
        return zip_path
    except PermissionError as e:
        logging.error(f"ログの移動に失敗しました（権限不足）: {e}")
    except Exception as e:
        logging.error(f"ログの移動に失敗しました: {e}")
    return None


# 各PhaseのデータをMQTTブローカにpublish（hostname をトピックに含める）
def publish_to_mqtt(phases_data, hostname):
    success_count = 0
    fail_count = 0
    for phase in sorted(phases_data.keys()):
        phase_json = phases_data[phase]
        if phase_json is None:
            continue

        topic = f"{MQTT_TOPIC_BASE}/{hostname}/phase{phase}"
        msg = json.dumps(phase_json, ensure_ascii=False)

        try:
            with MessageWriter(SERVICE, topic=topic, qos=MQTT_QOS, retain=MQTT_RETAIN) as w:
                w.publish(msg)
            success_count += 1
        except Exception as e:
            logging.error(f"  ✗ 失敗: {topic} - {e}")
            fail_count += 1
    
    return success_count, fail_count

# sentフォルダ内の30日より前のzipファイルを削除
def cleanup_old_zips():
    """sentフォルダ内の30日より前のzipファイルを削除する"""
    if not os.path.exists(SENT_DIR):
        return
    
    # 現在時刻と30日前の時刻を取得
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)
    
    # zipファイルを検索
    zip_pattern = os.path.join(SENT_DIR, "*.zip")
    zip_files = glob.glob(zip_pattern)
    
    deleted_count = 0
    for zip_file in zip_files:
        try:
            # ファイルの最終更新時刻を取得
            file_mtime = os.path.getmtime(zip_file)
            file_time = datetime.fromtimestamp(file_mtime)
            
            # 30日より前のファイルを削除
            if file_time < thirty_days_ago:
                os.remove(zip_file)
                deleted_count += 1
                logging.info(f"古いzipファイルを削除しました: {zip_file} (最終更新: {file_time.strftime('%Y-%m-%d %H:%M:%S')})")
        except OSError as e:
            logging.warning(f"zipファイルの削除に失敗しました: {zip_file} - {e}")
        except Exception as e:
            logging.warning(f"zipファイルの処理中にエラーが発生しました: {zip_file} - {e}")
    
    if deleted_count > 0:
        logging.info(f"古いzipファイルの削除完了: {deleted_count}個のファイルを削除しました")


# 指定されたタイムスタンプフォルダを処理（CSV保存、MQTT送信、移動）
def process_timestamp_dir(timestamp_dir):
    logging.info(f"処理中: {timestamp_dir}")
    
    # tmpに残っているログが実行中かどうかを確認
    if is_log_running(timestamp_dir):
        logging.warning(f"ログディレクトリ {timestamp_dir} は実行中の可能性があります。スキップします。")
        return False

    hostname = get_hostname()

    # campaign UUIDを取得（無い場合は sindan_*.json から1つ取得。同一ディレクトリ内では1つとみなす）
    try:
        campaign_uuid = get_campaign_uuid_from_dir(timestamp_dir)
    except FileNotFoundError:
        campaign_uuid = get_campaign_uuid_from_sindan(timestamp_dir)
        if campaign_uuid is None:
            logging.error(f"campaign JSONが無く、sindan_*.json からも campaign_uuid を取得できませんでした: {timestamp_dir} - スキップします")
            return False
        logging.info(f"campaign JSONが無いため、sindan_*.json から取得した campaign_uuid で送信・zip化します")

    # 各PhaseのJSONデータを読み込む
    phases_data = {}
    for phase in range(7):  # Phase 0-6
        phase_json = load_phase_json(phase, campaign_uuid, timestamp_dir)
        if phase_json:
            phases_data[phase] = phase_json
        else:
            phases_data[phase] = None

    if not any(phases_data.get(p) for p in range(7)):
        logging.warning(f"有効なPhaseデータが無いためスキップします: {timestamp_dir}")
        return False

    # CSVファイルに保存
    save_csv(phases_data, timestamp_dir)
    # MQTTブローカにpublish
    success_count, fail_count = publish_to_mqtt(phases_data, hostname)
    # 送信が成功した場合のみtmpディレクトリをsentに移動してzip化
    if success_count > 0:
        move_tmp_to_sent(timestamp_dir)
        return True
    else:
        logging.warning(f"送信に失敗したため、ディレクトリは移動しませんでした")
        return False


def main():
    logging.info("=== SINDAN JSONファイル収集・パブリッシュ処理 ===")
    
    # すべてのタイムスタンプフォルダを取得
    try:
        timestamp_dirs = get_all_timestamp_dirs()
        if not timestamp_dirs:
            logging.info("処理対象のタイムスタンプフォルダが見つかりませんでした")
            return
        
        logging.info(f"処理対象フォルダ数: {len(timestamp_dirs)}")
    except Exception as e:
        logging.error(f"エラー: {e}")
        return
    
    # 各タイムスタンプフォルダを処理
    total_success = 0
    total_fail = 0
    processed_count = 0
    
    for timestamp_dir in timestamp_dirs:
        try:
            if process_timestamp_dir(timestamp_dir):
                processed_count += 1
        except Exception as e:
            logging.error(f"{timestamp_dir} の処理中にエラーが発生しました: {e}")
            total_fail += 1
    
    # 30日より前のzipファイルを削除
    cleanup_old_zips()
    
    logging.info("=== 処理完了 ===")


if __name__ == "__main__":
    main()

