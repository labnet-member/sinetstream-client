# ====================================================
# SINET Streamライブラリを用いたSINDAN結果送信プログラム
# ====================================================

# ライブラリのインポート
import json
import logging
import os
import glob
import re
import shutil
import zipfile
from datetime import datetime, timedelta

from sinetstream import MessageWriter

from log_handler import setup_logging
from utils import get_hostname

# ==== 設定 =====
# ディレクトリ設定
UNSENT_DIR = os.path.expanduser("~/log/tmp")
SENT_DIR = os.path.expanduser("~/log/sent")
LOG_FILE = os.path.expanduser("~/log/python.log")

# SINETStream Writer 設定
SERVICE = "broker1"        # SINETStream のサービス名
MQTT_TOPIC_BASE = "sindan"  # MQTTトピックのベースパス
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

setup_logging(LOG_FILE, max_bytes=10 * 1024 * 1024, backup_count=4)


# すべてのタイムスタンプサブフォルダを取得（実行時から10分前までのフォルダのみ）
def get_all_timestamp_dirs():
    if not os.path.exists(UNSENT_DIR):
        return []
    
    now = datetime.now()
    ten_minutes_ago = now - timedelta(minutes=10)
    subdirs = []
    
    for item in os.listdir(UNSENT_DIR):
        item_path = os.path.join(UNSENT_DIR, item)
        if os.path.isdir(item_path) and len(item) == 14 and item.isdigit():
            folder_time = datetime.strptime(item, "%Y%m%d%H%M%S")
            if ten_minutes_ago <= folder_time <= now:
                subdirs.append(item_path)
    
    return sorted(subdirs)


# campaign_*.json または sindan_*.json から campaign_uuid を取得
def get_campaign_uuid(target_dir):
    """campaign UUID を返す．見つからなければ None．"""
    # campaign_*.json から取得
    campaign_files = glob.glob(os.path.join(target_dir, "campaign_*.json"))
    if campaign_files:
        with open(campaign_files[0], "r", encoding="utf-8") as f:
            uid = json.load(f).get("log_campaign_uuid")
            if uid:
                return uid

    # sindan_*.json から取得
    for layer in PHASE_LAYERS.values():
        for json_path in glob.glob(os.path.join(target_dir, f"sindan_{layer}_*.json")):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    if "wlan_environment" in json_path:
                        m = re.search(r'"detail"\s*:\s*"', content)
                        if m:
                            start = m.end()
                            em = re.search(r'",\s*"occurred_at"', content[start:])
                            if em:
                                end = start + em.start()
                                dv = content[start:end].replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
                                content = content[:start] + dv + content[end:]
                    uid = json.loads(content).get("log_campaign_uuid")
                    if uid:
                        return uid
            except (json.JSONDecodeError, IOError):
                continue
    return None


# 指定された Phase の JSON ファイルを読み込んで 1 つの JSON オブジェクトにまとめる
def load_phase_json(phase, campaign_uuid, target_dir):
    layer = PHASE_LAYERS.get(phase)
    if not layer:
        return None
    
    # 指定されたディレクトリから JSON ファイルを検索
    pattern = os.path.join(target_dir, f"sindan_{layer}_*.json")
    json_files = glob.glob(pattern)
    
    if not json_files:
        return None
    
    # campaign_uuid でフィルタリング
    phase_data = []
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                content = f.read()
                # wlan_environment ファイルの場合，改行をエスケープしてから JSON パース
                if "wlan_environment" in json_file:
                    # detail フィールドの改行を \n に置き換える
                    # detail フィールドの値を探す（"detail" : " から始まり，"occurred_at" の前で終わる）
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
        except (json.JSONDecodeError, IOError):
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
    
    # log_type と detail ごとに1つのオブジェクトにまとめる
    # Phase 4 (globalnet)・Phase 5 (dns)・Phase 6 (app)の場合は target ごとに階層化
    # それ以外の場合は，同じ log_type が複数ある場合は最新の occurred_at を持つものを優先
    data_dict = {}
    is_target_based = (layer == "globalnet" or layer == "dns" or layer == "app")
    
    for item in phase_data:
        log_type = item.get("log_type")
        detail = item.get("detail")
        occurred_at = item.get("occurred_at", "")
        target = item.get("target", "")
        
        if log_type:
            # wlan_environment の場合は CSV 形式を JSON 配列に変換
            if log_type == "wlan_environment" and detail:
                if detail and isinstance(detail, str):
                    # 改行文字で分割（\n または \r\n の両方に対応）
                    lines = detail.replace('\r\n', '\n').replace('\r', '\n').strip().split('\n')
                    if len(lines) >= 2:
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
                        if result:
                            detail = result
            
            if is_target_based and target:
                # Phase 4 (globalnet)・Phase 5 (dns)・Phase 6 (app)の場合: target を最初の階層にし，その中に log_type ごとの結果を並べる
                if target not in data_dict:
                    data_dict[target] = {}
                
                # detail の値を直接保存（数値の場合は数値に変換）
                if detail:
                    # 数値に変換できる場合は数値として保存
                    try:
                        # 小数点を含む場合は float，そうでなければ int
                        if '.' in str(detail):
                            value = float(detail)
                        else:
                            value = int(detail)
                    except (ValueError, TypeError):
                        value = detail
                else:
                    value = detail
                
                # 同じ target で同じ log_type が複数ある場合は，最新の occurred_at を持つものを優先
                # occurred_at を追跡するために，一時的にタプルで保存
                if log_type not in data_dict[target]:
                    data_dict[target][log_type] = (value, occurred_at)
                else:
                    existing_occurred_at = data_dict[target][log_type][1] if isinstance(data_dict[target][log_type], tuple) else ""
                    if not existing_occurred_at or occurred_at > existing_occurred_at:
                        data_dict[target][log_type] = (value, occurred_at)
            else:
                # Phase 4 以外の場合: 従来通り，最新の occurred_at を持つものを優先
                if log_type not in data_dict:
                    # 初めて見つけた log_type はそのまま保存（値は [detail, occurred_at] のタプル）
                    data_dict[log_type] = (detail, occurred_at)
                else:
                    # 既存のものと比較して，より新しい occurred_at を持つものを使用
                    existing_time = data_dict[log_type][1]
                    if occurred_at > existing_time:
                        data_dict[log_type] = (detail, occurred_at)
    
    # Phase 4・Phase 5・Phase 6の場合: タプルから値のみを抽出
    if is_target_based:
        for target in data_dict:
            for log_type in data_dict[target]:
                if isinstance(data_dict[target][log_type], tuple):
                    data_dict[target][log_type] = data_dict[target][log_type][0]
    # Phase 4・Phase 5・Phase 6以外の場合: タプルから detail のみを抽出
    else:
        data_dict = {k: v[0] for k, v in data_dict.items()}
    
    # Phase 情報を含むJSONオブジェクトにまとめる
    phase_json = {
        "phase": phase,
        "layer": layer,
        "campaign_uuid": campaign_uuid,
        "timestamp": latest_timestamp or datetime.now().isoformat(),
        "data": data_dict
    }
    
    return phase_json


# 各 Phase のデータを CSV ファイルに保存（指定されたディレクトリ内に保存）
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
            
            # JSONデータを文字列に変換（CSV 内のカンマや改行をエスケープ）
            data_json_str = json.dumps(data, ensure_ascii=False)
            # CSV 内の改行やカンマを含む可能性があるため，ダブルクォートで囲む
            data_json_str = data_json_str.replace('"', '""')  # エスケープ
            
            f.write(f'"{timestamp}",{phase_num},"{layer}","{campaign_uuid}",{data_count},"{data_json_str}"\n')
    
    return csv_path

# TMP_DIR をそのまま SENT_DIR に移動して ZIP 化
def move_tmp_to_sent(tmp_dir):
    try:
        # SENT_DIR が存在しない場合は作成を試みる
        if not os.path.exists(SENT_DIR):
            os.makedirs(SENT_DIR, exist_ok=True)
        
        # TMP_DIR が存在するか確認
        if not os.path.exists(tmp_dir):
            logging.warning(f"tmpディレクトリが見つかりません: {tmp_dir}")
            return None
        
        # TMP_DIR を SENT_DIR に移動
        dir_name = os.path.basename(tmp_dir)
        dest_dir = os.path.join(SENT_DIR, dir_name)
        if os.path.exists(dest_dir):
            # 既に存在する場合は，タイムスタンプを追加
            dest_dir = f"{dest_dir}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        shutil.move(tmp_dir, dest_dir)
        
        # ZIP ファイル名を決定
        zip_path = f"{dest_dir}.zip"
        if os.path.exists(zip_path):
            # 既に ZIP ファイルが存在する場合は，タイムスタンプを追加
            zip_path = f"{dest_dir}_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
        
        # ディレクトリを ZIP 化
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(dest_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # ZIP 内のパスは，DEST_DIR からの相対パスにする（ディレクトリ名を含む）
                    arcname = os.path.relpath(file_path, os.path.dirname(dest_dir))
                    zipf.write(file_path, arcname)
        
        # ZIP 化が成功したら元のディレクトリを削除
        shutil.rmtree(dest_dir)
        logging.info(f"ログを移動して ZIP 化しました: {tmp_dir} -> {zip_path}")
        return zip_path
    except Exception as e:
        logging.error(f"ログの移動に失敗しました: {e} - スキップします")
    return None


# 各 Phase のデータを MQTT ブローカに Publish（ホスト名をトピックに含める）
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

# SENT_DIR 内の30日より前の ZIP ファイルを削除
def cleanup_old_zips():
    """SENT_DIR 内の30日より前の ZIP ファイルを削除する"""
    if not os.path.exists(SENT_DIR):
        return
    
    # 現在時刻と 30 日前の時刻を取得
    now = datetime.now()
    thirty_days_ago = now - timedelta(days=30)
    
    # ZIP ファイルを検索
    zip_pattern = os.path.join(SENT_DIR, "*.zip")
    zip_files = glob.glob(zip_pattern)
    
    for zip_file in zip_files:
        try:
            file_mtime = os.path.getmtime(zip_file)
            file_time = datetime.fromtimestamp(file_mtime)
            if file_time < thirty_days_ago:
                os.remove(zip_file)
        except Exception as e:
            logging.warning(f"ZIP ファイルの削除に失敗しました: {zip_file} - {e}")



# 指定されたタイムスタンプフォルダを処理（CSV保存、MQTT送信、移動）
def process_timestamp_dir(timestamp_dir):
    hostname = get_hostname()

    # campaign UUIDを取得（campaign_*.json 優先，無ければ sindan_*.json から）
    campaign_uuid= get_campaign_uuid(timestamp_dir)

    # 各PhaseのJSONデータを読み込む
    phases_data = {}
    for phase in range(7):  # Phase 0-6
        phase_json = load_phase_json(phase, campaign_uuid, timestamp_dir)
        if phase_json:
            phases_data[phase] = phase_json
        else:
            phases_data[phase] = None

    if not any(phases_data.get(p) for p in range(7)):
        logging.warning(f"有効な Phase データが無いため，{timestamp_dir} をスキップします")
        return False

    # CSV ファイルに保存
    save_csv(phases_data, timestamp_dir)
    # MQTT ブローカに Publish
    success_count, fail_count = publish_to_mqtt(phases_data, hostname)
    # 送信が成功した場合のみ TMP_DIR を SENT_DIR に移動して ZIP 化
    if success_count > 0:
        move_tmp_to_sent(timestamp_dir)
        return True
    else:
        logging.warning(f"送信に失敗したため，{timestamp_dir} ディレクトリは移動されません")
        return False


def main():
    logging.info("=== SINDAN JSON ファイル収集・Publish 処理開始 ===")
    
    # すべてのタイムスタンプフォルダを取得
    timestamp_dirs = get_all_timestamp_dirs()
    # 各タイムスタンプフォルダを処理
    for timestamp_dir in timestamp_dirs:
        try:
            process_timestamp_dir(timestamp_dir)
        except Exception as e:
            logging.error(f"{timestamp_dir} の処理中にエラーが発生しました: {e}")
    
    # 30 日前の ZIP ファイルを削除
    cleanup_old_zips()
    
    logging.info("=== SINDAN JSON ファイル収集・Publish 処理完了 ===")


if __name__ == "__main__":
    main()

