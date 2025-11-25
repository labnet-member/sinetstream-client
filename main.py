#!/usr/bin/env python3
"""
SINDANクライアントのJSONログファイルを読み込み、
各Phaseの出力を1つにまとめたCSVファイルを保存し、
MQTTブローカにJSON形式でpublishするスクリプト
"""
import json
import os
import glob
import shutil
import psutil
from datetime import datetime
from sinetstream import MessageWriter

# ====== 設定 ======
SINDAN_LOG_DIR = os.path.expanduser("~/log/tmp")
OUTPUT_LOG_DIR = os.path.expanduser("./log")
SENT_LOG_DIR = os.path.expanduser("~/log/sent")
TMP_LOG_DIR = os.path.expanduser("~/log/tmp")

# ディレクトリの作成（エラーハンドリング付き）
def ensure_dir(dir_path):
    """ディレクトリが存在しない場合は作成（エラーハンドリング付き）"""
    try:
        os.makedirs(dir_path, exist_ok=True)
    except PermissionError as e:
        print(f"警告: ディレクトリ作成に失敗しました（権限不足）: {dir_path}")
        print(f"  エラー: {e}")
        return False
    except OSError as e:
        print(f"警告: ディレクトリ作成に失敗しました: {dir_path}")
        print(f"  エラー: {e}")
        return False
    return True

ensure_dir(OUTPUT_LOG_DIR)
ensure_dir(SENT_LOG_DIR)

SERVICE = "broker1"        # SINETStream のサービス名
QOS = 2                    # QoS2 固定
RETAIN = False

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


def is_log_running(log_dir):
    """キャンペーンログファイルの有無で実行中かどうかを判断"""
    if not os.path.exists(log_dir):
        return False
    
    # キャンペーンログファイルが存在するか確認
    pattern = os.path.join(log_dir, "campaign_*.json")
    campaign_files = glob.glob(pattern)
    
    # キャンペーンログファイルが存在しない場合は実行中と判断（スキップ）
    # キャンペーンログファイルが存在する場合は処理可能（実行中ではない）
    return len(campaign_files) == 0


def get_latest_timestamp_dir():
    """最新のタイムスタンプサブフォルダを取得"""
    if not os.path.exists(SINDAN_LOG_DIR):
        raise FileNotFoundError(f"ログディレクトリが見つかりません: {SINDAN_LOG_DIR}")
    
    # タイムスタンプ形式のサブディレクトリを検索（YYYYMMDDHHMMSS形式）
    subdirs = []
    for item in os.listdir(SINDAN_LOG_DIR):
        item_path = os.path.join(SINDAN_LOG_DIR, item)
        if os.path.isdir(item_path) and len(item) == 14 and item.isdigit():
            subdirs.append(item)
    
    if not subdirs:
        # タイムスタンプサブフォルダがない場合は、従来の方法でルートディレクトリを返す
        return SINDAN_LOG_DIR
    
    # 最新のタイムスタンプフォルダを取得（名前でソート）
    latest_dir = max(subdirs)
    return os.path.join(SINDAN_LOG_DIR, latest_dir)


def get_all_timestamp_dirs():
    """すべてのタイムスタンプサブフォルダを取得（ソート済み）"""
    if not os.path.exists(SINDAN_LOG_DIR):
        return []
    
    # タイムスタンプ形式のサブディレクトリを検索（YYYYMMDDHHMMSS形式）
    subdirs = []
    for item in os.listdir(SINDAN_LOG_DIR):
        item_path = os.path.join(SINDAN_LOG_DIR, item)
        if os.path.isdir(item_path) and len(item) == 14 and item.isdigit():
            subdirs.append(os.path.join(SINDAN_LOG_DIR, item))
    
    # 名前でソート（古い順）
    subdirs.sort()
    return subdirs


def get_latest_campaign_uuid():
    """最新のcampaign JSONファイルからUUIDを取得"""
    latest_dir = get_latest_timestamp_dir()
    pattern = os.path.join(latest_dir, "campaign_*.json")
    campaign_files = glob.glob(pattern)
    if not campaign_files:
        raise FileNotFoundError(f"campaign JSONファイルが見つかりません: {latest_dir}")
    
    # 最新のファイルを取得（タイムスタンプでソート）
    latest_file = max(campaign_files, key=os.path.getmtime)
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        campaign_data = json.load(f)
        # hostname を取得できるように変更。存在しない場合はフォールバック値を使用。
        campaign_uuid = campaign_data.get("log_campaign_uuid")
        hostname = campaign_data.get("hostname") or "unknown-host"
        return campaign_uuid, hostname


def get_campaign_uuid_from_dir(target_dir):
    """指定されたディレクトリのcampaign JSONファイルからUUIDを取得"""
    pattern = os.path.join(target_dir, "campaign_*.json")
    campaign_files = glob.glob(pattern)
    if not campaign_files:
        raise FileNotFoundError(f"campaign JSONファイルが見つかりません: {target_dir}")
    
    # 最新のファイルを取得（タイムスタンプでソート）
    latest_file = max(campaign_files, key=os.path.getmtime)
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        campaign_data = json.load(f)
        # hostname を取得できるように変更。存在しない場合はフォールバック値を使用。
        campaign_uuid = campaign_data.get("log_campaign_uuid")
        hostname = campaign_data.get("hostname") or "unknown-host"
        return campaign_uuid, hostname


def load_phase_json(phase, campaign_uuid, target_dir=None):
    """指定されたPhaseのJSONファイルを読み込んで1つのJSONオブジェクトにまとめる"""
    layer = PHASE_LAYERS.get(phase)
    if not layer:
        return None
    
    # 指定されたディレクトリまたは最新のタイムスタンプフォルダからJSONファイルを検索
    if target_dir is None:
        target_dir = get_latest_timestamp_dir()
    pattern = os.path.join(target_dir, f"sindan_{layer}_*.json")
    json_files = glob.glob(pattern)
    
    if not json_files:
        return None
    
    # campaign_uuidでフィルタリング
    phase_data = []
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if data.get("log_campaign_uuid") == campaign_uuid:
                    phase_data.append(data)
        except (json.JSONDecodeError, IOError) as e:
            # 警告は出力しない（大量のファイルがある場合にノイズになる）
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
    # log_typeをキーとして、detailを値とするオブジェクトを作成
    # 同じlog_typeが複数ある場合は、最新のoccurred_atを持つものを優先
    data_dict = {}
    for item in phase_data:
        log_type = item.get("log_type")
        detail = item.get("detail")
        occurred_at = item.get("occurred_at", "")
        if log_type:
            if log_type not in data_dict:
                # 初めて見つけたlog_typeはそのまま保存（値は[detail, occurred_at]のタプル）
                data_dict[log_type] = (detail, occurred_at)
            else:
                # 既存のものと比較して、より新しいoccurred_atを持つものを使用
                existing_time = data_dict[log_type][1]
                if occurred_at > existing_time:
                    data_dict[log_type] = (detail, occurred_at)
    
    # タプルからdetailのみを抽出
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


def save_csv(phases_data, target_dir):
    """各PhaseのデータをCSVファイルに保存（指定されたディレクトリ内に保存）"""
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


def move_tmp_to_sent(tmp_dir):
    """tmpディレクトリをそのまま~/log/sentに移動"""
    try:
        # SENT_LOG_DIRが存在しない場合は作成を試みる
        if not os.path.exists(SENT_LOG_DIR):
            if not ensure_dir(SENT_LOG_DIR):
                print(f"警告: {SENT_LOG_DIR} ディレクトリが作成できませんでした。ログの移動をスキップします。")
                return None
        
        # tmpディレクトリが存在するか確認
        if not os.path.exists(tmp_dir):
            print(f"警告: tmpディレクトリが見つかりません: {tmp_dir}")
            return None
        
        # tmpディレクトリをsentディレクトリに移動
        dir_name = os.path.basename(tmp_dir)
        dest_dir = os.path.join(SENT_LOG_DIR, dir_name)
        if os.path.exists(dest_dir):
            # 既に存在する場合は、タイムスタンプを追加
            dest_dir = f"{dest_dir}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        shutil.move(tmp_dir, dest_dir)
        print(f"tmpディレクトリを移動しました: {tmp_dir} -> {dest_dir}")
        return dest_dir
    except PermissionError as e:
        print(f"エラー: ログの移動に失敗しました（権限不足）: {e}")
    except Exception as e:
        print(f"エラー: ログの移動に失敗しました: {e}")
    return None


def publish_to_mqtt(phases_data, hostname):
    """各PhaseのデータをMQTTブローカにpublish（hostname をトピックに含める）"""
    success_count = 0
    fail_count = 0
    for phase in sorted(phases_data.keys()):
        phase_json = phases_data[phase]
        if phase_json is None:
            continue

        topic = f"sindan/{hostname}/phase{phase}"
        msg = json.dumps(phase_json, ensure_ascii=False)

        print(f"[MQTT] publish → {topic} (QoS={QOS})")
        try:
            with MessageWriter(SERVICE, topic=topic, qos=QOS, retain=RETAIN) as w:
                w.publish(msg)
            print(f"  ✓ 成功: {topic}")
            success_count += 1
        except Exception as e:
            print(f"  ✗ 失敗: {topic} - {e}")
            fail_count += 1
    
    return success_count, fail_count


def process_timestamp_dir(timestamp_dir):
    """指定されたタイムスタンプフォルダを処理（CSV保存、MQTT送信、移動）"""
    print(f"\n{'='*60}")
    print(f"処理中: {timestamp_dir}")
    
    # tmpに残っているログが実行中かどうかを確認
    if is_log_running(timestamp_dir):
        print(f"警告: ログディレクトリ {timestamp_dir} は実行中の可能性があります。スキップします。")
        return False
    
    # campaign UUIDを取得
    try:
        campaign_uuid, hostname = get_campaign_uuid_from_dir(timestamp_dir)
        print(f"Campaign UUID: {campaign_uuid}")
        print(f"Campaign Hostname: {hostname}")
    except FileNotFoundError as e:
        print(f"エラー: {e} - スキップします")
        return False
    
    # 各PhaseのJSONデータを読み込む
    print("\n=== Phaseデータの読み込み ===")
    phases_data = {}
    for phase in range(7):  # Phase 0-6
        phase_json = load_phase_json(phase, campaign_uuid, timestamp_dir)
        if phase_json:
            phases_data[phase] = phase_json
            data = phase_json.get("data", {})
            data_count = len(data) if isinstance(data, dict) else len(data)
            print(f"Phase {phase} ({PHASE_LAYERS[phase]}): {data_count}件のデータを読み込み")
        else:
            print(f"Phase {phase} ({PHASE_LAYERS[phase]}): データが見つかりませんでした")
            phases_data[phase] = None
    
    # CSVファイルに保存
    print("\n=== CSVファイルの保存 ===")
    csv_path = save_csv(phases_data, timestamp_dir)
    print(f"保存先: {csv_path}")
    
    # MQTTブローカにpublish
    print("\n=== MQTTブローカへの公開 ===")
    success_count, fail_count = publish_to_mqtt(phases_data, hostname)
    
    # 送信が成功した場合のみtmpディレクトリをsentに移動
    if success_count > 0:
        print("\n=== tmpディレクトリの移動 ===")
        move_tmp_to_sent(timestamp_dir)
        return True
    else:
        print(f"警告: 送信に失敗したため、ディレクトリは移動しませんでした")
        return False


def main():
    print("=== SINDAN JSONファイル収集・公開処理（一括処理） ===")
    
    # すべてのタイムスタンプフォルダを取得
    try:
        timestamp_dirs = get_all_timestamp_dirs()
        if not timestamp_dirs:
            print("処理対象のタイムスタンプフォルダが見つかりませんでした")
            return
        
        print(f"処理対象フォルダ数: {len(timestamp_dirs)}")
        for i, dir_path in enumerate(timestamp_dirs, 1):
            print(f"  {i}. {dir_path}")
    except Exception as e:
        print(f"エラー: {e}")
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
            print(f"エラー: {timestamp_dir} の処理中にエラーが発生しました: {e}")
            total_fail += 1
    
    print(f"\n{'='*60}")
    print("=== 一括処理完了 ===")


if __name__ == "__main__":
    main()

