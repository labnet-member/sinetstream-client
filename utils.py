"""
ユーティリティ関数
"""

import glob
import os
import socket


def get_hostname():
    """ホスト名を OS から取得（MQTT トピック・ZIP ファイル名に使用）"""
    return socket.gethostname() or "unknown-host"
