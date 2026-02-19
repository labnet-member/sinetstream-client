"""
カスタムログハンドラー（10MB でローテーション・.1 ~ .4 の4ファイルを使用）
"""

import logging
import os

class RotatingLogHandler(logging.Handler):
    def __init__(self, base_filename, max_bytes=10 * 1024 * 1024, backup_count=4):
        super().__init__()
        self.base_filename = base_filename
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.current_file = None
        self.current_file_num = None
        self._open_current_file()

    def _get_log_filename(self, num):
        """ログファイル名を取得（1 ~ 4の番号）"""
        return f"{self.base_filename}.{num}"

    def _open_current_file(self):
        """現在のログファイルを開く（必要に応じてローテーション）"""
        self.current_file_num = self._find_next_available_file()
        log_filename = self._get_log_filename(self.current_file_num)
        self.current_file = open(log_filename, "a", encoding="utf-8")

    def _find_next_available_file(self):
        """次に使用可能なログファイル番号を探す"""
        for num in range(1, self.backup_count + 1):
            filename = self._get_log_filename(num)
            if not os.path.exists(filename):
                return num
            if os.path.getsize(filename) < self.max_bytes:
                return num
        return 1

    def _rotate(self):
        """ログファイルをローテーション（4 → 3 → 2 → 1 の順にシフト，1 は削除）"""
        for num in range(self.backup_count, 1, -1):
            old_file = self._get_log_filename(num)
            new_file = self._get_log_filename(num - 1)
            if os.path.exists(old_file):
                if os.path.exists(new_file):
                    os.remove(new_file)
                os.rename(old_file, new_file)
        file_1 = self._get_log_filename(1)
        if os.path.exists(file_1):
            os.remove(file_1)

    def emit(self, record):
        """ログレコードを出力"""
        try:
            if self.current_file:
                current_size = os.path.getsize(self.current_file.name)
                if current_size >= self.max_bytes:
                    self.current_file.close()
                    self.current_file = None
                    if self.current_file_num == self.backup_count:
                        self._rotate()
                    self._open_current_file()

            if not self.current_file:
                self._open_current_file()

            if self.current_file:
                current_size = os.path.getsize(self.current_file.name)
                if current_size >= self.max_bytes:
                    self.current_file.close()
                    self.current_file = None
                    if self.current_file_num == self.backup_count:
                        self._rotate()
                    self._open_current_file()

            if not self.current_file:
                return
            msg = self.format(record)
            self.current_file.write(msg + "\n")
            self.current_file.flush()
        except Exception:
            self.handleError(record)

    def close(self):
        """ハンドラーを閉じる"""
        if self.current_file:
            self.current_file.close()
        super().close()


def setup_logging(
    log_file,
    max_bytes=10 * 1024 * 1024,
    backup_count=4,
    level=logging.INFO,
):
    """ログファイルのディレクトリが無ければ作成・RotatingLogHandler を登録"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handler = RotatingLogHandler(log_file, max_bytes=max_bytes, backup_count=backup_count)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.basicConfig(level=level, handlers=[handler])
