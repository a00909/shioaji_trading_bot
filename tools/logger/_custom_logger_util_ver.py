import logging
import threading
from tools.logger._custom_logging_formatter import CustomFormatter

_lock = threading.Lock()
_custom_handler = None


def get_custom_logger(name=None):
    """
    獲取並確保初始化 custom logger。
    安全支援多線程，並利用內建機制防止重複附加 Handler。
    """
    global _custom_handler

    # 1. 取得內建的 logger 實例（內建本身就是個池，名字相同必為同一個）
    logger = logging.getLogger(name)

    # 雙重檢查鎖（Double-checked locking），確保多線程安全
    if not _custom_handler or not logger.handlers:
        with _lock:
            # 初始化全域全域唯一的 Handler
            if not _custom_handler:
                _custom_handler = logging.StreamHandler()
                _custom_handler.setLevel(logging.INFO)
                _custom_handler.setFormatter(CustomFormatter())

            # 如果這個 logger 還沒有任何 handler，才加進去
            if not logger.handlers:
                logger.addHandler(_custom_handler)
                logger.setLevel(logging.INFO)

                # 如果是子 logger，通常建議關閉冒泡，避免 root logger 重複列印
                if name is not None:
                    logger.propagate = False

    return logger