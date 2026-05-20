import logging
import threading

from tools.logger._custom_logging_formatter import CustomFormatter


class CustomLogger:
    _instance_lock = threading.Lock()
    _handler = None

    @classmethod
    def get_logger(cls, name=None):
        logger = logging.getLogger(name)

        # 只有在 logger 沒有 handler 時才進鎖初始化
        if not logger.handlers:
            with cls._instance_lock:
                # 再次檢查，確保多線程安全
                if not logger.handlers:
                    if cls._handler is None:
                        cls._handler = logging.StreamHandler()
                        cls._handler.setLevel(logging.INFO)
                        cls._handler.setFormatter(CustomFormatter())

                    logger.addHandler(cls._handler)
                    logger.setLevel(logging.INFO)
                    if name is not None:
                        logger.propagate = False
        return logger
