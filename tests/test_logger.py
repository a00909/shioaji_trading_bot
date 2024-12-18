import logging

from tools.app import App
from tools.custom_logging_formatter import CustomFormatter
from tools.utils import init_custom_logger

# app = App(True)
init_custom_logger()

logger = logging.getLogger('test')

logger.debug("debug message")
logger.info("info message")
logger.warning("warning message")
logger.error("error message")
logger.critical("critical message")