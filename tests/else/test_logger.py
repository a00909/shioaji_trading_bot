import logging

from tools.utils import init_custom_logger

# app = App()
init_custom_logger()

logger = logging.getLogger('test')

logger.debug("debug message")
logger.info("info message")
logger.warning("warning message")
logger.error("error message")
logger.critical("critical message")