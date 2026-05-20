from tools.logger.custom_logger import CustomLogger

logger = CustomLogger.get_logger('test')

logger.debug("debug message")
logger.info("info message")
logger.warning("warning message")
logger.error("error message")
logger.critical("critical message")